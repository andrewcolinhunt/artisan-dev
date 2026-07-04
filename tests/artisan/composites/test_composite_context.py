"""Tests for the merged CompositeContext.

Covers input/run/output wiring, nested composites, and step_defaults
forwarding: each override knob defaults from the composite level, an explicit
per-op value wins, params is never forwarded, and skip_cache / compact honor
their None-sentinel semantics.
"""

from __future__ import annotations

from enum import StrEnum
from typing import ClassVar
from unittest.mock import MagicMock

import pytest

from artisan.composites.base.composite_context import CompositeContext, _NestedHandle
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.composites.base.results import CompositeResult
from artisan.operations.examples.data_generator import DataGenerator
from artisan.schemas.composites.composite_ref import CompositeRef
from artisan.schemas.enums import FailurePolicy
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

# ---------------------------------------------------------------------------
# Test composite
# ---------------------------------------------------------------------------


class InnerComposite(CompositeDefinition):
    """Simple composite with one input and one output."""

    name = "test_ctx_inner"
    description = "Inner composite"

    class InputRole(StrEnum):
        DATA = "data"

    class OutputRole(StrEnum):
        RESULT = "result"

    inputs: ClassVar[dict[str, InputSpec]] = {
        "data": InputSpec(artifact_type="data", required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "result": OutputSpec(artifact_type="data"),
    }

    def compose(self, ctx) -> None:
        ref = ctx.input("data")
        ctx.output("result", ref)


def _make_ctx(
    pipeline: MagicMock,
    *,
    step_defaults: dict | None = None,
    input_refs: dict | None = None,
) -> CompositeContext:
    return CompositeContext(
        pipeline=pipeline,
        input_refs=input_refs or {"data": OutputReference(source_step=0, role="data")},
        composite=InnerComposite(),
        step_name_prefix="my_composite",
        step_defaults=step_defaults or {},
    )


# ---------------------------------------------------------------------------
# input / run / output wiring
# ---------------------------------------------------------------------------


class TestInput:
    def test_input_returns_ref_with_output_reference(self):
        out_ref = OutputReference(source_step=0, role="data")
        ctx = _make_ctx(MagicMock(), input_refs={"data": out_ref})
        ref = ctx.input("data")
        assert ref.source is None
        assert ref.output_reference is out_ref
        assert ref.role == "data"

    def test_input_unknown_role_raises(self):
        ctx = _make_ctx(MagicMock())
        with pytest.raises(ValueError, match="Unknown input role"):
            ctx.input("nonexistent")


class TestRun:
    def test_run_delegates_to_pipeline_submit(self):
        pipeline = MagicMock()
        ctx = _make_ctx(pipeline)
        ctx.run(DataGenerator, inputs={"data": ctx.input("data")})
        pipeline.submit.assert_called_once()
        assert pipeline.submit.call_args.kwargs["name"] == "my_composite.data_generator"

    def test_run_returns_handle_with_step_future(self):
        pipeline = MagicMock()
        future = MagicMock()
        pipeline.submit.return_value = future
        ctx = _make_ctx(pipeline)
        handle = ctx.run(DataGenerator, inputs={"data": ctx.input("data")})
        assert handle._step_future is future
        assert future in ctx.get_child_futures()


class TestOutput:
    def test_output_records_output_reference(self):
        out_ref = OutputReference(source_step=1, role="result")
        ctx = _make_ctx(MagicMock())
        ref = CompositeRef(source=None, output_reference=out_ref, role="result")
        ctx.output("result", ref)
        assert ctx.get_output_map()["result"] is out_ref

    def test_output_missing_output_reference_raises(self):
        ctx = _make_ctx(MagicMock())
        ref = CompositeRef(source=None, output_reference=None, role="result")
        with pytest.raises(ValueError, match="has no OutputReference"):
            ctx.output("result", ref)


# ---------------------------------------------------------------------------
# step_defaults forwarding
# ---------------------------------------------------------------------------


class TestForwarding:
    def test_each_knob_defaults_from_composite_level(self):
        step_defaults = {
            "step_runner": "local",
            "runner_resources": {"cpus": 7},
            "batch_strategy": {"artifacts_per_unit": 9},
            "environment": "docker",
            "tool": {"executable": "samtools"},
            "compute_provider": "modal",
            "compute_resources": {"memory_gb": 3},
            "failure_policy": FailurePolicy.CONTINUE,
        }
        pipeline = MagicMock()
        ctx = _make_ctx(pipeline, step_defaults=step_defaults)
        ctx.run(DataGenerator)
        kwargs = pipeline.submit.call_args.kwargs
        assert kwargs["step_runner"] == "local"
        assert kwargs["runner_resources"] == {"cpus": 7}
        assert kwargs["batch_strategy"] == {"artifacts_per_unit": 9}
        assert kwargs["environment"] == "docker"
        assert kwargs["tool"] == {"executable": "samtools"}
        assert kwargs["compute_provider"] == "modal"
        assert kwargs["compute_resources"] == {"memory_gb": 3}
        assert kwargs["failure_policy"] == FailurePolicy.CONTINUE

    def test_explicit_per_op_value_wins(self):
        pipeline = MagicMock()
        ctx = _make_ctx(pipeline, step_defaults={"environment": "docker"})
        ctx.run(DataGenerator, environment="pixi")
        assert pipeline.submit.call_args.kwargs["environment"] == "pixi"

    def test_unset_knob_with_no_default_is_none(self):
        pipeline = MagicMock()
        ctx = _make_ctx(pipeline, step_defaults={})
        ctx.run(DataGenerator)
        kwargs = pipeline.submit.call_args.kwargs
        assert kwargs["environment"] is None
        assert kwargs["runner_resources"] is None
        assert kwargs["compute_provider"] is None

    def test_params_never_forwarded(self):
        pipeline = MagicMock()
        # A stray params key in step_defaults must never reach the child.
        ctx = _make_ctx(pipeline, step_defaults={"params": {"count": 99}})
        ctx.run(DataGenerator)
        assert pipeline.submit.call_args.kwargs["params"] is None

    def test_explicit_params_passthrough(self):
        pipeline = MagicMock()
        ctx = _make_ctx(pipeline, step_defaults={"params": {"count": 99}})
        ctx.run(DataGenerator, params={"count": 2})
        assert pipeline.submit.call_args.kwargs["params"] == {"count": 2}


class TestSkipCacheSentinel:
    def test_default_false_when_unset(self):
        pipeline = MagicMock()
        ctx = _make_ctx(pipeline, step_defaults={})
        ctx.run(DataGenerator)
        assert pipeline.submit.call_args.kwargs["skip_cache"] is False

    def test_inherits_composite_level(self):
        pipeline = MagicMock()
        ctx = _make_ctx(pipeline, step_defaults={"skip_cache": True})
        ctx.run(DataGenerator)
        assert pipeline.submit.call_args.kwargs["skip_cache"] is True

    def test_explicit_false_overrides_composite_true(self):
        pipeline = MagicMock()
        ctx = _make_ctx(pipeline, step_defaults={"skip_cache": True})
        ctx.run(DataGenerator, skip_cache=False)
        assert pipeline.submit.call_args.kwargs["skip_cache"] is False


class TestCompactSentinel:
    def test_default_true_when_unset(self):
        pipeline = MagicMock()
        ctx = _make_ctx(pipeline, step_defaults={})
        ctx.run(DataGenerator)
        assert pipeline.submit.call_args.kwargs["compact"] is True

    def test_inherits_composite_level(self):
        pipeline = MagicMock()
        ctx = _make_ctx(pipeline, step_defaults={"compact": False})
        ctx.run(DataGenerator)
        assert pipeline.submit.call_args.kwargs["compact"] is False

    def test_explicit_true_overrides_composite_false(self):
        pipeline = MagicMock()
        ctx = _make_ctx(pipeline, step_defaults={"compact": False})
        ctx.run(DataGenerator, compact=True)
        assert pipeline.submit.call_args.kwargs["compact"] is True


# ---------------------------------------------------------------------------
# Nested composites
# ---------------------------------------------------------------------------


class TestNestedComposite:
    def test_run_nested_delegates_to_submit_composite(self):
        pipeline = MagicMock()
        pipeline.submit_composite.return_value = CompositeResult(
            output_map={"result": OutputReference(source_step=5, role="result")},
            output_types={"result": "data"},
        )
        ctx = _make_ctx(pipeline)
        handle = ctx.run(InnerComposite, inputs={"data": ctx.input("data")})
        assert isinstance(handle, _NestedHandle)
        pipeline.submit_composite.assert_called_once()
        # submit(), not submit_composite(), must NOT be used for the nested op.
        pipeline.submit.assert_not_called()

    def test_nested_handle_output_returns_ref(self):
        pipeline = MagicMock()
        pipeline.submit_composite.return_value = CompositeResult(
            output_map={"result": OutputReference(source_step=5, role="result")},
            output_types={"result": "data"},
        )
        ctx = _make_ctx(pipeline)
        handle = ctx.run(InnerComposite, inputs={"data": ctx.input("data")})
        ref = handle.output("result")
        assert ref.output_reference.source_step == 5
        assert ref.role == "result"

    def test_nested_composite_inherits_defaults(self):
        pipeline = MagicMock()
        pipeline.submit_composite.return_value = CompositeResult(
            output_map={}, output_types={}
        )
        ctx = _make_ctx(pipeline, step_defaults={"environment": "docker"})
        ctx.run(InnerComposite, inputs={"data": ctx.input("data")})
        assert pipeline.submit_composite.call_args.kwargs["environment"] == "docker"
