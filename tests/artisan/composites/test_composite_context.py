"""Tests for CollapsedCompositeContext and ExpandedCompositeContext."""

from __future__ import annotations

import json
from enum import StrEnum
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest

from artisan.composites.base.composite_context import (
    CollapsedCompositeContext,
    ExpandedCompositeContext,
)
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.execution.models.artifact_source import ArtifactSource
from artisan.execution.models.execution_composite import CompositeIntermediates
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.composites.composite_ref import CompositeRef
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _art(aid: str = "a" * 32) -> MetricArtifact:
    return MetricArtifact(
        artifact_id=aid,
        artifact_type="metric",
        content=json.dumps({"value": 1}).encode(),
        original_name="test",
        extension=".json",
        origin_step_number=0,
    )


def _make_runtime_env() -> MagicMock:
    env = MagicMock()
    env.working_root_path = MagicMock()
    env.delta_root_path = MagicMock()
    env.staging_root_path = MagicMock()
    env.compute_backend_name = "local"
    env.shared_filesystem = True
    env.preserve_working = False
    return env


def _make_store() -> MagicMock:
    return MagicMock()


# ---------------------------------------------------------------------------
# Test composites
# ---------------------------------------------------------------------------


class InnerComposite(CompositeDefinition):
    """Simple composite for nested testing."""

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
        # In tests, this is called by the context — we'll mock the inner run
        ref = ctx.input("data")
        ctx.output("result", ref)


# ---------------------------------------------------------------------------
# Tests: CollapsedCompositeContext
# ---------------------------------------------------------------------------


class TestCollapsedInput:
    def test_input_returns_ref_with_source(self):
        source = ArtifactSource.from_artifacts([_art()])
        composite = InnerComposite()
        ctx = CollapsedCompositeContext(
            sources={"data": source},
            composite=composite,
            runtime_env=_make_runtime_env(),
            step_number=0,
            execution_run_id="r" * 32,
            intermediates=CompositeIntermediates.DISCARD,
            artifact_store=_make_store(),
        )
        ref = ctx.input("data")
        assert ref.source is source
        assert ref.output_reference is None
        assert ref.role == "data"

    def test_input_unknown_role_raises(self):
        ctx = CollapsedCompositeContext(
            sources={"data": ArtifactSource.from_artifacts([_art()])},
            composite=InnerComposite(),
            runtime_env=_make_runtime_env(),
            step_number=0,
            execution_run_id="r" * 32,
            intermediates=CompositeIntermediates.DISCARD,
            artifact_store=_make_store(),
        )
        with pytest.raises(ValueError, match="Unknown input role"):
            ctx.input("nonexistent")


class TestCollapsedOutput:
    def test_output_records_mapping(self):
        source = ArtifactSource.from_artifacts([_art()])
        composite = InnerComposite()
        ctx = CollapsedCompositeContext(
            sources={"data": source},
            composite=composite,
            runtime_env=_make_runtime_env(),
            step_number=0,
            execution_run_id="r" * 32,
            intermediates=CompositeIntermediates.DISCARD,
            artifact_store=_make_store(),
        )
        ref = CompositeRef(source=source, output_reference=None, role="data")
        ctx.output("result", ref)
        assert "result" in ctx.get_output_map()

    def test_output_unknown_role_raises(self):
        ctx = CollapsedCompositeContext(
            sources={"data": ArtifactSource.from_artifacts([_art()])},
            composite=InnerComposite(),
            runtime_env=_make_runtime_env(),
            step_number=0,
            execution_run_id="r" * 32,
            intermediates=CompositeIntermediates.DISCARD,
            artifact_store=_make_store(),
        )
        ref = CompositeRef(source=None, output_reference=None, role="x")
        with pytest.raises(ValueError, match="Unknown output role"):
            ctx.output("nonexistent", ref)


class TestCollapsedRunCreator:
    @patch("artisan.execution.executors.creator.run_creator_lifecycle")
    def test_run_creator_returns_handle(self, mock_lifecycle):
        """ctx.run() with a creator returns a handle with artifacts."""
        from artisan.execution.executors.creator import LifecycleResult
        from artisan.operations.examples.data_generator import DataGenerator

        art = _art("b" * 32)
        mock_lifecycle.return_value = LifecycleResult(
            input_artifacts={},
            artifacts={"datasets": [art]},
            edges=[],
            timings={},
        )

        source = ArtifactSource.from_artifacts([_art()])
        ctx = CollapsedCompositeContext(
            sources={"data": source},
            composite=InnerComposite(),
            runtime_env=_make_runtime_env(),
            step_number=0,
            execution_run_id="r" * 32,
            intermediates=CompositeIntermediates.DISCARD,
            artifact_store=_make_store(),
        )

        ref = ctx.input("data")
        handle = ctx.run(DataGenerator, inputs={"data": ref})
        out_ref = handle.output("datasets")
        assert out_ref.source is not None
        assert len(ctx.get_all_artifacts()) == 1


class TestCollapsedRunNested:
    def test_nested_composite_passthrough(self):
        """Nested composite that passes input through to output."""
        source = ArtifactSource.from_artifacts([_art()])
        outer_composite = InnerComposite()
        ctx = CollapsedCompositeContext(
            sources={"data": source},
            composite=outer_composite,
            runtime_env=_make_runtime_env(),
            step_number=0,
            execution_run_id="r" * 32,
            intermediates=CompositeIntermediates.DISCARD,
            artifact_store=_make_store(),
        )

        handle = ctx.run(InnerComposite, inputs={"data": ctx.input("data")})
        ref = handle.output("result")
        assert ref.source is not None


# ---------------------------------------------------------------------------
# Tests: ExpandedCompositeContext
# ---------------------------------------------------------------------------


class TestExpandedInput:
    def test_input_returns_ref_with_output_reference(self):
        out_ref = OutputReference(source_step=0, role="data")
        ctx = ExpandedCompositeContext(
            pipeline=MagicMock(),
            input_refs={"data": out_ref},
            composite=InnerComposite(),
            step_name_prefix="my_composite",
        )
        ref = ctx.input("data")
        assert ref.source is None
        assert ref.output_reference is out_ref

    def test_input_unknown_role_raises(self):
        ctx = ExpandedCompositeContext(
            pipeline=MagicMock(),
            input_refs={"data": OutputReference(source_step=0, role="data")},
            composite=InnerComposite(),
            step_name_prefix="my_composite",
        )
        with pytest.raises(ValueError, match="Unknown input role"):
            ctx.input("nonexistent")


class TestExpandedRun:
    def test_run_delegates_to_pipeline_submit(self):
        from artisan.operations.examples.data_generator import DataGenerator

        mock_pipeline = MagicMock()
        mock_future = MagicMock()
        mock_pipeline.submit.return_value = mock_future

        out_ref = OutputReference(source_step=0, role="data")
        ctx = ExpandedCompositeContext(
            pipeline=mock_pipeline,
            input_refs={"data": out_ref},
            composite=InnerComposite(),
            step_name_prefix="my_composite",
        )

        ref = ctx.input("data")
        handle = ctx.run(DataGenerator, inputs={"data": ref})

        mock_pipeline.submit.assert_called_once()
        call_kwargs = mock_pipeline.submit.call_args
        assert call_kwargs.kwargs["name"] == "my_composite.data_generator"


class TestExpandedOutput:
    def test_output_records_output_reference(self):
        out_ref = OutputReference(source_step=1, role="result")
        ctx = ExpandedCompositeContext(
            pipeline=MagicMock(),
            input_refs={"data": OutputReference(source_step=0, role="data")},
            composite=InnerComposite(),
            step_name_prefix="my_composite",
        )
        ref = CompositeRef(source=None, output_reference=out_ref, role="result")
        ctx.output("result", ref)
        assert ctx.get_output_map()["result"] is out_ref

    def test_output_collapsed_ref_raises(self):
        ctx = ExpandedCompositeContext(
            pipeline=MagicMock(),
            input_refs={"data": OutputReference(source_step=0, role="data")},
            composite=InnerComposite(),
            step_name_prefix="my_composite",
        )
        source = ArtifactSource.from_artifacts([_art()])
        ref = CompositeRef(source=source, output_reference=None, role="result")
        with pytest.raises(ValueError, match="has no OutputReference"):
            ctx.output("result", ref)
