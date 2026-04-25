"""Tests for CompositeRef, CompositeStepHandle, and ExpandedCompositeResult."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from artisan.execution.models.artifact_source import ArtifactSource
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.composites.composite_ref import (
    CompositeRef,
    CompositeStepHandle,
    ExpandedCompositeResult,
)
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.specs.output_spec import OutputSpec


def _make_artifact(aid: str = "a" * 32) -> MetricArtifact:
    return MetricArtifact(
        artifact_id=aid,
        artifact_type="metric",
        content=json.dumps({"value": 1}).encode(),
        original_name="test",
        extension=".json",
        origin_step_number=0,
    )


# ---------------------------------------------------------------------------
# CompositeRef
# ---------------------------------------------------------------------------


class TestCompositeRef:
    def test_collapsed_ref(self):
        source = ArtifactSource.from_artifacts([_make_artifact()])
        ref = CompositeRef(source=source, output_reference=None, role="data")
        assert ref.source is source
        assert ref.output_reference is None
        assert ref.role == "data"

    def test_expanded_ref(self):
        out_ref = OutputReference(source_step=0, role="data")
        ref = CompositeRef(source=None, output_reference=out_ref, role="data")
        assert ref.source is None
        assert ref.output_reference is out_ref

    def test_frozen(self):
        ref = CompositeRef(source=None, output_reference=None, role="x")
        with pytest.raises(AttributeError):
            ref.role = "y"


# ---------------------------------------------------------------------------
# CompositeStepHandle — collapsed mode
# ---------------------------------------------------------------------------


class TestCompositeStepHandleCollapsed:
    def test_output_returns_ref_with_source(self):
        art = _make_artifact()
        handle = CompositeStepHandle(
            artifacts={"data": [art]},
            operation_outputs={"data": OutputSpec(artifact_type="data")},
        )
        ref = handle.output("data")
        assert ref.source is not None
        assert ref.output_reference is None
        assert ref.role == "data"

    def test_output_unknown_role_raises(self):
        handle = CompositeStepHandle(
            artifacts={"data": [_make_artifact()]},
            operation_outputs={"data": OutputSpec(artifact_type="data")},
        )
        with pytest.raises(ValueError, match="Unknown output role"):
            handle.output("nonexistent")

    def test_output_no_validation_when_no_specs(self):
        """When operation_outputs is None, skip role validation."""
        art = _make_artifact()
        handle = CompositeStepHandle(artifacts={"any_role": [art]})
        ref = handle.output("any_role")
        assert ref.source is not None


# ---------------------------------------------------------------------------
# CompositeStepHandle — expanded mode
# ---------------------------------------------------------------------------


class TestCompositeStepHandleExpanded:
    def test_output_returns_ref_with_output_reference(self):
        mock_future = MagicMock()
        out_ref = OutputReference(source_step=1, role="result")
        mock_future.output.return_value = out_ref

        handle = CompositeStepHandle(
            step_future=mock_future,
            operation_outputs={"result": OutputSpec(artifact_type="data")},
        )
        ref = handle.output("result")
        assert ref.source is None
        assert ref.output_reference is out_ref
        mock_future.output.assert_called_once_with("result")


# ---------------------------------------------------------------------------
# ExpandedCompositeResult
# ---------------------------------------------------------------------------


class TestExpandedCompositeResult:
    def test_output_returns_reference(self):
        out_ref = OutputReference(source_step=2, role="metrics")
        result = ExpandedCompositeResult(
            output_map={"metrics": out_ref},
            output_types={"metrics": "metric"},
        )
        assert result.output("metrics") is out_ref

    def test_output_unknown_role_raises(self):
        result = ExpandedCompositeResult(
            output_map={"metrics": OutputReference(source_step=0, role="metrics")},
            output_types={"metrics": "metric"},
        )
        with pytest.raises(ValueError, match="Unknown output role"):
            result.output("nonexistent")

    def test_output_roles(self):
        """Returns frozenset of declared output role names."""
        ref_data = OutputReference(source_step=0, role="data")
        ref_metrics = OutputReference(source_step=1, role="metrics")
        result = ExpandedCompositeResult(
            output_map={"data": ref_data, "metrics": ref_metrics},
            output_types={"data": "data", "metrics": "metric"},
        )
        assert result.output_roles == frozenset({"data", "metrics"})

    def test_duck_types_with_step_future(self):
        """ExpandedCompositeResult.output() has same signature as StepFuture.output()."""
        out_ref = OutputReference(source_step=0, role="data")
        result = ExpandedCompositeResult(
            output_map={"data": out_ref},
            output_types={"data": "data"},
        )
        # Same interface: .output(role) -> OutputReference
        ref = result.output("data")
        assert isinstance(ref, OutputReference)


class TestExpandedCompositeResultWait:
    """Tests for the .wait() method added in PR 2."""

    def _make_done_future(self):
        from concurrent.futures import Future

        from artisan.orchestration.step_future import StepFuture
        from artisan.schemas.orchestration.step_result import StepResult

        future: Future = Future()
        future.set_result(
            StepResult(
                step_name="x",
                step_number=0,
                success=True,
                total_count=0,
                succeeded_count=0,
                failed_count=0,
            )
        )
        return StepFuture(
            step_number=0,
            step_name="x",
            output_roles=frozenset({"out"}),
            output_types={"out": None},
            future=future,
        )

    def _make_pending_future(self):
        from concurrent.futures import Future

        from artisan.orchestration.step_future import StepFuture

        future: Future = Future()  # never resolved
        return StepFuture(
            step_number=1,
            step_name="y",
            output_roles=frozenset({"out"}),
            output_types={"out": None},
            future=future,
        )

    def test_wait_drains_done_futures(self):
        """wait() returns immediately when all child futures are done."""
        f1 = self._make_done_future()
        f2 = self._make_done_future()
        result = ExpandedCompositeResult(
            output_map={},
            output_types={},
            child_futures=[f1, f2],
        )
        returned = result.wait()
        assert returned is result
        assert f1.done is True
        assert f2.done is True

    def test_wait_returns_self_for_chaining(self):
        """wait() returns self so callers can chain ``.output(role)``."""
        result = ExpandedCompositeResult(
            output_map={},
            output_types={},
            child_futures=[],
        )
        assert result.wait() is result

    def test_wait_with_no_children_is_noop(self):
        """Empty child_futures (e.g. nested composite case) returns immediately."""
        result = ExpandedCompositeResult(
            output_map={},
            output_types={},
            child_futures=None,
        )
        result.wait()
        assert result._child_futures == []

    def test_wait_timeout_raises(self):
        """Timeout deadline raises TimeoutError."""
        pending = self._make_pending_future()
        result = ExpandedCompositeResult(
            output_map={},
            output_types={},
            child_futures=[pending],
        )
        with pytest.raises(TimeoutError):
            result.wait(timeout=0.05)

    def test_child_futures_default_empty(self):
        """Omitting child_futures keeps backwards-compat default."""
        result = ExpandedCompositeResult(output_map={}, output_types={})
        assert result._child_futures == []
