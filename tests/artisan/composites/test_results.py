"""Tests for CompositeStepHandle and CompositeResult."""

from __future__ import annotations

from concurrent.futures import Future
from unittest.mock import MagicMock

import pytest

from artisan.composites.base.results import CompositeResult, CompositeStepHandle
from artisan.orchestration.step_future import StepFuture
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.specs.output_spec import OutputSpec

# ---------------------------------------------------------------------------
# CompositeStepHandle
# ---------------------------------------------------------------------------


class TestCompositeStepHandle:
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

    def test_output_unknown_role_raises(self):
        handle = CompositeStepHandle(
            step_future=MagicMock(),
            operation_outputs={"result": OutputSpec(artifact_type="data")},
        )
        with pytest.raises(ValueError, match="Unknown output role"):
            handle.output("nonexistent")

    def test_output_without_step_future_raises(self):
        handle = CompositeStepHandle()
        with pytest.raises(ValueError, match="no step_future"):
            handle.output("result")


# ---------------------------------------------------------------------------
# CompositeResult
# ---------------------------------------------------------------------------


class TestCompositeResult:
    def test_output_returns_reference(self):
        out_ref = OutputReference(source_step=2, role="metrics")
        result = CompositeResult(
            output_map={"metrics": out_ref},
            output_types={"metrics": "metric"},
        )
        assert result.output("metrics") is out_ref

    def test_output_unknown_role_raises(self):
        result = CompositeResult(
            output_map={"metrics": OutputReference(source_step=0, role="metrics")},
            output_types={"metrics": "metric"},
        )
        with pytest.raises(ValueError, match="Unknown output role"):
            result.output("nonexistent")

    def test_output_roles(self):
        result = CompositeResult(
            output_map={
                "data": OutputReference(source_step=0, role="data"),
                "metrics": OutputReference(source_step=1, role="metrics"),
            },
            output_types={"data": "data", "metrics": "metric"},
        )
        assert result.output_roles == frozenset({"data", "metrics"})

    def test_duck_types_with_step_future(self):
        """CompositeResult.output() returns an OutputReference like StepFuture."""
        result = CompositeResult(
            output_map={"data": OutputReference(source_step=0, role="data")},
            output_types={"data": "data"},
        )
        assert isinstance(result.output("data"), OutputReference)


class TestCompositeResultWait:
    def _make_done_future(self) -> StepFuture:
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

    def _make_pending_future(self) -> StepFuture:
        future: Future = Future()  # never resolved
        return StepFuture(
            step_number=1,
            step_name="y",
            output_roles=frozenset({"out"}),
            output_types={"out": None},
            future=future,
        )

    def test_wait_drains_done_futures(self):
        f1 = self._make_done_future()
        f2 = self._make_done_future()
        result = CompositeResult(output_map={}, output_types={}, child_futures=[f1, f2])
        returned = result.wait()
        assert returned is result
        assert f1.done is True
        assert f2.done is True

    def test_wait_returns_self_for_chaining(self):
        result = CompositeResult(output_map={}, output_types={}, child_futures=[])
        assert result.wait() is result

    def test_wait_with_no_children_is_noop(self):
        result = CompositeResult(output_map={}, output_types={}, child_futures=None)
        result.wait()
        assert result._child_futures == []

    def test_wait_timeout_raises(self):
        pending = self._make_pending_future()
        result = CompositeResult(
            output_map={}, output_types={}, child_futures=[pending]
        )
        with pytest.raises(TimeoutError):
            result.wait(timeout=0.05)

    def test_child_futures_default_empty(self):
        result = CompositeResult(output_map={}, output_types={})
        assert result._child_futures == []
