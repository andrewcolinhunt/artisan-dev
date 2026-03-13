"""Tests for pipeline_manager.py failure handling and cancellation.

Tests that PipelineManager never raises from step execution,
that finalize() always returns a summary, and that cancel()
correctly stops the pipeline.
"""

from __future__ import annotations

from concurrent.futures import Future
from enum import StrEnum, auto
from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.pipeline_manager import (
    PipelineManager,
    _extract_source_steps,
)
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


# Minimal mock operation for testing
class _MockOp(OperationDefinition):
    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_op"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.DATA, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": ["data"]},
        ),
    }

    def preprocess(self, inputs: Any) -> dict:
        return {}

    def execute(self, inputs: Any, output_dir: Any) -> Any:
        return None


def _make_pipeline(tmp_path) -> PipelineManager:
    """Create a minimal PipelineManager without Prefect."""
    config = PipelineConfig(
        name="test",
        delta_root=tmp_path / "delta",
        staging_root=tmp_path / "staging",
        working_root=tmp_path / "working",
    )
    return PipelineManager(config)


class TestRunReturnsFailedStepResult:
    """Tests for F22: _run() returns StepResult instead of raising."""

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_failed_step_appears_in_step_results(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """A step that raises should produce StepResult(success=False)."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.side_effect = RuntimeError("something broke")

        pipeline = _make_pipeline(tmp_path)
        result = pipeline.run(_MockOp, inputs={"data": ["a" * 32]})

        assert result.success is False
        assert "error" in result.metadata
        assert "RuntimeError" in result.metadata["error"]
        assert result in pipeline._step_results

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_failed_step_records_failure_in_tracker(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """Failed step should call record_step_failed on the tracker."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.side_effect = ValueError("bad input")

        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": ["a" * 32]})

        mock_tracker.record_step_failed.assert_called_once()
        call_args = mock_tracker.record_step_failed.call_args
        assert "ValueError" in call_args[0][1]


class TestResilientFinalize:
    """Tests for F23: finalize() survives failed futures."""

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_finalize_survives_failed_future(self, mock_tracker_cls, tmp_path):
        """finalize() should return summary even if a future raised."""
        mock_tracker_cls.return_value = MagicMock()

        pipeline = _make_pipeline(tmp_path)

        # Simulate a failed future
        failed_future = Future()
        failed_future.set_exception(RuntimeError("step exploded"))
        mock_step_future = MagicMock()
        mock_step_future.result.side_effect = RuntimeError("step exploded")
        pipeline._active_futures[0] = mock_step_future

        # finalize should NOT raise
        summary = pipeline.finalize()

        assert "pipeline_name" in summary
        assert "total_steps" in summary
        assert "overall_success" in summary

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_finalize_includes_all_step_results(self, mock_tracker_cls, tmp_path):
        """finalize() summary includes results from all steps."""
        mock_tracker_cls.return_value = MagicMock()

        pipeline = _make_pipeline(tmp_path)
        pipeline._step_results = [
            StepResult(
                step_name="op1",
                step_number=0,
                success=True,
                succeeded_count=5,
                failed_count=0,
            ),
            StepResult(
                step_name="op2",
                step_number=1,
                success=False,
                succeeded_count=0,
                failed_count=3,
            ),
        ]

        summary = pipeline.finalize()

        assert summary["total_steps"] == 2
        assert summary["overall_success"] is False
        assert len(summary["steps"]) == 2


class TestResilientPredecessorWaiting:
    """Tests for F24: _wait_for_predecessors() survives failed predecessors."""

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_predecessor_failure_does_not_raise(self, mock_tracker_cls, tmp_path):
        """Failed predecessor should log warning, not raise."""
        mock_tracker_cls.return_value = MagicMock()

        pipeline = _make_pipeline(tmp_path)

        # Simulate a failed predecessor
        mock_future = MagicMock()
        mock_future.result.side_effect = RuntimeError("pred failed")
        pipeline._active_futures[0] = mock_future

        inputs = {"data": OutputReference(source_step=0, role="data")}

        # Should NOT raise
        pipeline._wait_for_predecessors(inputs)


class TestEmptyInputsHandling:
    """Tests for empty-inputs detection and pipeline stopping."""

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_pipeline_stops_after_empty_inputs(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """Step with skipped metadata triggers pipeline stop; next step skips."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        # Step 0: execute_step returns a skipped result
        skipped_result = StepResult(
            step_name="mock_op",
            step_number=0,
            success=True,
            total_count=0,
            succeeded_count=0,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
            metadata={"skipped": True, "skip_reason": "all_inputs_empty"},
        )
        mock_execute.return_value = skipped_result

        pipeline = _make_pipeline(tmp_path)

        # Run step 0 — should trigger _stopped
        result0 = pipeline.run(_MockOp, inputs={"data": ["a" * 32]})
        assert result0.metadata.get("skipped") is True
        mock_tracker.record_step_skipped.assert_called_once()
        mock_tracker.record_step_completed.assert_not_called()

        # Run step 1 — should be immediately skipped without calling execute_step
        mock_execute.reset_mock()
        result1 = pipeline.run(
            _MockOp,
            inputs={"data": result0.output("output")},
        )
        assert result1.metadata.get("skipped") is True
        assert result1.metadata.get("skip_reason") == "pipeline_stopped"
        mock_execute.assert_not_called()

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_completed_zero_outputs_still_cached(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """A step with zero outputs but no skipped metadata is still cached normally."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        # execute_step returns a result with 0 succeeded but NO skipped metadata
        zero_result = StepResult(
            step_name="mock_op",
            step_number=0,
            success=True,
            total_count=0,
            succeeded_count=0,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )
        mock_execute.return_value = zero_result

        pipeline = _make_pipeline(tmp_path)
        result = pipeline.run(_MockOp, inputs={"data": ["a" * 32]})

        assert result.succeeded_count == 0
        mock_tracker.record_step_completed.assert_called_once()
        mock_tracker.record_step_skipped.assert_not_called()
        # Pipeline should NOT be stopped
        assert pipeline._stopped is False


class TestExtractSourceSteps:
    """Tests for _extract_source_steps helper."""

    def test_dict_inputs(self):
        inputs = {
            "a": OutputReference(source_step=0, role="data"),
            "b": OutputReference(source_step=2, role="metric"),
        }
        assert _extract_source_steps(inputs) == {0, 2}

    def test_list_inputs(self):
        inputs = [
            OutputReference(source_step=1, role="data"),
            OutputReference(source_step=3, role="data"),
        ]
        assert _extract_source_steps(inputs) == {1, 3}

    def test_none_inputs(self):
        assert _extract_source_steps(None) == set()


class TestStepNameOverride:
    """Tests for the name parameter on run()/submit()."""

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_custom_name_propagates_to_result(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """run(name='custom') should set result.step_name to 'custom'."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            success=True,
            total_count=3,
            succeeded_count=3,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )

        pipeline = _make_pipeline(tmp_path)
        result = pipeline.run(_MockOp, inputs={"data": ["a" * 32]}, name="acyl_rmsd")

        assert result.step_name == "acyl_rmsd"

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_default_name_uses_operation_name(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """run() without name should use operation.name as step_name."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            success=True,
            total_count=3,
            succeeded_count=3,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )

        pipeline = _make_pipeline(tmp_path)
        result = pipeline.run(_MockOp, inputs={"data": ["a" * 32]})

        assert result.step_name == "mock_op"

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_custom_name_appears_in_start_record(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """StepStartRecord should contain the custom name, not operation.name."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            success=True,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )

        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": ["a" * 32]}, name="compute_metrics")

        mock_tracker.record_step_start.assert_called_once()
        start_record = mock_tracker.record_step_start.call_args[0][0]
        assert start_record.step_name == "compute_metrics"

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_custom_name_on_failed_step(self, mock_tracker_cls, mock_execute, tmp_path):
        """Failed step with custom name should still use that name."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.side_effect = RuntimeError("boom")

        pipeline = _make_pipeline(tmp_path)
        result = pipeline.run(_MockOp, inputs={"data": ["a" * 32]}, name="custom_fail")

        assert result.success is False
        assert result.step_name == "custom_fail"


class TestPipelineOutputByName:
    """Tests for pipeline.output(name, role) name-based step lookup."""

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_returns_correct_reference(self, mock_tracker_cls, mock_execute, tmp_path):
        """pipeline.output(name, role) returns OutputReference with correct fields."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            success=True,
            total_count=3,
            succeeded_count=3,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )

        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": ["a" * 32]}, name="foo")

        ref = pipeline.output("foo", "output")
        assert isinstance(ref, OutputReference)
        assert ref.source_step == 0
        assert ref.role == "output"

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_unknown_name_raises(self, mock_tracker_cls, tmp_path):
        """pipeline.output() with unknown name raises ValueError."""
        mock_tracker_cls.return_value = MagicMock()

        pipeline = _make_pipeline(tmp_path)

        import pytest

        with pytest.raises(ValueError, match="No step named 'nonexistent'"):
            pipeline.output("nonexistent", "role")

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_unknown_role_raises(self, mock_tracker_cls, mock_execute, tmp_path):
        """pipeline.output() with invalid role delegates to StepResult.output()."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            success=True,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )

        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": ["a" * 32]}, name="foo")

        import pytest

        with pytest.raises(ValueError, match="Output role 'bad_role' not available"):
            pipeline.output("foo", "bad_role")

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_last_wins_for_duplicate_names(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """When two steps share a name, output() returns the second step's reference."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        # Step 0
        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            success=True,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )
        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": ["a" * 32]}, name="dup")

        # Step 1
        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=1,
            success=True,
            total_count=2,
            succeeded_count=2,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )
        pipeline.run(_MockOp, inputs={"data": ["a" * 32]}, name="dup")

        ref = pipeline.output("dup", "output")
        assert ref.source_step == 1

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_works_with_default_name(self, mock_tracker_cls, mock_execute, tmp_path):
        """Step without explicit name= can be looked up by operation.name."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            success=True,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )

        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": ["a" * 32]})

        ref = pipeline.output("mock_op", "output")
        assert ref.source_step == 0
        assert ref.role == "output"

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_contains_still_works(self, mock_tracker_cls, mock_execute, tmp_path):
        """'name in pipeline' returns True after running a step with that name."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            success=True,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )

        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": ["a" * 32]}, name="foo")

        assert "foo" in pipeline
        assert "bar" not in pipeline

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_output_with_explicit_step_number(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """output(name, role, step_number=N) returns the step with that number."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        # Step 0
        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            success=True,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )
        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": ["a" * 32]}, name="dup")

        # Step 1
        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=1,
            success=True,
            total_count=2,
            succeeded_count=2,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )
        pipeline.run(_MockOp, inputs={"data": ["a" * 32]}, name="dup")

        ref = pipeline.output("dup", "output", step_number=0)
        assert ref.source_step == 0
        assert ref.role == "output"

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_output_step_number_not_found(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """step_number that doesn't exist for given name raises ValueError."""
        import pytest

        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            success=True,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )
        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": ["a" * 32]}, name="foo")

        with pytest.raises(ValueError, match="has no entry with step_number=99"):
            pipeline.output("foo", "output", step_number=99)

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_output_step_number_wrong_name(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """step_number exists under a different name raises ValueError."""
        import pytest

        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        # Step 0 named "alpha"
        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            success=True,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )
        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": ["a" * 32]}, name="alpha")

        # Step 1 named "beta"
        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=1,
            success=True,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )
        pipeline.run(_MockOp, inputs={"data": ["a" * 32]}, name="beta")

        # step_number=1 belongs to "beta", not "alpha"
        with pytest.raises(ValueError, match="has no entry with step_number=1"):
            pipeline.output("alpha", "output", step_number=1)

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_output_single_step_with_step_number(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """Explicit step_number works even for non-ambiguous single-step case."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            success=True,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )
        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": ["a" * 32]}, name="solo")

        ref = pipeline.output("solo", "output", step_number=0)
        assert ref.source_step == 0
        assert ref.role == "output"

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_named_steps_preserves_all_entries(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """After 3 steps with same name, all 3 are retrievable via step_number."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        pipeline = _make_pipeline(tmp_path)
        for i in range(3):
            mock_execute.return_value = StepResult(
                step_name="mock_op",
                step_number=i,
                success=True,
                total_count=1,
                succeeded_count=1,
                failed_count=0,
                output_roles=frozenset(["output"]),
                output_types={"output": "data"},
            )
            pipeline.run(_MockOp, inputs={"data": ["a" * 32]}, name="repeat")

        for i in range(3):
            ref = pipeline.output("repeat", "output", step_number=i)
            assert ref.source_step == i


class TestCancellation:
    """Tests for PipelineManager.cancel() and signal handling."""

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_cancel_sets_event_idempotent(self, mock_tracker_cls, tmp_path):
        """cancel() sets the internal event and is idempotent."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        assert not pipeline._cancel_event.is_set()
        pipeline.cancel()
        assert pipeline._cancel_event.is_set()
        # Second call is a no-op (no error)
        pipeline.cancel()
        assert pipeline._cancel_event.is_set()

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_submit_skips_steps_when_cancelled(self, mock_tracker_cls, tmp_path):
        """submit() should skip steps when cancel event is set."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        pipeline = _make_pipeline(tmp_path)
        pipeline.cancel()  # Cancel before any steps run

        result_future = pipeline.submit(_MockOp, inputs={"data": ["a" * 32]})
        result = result_future.result()

        assert result.metadata.get("skipped") is True
        assert result.metadata.get("skip_reason") == "cancelled"

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_finalize_returns_cleanly_after_cancellation(
        self, mock_tracker_cls, tmp_path
    ):
        """finalize() should return summary dict after cancel."""
        mock_tracker_cls.return_value = MagicMock()

        pipeline = _make_pipeline(tmp_path)
        pipeline.cancel()

        summary = pipeline.finalize()

        assert "pipeline_name" in summary
        assert "overall_success" in summary

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_cancel_during_predecessor_wait(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """Dependent step returns promptly when cancelled during predecessor wait."""
        import threading
        import time

        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        barrier = threading.Event()

        def slow_execute(**_kwargs):
            barrier.wait(timeout=10)
            return StepResult(
                step_name="mock_op",
                step_number=0,
                success=True,
                total_count=1,
                succeeded_count=1,
                failed_count=0,
                output_roles=frozenset(["output"]),
                output_types={"output": "data"},
            )

        mock_execute.side_effect = slow_execute

        pipeline = _make_pipeline(tmp_path)
        step0 = pipeline.submit(_MockOp, inputs={"data": ["a" * 32]}, name="slow")

        # Submit the dependent step from another thread (submit() blocks
        # in _wait_for_predecessors on the calling thread)
        dep_result_holder: list[StepResult] = []

        def submit_dependent():
            future = pipeline.submit(
                _MockOp,
                inputs={"data": OutputReference(source_step=0, role="output")},
                name="dependent",
            )
            dep_result_holder.append(future.result(timeout=5))

        dep_thread = threading.Thread(target=submit_dependent)
        dep_thread.start()

        # Give the dependent submit time to reach _wait_for_predecessors
        time.sleep(0.3)
        pipeline.cancel()

        # The dependent thread should finish within ~2s (not hang)
        dep_thread.join(timeout=3.0)
        assert not dep_thread.is_alive(), "Dependent step hung after cancel"
        assert len(dep_result_holder) == 1
        result = dep_result_holder[0]
        assert result.metadata.get("skipped") is True
        assert result.metadata.get("skip_reason") == "cancelled"

        # Release step0 so the executor can shut down
        barrier.set()
        step0.result(timeout=5)

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_queued_run_closure_exits_on_cancel(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """_run closure bails out immediately if cancelled while queued."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            success=True,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )

        pipeline = _make_pipeline(tmp_path)

        # Submit a step, then cancel before it can run.
        # Use a single-thread executor so the closure is queued.
        # We cancel after submit but the closure checks cancel at start.
        step0 = pipeline.submit(_MockOp, inputs={"data": ["a" * 32]}, name="step0")
        step0.result(timeout=5)  # let step0 finish

        # Now cancel and submit another step
        pipeline.cancel()
        step1 = pipeline.submit(
            _MockOp,
            inputs={"data": OutputReference(source_step=0, role="output")},
            name="step1",
        )
        result = step1.result(timeout=5)

        assert result.metadata.get("skipped") is True
        assert result.metadata.get("skip_reason") == "cancelled"

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_composite_skipped_on_cancel(self, mock_tracker_cls, tmp_path):
        """_submit_composite returns skipped result when cancelled."""
        mock_tracker_cls.return_value = MagicMock()

        pipeline = _make_pipeline(tmp_path)
        pipeline.cancel()

        # Call _submit_composite directly with a mock composite class

        mock_composite = MagicMock()
        mock_composite.name = "test_composite"
        mock_composite.outputs = {
            "output": MagicMock(artifact_type="data"),
        }
        mock_composite.inputs = {
            "data": MagicMock(artifact_type="data", required=True),
        }

        result_future = pipeline._submit_composite(
            composite_class=mock_composite,
            inputs={"data": ["a" * 32]},
            params=None,
            backend=None,
            resources=None,
            execution=None,
            intermediates="discard",
            failure_policy=None,
            compact=False,
            name="test_composite",
        )
        result = result_future.result()
        assert result.metadata.get("skipped") is True
        assert result.metadata.get("skip_reason") == "cancelled"

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_finalize_cancel_during_future_wait(self, mock_tracker_cls, tmp_path):
        """finalize() returns cleanly when cancel fires while waiting on futures."""
        import threading
        import time

        mock_tracker_cls.return_value = MagicMock()

        pipeline = _make_pipeline(tmp_path)

        # Create a blocking future and inject it into _active_futures
        blocker = threading.Event()
        future = Future()

        def _resolve_after_cancel():
            # Wait for cancel, then let the future sit until the 5s grace
            blocker.wait(timeout=10)
            future.set_result(None)

        resolver = threading.Thread(target=_resolve_after_cancel)
        resolver.start()

        pipeline._active_futures[0] = future

        # Fire cancel after a short delay so finalize's polling loop exits
        def _cancel_later():
            time.sleep(0.5)
            pipeline.cancel()
            blocker.set()

        cancel_thread = threading.Thread(target=_cancel_later)
        cancel_thread.start()

        start = time.time()
        summary = pipeline.finalize()
        elapsed = time.time() - start

        cancel_thread.join(timeout=2)
        resolver.join(timeout=2)

        assert "pipeline_name" in summary
        assert "overall_success" in summary
        # Should finish within ~7s (0.5s cancel delay + 5s grace + margin)
        assert elapsed < 8.0
        # Signal handlers should be restored
        assert pipeline._prev_sigint is None

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_finalize_cancel_already_set(self, mock_tracker_cls, tmp_path):
        """finalize() with pre-set cancel skips polling and uses short timeout."""
        import time

        mock_tracker_cls.return_value = MagicMock()

        pipeline = _make_pipeline(tmp_path)
        pipeline.cancel()

        # Inject a future that will time out
        future = Future()
        pipeline._active_futures[0] = future

        start = time.time()
        summary = pipeline.finalize()
        elapsed = time.time() - start

        assert "pipeline_name" in summary
        # Should finish in ~5s (the grace timeout), not hang
        assert elapsed < 7.0


class TestStepRegistry:
    """Tests for _step_registry: output() works before step completion."""

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_output_before_submit_completes(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """output() returns OutputReference for a submitted-but-not-completed step."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        # Make execute_step block until we release it
        import threading

        barrier = threading.Event()

        def slow_execute(**_kwargs):
            barrier.wait(timeout=5)
            return StepResult(
                step_name="mock_op",
                step_number=0,
                success=True,
                total_count=1,
                succeeded_count=1,
                failed_count=0,
                output_roles=frozenset(["output"]),
                output_types={"output": "data"},
            )

        mock_execute.side_effect = slow_execute

        pipeline = _make_pipeline(tmp_path)
        future = pipeline.submit(_MockOp, inputs={"data": ["a" * 32]}, name="gen")

        # Step is still running — output() should work via _step_registry
        ref = pipeline.output("gen", "output")
        assert isinstance(ref, OutputReference)
        assert ref.source_step == 0
        assert ref.role == "output"
        assert ref.artifact_type == "data"

        # Release the step and clean up
        barrier.set()
        future.result(timeout=5)

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_output_after_cancellation(self, mock_tracker_cls, tmp_path):
        """output() works for steps skipped due to cancellation."""
        mock_tracker_cls.return_value = MagicMock()

        pipeline = _make_pipeline(tmp_path)
        pipeline.cancel()

        pipeline.submit(_MockOp, inputs={"data": ["a" * 32]}, name="cancelled_step")

        ref = pipeline.output("cancelled_step", "output")
        assert isinstance(ref, OutputReference)
        assert ref.source_step == 0
        assert ref.role == "output"

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_output_invalid_name_raises(self, mock_tracker_cls, tmp_path):
        """output() with nonexistent step name raises ValueError."""
        import pytest

        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        with pytest.raises(ValueError, match="No step named 'ghost'"):
            pipeline.output("ghost", "output")

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_output_invalid_role_raises(self, mock_tracker_cls, mock_execute, tmp_path):
        """output() with wrong role raises ValueError."""
        import pytest

        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            success=True,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )

        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": ["a" * 32]}, name="foo")

        with pytest.raises(ValueError, match="Output role 'bad' not available"):
            pipeline.output("foo", "bad")
