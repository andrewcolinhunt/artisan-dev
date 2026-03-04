"""Tests for pipeline_manager.py failure handling (Phase 4).

Tests that PipelineManager never raises from step execution,
and that finalize() always returns a summary.
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

        with pytest.raises(ValueError, match="No completed step named 'nonexistent'"):
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
