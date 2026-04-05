"""Tests for post_step parameter on submit() and run()."""

from __future__ import annotations

from enum import StrEnum, auto
from typing import ClassVar
from unittest.mock import patch

import pytest

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.pipeline_manager import PipelineManager
from artisan.orchestration.step_future import StepFuture
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


# ---------------------------------------------------------------------------
# Test operations
# ---------------------------------------------------------------------------


class ProducerOp(OperationDefinition):
    """Generative operation that produces a 'data' output."""

    class OutputRole(StrEnum):
        data = auto()

    name: ClassVar[str] = "Producer"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.data: OutputSpec(artifact_type=ArtifactTypes.DATA),
    }

    def execute_curator(self, execute_input):
        from artisan.schemas.execution.curator_result import ArtifactResult

        return ArtifactResult(success=True)


class ConsumerOp(OperationDefinition):
    """Operation that consumes 'data' and produces 'data'."""

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        data = auto()

    name: ClassVar[str] = "Consumer"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.DATA),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.data: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": ["data"]},
        ),
    }

    def preprocess(self, inputs):
        return {}

    def execute(self, inputs, output_dir):
        pass


class MismatchedConsumerOp(OperationDefinition):
    """Operation that expects 'metrics' — won't match ProducerOp's 'data'."""

    class InputRole(StrEnum):
        metrics = auto()

    class OutputRole(StrEnum):
        metrics = auto()

    name: ClassVar[str] = "MismatchedConsumer"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.metrics: InputSpec(artifact_type=ArtifactTypes.METRIC),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.metrics: OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            infer_lineage_from={"inputs": ["metrics"]},
        ),
    }

    def preprocess(self, inputs):
        return {}

    def execute(self, inputs, output_dir):
        pass


class DownstreamOp(OperationDefinition):
    """Operation that consumes 'data' — used for downstream wiring tests."""

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        result = auto()

    name: ClassVar[str] = "Downstream"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.DATA),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.result: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": ["data"]},
        ),
    }

    def preprocess(self, inputs):
        return {}

    def execute(self, inputs, output_dir):
        pass


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


def _mock_execute_step(**kwargs):
    from artisan.orchestration.engine.step_executor import build_step_result

    return build_step_result(
        operation=kwargs["operation_class"],
        step_number=kwargs["step_number"],
        succeeded_count=5,
        failed_count=0,
        failure_policy=kwargs["failure_policy"],
    )


_EXECUTE_STEP = "artisan.orchestration.pipeline_manager.execute_step"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPostStepFutureIdentity:
    """Returned StepFuture points to the post_step, not the main step."""

    @patch(_EXECUTE_STEP, side_effect=_mock_execute_step)
    def test_submit_returns_post_step_future(self, mock_exec, tmp_path):
        """submit() with post_step returns StepFuture for the post_step."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        future = pipeline.submit(ProducerOp, post_step=ConsumerOp)
        assert isinstance(future, StepFuture)
        # Post_step is step 1 (main step is step 0)
        assert future.step_number == 1
        assert future.step_name == "Producer.post"

    @patch(_EXECUTE_STEP, side_effect=_mock_execute_step)
    def test_run_returns_post_step_result(self, mock_exec, tmp_path):
        """run() with post_step returns StepResult from the post_step."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        result = pipeline.run(ProducerOp, post_step=ConsumerOp)
        assert isinstance(result, StepResult)
        assert result.step_number == 1
        assert result.step_name == "Producer.post"


class TestPostStepNumbering:
    """Post_step consumes two step numbers."""

    @patch(_EXECUTE_STEP, side_effect=_mock_execute_step)
    def test_two_step_numbers_consumed(self, mock_exec, tmp_path):
        """Main step gets step 0, post_step gets step 1."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        future = pipeline.submit(ProducerOp, post_step=ConsumerOp)
        # Post_step is step 1
        assert future.step_number == 1
        # Next step should get step 2
        future2 = pipeline.submit(
            DownstreamOp, inputs={"data": future.output("data")}
        )
        assert future2.step_number == 2

    @patch(_EXECUTE_STEP, side_effect=_mock_execute_step)
    def test_step_name_uses_custom_name(self, mock_exec, tmp_path):
        """Custom name on main step propagates to post_step name."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        future = pipeline.submit(
            ProducerOp, post_step=ConsumerOp, name="my_step"
        )
        assert future.step_name == "my_step.post"


class TestPostStepDownstreamWiring:
    """Downstream OutputReference resolution works through post_step."""

    @patch(_EXECUTE_STEP, side_effect=_mock_execute_step)
    def test_output_reference_points_to_post_step(self, mock_exec, tmp_path):
        """output() on the returned future references the post_step number."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        future = pipeline.submit(ProducerOp, post_step=ConsumerOp)
        ref = future.output("data")
        # source_step should be the post_step number, not the main step
        assert ref.source_step == 1
        assert ref.role == "data"

    @patch(_EXECUTE_STEP, side_effect=_mock_execute_step)
    def test_downstream_step_wires_to_post_step(self, mock_exec, tmp_path):
        """A step wired from post_step output gets correct source_step."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        future = pipeline.submit(ProducerOp, post_step=ConsumerOp)
        downstream = pipeline.submit(
            DownstreamOp, inputs={"data": future.output("data")}
        )
        result = downstream.result()
        assert result.success is True
        assert result.step_number == 2


class TestPostStepCaching:
    """Both steps cache independently."""

    @patch(_EXECUTE_STEP, side_effect=_mock_execute_step)
    def test_both_steps_cached_on_rerun(self, mock_exec, tmp_path):
        """Second run with identical inputs hits cache for both steps."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"

        # First run populates cache
        p1 = PipelineManager.create(
            name="test", delta_root=delta, staging_root=staging
        )
        p1.run(ProducerOp, post_step=ConsumerOp)
        first_call_count = mock_exec.call_count

        # Second run — both steps should be cached
        p2 = PipelineManager.create(
            name="test", delta_root=delta, staging_root=staging
        )
        result = p2.run(ProducerOp, post_step=ConsumerOp)
        assert result.success is True
        # No new execute_step calls (both cached)
        assert mock_exec.call_count == first_call_count

    @patch(_EXECUTE_STEP, side_effect=_mock_execute_step)
    def test_main_cached_post_step_runs(self, mock_exec, tmp_path):
        """When main step is cached but post_step is new, post_step executes."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"

        # First run: just the producer (no post_step) — populates cache
        p1 = PipelineManager.create(
            name="test", delta_root=delta, staging_root=staging
        )
        p1.run(ProducerOp)
        first_call_count = mock_exec.call_count

        # Second run: same producer but with post_step. Producer is cached,
        # but ConsumerOp as post_step is new.
        p2 = PipelineManager.create(
            name="test", delta_root=delta, staging_root=staging
        )
        result = p2.run(ProducerOp, post_step=ConsumerOp)
        assert result.success is True
        # One new execute_step call (only the post_step)
        assert mock_exec.call_count == first_call_count + 1


class TestPostStepRoleMismatch:
    """Role mismatch between main outputs and post_step inputs."""

    @patch(_EXECUTE_STEP, side_effect=_mock_execute_step)
    def test_role_mismatch_raises_validation_error(self, mock_exec, tmp_path):
        """Post_step with mismatched input roles raises ValueError."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        # ProducerOp outputs "data", MismatchedConsumerOp expects "metrics"
        with pytest.raises(ValueError, match="Unknown input roles"):
            pipeline.submit(ProducerOp, post_step=MismatchedConsumerOp)


class TestPostStepNone:
    """post_step=None is a no-op (backward compatibility)."""

    @patch(_EXECUTE_STEP, side_effect=_mock_execute_step)
    def test_post_step_none_is_noop(self, mock_exec, tmp_path):
        """submit() with post_step=None behaves identically to without it."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        future = pipeline.submit(ProducerOp, post_step=None)
        assert future.step_number == 0
        assert future.step_name == "Producer"


class TestPostStepEarlyExit:
    """Post_step is skipped when pipeline is stopped or cancelled."""

    @patch(_EXECUTE_STEP, side_effect=_mock_execute_step)
    def test_post_step_skipped_when_stopped(self, mock_exec, tmp_path):
        """When pipeline._stopped is True, post_step is not invoked."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        # Force the pipeline into stopped state
        pipeline._stopped = True
        future = pipeline.submit(ProducerOp, post_step=ConsumerOp)
        result = future.result()
        # Step is skipped (stopped), only one step number consumed
        assert result.step_number == 0
        assert result.metadata.get("skip_reason") == "pipeline_stopped"
        # execute_step never called
        assert mock_exec.call_count == 0

    @patch(_EXECUTE_STEP, side_effect=_mock_execute_step)
    def test_post_step_skipped_when_cancelled(self, mock_exec, tmp_path):
        """When pipeline is cancelled, post_step is not invoked."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        pipeline._cancel_event.set()
        future = pipeline.submit(ProducerOp, post_step=ConsumerOp)
        result = future.result()
        assert result.step_number == 0
        assert result.metadata.get("skip_reason") == "cancelled"
        assert mock_exec.call_count == 0
