"""Tests for PipelineManager.submit() async execution."""

from __future__ import annotations

import time
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


class MockOp(OperationDefinition):
    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        data = auto()

    name: ClassVar[str] = "MockOp"
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


class IngestMockOp(OperationDefinition):
    class OutputRole(StrEnum):
        file = auto()

    name: ClassVar[str] = "Ingest"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.file: OutputSpec(artifact_type=ArtifactTypes.DATA),
    }

    def execute_curator(self, execute_input):
        from artisan.schemas.execution.curator_result import ArtifactResult

        return ArtifactResult(success=True)


def _mock_execute_step(**kwargs):
    from artisan.orchestration.engine.step_executor import build_step_result

    return build_step_result(
        operation=kwargs["operation_class"],
        step_number=kwargs["step_number"],
        succeeded_count=5,
        failed_count=0,
        failure_policy=kwargs["failure_policy"],
    )


def _mock_execute_step_slow(**kwargs):
    """Simulate a slow step."""
    time.sleep(0.2)
    return _mock_execute_step(**kwargs)


def _mock_execute_step_error(**kwargs):
    """Simulate a failed step."""
    raise RuntimeError("Step execution failed")


class TestSubmit:
    """Tests for submit() non-blocking behavior."""

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_submit_returns_step_future(self, mock_exec, tmp_path):
        """submit() returns StepFuture, not StepResult."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        future = pipeline.submit(IngestMockOp, inputs=None)
        assert isinstance(future, StepFuture)

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_submit_cache_hit_resolved(self, mock_exec, tmp_path):
        """done immediately on cache hit."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"

        # First run to populate cache
        p1 = PipelineManager.create(name="test", delta_root=delta, staging_root=staging)
        p1.run(IngestMockOp, inputs=None)

        # Second run — cache hit
        p2 = PipelineManager.create(name="test", delta_root=delta, staging_root=staging)
        future = p2.submit(IngestMockOp, inputs=None)
        assert future.done is True

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_submit_output_never_blocks(self, mock_exec, tmp_path):
        """output() returns immediately even when step not done."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        future = pipeline.submit(IngestMockOp, inputs=None)
        ref = future.output("file")
        assert ref.source_step == 0
        assert ref.role == "file"

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_submit_status_transitions(self, mock_exec, tmp_path):
        """Status transitions to 'completed'."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        future = pipeline.submit(IngestMockOp, inputs=None)
        # Wait for completion
        future.result()
        assert future.status == "completed"

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_submit_result_blocks(self, mock_exec, tmp_path):
        """result() blocks and returns StepResult."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        future = pipeline.submit(IngestMockOp, inputs=None)
        result = future.result()
        assert isinstance(result, StepResult)
        assert result.step_name == "Ingest"

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step_slow,
    )
    def test_submit_result_timeout(self, mock_exec, tmp_path):
        """TimeoutError on result()."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        future = pipeline.submit(IngestMockOp, inputs=None)
        with pytest.raises(TimeoutError):
            future.result(timeout=0.01)
        # Wait for the step to actually finish to avoid teardown issues
        future.result()

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step_error,
    )
    def test_submit_error_returns_failed_result(self, mock_exec, tmp_path):
        """Failed step returns StepResult(success=False) instead of raising."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        future = pipeline.submit(IngestMockOp, inputs=None)
        result = future.result()
        assert isinstance(result, StepResult)
        assert result.success is False
        assert "error" in result.metadata
        assert "RuntimeError" in result.metadata["error"]

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_run_is_submit_result(self, mock_exec, tmp_path):
        """run() returns same StepResult as submit().result()."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        result = pipeline.run(IngestMockOp, inputs=None)
        assert isinstance(result, StepResult)
        assert result.step_name == "Ingest"

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_mixed_run_submit(self, mock_exec, tmp_path):
        """submit() followed by run() works."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        future = pipeline.submit(IngestMockOp, inputs=None)
        result2 = pipeline.run(MockOp, inputs={"data": future.output("file")})
        assert result2.step_name == "MockOp"
        assert result2.success is True

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_submit_predecessor_wait(self, mock_exec, tmp_path):
        """submit() blocks on upstream future completion."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        future0 = pipeline.submit(IngestMockOp, inputs=None)
        # This should wait for future0 internally via _wait_for_predecessors
        future1 = pipeline.submit(MockOp, inputs={"data": future0.output("file")})
        result = future1.result()
        assert result.step_name == "MockOp"
