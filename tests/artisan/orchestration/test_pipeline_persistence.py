"""Tests for step persistence and caching in PipelineManager."""

from __future__ import annotations

from enum import StrEnum, auto
from typing import ClassVar
from unittest.mock import patch

import polars as pl

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.pipeline_manager import PipelineManager
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class MockOp(OperationDefinition):
    """Mock operation for testing."""

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        data = auto()
        metrics = auto()

    name: ClassVar[str] = "MockOp"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.DATA),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.data: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": ["data"]},
        ),
        OutputRole.metrics: OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            infer_lineage_from={"outputs": ["data"]},
        ),
    }

    def preprocess(self, inputs):
        return {}

    def execute(self, inputs, output_dir):
        pass


class IngestMockOp(OperationDefinition):
    """Mock ingest operation (curator-style)."""

    class OutputRole(StrEnum):
        file = auto()

    name: ClassVar[str] = "Ingest"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.file: OutputSpec(artifact_type=ArtifactTypes.DATA),
    }

    seed: int = 42

    def execute_curator(self, execute_input):
        from artisan.schemas.execution.curator_result import ArtifactResult

        return ArtifactResult(success=True)


def _mock_execute_step(**kwargs):
    """Return a fake StepResult."""
    from artisan.orchestration.engine.step_executor import build_step_result

    return build_step_result(
        operation=kwargs["operation_class"],
        step_number=kwargs["step_number"],
        succeeded_count=5,
        failed_count=0,
        failure_policy=kwargs["failure_policy"],
    )


class TestPersistence:
    """Tests for delta table persistence via run()."""

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_run_writes_to_delta(self, mock_exec, tmp_path):
        """steps table has rows after run()."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        pipeline.run(IngestMockOp, inputs=None)

        steps_path = tmp_path / "delta" / "orchestration/steps"
        assert steps_path.exists()
        df = pl.read_delta(str(steps_path))
        # Should have running + completed rows
        assert len(df) == 2
        statuses = set(df["status"].to_list())
        assert "running" in statuses
        assert "completed" in statuses

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_run_cache_hit(self, mock_exec, tmp_path):
        """Second run with same params skips execution."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"

        # First run
        p1 = PipelineManager.create(name="test", delta_root=delta, staging_root=staging)
        p1.run(IngestMockOp, inputs=None)
        assert mock_exec.call_count == 1

        # Second run — same operation, same step position, same params
        p2 = PipelineManager.create(name="test", delta_root=delta, staging_root=staging)
        result = p2.run(IngestMockOp, inputs=None)
        # execute_step NOT called again
        assert mock_exec.call_count == 1
        assert result.step_name == "Ingest"
        assert result.success is True

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_upstream_change_invalidates(self, mock_exec, tmp_path):
        """Changed upstream params cause downstream re-execution."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"

        # First run: Ingest -> MockOp
        p1 = PipelineManager.create(name="test", delta_root=delta, staging_root=staging)
        step0 = p1.run(IngestMockOp, inputs=None)
        p1.run(MockOp, inputs={"data": step0.output("file")})
        assert mock_exec.call_count == 2

        # Second run with different params on step 0
        p2 = PipelineManager.create(name="test", delta_root=delta, staging_root=staging)
        step0b = p2.run(IngestMockOp, inputs=None, params={"seed": 99})
        p2.run(MockOp, inputs={"data": step0b.output("file")})
        # Step 0 should re-execute (different params), step 1 also (upstream changed)
        assert mock_exec.call_count == 4

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_pipeline_run_id_generated(self, mock_exec, tmp_path):
        """pipeline_run_id is non-empty and contains pipeline name."""
        pipeline = PipelineManager.create(
            name="example_pipeline",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        assert pipeline.config.pipeline_run_id != ""
        assert "example_pipeline" in pipeline.config.pipeline_run_id

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_finalize_waits_and_shuts_down(self, mock_exec, tmp_path):
        """Finalize waits for futures and shuts down executor."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        pipeline.run(IngestMockOp, inputs=None)
        summary = pipeline.finalize()
        assert summary["total_steps"] == 1
        assert summary["overall_success"] is True
        assert pipeline._executor is None
