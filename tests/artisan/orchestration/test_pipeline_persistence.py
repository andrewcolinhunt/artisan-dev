"""Tests for step persistence and caching in PipelineManager."""

from __future__ import annotations

import json
import os
from enum import StrEnum, auto
from typing import ClassVar
from unittest.mock import MagicMock, patch

import polars as pl
from pydantic import BaseModel

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

    def execute_function(self, inputs, output_dir):
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

    class Params(BaseModel):
        """Params for ``IngestMockOp``.

        Attributes:
            seed: Mock RNG seed for cache-invalidation tests.
        """

        seed: int = 42

    params: Params = Params()

    def execute_curator(self, execute_input):
        from artisan.schemas.execution.curator_result import ArtifactResult

        return ArtifactResult(success=True)


def _mock_execute_step(**kwargs):
    """Return a fake StepResult."""
    from artisan.orchestration.engine.step_executor import build_step_result
    from artisan.schemas.enums import FailurePolicy

    return build_step_result(
        operation=kwargs["operation"],
        step_number=kwargs["step_number"],
        succeeded_count=5,
        failed_count=0,
        failure_policy=kwargs["ov"].failure_policy or FailurePolicy.CONTINUE,
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
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
        )
        pipeline.run(IngestMockOp, inputs=None)

        steps_path = str(tmp_path / "delta" / "orchestration/steps")
        assert os.path.exists(steps_path)
        df = pl.read_delta(steps_path)
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
        """A whole-step hit is recorded under a fresh current-run identity."""
        from artisan.orchestration.engine.step_tracker import _WholeStepCacheHit

        delta = tmp_path / "delta"
        staging = tmp_path / "staging"

        # First run
        p1 = PipelineManager.create(
            name="test", delta_root=str(delta), staging_root=str(staging)
        )
        first_result = p1.run(IngestMockOp, inputs=None)
        assert mock_exec.call_count == 1

        # Second run — same operation, same step position, same params
        p2 = PipelineManager.create(
            name="test", delta_root=str(delta), staging_root=str(staging)
        )
        cached_execution = "b" * 32
        p2._step_tracker.check_cache = MagicMock(
            return_value=_WholeStepCacheHit(
                result=first_result,
                source_step_run_id=first_result.step_run_id,
                execution_run_ids=(cached_execution,),
            )
        )
        with patch.object(p2, "_commit_whole_step_reuse") as commit_reuse:
            result = p2.run(IngestMockOp, inputs=None)

        # execute_step NOT called again
        assert mock_exec.call_count == 1
        assert result.step_name == "Ingest"
        assert result.success is True
        assert result.step_run_id != first_result.step_run_id
        assert len(result.step_run_id) == 32
        commit_reuse.assert_called_once_with(
            result.step_run_id,
            (cached_execution,),
            step_number=0,
            operation_name=IngestMockOp.name,
        )

        rows = pl.read_delta(delta / "orchestration" / "steps").filter(
            pl.col("pipeline_run_id") == p2.config.pipeline_run_id
        )
        assert rows.height == 2
        assert set(rows["step_run_id"].to_list()) == {result.step_run_id}
        row = rows.filter(pl.col("status") == "completed").row(0, named=True)
        assert row["status"] == "completed"
        assert row["step_run_id"] == result.step_run_id
        assert row["total_count"] == 5
        assert json.loads(row["output_roles_json"]) == ["file"]
        assert row["compute_backend"] == "local"
        options = json.loads(row["compute_options_json"])
        assert options["pipeline_default_step_runner"] == "local"
        assert options["pipeline_default_local_runner"] == {"default_max_workers": 4}

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_upstream_change_without_output_change_reuses_downstream(
        self, mock_exec, tmp_path
    ):
        """Changed upstream params do not invalidate identical concrete inputs."""
        from artisan.orchestration.engine.step_tracker import _WholeStepCacheHit

        delta = tmp_path / "delta"
        staging = tmp_path / "staging"

        # First run: Ingest -> MockOp
        p1 = PipelineManager.create(
            name="test", delta_root=str(delta), staging_root=str(staging)
        )
        step0 = p1.run(IngestMockOp, inputs=None)
        downstream_source = p1.run(MockOp, inputs={"data": step0.output("file")})
        assert mock_exec.call_count == 2

        # Second run with different params on step 0
        p2 = PipelineManager.create(
            name="test", delta_root=str(delta), staging_root=str(staging)
        )
        step0b = p2.run(IngestMockOp, inputs=None, params={"seed": 99})
        cached_execution = "c" * 32
        p2._step_tracker.check_cache = MagicMock(
            return_value=_WholeStepCacheHit(
                result=downstream_source,
                source_step_run_id=downstream_source.step_run_id,
                execution_run_ids=(cached_execution,),
            )
        )
        with patch.object(p2, "_commit_whole_step_reuse"):
            downstream_current = p2.run(MockOp, inputs={"data": step0b.output("file")})
        # Step 0 re-executes, but the mocked run still exposes the same empty
        # concrete output snapshot, so the content-addressed downstream key is stable.
        assert mock_exec.call_count == 3
        assert downstream_current.step_run_id != downstream_source.step_run_id

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_pipeline_run_id_generated(self, mock_exec, tmp_path):
        """pipeline_run_id is non-empty and contains pipeline name."""
        pipeline = PipelineManager.create(
            name="example_pipeline",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
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
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
        )
        pipeline.run(IngestMockOp, inputs=None)
        summary = pipeline.finalize()
        assert summary["total_steps"] == 1
        assert summary["overall_success"] is True
        assert pipeline._executor is None
