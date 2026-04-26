"""Tests for the module-level list_runs() function in run_history."""

from __future__ import annotations

from enum import StrEnum, auto
from typing import ClassVar
from unittest.mock import patch

import polars as pl
import pytest

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration import PipelineManager, list_runs
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


@pytest.fixture
def pipeline_env(tmp_path):
    """Tmp paths for delta/staging roots."""
    return {
        "delta": str(tmp_path / "delta"),
        "staging": str(tmp_path / "staging"),
    }


class _IngestMockOp(OperationDefinition):
    """Minimal curator op for list_runs tests."""

    class OutputRole(StrEnum):
        file = auto()

    name: ClassVar[str] = "ingest_for_run_history_tests"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.file: OutputSpec(artifact_type=ArtifactTypes.DATA),
    }

    def execute_curator(self, *args, **kwargs):
        from artisan.schemas.execution.curator_result import ArtifactResult

        return ArtifactResult(success=True)


def _mock_execute_step(**kwargs):
    """Mock that returns a successful StepResult via build_step_result."""
    from artisan.orchestration.engine.step_executor import build_step_result

    return build_step_result(
        operation=kwargs["operation_class"],
        step_number=kwargs["step_number"],
        succeeded_count=1,
        failed_count=0,
        failure_policy=kwargs["failure_policy"],
    )


class TestListRuns:
    """Tests for the module-level list_runs() function."""

    def test_list_runs_empty(self, pipeline_env):
        """Empty Delta root returns empty DataFrame."""
        runs = list_runs(pipeline_env["delta"])
        assert isinstance(runs, pl.DataFrame)
        assert len(runs) == 0

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_list_runs_single_run(self, mock_exec, pipeline_env):
        """One pipeline produces one row."""
        pipeline = PipelineManager.create(
            name="single_run_test",
            delta_root=pipeline_env["delta"],
            staging_root=pipeline_env["staging"],
        )
        pipeline.run(_IngestMockOp, inputs=None)

        runs = list_runs(pipeline_env["delta"])
        assert len(runs) == 1
        assert "pipeline_run_id" in runs.columns

    def test_list_runs_with_custom_storage(self, pipeline_env):
        """Custom StorageConfig is plumbed through correctly."""
        storage = StorageConfig()
        runs = list_runs(pipeline_env["delta"], storage=storage)
        assert isinstance(runs, pl.DataFrame)
        assert len(runs) == 0

    def test_list_runs_kwarg_only_storage(self, pipeline_env):
        """`storage` is keyword-only — guard against accidental positional use."""
        with pytest.raises(TypeError):
            list_runs(pipeline_env["delta"], StorageConfig())  # type: ignore[misc]

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_list_runs_multiple_runs(self, mock_exec, pipeline_env):
        """Three runs produce three rows with documented columns and ordering."""
        for i in range(3):
            pipeline = PipelineManager.create(
                name=f"multi_run_test_{i}",
                delta_root=pipeline_env["delta"],
                staging_root=pipeline_env["staging"],
            )
            # skip_cache=True so each run records a distinct step row;
            # otherwise reruns of the same op with no params dedup to the
            # first run's pipeline_run_id.
            pipeline.run(_IngestMockOp, inputs=None, skip_cache=True)

        runs = list_runs(pipeline_env["delta"])
        assert len(runs) == 3

        # Columns documented on list_runs / StepTracker.list_runs.
        expected_cols = {
            "pipeline_run_id",
            "step_count",
            "last_status",
            "started_at",
            "ended_at",
        }
        assert expected_cols.issubset(set(runs.columns))

        # Documented ordering — most-recent-first by started_at.
        started_at = runs["started_at"].to_list()
        assert started_at == sorted(started_at, reverse=True)


class TestPipelineConfigPublic:
    """PipelineConfig is now part of the public artisan.orchestration surface."""

    def test_importable_from_orchestration(self):
        from artisan.orchestration import PipelineConfig

        cfg = PipelineConfig(
            name="x",
            delta_root="/tmp/d",
            staging_root="/tmp/s",
        )
        assert cfg.name == "x"

    def test_pipeline_config_is_in_all(self):
        import artisan.orchestration as orch

        assert "PipelineConfig" in orch.__all__
        assert "list_runs" in orch.__all__
