"""Tests for the module-level list_runs() function in run_history."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from fixtures.inspection_curator import commit_curator_record

from artisan.orchestration import list_runs
from artisan.schemas.execution.storage_config import StorageConfig


@pytest.fixture
def pipeline_env(tmp_path):
    """Tmp paths for delta/staging roots."""
    return {
        "delta": str(tmp_path / "delta"),
        "staging": str(tmp_path / "staging"),
    }


class TestListRuns:
    """Tests for the module-level list_runs() function."""

    def test_list_runs_empty(self, pipeline_env):
        """Empty Delta root returns empty DataFrame."""
        runs = list_runs(pipeline_env["delta"])
        assert isinstance(runs, pl.DataFrame)
        assert len(runs) == 0

    def test_list_runs_single_run(self, pipeline_env: dict[str, str]) -> None:
        """One pipeline produces one row."""
        commit_curator_record(
            Path(pipeline_env["delta"]), run_id="single-run", success=True
        )

        runs = list_runs(pipeline_env["delta"])
        assert len(runs) == 1
        assert runs["pipeline_run_id"].to_list() == ["single-run"]

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

    def test_list_runs_multiple_runs(self, pipeline_env: dict[str, str]) -> None:
        """Three runs produce three rows with documented columns and ordering."""
        for i in range(3):
            commit_curator_record(
                Path(pipeline_env["delta"]), run_id=f"run-{i}", success=True
            )

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
