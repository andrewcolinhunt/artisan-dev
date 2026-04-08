"""Tests for SLURM log capture in dispatch and step_executor."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import polars as pl

from artisan.orchestration.engine.dispatch import (
    _capture_slurm_logs,
    _collect_results,
    _find_staging_dir,
    _patch_worker_logs,
)
from artisan.schemas.execution.unit_result import UnitResult


def _result(**overrides: object) -> UnitResult:
    """Build a UnitResult with sensible defaults."""
    defaults = dict(success=True, error=None, item_count=1, execution_run_ids=[])
    return UnitResult(**{**defaults, **overrides})


class TestCaptureSlurrmLogs:
    """Tests for _capture_slurm_logs helper."""

    def test_captures_slurm_logs(self) -> None:
        """SLURM future with .logs() populates worker_log in result."""
        future = MagicMock()
        future.logs.return_value = ("stdout data", "stderr data")
        result = _result()

        new_result = _capture_slurm_logs(future, result)

        assert new_result.worker_log is not None
        assert "stdout data" in new_result.worker_log
        assert "stderr data" in new_result.worker_log

    def test_handles_log_failure(self) -> None:
        """Exception from .logs() is swallowed; result still intact."""
        future = MagicMock()
        future.logs.side_effect = FileNotFoundError("cleaned up")
        result = _result()

        new_result = _capture_slurm_logs(future, result)

        assert new_result.worker_log is None
        assert new_result.success is True

    def test_skips_non_slurm_futures(self) -> None:
        """Future without .logs() is silently skipped."""
        future = MagicMock(spec=[])  # No attributes at all
        result = _result()

        new_result = _capture_slurm_logs(future, result)

        assert new_result.worker_log is None

    def test_batched_item_future(self) -> None:
        """SlurmBatchedItemFuture uses .slurm_job_future.logs()."""
        parent_future = MagicMock()
        parent_future.logs.return_value = ("batch stdout", "")

        future = MagicMock(spec=["slurm_job_future"])
        future.slurm_job_future = parent_future

        result = _result()
        new_result = _capture_slurm_logs(future, result)

        assert new_result.worker_log is not None
        assert "batch stdout" in new_result.worker_log


class TestCollectResultsSlurmLogs:
    """Tests for _collect_results with SLURM log enrichment."""

    def test_collect_results_captures_slurm_logs(self) -> None:
        """Results include worker_log when futures have .logs()."""
        future = MagicMock()
        future.result.return_value = _result(execution_run_ids=["run1"])
        future.logs.return_value = ("worker output", "")

        results = _collect_results([future])

        assert len(results) == 1
        assert results[0].worker_log is not None
        assert "worker output" in results[0].worker_log

    def test_collect_results_skips_non_slurm(self) -> None:
        """Local futures without .logs() work fine."""
        future = MagicMock(spec=["result"])
        future.result.return_value = _result(execution_run_ids=["run1"])

        results = _collect_results([future])

        assert len(results) == 1
        assert results[0].worker_log is None


class TestPatchWorkerLogs:
    """Tests for _patch_worker_logs in dispatch."""

    def test_patches_staged_parquet(self, tmp_path: Path) -> None:
        """worker_log is written into existing staged parquet."""
        # Create a staged parquet with expected structure
        staging_dir = tmp_path / "step_1_op" / "ab" / "abcdef123456"
        staging_dir.mkdir(parents=True)
        parquet_path = staging_dir / "executions.parquet"
        pl.DataFrame(
            {"execution_run_id": ["abcdef123456"], "success": [True]}
        ).write_parquet(parquet_path)

        results = [
            _result(
                execution_run_ids=["abcdef123456"],
                worker_log="slurm output here",
            )
        ]
        _patch_worker_logs(results, tmp_path)

        df = pl.read_parquet(parquet_path)
        assert "worker_log" in df.columns
        assert df["worker_log"][0] == "slurm output here"

    def test_no_error_when_file_missing(self, tmp_path: Path) -> None:
        """Missing parquet file doesn't cause error."""
        results = [
            _result(
                execution_run_ids=["nonexistent"],
                worker_log="some log",
            )
        ]
        # Should not raise
        _patch_worker_logs(results, tmp_path)

    def test_skips_results_without_worker_log(self, tmp_path: Path) -> None:
        """Results without worker_log are skipped."""
        results = [_result(execution_run_ids=["run1"])]
        _patch_worker_logs(results, tmp_path)  # no error


class TestFindStagingDir:
    """Tests for _find_staging_dir helper."""

    def test_finds_existing_dir(self, tmp_path: Path) -> None:
        """Locates staging dir by run_id prefix."""
        run_id = "abcdef123456"
        staging_dir = tmp_path / "step_1_op" / "ab" / run_id
        staging_dir.mkdir(parents=True)

        result = _find_staging_dir(str(tmp_path), run_id)
        assert result == str(staging_dir)

    def test_returns_none_when_missing(self, tmp_path: Path) -> None:
        """Returns None when run_id not found."""
        result = _find_staging_dir(str(tmp_path), "nonexistent123")
        assert result is None
