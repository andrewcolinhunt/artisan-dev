"""Tests for backend-neutral worker-log persistence."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import fsspec
import polars as pl

from artisan.orchestration.engine.worker_logs import (
    _find_staging_dir,
    persist_worker_logs,
)
from artisan.schemas.execution.unit_result import UnitResult
from artisan.utils.log_paths import failure_log_relative_path


def _result(**overrides: object) -> UnitResult:
    defaults = {
        "success": True,
        "error": None,
        "item_count": 1,
        "execution_run_ids": [],
    }
    return UnitResult(**{**defaults, **overrides})


class TestPersistWorkerLogs:
    def test_patches_staged_execution_record(self, tmp_path: Path) -> None:
        run_id = "abcdef123456"
        staging_dir = tmp_path / "1_op" / "ab" / "cd" / run_id
        staging_dir.mkdir(parents=True)
        parquet_path = staging_dir / "executions.parquet"
        pl.DataFrame({"execution_run_id": [run_id], "success": [True]}).write_parquet(
            parquet_path
        )
        results = [_result(execution_run_ids=[run_id], worker_log="worker output")]

        fs = fsspec.filesystem("file")
        persist_worker_logs(results, str(tmp_path), None, "op", 1, fs=fs)

        frame = pl.read_parquet(parquet_path)
        assert frame["worker_log"][0] == "worker output"

    def test_patches_record_on_configured_memory_filesystem(self) -> None:
        fs = fsspec.filesystem("memory")
        run_id = "abcdef123456"
        staging_root = f"memory://worker-logs-{uuid4().hex}"
        staging_dir = f"{staging_root}/1_op/ab/cd/{run_id}"
        parquet_path = f"{staging_dir}/executions.parquet"
        fs.makedirs(staging_dir, exist_ok=True)
        with fs.open(parquet_path, "wb") as file:
            pl.DataFrame(
                {"execution_run_id": [run_id], "success": [True]}
            ).write_parquet(file)
        results = [_result(execution_run_ids=[run_id], worker_log="worker output")]

        persist_worker_logs(results, staging_root, None, "op", 1, fs=fs)

        with fs.open(parquet_path, "rb") as file:
            frame = pl.read_parquet(file)
        assert frame["worker_log"][0] == "worker output"

    def test_appends_opaque_worker_log_to_existing_failure_log(
        self, tmp_path: Path
    ) -> None:
        run_id = "abcdef123456"
        staging_dir = tmp_path / "staging" / "1_op" / "ab" / "cd" / run_id
        staging_dir.mkdir(parents=True)
        pl.DataFrame({"execution_run_id": [run_id]}).write_parquet(
            staging_dir / "executions.parquet"
        )
        failure_dir = tmp_path / "failures" / "20260919"
        failure_dir.mkdir(parents=True)
        failure_log = (
            tmp_path
            / "failures"
            / failure_log_relative_path(run_id, datetime(2026, 9, 19, tzinfo=UTC))
        )
        failure_log.write_text("operation error")
        results = [
            _result(
                success=False,
                error="failed",
                execution_run_ids=[run_id],
                worker_log="provider output without structured separators",
            )
        ]

        persist_worker_logs(
            results,
            str(tmp_path / "staging"),
            str(tmp_path / "failures"),
            "op",
            1,
            fs=fsspec.filesystem("file"),
        )

        assert (
            "=== Worker Log ===\nprovider output without structured separators"
            in failure_log.read_text()
        )

    def test_missing_staged_record_is_ignored(self, tmp_path: Path) -> None:
        results = [
            _result(
                execution_run_ids=["nonexistent"],
                worker_log="some log",
            )
        ]

        persist_worker_logs(
            results,
            str(tmp_path),
            None,
            "op",
            1,
            fs=fsspec.filesystem("file"),
        )

    def test_result_without_worker_log_is_ignored(self, tmp_path: Path) -> None:
        persist_worker_logs(
            [_result()],
            str(tmp_path),
            None,
            "op",
            1,
            fs=fsspec.filesystem("file"),
        )


class TestFindStagingDir:
    def test_finds_existing_sharded_directory(self, tmp_path: Path) -> None:
        run_id = "abcdef123456"
        staging_dir = tmp_path / "1_op" / "ab" / "cd" / run_id
        staging_dir.mkdir(parents=True)

        assert _find_staging_dir(
            str(tmp_path),
            run_id,
            1,
            "op",
            fs=fsspec.filesystem("file"),
        ) == str(staging_dir)

    def test_returns_none_when_directory_is_missing(self, tmp_path: Path) -> None:
        assert (
            _find_staging_dir(
                str(tmp_path),
                "nonexistent",
                1,
                "op",
                fs=fsspec.filesystem("file"),
            )
            is None
        )


def test_worker_logs_do_not_cross_append_same_step_and_operation(
    tmp_path: Path,
) -> None:
    from artisan.orchestration.engine.worker_logs import _append_worker_log

    paths = [
        tmp_path / failure_log_relative_path(run_id, datetime(2026, 9, 19, tzinfo=UTC))
        for run_id in ("one", "two")
    ]
    paths[0].parent.mkdir(parents=True)
    for path in paths:
        path.write_text("original")
    _append_worker_log(str(tmp_path), "one", "worker one")
    _append_worker_log(str(tmp_path), "missing", "missing worker")
    assert "worker one" in paths[0].read_text()
    assert paths[1].read_text() == "original"
