"""Provider logs remain readable without changing worker execution seals."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.orchestration.engine.worker_logs import persist_worker_logs
from artisan.schemas.execution.unit_result import UnitResult
from artisan.utils.log_paths import failure_log_relative_path, worker_log_path
from artisan.visualization.inspect import inspect_worker_log


def _result(**overrides: object) -> UnitResult:
    return UnitResult(
        **{
            "success": True,
            "error": None,
            "item_count": 1,
            "execution_run_ids": ["a" * 32],
            "worker_log": "worker output",
            **overrides,
        }
    )


@pytest.mark.parametrize("sealed", [False, True])
def test_provider_log_does_not_require_or_mutate_staging(tmp_path, sealed):
    fs = LocalFileSystem()
    staging = tmp_path / "staging" / "execution"
    staging.mkdir(parents=True)
    seal = staging / "executions.parquet"
    if sealed:
        pl.DataFrame({"execution_run_id": ["a" * 32]}).write_parquet(seal)
    before = {p.name: p.read_bytes() for p in staging.iterdir()}
    persist_worker_logs([_result()], str(tmp_path / "delta"), None, fs=fs)
    assert {p.name: p.read_bytes() for p in staging.iterdir()} == before
    assert inspect_worker_log(str(tmp_path / "delta"), "a" * 32) == "worker output"


def test_provider_log_survives_staging_cleanup_and_identical_retry(tmp_path):
    root = str(tmp_path / "delta")
    fs = LocalFileSystem()
    persist_worker_logs([_result()], root, None, fs=fs)
    persist_worker_logs([_result()], root, None, fs=fs)
    assert inspect_worker_log(root, "a" * 32) == "worker output"
    assert inspect_worker_log(root, "b" * 32) is None


def test_conflicting_provider_log_warns_and_preserves_original(tmp_path, caplog):
    root = str(tmp_path / "delta")
    fs = LocalFileSystem()
    persist_worker_logs([_result()], root, None, fs=fs)
    persist_worker_logs([_result(worker_log="changed")], root, None, fs=fs)
    assert inspect_worker_log(root, "a" * 32) == "worker output"
    assert "Failed to persist provider log" in caplog.text


def test_unsafe_worker_log_id_cannot_create_another_path(tmp_path, caplog):
    root = str(tmp_path / "delta")
    persist_worker_logs(
        [_result(execution_run_ids=["../escape"])], root, None, fs=LocalFileSystem()
    )
    assert not (tmp_path / "delta" / "_artisan" / "escape.log").exists()
    assert "Failed to persist provider log" in caplog.text
    with pytest.raises(ValueError, match="literal path component"):
        inspect_worker_log(root, "../escape")


def test_provider_log_appends_to_matching_local_failure_log(tmp_path):
    run_id = "a" * 32
    root = tmp_path / "failures"
    path = root / failure_log_relative_path(run_id, datetime(2026, 9, 19, tzinfo=UTC))
    path.parent.mkdir(parents=True)
    path.write_text("operation error")
    persist_worker_logs(
        [_result(success=False)],
        str(tmp_path / "delta"),
        str(root),
        fs=LocalFileSystem(),
    )
    assert "=== Worker Log ===\nworker output" in path.read_text()


def test_result_without_provider_log_creates_no_diagnostic(tmp_path):
    root = str(tmp_path / "delta")
    fs = LocalFileSystem()
    persist_worker_logs([_result(worker_log=None)], root, None, fs=fs)
    assert not fs.exists(worker_log_path(root, "a" * 32))


def test_provider_log_uses_configured_s3_store(s3_fs):
    from fixtures.store_format import publish_test_store

    fs, storage, root = s3_fs
    publish_test_store(root, fs, storage.delta_storage_options())
    persist_worker_logs([_result()], root, None, fs=fs)
    assert (
        inspect_worker_log(
            root, "a" * 32, fs=fs, storage_options=storage.delta_storage_options()
        )
        == "worker output"
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
