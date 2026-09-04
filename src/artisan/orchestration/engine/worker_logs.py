"""Persist provider worker logs before staged records are committed."""

from __future__ import annotations

import logging
import os

import polars as pl
from fsspec import AbstractFileSystem

from artisan.schemas.execution.unit_result import UnitResult
from artisan.utils.path import shard_uri, uri_join

logger = logging.getLogger(__name__)


def persist_worker_logs(
    results: list[UnitResult],
    staging_root: str,
    failure_logs_root: str | None,
    operation_name: str,
    step_number: int,
    *,
    fs: AbstractFileSystem,
) -> None:
    """Write provider logs into staged records and failure logs.

    Args:
        results: Results that may contain provider-captured worker logs.
        staging_root: Root staging directory.
        failure_logs_root: Directory containing human-readable failure logs.
        operation_name: Operation name used in the staging path.
        step_number: Pipeline step number used in the staging path.
        fs: Configured filesystem containing staged records.
    """
    for result in results:
        if not result.worker_log:
            continue
        for run_id in result.execution_run_ids:
            _patch_staged_record(
                staging_root,
                run_id,
                operation_name,
                step_number,
                result.worker_log,
                fs=fs,
            )
            if not result.success and failure_logs_root:
                _append_worker_log(
                    failure_logs_root,
                    run_id,
                    result.worker_log,
                )


def _patch_staged_record(
    staging_root: str,
    execution_run_id: str,
    operation_name: str,
    step_number: int,
    worker_log: str,
    *,
    fs: AbstractFileSystem,
) -> None:
    """Patch one staged execution record, best-effort."""
    try:
        staging_dir = _find_staging_dir(
            staging_root,
            execution_run_id,
            step_number,
            operation_name,
            fs=fs,
        )
        if staging_dir is None:
            return
        parquet_path = uri_join(staging_dir, "executions.parquet")
        if not fs.exists(parquet_path):
            return
        with fs.open(parquet_path, "rb") as file:
            frame = pl.read_parquet(file)
        with fs.open(parquet_path, "wb") as file:
            frame.with_columns(pl.lit(worker_log).alias("worker_log")).write_parquet(
                file,
                compression="zstd",
            )
    except Exception:
        logger.debug(
            "Failed to persist worker_log for %s",
            execution_run_id,
            exc_info=True,
        )


def _append_worker_log(
    failure_logs_root: str,
    execution_run_id: str,
    worker_log: str,
) -> None:
    """Append opaque provider output to an existing failure log, best-effort."""
    try:
        for entry in os.listdir(failure_logs_root):
            step_dir = os.path.join(failure_logs_root, entry)
            if not os.path.isdir(step_dir):
                continue
            log_path = os.path.join(step_dir, f"{execution_run_id}.log")
            if os.path.exists(log_path):
                with open(log_path, "a") as file:
                    file.write(f"\n\n=== Worker Log ===\n{worker_log}")
                return
    except Exception:
        logger.debug(
            "Failed to append worker log to failure log for %s",
            execution_run_id,
            exc_info=True,
        )


def _find_staging_dir(
    staging_root: str,
    execution_run_id: str,
    step_number: int,
    operation_name: str,
    *,
    fs: AbstractFileSystem,
) -> str | None:
    """Return the sharded staging directory when it exists."""
    candidate = shard_uri(
        staging_root,
        execution_run_id,
        step_number=step_number,
        operation_name=operation_name,
    )
    return candidate if fs.isdir(candidate) else None
