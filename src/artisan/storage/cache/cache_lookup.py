"""Delta Lake execution cache lookup.

Query the executions table for a previous successful run with the same
deterministic execution_spec_id. On a cache hit the caller skips
execution entirely and reuses existing artifacts.

Complements the file-based cache in ``execution/cache_validation.py``,
which validates a specific sandbox directory. This module performs a
global lookup across all prior executions.
"""

from __future__ import annotations

import polars as pl
from fsspec import AbstractFileSystem

from artisan.schemas.enums import CacheValidationReason
from artisan.schemas.execution.cache_result import CacheHit, CacheMiss


def cache_lookup(
    executions_path: str,
    execution_spec_id: str,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
) -> CacheHit | CacheMiss:
    """Look up a cached execution by its deterministic spec ID.

    Query the executions Delta table for a prior successful run matching
    ``execution_spec_id``. On a hit, return the cached execution's
    identifiers so the caller can skip re-execution.

    Args:
        executions_path: URI/path to the executions Delta table.
        execution_spec_id: Deterministic ID computed from operation,
            inputs, and merged params.
        fs: Filesystem implementation (LocalFileSystem, S3FileSystem, etc.).
        storage_options: Credentials/config passed to delta-rs calls.

    Returns:
        ``CacheHit`` identifying the prior execution when a successful
        match exists, or ``CacheMiss`` with a reason of
        ``NO_PREVIOUS_EXECUTION`` or ``EXECUTION_FAILED``.
    """
    storage_options = storage_options or {}

    if not fs.exists(executions_path):
        return CacheMiss(
            execution_spec_id,
            reason=CacheValidationReason.NO_PREVIOUS_EXECUTION,
        )

    # Query for successful execution with this spec_id
    result = (
        pl.scan_delta(executions_path, storage_options=storage_options)
        .filter(pl.col("execution_spec_id") == execution_spec_id)
        .filter(pl.col("success") == True)  # noqa: E712
        .sort("timestamp_start", descending=True)  # Most recent first
        .limit(1)
        .collect()
    )

    if result.is_empty():
        # Check if there's a failed execution (for better error message)
        any_exec = (
            pl.scan_delta(executions_path, storage_options=storage_options)
            .filter(pl.col("execution_spec_id") == execution_spec_id)
            .limit(1)
            .collect()
        )
        reason = (
            CacheValidationReason.EXECUTION_FAILED
            if not any_exec.is_empty()
            else CacheValidationReason.NO_PREVIOUS_EXECUTION
        )
        return CacheMiss(execution_spec_id, reason=reason)

    row = result.row(0, named=True)

    return CacheHit(
        execution_run_id=row["execution_run_id"],
        execution_spec_id=row["execution_spec_id"],
    )
