"""Delta Lake execution cache lookup.

Query the executions table for a previous successful run with the same
deterministic execution_spec_id. On a cache hit the caller skips
execution entirely and reuses existing artifacts.

Complements the file-based cache in ``execution/cache_validation.py``,
which validates a specific sandbox directory. This module performs a
global lookup across all prior executions.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from artisan.schemas.enums import CacheValidationReason, TablePath
from artisan.schemas.execution.cache_result import CacheHit, CacheMiss


def cache_lookup(
    executions_path: Path | str,
    execution_spec_id: str,
    execution_edges_path: Path | str | None = None,
) -> CacheHit | CacheMiss:
    """Look up a cached execution by its deterministic spec ID.

    Query the executions Delta table for a prior successful run matching
    ``execution_spec_id``. On a hit, return the cached inputs and outputs
    so the caller can skip re-execution.

    Args:
        executions_path: Path to the executions Delta table.
        execution_spec_id: Deterministic ID computed from operation,
            inputs, and merged params.
        execution_edges_path: Path to the execution_edges Delta table.
            If None, inferred from ``executions_path`` by navigating two
            levels up to the delta root.

    Returns:
        ``CacheHit`` with prior execution's inputs and outputs when a
        successful match exists, or ``CacheMiss`` with a reason of
        ``NO_PREVIOUS_EXECUTION`` or ``EXECUTION_FAILED``.
    """
    records_path = Path(executions_path)

    if not records_path.exists():
        return CacheMiss(
            execution_spec_id,
            reason=CacheValidationReason.NO_PREVIOUS_EXECUTION,
        )

    # Derive provenance path from delta_root if not provided
    if execution_edges_path is None:
        # Infer delta_root from the executions table path
        # (delta_root / "orchestration/executions" -> delta_root)
        delta_root = records_path.parent.parent
        provenance_path = delta_root / TablePath.EXECUTION_EDGES
    else:
        provenance_path = Path(execution_edges_path)

    # Query for successful execution with this spec_id
    result = (
        pl.scan_delta(str(records_path))
        .filter(pl.col("execution_spec_id") == execution_spec_id)
        .filter(pl.col("success") == True)  # noqa: E712
        .sort("timestamp_start", descending=True)  # Most recent first
        .limit(1)
        .collect()
    )

    if result.is_empty():
        # Check if there's a failed execution (for better error message)
        any_exec = (
            pl.scan_delta(str(records_path))
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
    execution_run_id = row["execution_run_id"]

    # Query execution_edges for inputs/outputs
    inputs: list[dict] = []
    outputs: list[dict] = []

    if provenance_path.exists():
        provenance_df = (
            pl.scan_delta(str(provenance_path))
            .filter(pl.col("execution_run_id") == execution_run_id)
            .collect()
        )

        for prov_row in provenance_df.iter_rows(named=True):
            entry = {"role": prov_row["role"], "artifact_id": prov_row["artifact_id"]}
            if prov_row["direction"] == "input":
                inputs.append(entry)
            else:
                outputs.append(entry)

    return CacheHit(
        execution_run_id=execution_run_id,
        execution_spec_id=row["execution_spec_id"],
        inputs=inputs,
        outputs=outputs,
    )
