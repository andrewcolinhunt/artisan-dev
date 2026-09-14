"""Authoritative current-run execution and output membership queries."""

from __future__ import annotations

import re
from typing import Any

import polars as pl
from fsspec import AbstractFileSystem

from artisan.errors import ArtifactIntegrityError, PersistenceIntegrityError
from artisan.schemas.enums import TablePath
from artisan.utils.path import uri_join

_HEX_ID = re.compile(r"[0-9a-f]{32}")
_ACCEPTED_OUTPUT_STATUSES = frozenset({"completed", "succeeded", "partial"})

_MEMBERSHIP_SCHEMA: dict[str, Any] = {
    "pipeline_run_id": pl.String,
    "current_step_run_id": pl.String,
    "current_step_number": pl.Int32,
    "status": pl.String,
    "execution_run_id": pl.String,
    "cache_hit": pl.Boolean,
    "success": pl.Boolean,
    "operation_name": pl.String,
    "error": pl.String,
    "error_envelope": pl.String,
    "metadata": pl.String,
}

_OUTPUT_SCHEMA: dict[str, Any] = {
    **_MEMBERSHIP_SCHEMA,
    "role": pl.String,
    "artifact_id": pl.String,
    "artifact_type": pl.String,
    "origin_step_number": pl.Int32,
}


def load_execution_membership(
    delta_root: str,
    *,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
    pipeline_run_id: str | None = None,
    step_run_id: str | None = None,
) -> pl.DataFrame:
    """Derive direct and reused executions for exact current step attempts.

    Args:
        delta_root: Root containing the format-2 Delta tables.
        fs: Filesystem used for table existence checks.
        storage_options: Delta-rs storage options for cloud backends.
        pipeline_run_id: Optional exact pipeline-run restriction.
        step_run_id: Optional exact current step-attempt restriction.

    Returns:
        One row per current-step/execution participation. An unknown run or
        attempt returns an empty frame with a stable schema.

    Raises:
        PersistenceIntegrityError: If cache reuse or execution ownership is
            malformed, dangling, duplicated, or contradictory.
    """
    options = storage_options or {}
    _require_tables(
        delta_root,
        fs,
        (TablePath.STEPS, TablePath.EXECUTIONS, TablePath.CACHE_REUSE),
    )
    steps = _read_steps(delta_root, options)
    executions = _read_executions(delta_root, options)
    reuse = _read_cache_reuse(delta_root, options)
    _validate_relations(steps, executions, reuse)

    attempts = _latest_attempts(steps)
    if pipeline_run_id is not None:
        attempts = attempts.filter(pl.col("pipeline_run_id") == pipeline_run_id)
    if step_run_id is not None:
        attempts = attempts.filter(pl.col("current_step_run_id") == step_run_id)
    if attempts.is_empty():
        return pl.DataFrame(schema=_MEMBERSHIP_SCHEMA)

    execution_columns = [
        "execution_run_id",
        "step_run_id",
        "success",
        "operation_name",
        "error",
        "error_envelope",
        "metadata",
    ]
    direct = (
        attempts.join(
            executions.select(execution_columns),
            left_on="current_step_run_id",
            right_on="step_run_id",
            how="inner",
        )
        .with_columns(pl.lit(False).alias("cache_hit"))
        .select(*_MEMBERSHIP_SCHEMA)
    )
    reused = (
        attempts.join(reuse, on="current_step_run_id", how="inner")
        .join(
            executions.select(execution_columns),
            left_on="cached_execution_run_id",
            right_on="execution_run_id",
            how="inner",
        )
        .with_columns(
            pl.col("cached_execution_run_id").alias("execution_run_id"),
            pl.lit(True).alias("cache_hit"),
        )
        .select(*_MEMBERSHIP_SCHEMA)
    )
    if direct.is_empty() and reused.is_empty():
        return pl.DataFrame(schema=_MEMBERSHIP_SCHEMA)
    return pl.concat([direct, reused], how="vertical").unique(
        subset=["current_step_run_id", "execution_run_id"], maintain_order=True
    )


def load_accepted_outputs(
    delta_root: str,
    *,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
    pipeline_run_id: str | None = None,
    step_run_id: str | None = None,
    role: str | None = None,
) -> pl.DataFrame:
    """Project accepted direct-and-reused successful output edges."""
    options = storage_options or {}
    membership = load_execution_membership(
        delta_root,
        fs=fs,
        storage_options=options,
        pipeline_run_id=pipeline_run_id,
        step_run_id=step_run_id,
    ).filter(pl.col("status").is_in(_ACCEPTED_OUTPUT_STATUSES) & pl.col("success"))
    if membership.is_empty():
        return pl.DataFrame(schema=_OUTPUT_SCHEMA)

    _require_tables(
        delta_root,
        fs,
        (TablePath.EXECUTION_EDGES, TablePath.ARTIFACT_INDEX),
    )
    edges = (
        pl.scan_delta(
            uri_join(delta_root, TablePath.EXECUTION_EDGES),
            storage_options=options,
        )
        .filter(pl.col("direction") == "output")
        .select("execution_run_id", "role", "artifact_id")
        .collect()
    )
    if role is not None:
        edges = edges.filter(pl.col("role") == role)
    outputs = membership.join(edges, on="execution_run_id", how="inner")
    if outputs.is_empty():
        return pl.DataFrame(schema=_OUTPUT_SCHEMA)

    index = (
        pl.scan_delta(
            uri_join(delta_root, TablePath.ARTIFACT_INDEX),
            storage_options=options,
        )
        .select("artifact_id", "artifact_type", "origin_step_number")
        .collect()
    )
    _validate_output_index(outputs.select("artifact_id"), index)
    return (
        outputs.join(index, on="artifact_id", how="inner")
        .select(*_OUTPUT_SCHEMA)
        .unique(
            subset=["current_step_run_id", "role", "artifact_id"],
            maintain_order=True,
        )
        .sort("current_step_number", "role", "artifact_id")
    )


def validate_cached_executions(
    delta_root: str,
    current_step_run_id: str,
    cached_execution_run_ids: set[str] | list[str],
    *,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
    files_root: str | None = None,
) -> list[str]:
    """Bulk-validate execution IDs and all cached output artifacts.

    Returns the sorted, deduplicated execution IDs ready for staging.
    """
    _require_hex(current_step_run_id, "current_step_run_id")
    execution_ids = sorted(set(cached_execution_run_ids))
    for execution_id in execution_ids:
        _require_hex(execution_id, "cached_execution_run_id")
    if not execution_ids:
        return []

    options = storage_options or {}
    _require_tables(
        delta_root,
        fs,
        (TablePath.EXECUTIONS, TablePath.EXECUTION_EDGES, TablePath.ARTIFACT_INDEX),
    )
    executions = _read_executions(delta_root, options).filter(
        pl.col("execution_run_id").is_in(execution_ids)
    )
    counts = executions.group_by("execution_run_id").len()
    found = set(counts["execution_run_id"].to_list())
    missing = sorted(set(execution_ids) - found)
    duplicated = sorted(counts.filter(pl.col("len") != 1)["execution_run_id"].to_list())
    if missing or duplicated:
        msg = f"Invalid cached execution references: missing={missing!r}, duplicated={duplicated!r}"
        raise PersistenceIntegrityError(msg)
    if executions.filter(pl.col("step_run_id") == current_step_run_id).height:
        msg = f"Step {current_step_run_id} cannot reuse an execution it directly owns"
        raise PersistenceIntegrityError(msg)

    edges = (
        pl.scan_delta(
            uri_join(delta_root, TablePath.EXECUTION_EDGES),
            storage_options=options,
        )
        .filter(
            pl.col("execution_run_id").is_in(execution_ids)
            & (pl.col("direction") == "output")
        )
        .select("artifact_id")
        .collect()
    )
    if edges.is_empty():
        return execution_ids
    index = (
        pl.scan_delta(
            uri_join(delta_root, TablePath.ARTIFACT_INDEX),
            storage_options=options,
        )
        .select("artifact_id", "artifact_type")
        .collect()
    )
    _validate_output_index(edges, index)
    _validate_output_content(
        delta_root,
        edges,
        index,
        fs=fs,
        storage_options=options,
        files_root=files_root,
    )
    return execution_ids


def _read_steps(delta_root: str, options: dict[str, str]) -> pl.DataFrame:
    """Read the ownership and lifecycle columns used by membership."""
    return (
        pl.scan_delta(uri_join(delta_root, TablePath.STEPS), storage_options=options)
        .select(
            "step_run_id",
            "pipeline_run_id",
            "step_number",
            "status",
            "timestamp",
        )
        .collect()
    )


def _read_executions(delta_root: str, options: dict[str, str]) -> pl.DataFrame:
    """Read execution identity and inspection fields once."""
    return (
        pl.scan_delta(
            uri_join(delta_root, TablePath.EXECUTIONS), storage_options=options
        )
        .select(
            "execution_run_id",
            "step_run_id",
            "success",
            "operation_name",
            "error",
            "error_envelope",
            "metadata",
        )
        .collect()
    )


def _read_cache_reuse(delta_root: str, options: dict[str, str]) -> pl.DataFrame:
    """Read and pair-deduplicate the exact physical reuse relation."""
    return (
        pl.scan_delta(
            uri_join(delta_root, TablePath.CACHE_REUSE), storage_options=options
        )
        .select("current_step_run_id", "cached_execution_run_id")
        .collect()
        .unique(maintain_order=True)
    )


def _latest_attempts(steps: pl.DataFrame) -> pl.DataFrame:
    """Select the latest lifecycle event for every exact step attempt."""
    if steps.is_empty():
        return pl.DataFrame(
            schema={
                "pipeline_run_id": pl.String,
                "current_step_run_id": pl.String,
                "current_step_number": pl.Int32,
                "status": pl.String,
            }
        )
    return (
        steps.sort("timestamp", descending=True)
        .unique(subset=["step_run_id"], keep="first")
        .select(
            "pipeline_run_id",
            pl.col("step_run_id").alias("current_step_run_id"),
            pl.col("step_number").alias("current_step_number"),
            "status",
        )
    )


def _validate_relations(
    steps: pl.DataFrame,
    executions: pl.DataFrame,
    reuse: pl.DataFrame,
) -> None:
    """Validate ownership and all cache-reuse foreign-key invariants."""
    if not steps.is_empty():
        conflicting = (
            steps.group_by("step_run_id")
            .agg(
                pl.col("pipeline_run_id").n_unique().alias("runs"),
                pl.col("step_number").n_unique().alias("steps"),
            )
            .filter((pl.col("runs") != 1) | (pl.col("steps") != 1))
        )
        if not conflicting.is_empty():
            ids = conflicting["step_run_id"].to_list()
            msg = f"Step attempts have conflicting owners: {ids!r}"
            raise PersistenceIntegrityError(msg)

    duplicate_execs = (
        executions.group_by("execution_run_id").len().filter(pl.col("len") != 1)
    )
    if not duplicate_execs.is_empty():
        ids = duplicate_execs["execution_run_id"].to_list()
        msg = f"Duplicate execution IDs: {ids!r}"
        raise PersistenceIntegrityError(msg)
    if reuse.is_empty():
        return
    for value in reuse["current_step_run_id"].to_list():
        _require_hex(value, "current_step_run_id")
    for value in reuse["cached_execution_run_id"].to_list():
        _require_hex(value, "cached_execution_run_id")

    missing_steps = reuse.join(
        steps.select(pl.col("step_run_id").unique()),
        left_on="current_step_run_id",
        right_on="step_run_id",
        how="anti",
    )
    missing_execs = reuse.join(
        executions.select(pl.col("execution_run_id").unique()),
        left_on="cached_execution_run_id",
        right_on="execution_run_id",
        how="anti",
    )
    if not missing_steps.is_empty() or not missing_execs.is_empty():
        msg = (
            "Dangling cache reuse relation: "
            f"steps={missing_steps['current_step_run_id'].to_list()!r}, "
            f"executions={missing_execs['cached_execution_run_id'].to_list()!r}"
        )
        raise PersistenceIntegrityError(msg)
    self_owned = reuse.join(
        executions.select("execution_run_id", "step_run_id"),
        left_on="cached_execution_run_id",
        right_on="execution_run_id",
        how="inner",
    ).filter(pl.col("current_step_run_id") == pl.col("step_run_id"))
    if not self_owned.is_empty():
        msg = "Cache reuse relation targets a directly owned execution"
        raise PersistenceIntegrityError(msg)


def _validate_output_index(outputs: pl.DataFrame, index: pl.DataFrame) -> None:
    """Require one authoritative index type for every output artifact."""
    artifact_ids = sorted(set(outputs["artifact_id"].to_list()))
    relevant = index.filter(pl.col("artifact_id").is_in(artifact_ids))
    type_counts = relevant.group_by("artifact_id").agg(
        pl.col("artifact_type").n_unique().alias("types")
    )
    found = set(type_counts["artifact_id"].to_list())
    missing = sorted(set(artifact_ids) - found)
    conflicting = sorted(
        type_counts.filter(pl.col("types") != 1)["artifact_id"].to_list()
    )
    if missing or conflicting:
        msg = f"Cached output index integrity failed: missing={missing!r}, conflicting={conflicting!r}"
        raise ArtifactIntegrityError(msg)


def _validate_output_content(
    delta_root: str,
    outputs: pl.DataFrame,
    index: pl.DataFrame,
    *,
    fs: AbstractFileSystem,
    storage_options: dict[str, str],
    files_root: str | None,
) -> None:
    """Hydrate cached outputs through D1's typed integrity boundary."""
    from artisan.storage.core.artifact_store import ArtifactStore

    artifact_ids = sorted(set(outputs["artifact_id"].to_list()))
    relevant = index.filter(pl.col("artifact_id").is_in(artifact_ids))
    store = ArtifactStore(
        delta_root,
        fs=fs,
        storage_options=storage_options,
        files_root=files_root,
    )
    for artifact_type in relevant["artifact_type"].unique().to_list():
        typed_ids = relevant.filter(pl.col("artifact_type") == artifact_type)[
            "artifact_id"
        ].to_list()
        loaded = store.get_artifacts_by_type(typed_ids, artifact_type)
        missing = sorted(set(typed_ids) - set(loaded))
        if missing:
            msg = f"Cached outputs are missing content rows: {missing!r}"
            raise ArtifactIntegrityError(msg)


def _require_tables(
    delta_root: str,
    fs: AbstractFileSystem,
    tables: tuple[TablePath, ...],
) -> None:
    """Fail closed when a format-2 framework table is missing."""
    missing = [
        table.value for table in tables if not fs.exists(uri_join(delta_root, table))
    ]
    if missing:
        msg = f"Format-2 store is missing required tables: {missing!r}"
        raise PersistenceIntegrityError(msg)


def _require_hex(value: object, field: str) -> None:
    """Require the canonical occurrence-ID representation."""
    if not isinstance(value, str) or _HEX_ID.fullmatch(value) is None:
        msg = f"{field} must be a 32-character lowercase hexadecimal ID"
        raise PersistenceIntegrityError(msg)
