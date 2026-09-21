"""Authoritative current-run execution and output membership queries."""

from __future__ import annotations

import json
import re
from typing import Any

import polars as pl
from fsspec import AbstractFileSystem

from artisan.errors import (
    ArtifactIntegrityError,
    PersistenceIntegrityError,
    StoreIntegrityError,
)
from artisan.schemas.enums import TablePath
from artisan.storage.core.committed_scan import read_committed
from artisan.storage.core.store_format import assert_store_format
from artisan.utils.path import uri_join

_HEX_ID = re.compile(r"[0-9a-f]{32}")
_ACCEPTED_OUTPUT_STATUSES = frozenset({"succeeded", "partial"})

_MEMBERSHIP_SCHEMA: dict[str, Any] = {
    "pipeline_run_id": pl.String,
    "current_step_run_id": pl.String,
    "current_step_number": pl.Int32,
    "status": pl.String,
    "execution_run_id": pl.String,
    "cache_hit": pl.Boolean,
    "success": pl.Boolean,
    "operation_name": pl.String,
    "execution_step_number": pl.Int32,
    "timestamp_start": pl.Datetime("us", "UTC"),
    "error": pl.String,
    "error_envelope": pl.String,
    "metadata": pl.String,
    "command_recording": pl.String,
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
        delta_root: Root containing the current release's Delta tables.
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
    assert_store_format(delta_root, fs, options)
    _require_tables(
        delta_root,
        fs,
        (TablePath.STEPS, TablePath.EXECUTIONS, TablePath.CACHE_REUSE),
    )
    steps = _read_steps(delta_root, fs, options)
    executions = _read_executions(delta_root, fs, options)
    reuse = _read_cache_reuse(delta_root, fs, options)
    _validate_relations(steps, executions, reuse)

    attempts = _current_attempts(delta_root, fs, options)
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
        "execution_step_number",
        "timestamp_start",
        "error",
        "error_envelope",
        "metadata",
        "command_recording",
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
        read_committed(
            delta_root,
            TablePath.EXECUTION_EDGES,
            fs=fs,
            storage_options=options,
        )
        .filter(pl.col("direction") == "output")
        .select("execution_run_id", "role", "artifact_id")
    )
    if role is not None:
        edges = edges.filter(pl.col("role") == role)
    outputs = membership.join(edges, on="execution_run_id", how="inner")
    if outputs.is_empty():
        return pl.DataFrame(schema=_OUTPUT_SCHEMA)

    index = read_committed(
        delta_root,
        TablePath.ARTIFACT_INDEX,
        fs=fs,
        storage_options=options,
    ).select("artifact_id", "artifact_type", "origin_step_number")
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


def load_run_step_outputs(
    delta_root: str,
    *,
    pipeline_run_id: str,
    step_number: int,
    include_prior_steps: bool,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Read accepted outputs from the latest attempts at or through a boundary.

    Args:
        delta_root: Root containing the supported Delta tables.
        pipeline_run_id: Exact source run to select.
        step_number: Required inclusive boundary in that run.
        include_prior_steps: Include existing positions below the boundary.
        fs: Source filesystem.
        storage_options: Delta-rs storage options for cloud backends.

    Returns:
        Accepted output edges for the selected terminal attempt IDs.

    Raises:
        ValueError: If the run or boundary is absent, or selected attempts
            are pending or running.
        PersistenceIntegrityError: If source lifecycle or membership is invalid.
    """
    from artisan.orchestration.engine.step_tracker import StepTracker

    options = storage_options or {}
    assert_store_format(delta_root, fs, options)
    states = StepTracker(
        delta_root, fs=fs, storage_options=options
    ).load_current_states(pipeline_run_id)
    if not states:
        msg = f"Unknown source run {pipeline_run_id!r}"
        raise ValueError(msg)
    if not any(state.step_number == step_number for state in states):
        msg = f"Source run {pipeline_run_id!r} has no step {step_number}"
        raise ValueError(msg)
    selected = [
        state
        for state in states
        if state.step_number == step_number
        or (include_prior_steps and state.step_number < step_number)
    ]
    unresolved = [
        (state.step_number, state.step_run_id)
        for state in selected
        if state.status.value in {"pending", "running"}
    ]
    if unresolved:
        msg = f"Source run {pipeline_run_id!r} has unresolved attempts: {unresolved!r}"
        raise ValueError(msg)
    # Freeze exact terminal attempts before reading their accepted output edges.
    selected_ids = [state.step_run_id for state in selected]
    return load_accepted_outputs(
        delta_root,
        fs=fs,
        storage_options=options,
        pipeline_run_id=pipeline_run_id,
    ).filter(pl.col("current_step_run_id").is_in(selected_ids))


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
    options = storage_options or {}
    assert_store_format(delta_root, fs, options)
    _require_hex(current_step_run_id, "current_step_run_id")
    execution_ids = sorted(set(cached_execution_run_ids))
    for execution_id in execution_ids:
        _require_hex(execution_id, "cached_execution_run_id")
    if not execution_ids:
        return []

    _require_tables(
        delta_root,
        fs,
        (TablePath.EXECUTIONS, TablePath.EXECUTION_EDGES, TablePath.ARTIFACT_INDEX),
    )
    executions = _read_executions(delta_root, fs, options).filter(
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
        read_committed(
            delta_root,
            TablePath.EXECUTION_EDGES,
            fs=fs,
            storage_options=options,
        )
        .filter(
            pl.col("execution_run_id").is_in(execution_ids)
            & (pl.col("direction") == "output")
        )
        .select("artifact_id")
    )
    if edges.is_empty():
        return execution_ids
    index = read_committed(
        delta_root,
        TablePath.ARTIFACT_INDEX,
        fs=fs,
        storage_options=options,
    ).select("artifact_id", "artifact_type")
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


def validate_staged_execution(
    delta_root: str,
    frames: dict[str, pl.DataFrame],
    *,
    execution_run_id: str,
    step_run_id: str,
    step_number: int,
    operation_name: str,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
    files_root: str | None = None,
) -> None:
    """Validate a sealed recovery candidate against staged and committed data."""
    from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
    from artisan.storage.core.artifact_store import ArtifactStore
    from artisan.storage.core.table_schemas import get_schema

    executions = frames.get(TablePath.EXECUTIONS.value)
    if executions is None or executions.height != 1:
        msg = "Recovery requires exactly one execution"
        raise StoreIntegrityError(msg)
    record = executions.row(0, named=True)
    if (
        record["execution_run_id"] != execution_run_id
        or record["step_run_id"] != step_run_id
        or record["origin_step_number"] != step_number
        or record["operation_name"] != operation_name
        or record["success"] is not True
        or record["replay_of_execution_run_id"] is not None
    ):
        msg = "Recovery execution ownership or outcome is invalid"
        raise StoreIntegrityError(msg)
    for field in ("execution_run_id", "execution_spec_id", "step_run_id"):
        _require_hex(record[field], field)
    owner = _validate_staged_replay(delta_root, record, fs, storage_options or {})
    edges = frames.get(
        TablePath.EXECUTION_EDGES.value,
        pl.DataFrame(schema=get_schema(TablePath.EXECUTION_EDGES)),
    )
    provenance = frames.get(
        TablePath.ARTIFACT_EDGES.value,
        pl.DataFrame(schema=get_schema(TablePath.ARTIFACT_EDGES)),
    )
    references: set[str] = set()
    for row in edges.iter_rows(named=True):
        if (
            row["execution_run_id"] != execution_run_id
            or row["direction"] not in {"input", "output"}
            or not row["role"]
        ):
            msg = "Recovery execution edges are invalid"
            raise StoreIntegrityError(msg)
        _require_hex(row["artifact_id"], "artifact_id")
        references.add(row["artifact_id"])
    for row in provenance.iter_rows(named=True):
        edge = ArtifactProvenanceEdge.model_validate(row)
        if edge.execution_run_id != execution_run_id:
            msg = "Recovery provenance belongs to another execution"
            raise StoreIntegrityError(msg)
        references.update((edge.source_artifact_id, edge.target_artifact_id))
    types = ArtifactStore(
        delta_root,
        fs=fs,
        storage_options=storage_options,
        files_root=files_root,
    ).validate_staged_artifacts(frames, references)
    output_types = json.loads(owner["output_types_json"])
    for row in edges.filter(pl.col("direction") == "output").iter_rows(named=True):
        if row["role"] not in output_types or output_types[row["role"]] not in {
            None,
            types[row["artifact_id"]],
        }:
            msg = "Recovery output edges disagree with source role declarations"
            raise StoreIntegrityError(msg)
    for row in provenance.iter_rows(named=True):
        for side in ("source", "target"):
            if types[row[f"{side}_artifact_id"]] != row[f"{side}_artifact_type"]:
                msg = "Recovery provenance artifact types conflict"
                raise StoreIntegrityError(msg)


def _validate_staged_replay(
    delta_root: str,
    record: dict[str, Any],
    fs: AbstractFileSystem,
    storage_options: dict[str, str],
) -> dict[str, Any]:
    """Deserialize diagnostics and verify available original owner evidence."""
    from artisan.schemas.execution.command_record import CommandRecording
    from artisan.schemas.execution.replay import ReplaySnapshot

    try:
        CommandRecording.model_validate_json(record["command_recording"])
        snapshot = ReplaySnapshot.model_validate_json(record["replay_snapshot"])
    except (ValueError, TypeError) as exc:
        msg = "Recovery command or replay evidence is malformed"
        raise StoreIntegrityError(msg) from exc
    if snapshot.diagnostic is not None:
        msg = "Diagnostic execution cannot be recovered"
        raise StoreIntegrityError(msg)
    owner = (
        read_committed(
            delta_root,
            TablePath.STEPS,
            fs=fs,
            storage_options=storage_options,
        )
        .filter(pl.col("step_run_id") == record["step_run_id"])
        .sort("state_sequence")
    )
    if owner.is_empty():
        msg = "Recovery source attempt is missing"
        raise StoreIntegrityError(msg)
    # Failed/cancelled results can clear accepted-output metadata; recovery
    # validates the operation's original declaration, retained in its first row.
    source_step = owner.row(0, named=True)
    if snapshot.source is not None and (
        snapshot.source.step_run_id != record["step_run_id"]
        or snapshot.source.step_number != record["origin_step_number"]
        or snapshot.source.execution_spec_id != record["execution_spec_id"]
        or snapshot.source.pipeline_run_id not in {None, source_step["pipeline_run_id"]}
    ):
        msg = "Recovery replay source ownership conflicts"
        raise StoreIntegrityError(msg)
    if snapshot.operation is not None:
        identity = snapshot.operation.identity
        if identity.name != record["operation_name"] or (
            f"{identity.module}.{identity.qualname}" != source_step["operation_class"]
        ):
            msg = "Recovery operation ownership conflicts"
            raise StoreIntegrityError(msg)
    return source_step


def _read_steps(
    delta_root: str,
    fs: AbstractFileSystem,
    options: dict[str, str],
) -> pl.DataFrame:
    """Read the ownership and lifecycle columns used by membership."""
    return read_committed(
        delta_root,
        TablePath.STEPS,
        fs=fs,
        storage_options=options,
    ).select(
        "step_run_id",
        "pipeline_run_id",
        "step_number",
        "step_name",
        "status",
        "timestamp",
    )


def _read_executions(
    delta_root: str,
    fs: AbstractFileSystem,
    options: dict[str, str],
) -> pl.DataFrame:
    """Read execution identity and inspection fields once."""
    return read_committed(
        delta_root,
        TablePath.EXECUTIONS,
        fs=fs,
        storage_options=options,
    ).select(
        "execution_run_id",
        "step_run_id",
        "success",
        "operation_name",
        pl.col("origin_step_number").alias("execution_step_number"),
        "timestamp_start",
        "error",
        "error_envelope",
        "metadata",
        "command_recording",
    )


def _read_cache_reuse(
    delta_root: str,
    fs: AbstractFileSystem,
    options: dict[str, str],
) -> pl.DataFrame:
    """Read and pair-deduplicate the exact physical reuse relation."""
    return (
        read_committed(
            delta_root,
            TablePath.CACHE_REUSE,
            fs=fs,
            storage_options=options,
        )
        .select("current_step_run_id", "cached_execution_run_id")
        .unique(maintain_order=True)
    )


def _current_attempts(
    delta_root: str,
    fs: AbstractFileSystem,
    options: dict[str, str],
) -> pl.DataFrame:
    """Project centrally validated current lifecycle attempts."""
    from artisan.orchestration.engine.step_tracker import StepTracker

    states = StepTracker(
        delta_root,
        storage_options=options,
        fs=fs,
    ).load_all_current_states()
    if not states:
        return pl.DataFrame(
            schema={
                "pipeline_run_id": pl.String,
                "current_step_run_id": pl.String,
                "current_step_number": pl.Int32,
                "status": pl.String,
            }
        )
    return pl.DataFrame(
        [
            {
                "pipeline_run_id": state.pipeline_run_id,
                "current_step_run_id": state.step_run_id,
                "current_step_number": state.step_number,
                "status": state.status.value,
            }
            for state in states
        ],
        schema={
            "pipeline_run_id": pl.String,
            "current_step_run_id": pl.String,
            "current_step_number": pl.Int32,
            "status": pl.String,
        },
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
                pl.col("step_name").n_unique().alias("step_names"),
            )
            .filter(
                (pl.col("runs") != 1)
                | (pl.col("steps") != 1)
                | (pl.col("step_names") != 1)
            )
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
    """Hydrate cached outputs through the typed artifact integrity boundary."""
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
    """Fail closed when a required framework table is missing."""
    missing = [
        table.value for table in tables if not fs.exists(uri_join(delta_root, table))
    ]
    if missing:
        msg = f"Store is missing required tables: {missing!r}"
        raise PersistenceIntegrityError(msg)


def _require_hex(value: object, field: str) -> None:
    """Require the canonical occurrence-ID representation."""
    if not isinstance(value, str) or _HEX_ID.fullmatch(value) is None:
        msg = f"{field} must be a 32-character lowercase hexadecimal ID"
        raise PersistenceIntegrityError(msg)
