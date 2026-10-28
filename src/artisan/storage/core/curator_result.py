"""Curator result summaries derived from sealed or committed execution evidence."""

from __future__ import annotations

import json
from typing import Any

import polars as pl
from fsspec import AbstractFileSystem

from artisan.errors import PersistenceIntegrityError
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import TablePath
from artisan.storage.core.committed_scan import (
    filter_committed_rows,
    read_logical_commits,
)
from artisan.storage.core.table_schemas import get_physical_schema
from artisan.storage.io.delta_files import DeltaSnapshot
from artisan.utils.path import uri_join


def dynamic_output_roles(options_json: str) -> bool:
    """Read the framework-owned marker without coercing persisted values."""
    try:
        options = json.loads(options_json)
        if not isinstance(options, dict):
            msg = "compute options must be an object"
            raise ValueError(msg)
        value = options.get("dynamic_output_roles", False)
        if type(value) is not bool:
            msg = "dynamic_output_roles must be a Boolean"
            raise ValueError(msg)
    except (TypeError, ValueError) as exc:
        raise PersistenceIntegrityError(str(exc)) from exc
    return value


def read_result_evidence(
    delta_root: str,
    *,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
    staged: dict[str, pl.DataFrame] | None = None,
    execution_run_ids: set[str] | None = None,
    step_run_id: str | None = None,
) -> dict[str, pl.DataFrame]:
    """Read only the membership and authored metadata needed for one result."""
    if execution_run_ids is None and step_run_id is None:
        msg = "Curator result evidence requires an execution or step scope"
        raise ValueError(msg)
    controls = read_logical_commits(delta_root, fs=fs, storage_options=storage_options)

    def rows(table: TablePath, columns: list[str], predicate: pl.Expr) -> pl.DataFrame:
        committed = _result_rows(
            delta_root,
            table,
            columns,
            predicate,
            controls,
            fs=fs,
            storage_options=storage_options,
        )
        pending = (staged or {}).get(table.value)
        return (
            committed
            if pending is None
            else pl.concat(
                [committed, pending.filter(predicate).select(columns)]
            ).unique()
        )

    reuse = rows(
        TablePath.CACHE_REUSE,
        ["current_step_run_id", "cached_execution_run_id"],
        pl.col("current_step_run_id") == step_run_id
        if step_run_id is not None
        else pl.lit(False),
    )
    ids = set(execution_run_ids or ()) | set(reuse["cached_execution_run_id"])
    predicate = pl.col("execution_run_id").is_in(ids)
    if step_run_id is not None:
        predicate |= pl.col("step_run_id") == step_run_id
    executions = rows(
        TablePath.EXECUTIONS,
        ["execution_run_id", "step_run_id", "success", "metadata"],
        predicate,
    )
    edges = rows(
        TablePath.EXECUTION_EDGES,
        ["execution_run_id", "direction", "role", "artifact_id"],
        pl.col("execution_run_id").is_in(executions["execution_run_id"].to_list()),
    )
    index = rows(
        TablePath.ARTIFACT_INDEX,
        ["artifact_id", "artifact_type"],
        pl.col("artifact_id").is_in(edges["artifact_id"].unique().to_list()),
    )
    return {
        TablePath.EXECUTIONS.value: executions,
        TablePath.EXECUTION_EDGES.value: edges,
        TablePath.ARTIFACT_INDEX.value: index,
        TablePath.CACHE_REUSE.value: reuse,
    }


def _result_rows(
    delta_root: str,
    table: TablePath,
    columns: list[str],
    predicate: pl.Expr,
    controls: pl.DataFrame,
    *,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None,
) -> pl.DataFrame:
    """Locate result membership before decoding any execution metadata."""
    schema = get_physical_schema(table)
    snapshot = DeltaSnapshot(
        uri_join(delta_root, table.value),
        schema,
        fs=fs,
        storage_options=storage_options,
    )
    owner = (
        "current_step_run_id" if table is TablePath.CACHE_REUSE else "logical_commit_id"
    )
    projection = list(dict.fromkeys([*predicate.meta.root_names(), owner]))
    selected: dict[str, set[int]] = {}
    for filename, positions, frame in snapshot.iter_located(projection):
        located = frame.with_columns(
            pl.Series("_row_ordinal", positions, dtype=pl.UInt64)
        )
        found = filter_committed_rows(located, table.value, controls).filter(predicate)
        if not found.is_empty():
            selected.setdefault(filename, set()).update(found["_row_ordinal"])
    frames = list(snapshot.iter_frames(columns, rows_by_file=selected))
    return (
        pl.concat(frames)
        if frames
        else pl.DataFrame(schema={name: schema[name] for name in columns})
    )


def curator_result_summary(
    frames: dict[str, pl.DataFrame], execution_run_ids: set[str]
) -> tuple[dict[str, str], dict[str, Any]]:
    """Recover one successful curator's actual role types and authored metadata."""
    executions = frames[TablePath.EXECUTIONS.value].filter(
        pl.col("execution_run_id").is_in(execution_run_ids)
    )
    if (
        len(execution_run_ids) != 1
        or executions.height != 1
        or executions["success"][0] is not True
    ):
        msg = "Curator result requires exactly one successful execution"
        raise PersistenceIntegrityError(msg)
    try:
        metadata = json.loads(executions["metadata"][0] or "{}")
        if not isinstance(metadata, dict) or any(
            key in metadata for key in ("skip_reason", "status", "cancelled", "skipped")
        ):
            msg = "Invalid curator execution metadata"
            raise ValueError(msg)
    except (TypeError, ValueError) as exc:
        msg = "Invalid curator execution metadata"
        raise PersistenceIntegrityError(msg) from exc
    metadata.pop("timings", None)
    outputs = frames[TablePath.EXECUTION_EDGES.value].filter(
        pl.col("execution_run_id").is_in(execution_run_ids)
        & (pl.col("direction") == "output")
    )
    index = (
        frames[TablePath.ARTIFACT_INDEX.value]
        .select("artifact_id", "artifact_type")
        .unique()
    )
    types: dict[str, set[str]] = {}
    for row in outputs.join(index, on="artifact_id", how="left").iter_rows(named=True):
        if (
            not isinstance(row["role"], str)
            or not row["role"]
            or not isinstance(row["artifact_type"], str)
            or not row["artifact_type"]
            or row["artifact_type"] == ArtifactTypes.ANY
        ):
            msg = "Curator output has an invalid role or missing concrete artifact type"
            raise PersistenceIntegrityError(msg)
        types.setdefault(row["role"], set()).add(row["artifact_type"])
    selected = index.filter(
        pl.col("artifact_id").is_in(outputs["artifact_id"].to_list())
    )
    if selected["artifact_id"].n_unique() != selected.height:
        msg = "Curator output has conflicting artifact types"
        raise PersistenceIntegrityError(msg)
    return {
        role: next(iter(values)) if len(values) == 1 else ArtifactTypes.ANY
        for role, values in types.items()
    }, metadata


def validate_dynamic_result(
    row: dict[str, Any], frames: dict[str, pl.DataFrame]
) -> None:
    """Cross-check a marked usable terminal payload against its exact membership."""
    if not dynamic_output_roles(row["compute_options_json"]) or row["status"] not in {
        "succeeded",
        "partial",
    }:
        return
    direct = (
        frames[TablePath.EXECUTIONS.value]
        .filter(pl.col("step_run_id") == row["step_run_id"])["execution_run_id"]
        .to_list()
    )
    reused = (
        frames[TablePath.CACHE_REUSE.value]
        .filter(pl.col("current_step_run_id") == row["step_run_id"])[
            "cached_execution_run_id"
        ]
        .to_list()
    )
    if set(direct) & set(reused) or len(reused) != len(set(reused)):
        msg = "Curator attempt has repeated or self-owned cache reuse"
        raise PersistenceIntegrityError(msg)
    actual_types, metadata = curator_result_summary(frames, set(direct + reused))
    try:
        roles = json.loads(row["output_roles_json"])
        declared_types = json.loads(row["output_types_json"])
        recorded_metadata = json.loads(row["metadata"] or "{}")
        if (
            not isinstance(roles, list)
            or any(not isinstance(role, str) for role in roles)
            or len(roles) != len(set(roles))
            or set(roles) != set(actual_types)
            or declared_types != actual_types
            or not isinstance(recorded_metadata, dict)
        ):
            msg = "terminal output map differs from accepted membership"
            raise ValueError(msg)
        recorded_metadata.pop("timings", None)
        if recorded_metadata != metadata:
            msg = "terminal metadata differs from curator execution"
            raise ValueError(msg)
    except (TypeError, ValueError) as exc:
        msg = f"Step attempt {row['step_run_id']} has invalid dynamic result: {exc}"
        raise PersistenceIntegrityError(msg) from exc
