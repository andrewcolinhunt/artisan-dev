"""Write execution outputs and provenance to staged Parquet files.

Each execution run stages its artifacts, execution record, and edges
into a sharded directory of Parquet files.  The commit layer later
merges these into the Delta Lake tables.
"""

from __future__ import annotations

import io
import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import polars as pl
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.external import validate_persistable_uri
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.orchestration.step_lifecycle import CancellationAcknowledgement
from artisan.storage.core.table_schemas import ARTIFACT_EDGES_SCHEMA, get_schema
from artisan.storage.io.publication import publish_immutable_bytes
from artisan.storage.io.worker_seal import (
    STAGING_INVENTORY_KEY,
    build_staging_inventory,
    ensure_unsealed,
)
from artisan.utils.json import artisan_json_default
from artisan.utils.path import shard_uri


def _sync_local_staging(staging_path: str) -> None:
    """Make local payloads durable before publishing their execution seal."""
    for entry in os.listdir(staging_path):
        entry_path = os.path.join(staging_path, entry)
        if os.path.isfile(entry_path):
            fd = os.open(entry_path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    fd = os.open(staging_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _create_staging_path(
    staging_root: str,
    execution_run_id: str,
    step_number: int,
    operation_name: str | None,
    fs: AbstractFileSystem,
) -> str:
    """Create and return the sharded staging directory for one execution run."""
    staging_path = shard_uri(
        staging_root,
        execution_run_id,
        step_number=step_number,
        operation_name=operation_name,
    )
    ensure_unsealed(staging_path, fs)
    fs.makedirs(staging_path, exist_ok=True)
    return staging_path


@dataclass
class StagingResult:
    """Describe an execution outcome and its locally or remotely staged outputs.

    Attributes:
        success: Whether the execution completed without error.
        error: Error message when ``success`` is False.
        staging_path: Directory containing the staged Parquet files.
        execution_run_id: Unique identifier for this execution run.
        artifact_ids: New or passed-through output IDs (empty on failure).
        cancellation_acknowledgement: Evidence of a worker cancellation request,
            if one was made.
    """

    success: bool
    error: str | None = None
    staging_path: str | None = None
    execution_run_id: str | None = None
    artifact_ids: list[str] = field(default_factory=list)
    cancellation_acknowledgement: CancellationAcknowledgement | None = None


def _stage_artifacts(
    artifacts: dict[str, list[Artifact]],
    artifact_edges: list[ArtifactProvenanceEdge],
    step_number: int,
    staging_path: str,
    fs: AbstractFileSystem,
) -> list[str]:
    """Stage artifact data, index, and edges to Parquet files.

    Returns:
        Flat list of all staged artifact IDs.
    """
    _stage_artifacts_by_type(artifacts, staging_path, fs)
    _stage_artifact_index(artifacts, step_number, staging_path, fs)
    _stage_artifact_locations(artifacts, staging_path, fs)
    _stage_artifact_edges(artifact_edges, staging_path, fs)

    return [
        artifact.artifact_id
        for artifact_list in artifacts.values()
        for artifact in artifact_list
        if artifact.artifact_id is not None
    ]


def _stage_execution(
    execution_run_id: str,
    execution_spec_id: str,
    operation_name: str,
    step_number: int,
    execution_edges: pl.DataFrame,
    staging_path: str,
    fs: AbstractFileSystem,
    success: bool,
    error: str | None,
    timestamp_start: datetime,
    timestamp_end: datetime,
    worker_id: int,
    params: dict[str, Any] | None,
    compute_backend: str,
    command_recording: CommandRecording,
    replay_snapshot: ReplaySnapshot,
    replay_of_execution_run_id: str | None,
    result_metadata: dict[str, Any] | None = None,
    user_overrides: dict[str, Any] | None = None,
    tool_output: str | None = None,
    worker_log: str | None = None,
    step_run_id: str | None = None,
    error_envelope: dict[str, Any] | None = None,
) -> None:
    """Stage edges, flush local payloads, and publish the execution seal last."""
    ensure_unsealed(staging_path, fs)
    _stage_execution_edges(execution_edges, staging_path, fs)
    _write_execution_record(
        command_recording=command_recording,
        replay_snapshot=replay_snapshot,
        replay_of_execution_run_id=replay_of_execution_run_id,
        execution_run_id=execution_run_id,
        execution_spec_id=execution_spec_id,
        operation_name=operation_name,
        step_number=step_number,
        success=success,
        error=error,
        timestamp_start=timestamp_start,
        timestamp_end=timestamp_end,
        worker_id=worker_id,
        staging_path=staging_path,
        fs=fs,
        params=params,
        compute_backend=compute_backend,
        result_metadata=result_metadata,
        user_overrides=user_overrides,
        tool_output=tool_output,
        worker_log=worker_log,
        step_run_id=step_run_id,
        error_envelope=error_envelope,
    )


def _stage_artifacts_by_type(
    artifacts: dict[str, list[Artifact]], staging_path: str, fs: AbstractFileSystem
) -> None:
    """Stage artifacts grouped by type using model-owned serialization."""
    by_type: dict[str, list[Artifact]] = {}
    for artifact_list in artifacts.values():
        for artifact in artifact_list:
            by_type.setdefault(artifact.artifact_type, []).append(artifact)

    for type_key, typed_artifacts in by_type.items():
        type_def = ArtifactTypeDef.get(type_key)
        rows = [a.to_row() for a in typed_artifacts]
        df = pl.DataFrame(rows, schema=type_def.polars_schema())
        with fs.open(f"{staging_path}/{type_def.parquet_filename()}", "wb") as f:
            df.write_parquet(f, compression="zstd")


def _stage_artifact_index(
    artifacts: dict[str, list[Artifact]],
    step_number: int,
    staging_path: str,
    fs: AbstractFileSystem,
) -> None:
    """Write an index Parquet listing every artifact with its type and metadata."""
    rows = [
        {
            "artifact_id": artifact.artifact_id,
            "artifact_type": artifact.artifact_type,
            "origin_step_number": step_number,
            "metadata": json.dumps(artifact.metadata, default=artisan_json_default),
        }
        for artifact_list in artifacts.values()
        for artifact in artifact_list
    ]
    if rows:
        with fs.open(f"{staging_path}/index.parquet", "wb") as f:
            pl.DataFrame(
                rows, schema=get_schema(TablePath.ARTIFACT_INDEX)
            ).write_parquet(f, compression="zstd")


def _stage_artifact_locations(
    artifacts: dict[str, list[Artifact]],
    staging_path: str,
    fs: AbstractFileSystem,
) -> None:
    """Write global location rows for externally backed artifacts."""
    rows: list[dict[str, str]] = []
    for artifact_list in artifacts.values():
        for artifact in artifact_list:
            if not artifact.EXTERNALLY_BACKED or artifact.artifact_id is None:
                continue
            locator = next(iter(artifact.LOCATOR_FIELDS))
            uri = getattr(artifact, locator)
            if uri is None:
                continue
            validate_persistable_uri(uri)
            rows.append({"artifact_id": artifact.artifact_id, "uri": uri})
    if rows:
        df = pl.DataFrame(
            rows,
            schema=get_schema(TablePath.ARTIFACT_LOCATIONS),
        ).unique(maintain_order=True)
        with fs.open(f"{staging_path}/locations.parquet", "wb") as stream:
            df.write_parquet(stream, compression="zstd")


def _stage_artifact_edges(
    artifact_edges: list[ArtifactProvenanceEdge],
    staging_path: str,
    fs: AbstractFileSystem,
) -> None:
    """Write artifact provenance edges to Parquet. No-op when empty."""
    if not artifact_edges:
        return
    df = pl.DataFrame(
        [
            {
                "execution_run_id": edge.execution_run_id,
                "source_artifact_id": edge.source_artifact_id,
                "target_artifact_id": edge.target_artifact_id,
                "source_artifact_type": edge.source_artifact_type,
                "target_artifact_type": edge.target_artifact_type,
                "source_role": edge.source_role,
                "target_role": edge.target_role,
                "group_id": edge.group_id,
                "step_boundary": edge.step_boundary,
            }
            for edge in artifact_edges
        ],
        schema=ARTIFACT_EDGES_SCHEMA,
    ).unique(maintain_order=True)
    with fs.open(f"{staging_path}/artifact_edges.parquet", "wb") as f:
        df.write_parquet(f, compression="zstd")


def _stage_execution_edges(
    execution_edges: pl.DataFrame,
    staging_path: str,
    fs: AbstractFileSystem,
) -> None:
    """Write execution input/output edges to Parquet. No-op when empty."""
    if execution_edges.is_empty():
        return
    with fs.open(f"{staging_path}/execution_edges.parquet", "wb") as f:
        execution_edges.cast(
            pl.Schema(get_schema(TablePath.EXECUTION_EDGES))
        ).write_parquet(f, compression="zstd")


def _write_execution_record(
    execution_run_id: str,
    execution_spec_id: str,
    operation_name: str,
    step_number: int,
    success: bool,
    error: str | None,
    timestamp_start: datetime,
    timestamp_end: datetime,
    worker_id: int,
    staging_path: str,
    fs: AbstractFileSystem,
    command_recording: CommandRecording,
    replay_snapshot: ReplaySnapshot,
    replay_of_execution_run_id: str | None,
    params: dict[str, Any] | None = None,
    compute_backend: str = "local",
    result_metadata: dict[str, Any] | None = None,
    user_overrides: dict[str, Any] | None = None,
    tool_output: str | None = None,
    worker_log: str | None = None,
    step_run_id: str | None = None,
    error_envelope: dict[str, Any] | None = None,
) -> None:
    """Serialize one execution record row to ``executions.parquet``."""
    from artisan.execution.recording.commands import sanitize_diagnostic

    if replay_of_execution_run_id is not None:
        params = sanitize_diagnostic(params)
        user_overrides = sanitize_diagnostic(user_overrides)
        result_metadata = sanitize_diagnostic(result_metadata)
        error = sanitize_diagnostic(error)
        error_envelope = sanitize_diagnostic(error_envelope)
    row = {
        "replay_snapshot": ReplaySnapshot.model_validate_json(
            replay_snapshot.model_dump_json()
        ).model_dump_json(),
        "replay_of_execution_run_id": replay_of_execution_run_id,
        "execution_run_id": execution_run_id,
        "execution_spec_id": execution_spec_id,
        "step_run_id": step_run_id,
        "origin_step_number": step_number,
        "operation_name": operation_name,
        "params": json.dumps(params or {}, default=artisan_json_default),
        "user_overrides": json.dumps(
            user_overrides or {}, default=artisan_json_default
        ),
        "timestamp_start": timestamp_start,
        "timestamp_end": timestamp_end,
        "source_worker": worker_id,
        "success": success,
        "error": error,
        "command_recording": CommandRecording.model_validate(
            command_recording.model_dump()
        ).model_dump_json(),
        "error_envelope": (
            json.dumps(error_envelope, default=artisan_json_default)
            if error_envelope is not None
            else None
        ),
        "tool_output": tool_output,
        "worker_log": worker_log,
        "compute_backend": compute_backend,
        "metadata": json.dumps(result_metadata or {}, default=artisan_json_default),
    }
    df = pl.DataFrame([row], schema=get_schema(TablePath.EXECUTIONS))
    inventory = build_staging_inventory(staging_path, fs)
    if isinstance(fs, LocalFileSystem):
        _sync_local_staging(str(fs._strip_protocol(staging_path)))
    encoded = io.BytesIO()
    df.write_parquet(
        encoded,
        compression="zstd",
        metadata={STAGING_INVENTORY_KEY: inventory.decode("utf-8")},
    )
    publish_immutable_bytes(
        fs, f"{staging_path}/executions.parquet", encoded.getvalue()
    )
