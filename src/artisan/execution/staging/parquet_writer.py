"""Write execution outputs and provenance to staged Parquet files.

Each execution run stages its artifacts, execution record, and edges
into a sharded directory of Parquet files.  The commit layer later
merges these into the Delta Lake tables.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.storage.core.table_schemas import ARTIFACT_EDGES_SCHEMA
from artisan.utils.json import artisan_json_default
from artisan.utils.path import shard_path


def _sync_staging_to_nfs(staging_path: Path) -> None:
    """Flush staged files and directory metadata to NFS."""
    for path in staging_path.iterdir():
        if path.is_file():
            fd = os.open(path, os.O_RDONLY)
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
    staging_root: Path,
    execution_run_id: str,
    step_number: int,
    operation_name: str | None = None,
) -> Path:
    """Create and return the sharded staging directory for one execution run."""
    staging_path = shard_path(
        staging_root,
        execution_run_id,
        step_number=step_number,
        operation_name=operation_name,
    )
    staging_path.mkdir(parents=True, exist_ok=True)
    return staging_path


@dataclass
class StagingResult:
    """Outcome of staging an execution run's outputs to disk.

    Attributes:
        success: Whether the execution completed without error.
        error: Error message when ``success`` is False.
        staging_path: Directory containing the staged Parquet files.
        execution_run_id: Unique identifier for this execution run.
        artifact_ids: IDs of artifacts produced (empty on failure).
    """

    success: bool
    error: str | None = None
    staging_path: Path | None = None
    execution_run_id: str | None = None
    artifact_ids: list[str] = field(default_factory=list)


def _stage_artifacts(
    artifacts: dict[str, list[Artifact]],
    artifact_edges: list[ArtifactProvenanceEdge],
    step_number: int,
    staging_path: Path,
) -> list[str]:
    """Stage artifact data, index, and edges to Parquet files.

    Returns:
        Flat list of all staged artifact IDs.
    """
    _stage_artifacts_by_type(artifacts, staging_path)
    _stage_artifact_index(artifacts, step_number, staging_path)
    _stage_artifact_edges(artifact_edges, staging_path)

    return [
        artifact.artifact_id
        for artifact_list in artifacts.values()
        for artifact in artifact_list
    ]


def _stage_execution(
    execution_run_id: str,
    execution_spec_id: str,
    operation_name: str,
    step_number: int,
    execution_edges: pl.DataFrame,
    staging_path: Path,
    success: bool,
    error: str | None,
    timestamp_start: datetime,
    timestamp_end: datetime,
    worker_id: int,
    params: dict[str, Any] | None,
    compute_backend: str,
    shared_filesystem: bool = False,
    result_metadata: dict[str, Any] | None = None,
    user_overrides: dict[str, Any] | None = None,
    tool_output: str | None = None,
    worker_log: str | None = None,
    step_run_id: str | None = None,
) -> None:
    """Stage execution record and edges, optionally flushing to NFS."""
    _stage_execution_edges(execution_edges, staging_path)
    _write_execution_record(
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
        params=params,
        compute_backend=compute_backend,
        result_metadata=result_metadata,
        user_overrides=user_overrides,
        tool_output=tool_output,
        worker_log=worker_log,
        step_run_id=step_run_id,
    )
    if shared_filesystem:
        _sync_staging_to_nfs(staging_path)


def _stage_artifacts_by_type(
    artifacts: dict[str, list[Artifact]], staging_path: Path
) -> None:
    """Stage artifacts grouped by type using model-owned serialization."""
    by_type: dict[str, list[Artifact]] = {}
    for artifact_list in artifacts.values():
        for artifact in artifact_list:
            by_type.setdefault(artifact.artifact_type, []).append(artifact)

    for type_key, typed_artifacts in by_type.items():
        type_def = ArtifactTypeDef.get(type_key)
        rows = [a.to_row() for a in typed_artifacts]
        pl.DataFrame(rows, schema=type_def.polars_schema()).write_parquet(
            staging_path / type_def.parquet_filename(), compression="zstd"
        )


def _stage_artifact_index(
    artifacts: dict[str, list[Artifact]],
    step_number: int,
    staging_path: Path,
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
        pl.DataFrame(rows).write_parquet(
            staging_path / "index.parquet",
            compression="zstd",
        )


def _stage_artifact_edges(
    artifact_edges: list[ArtifactProvenanceEdge],
    staging_path: Path,
) -> None:
    """Write artifact provenance edges to Parquet. No-op when empty."""
    if not artifact_edges:
        return
    pl.DataFrame(
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
    ).write_parquet(staging_path / "artifact_edges.parquet", compression="zstd")


def _stage_execution_edges(
    execution_edges: pl.DataFrame,
    staging_path: Path,
) -> None:
    """Write execution input/output edges to Parquet. No-op when empty."""
    if execution_edges.is_empty():
        return
    execution_edges.write_parquet(
        staging_path / "execution_edges.parquet",
        compression="zstd",
    )


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
    staging_path: Path,
    params: dict[str, Any] | None = None,
    compute_backend: str = "local",
    result_metadata: dict[str, Any] | None = None,
    user_overrides: dict[str, Any] | None = None,
    tool_output: str | None = None,
    worker_log: str | None = None,
    step_run_id: str | None = None,
) -> None:
    """Serialize one execution record row to ``executions.parquet``."""
    row = {
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
        "tool_output": tool_output,
        "worker_log": worker_log,
        "compute_backend": compute_backend,
        "metadata": json.dumps(result_metadata or {}, default=artisan_json_default),
    }
    pl.DataFrame([row]).cast(
        {
            "error": pl.String,
            "tool_output": pl.String,
            "worker_log": pl.String,
            "step_run_id": pl.String,
        }
    ).write_parquet(
        staging_path / "executions.parquet",
        compression="zstd",
    )
