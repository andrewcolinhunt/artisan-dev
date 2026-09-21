"""Record execution outcomes by staging artifacts and metadata to Parquet.

Public API:
    build_execution_edges: Create execution-level input/output edge DataFrame.
    error_envelope_dict: Extract the ArtisanError envelope dict for a failure.
    record_execution_success: Stage a successful execution run's outputs.
    record_passthrough: Stage an execution run that created no new artifacts.
    record_execution_failure: Stage a failed execution run's error record.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.execution.recording.commands import sanitize_diagnostic
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.storage.io.worker_seal import EXECUTION_SEAL_FILENAME
from artisan.utils.path import shard_uri, uri_join

if TYPE_CHECKING:
    from artisan.schemas.execution.execution_context import ExecutionContext

from artisan.utils.log_paths import failure_log_relative_path

logger = logging.getLogger(__name__)

_MAX_TOOL_OUTPUT_CHARS = 500_000


def execution_is_sealed(execution_context: ExecutionContext) -> bool:
    """Check publication before an executor attempts failure fallback writes."""
    directory = shard_uri(
        execution_context.staging_root,
        execution_context.execution_run_id,
        step_number=execution_context.step_number,
        operation_name=execution_context.operation_name,
    )
    return bool(
        execution_context.fs.exists(uri_join(directory, EXECUTION_SEAL_FILENAME))
    )


def error_envelope_dict(exc: BaseException) -> dict[str, Any] | None:
    """Return the ArtisanError envelope dict for a caught failure.

    Reads ``exc`` directly, or its one-level ``__cause__`` (matching
    ``ArtisanError.to_dict``'s cause depth), so a re-parented
    ``ExternalToolError`` or the tool-endpoint client's ``ArtisanError``
    wrapped in a private ``_ExecuteFailure`` still yields its structured
    code. Returns ``None`` for an unstructured failure — the caller then
    persists only the error string, and ``inspect_failures`` degrades to
    that string plus the failure-log pointer.

    Args:
        exc: The caught exception at an executor failure path.

    Returns:
        The envelope dict from ``ArtisanError.to_dict()``, or ``None``.
    """
    from artisan.errors import ArtisanError

    if isinstance(exc, ArtisanError):
        return exc.to_dict()
    if isinstance(exc.__cause__, ArtisanError):
        return exc.__cause__.to_dict()
    return None


def _read_tool_output(log_path: str | None) -> str | None:
    """Read up to the last 500K log characters, or None if absent or unreadable."""
    if log_path is None or not os.path.exists(log_path):
        return None
    try:
        with open(log_path, errors="replace") as f:
            content = f.read()
    except OSError:
        return None
    if len(content) > _MAX_TOOL_OUTPUT_CHARS:
        return "[truncated]\n" + content[-_MAX_TOOL_OUTPUT_CHARS:]
    return content


def build_execution_edges(
    execution_run_id: str,
    inputs: dict[str, list[str]],
    outputs: dict[str, list[str]],
) -> pl.DataFrame:
    """Create execution input/output edges as a DataFrame.

    Uses ``pl.lit()`` for scalar columns (execution_run_id, direction, role) to
    avoid materializing N Python string copies per column. Only the artifact_id
    list — which already exists in memory — is converted to Arrow.

    Args:
        execution_run_id: Execution run identifier.
        inputs: Mapping of role -> artifact IDs (input direction).
        outputs: Mapping of role -> artifact IDs (output direction).

    Returns:
        DataFrame with columns: execution_run_id, direction, role, artifact_id.
    """
    _schema = {
        "execution_run_id": pl.Utf8,
        "direction": pl.Utf8,
        "role": pl.Utf8,
        "artifact_id": pl.Utf8,
    }
    parts: list[pl.DataFrame] = []
    for direction, role_map in [("input", inputs), ("output", outputs)]:
        for role, ids in role_map.items():
            if not ids:
                continue
            parts.append(
                pl.DataFrame({"artifact_id": ids})
                .with_columns(
                    pl.lit(execution_run_id).alias("execution_run_id"),
                    pl.lit(direction).alias("direction"),
                    pl.lit(role).alias("role"),
                )
                .select(["execution_run_id", "direction", "role", "artifact_id"])
            )
    if not parts:
        return pl.DataFrame(schema=_schema)
    # Edges are relations; occurrence order and repetition live in replay inputs.
    return pl.concat(parts).unique(maintain_order=True)


def record_execution_success(
    execution_context: ExecutionContext,
    artifacts: dict[str, list[Artifact]],
    lineage_edges: list[ArtifactProvenanceEdge],
    inputs: dict[str, list[str]],
    timestamp_end: datetime,
    command_recording: CommandRecording,
    replay_snapshot: ReplaySnapshot,
    replay_of_execution_run_id: str | None,
    params: dict[str, Any] | None = None,
    result_metadata: dict[str, Any] | None = None,
    user_overrides: dict[str, Any] | None = None,
    tool_output: str | None = None,
) -> StagingResult:
    """Stage artifacts, lineage edges, and execution record for a successful run.

    Args:
        execution_context: Immutable context for the current execution.
        artifacts: Finalized artifacts keyed by output role.
        lineage_edges: Provenance edges linking inputs to outputs.
        inputs: Original input artifact IDs keyed by role.
        timestamp_end: Wall-clock end time of the execution.
        command_recording: Explicit framework-owned subprocess evidence.
        replay_snapshot: Immutable replay evidence captured for this execution.
        replay_of_execution_run_id: Source execution ID for a diagnostic replay,
            or None for an ordinary execution.
        params: Serialized operation parameters.
        result_metadata: Arbitrary metadata to persist with the execution record.
        user_overrides: User-provided parameter overrides before default merge.
        tool_output: Captured tool stdout/stderr.

    Returns:
        StagingResult with ``success=True`` and the staged artifact IDs.
    """
    from artisan.execution.recording.parquet_writer import (
        StagingResult,
        _create_staging_path,
        _stage_artifacts,
        _stage_execution,
    )

    fs = execution_context.fs
    staging_path = _create_staging_path(
        execution_context.staging_root,
        execution_context.execution_run_id,
        execution_context.step_number,
        execution_context.operation_name,
        fs,
    )
    artifact_ids = _stage_artifacts(
        artifacts,
        lineage_edges,
        execution_context.step_number,
        staging_path,
        fs,
    )
    output_ids = {
        role: [a.artifact_id for a in arts if a.artifact_id is not None]
        for role, arts in artifacts.items()
    }
    execution_edges = build_execution_edges(
        execution_run_id=execution_context.execution_run_id,
        inputs=inputs,
        outputs=output_ids,
    )
    _stage_execution(
        command_recording=command_recording,
        replay_snapshot=replay_snapshot,
        replay_of_execution_run_id=replay_of_execution_run_id,
        execution_run_id=execution_context.execution_run_id,
        execution_spec_id=execution_context.execution_spec_id,
        operation_name=execution_context.operation_name,
        step_number=execution_context.step_number,
        execution_edges=execution_edges,
        staging_path=staging_path,
        fs=fs,
        success=True,
        error=None,
        timestamp_start=execution_context.timestamp_start,
        timestamp_end=timestamp_end,
        worker_id=execution_context.worker_id,
        params=params,
        compute_backend=execution_context.compute_backend,
        result_metadata=result_metadata,
        user_overrides=user_overrides,
        tool_output=sanitize_diagnostic(tool_output),
        step_run_id=execution_context.step_run_id,
    )
    return StagingResult(
        success=True,
        staging_path=staging_path,
        execution_run_id=execution_context.execution_run_id,
        artifact_ids=artifact_ids,
    )


def record_passthrough(
    execution_context: ExecutionContext,
    passthrough: dict[str, list[str]],
    lineage_edges: list[ArtifactProvenanceEdge] | None,
    inputs: dict[str, list[str]],
    timestamp_end: datetime,
    command_recording: CommandRecording,
    replay_snapshot: ReplaySnapshot,
    replay_of_execution_run_id: str | None,
    params: dict[str, Any] | None = None,
    result_metadata: dict[str, Any] | None = None,
    user_overrides: dict[str, Any] | None = None,
) -> StagingResult:
    """Stage execution record and edges for a run that created no new artifacts.

    Used by curator passthrough operations, which route existing artifact IDs
    instead of producing new artifacts. Any provided ``lineage_edges`` are
    stamped with the execution's run ID before staging — operations build
    edges with a sentinel run ID because they do not know it yet, and the
    recorder finalizes it here (mirroring the artifact-result path).

    Args:
        execution_context: Immutable context for the current execution.
        passthrough: Output role -> passed-through artifact IDs.
        lineage_edges: Provenance edges to stage, or None/empty to stage none.
        inputs: Original input artifact IDs keyed by role.
        timestamp_end: Wall-clock end time of the execution.
        command_recording: Explicit framework-owned subprocess evidence.
        replay_snapshot: Immutable replay evidence captured for this execution.
        replay_of_execution_run_id: Source execution ID for a diagnostic replay,
            or None for an ordinary execution.
        params: Serialized operation parameters.
        result_metadata: Arbitrary metadata to persist with the execution record.
        user_overrides: User-provided parameter overrides before default merge.

    Returns:
        StagingResult with ``success=True`` and the passed-through artifact IDs.
    """
    from artisan.execution.recording.parquet_writer import (
        StagingResult,
        _create_staging_path,
        _stage_artifact_edges,
        _stage_execution,
    )

    fs = execution_context.fs
    staging_path = _create_staging_path(
        execution_context.staging_root,
        execution_context.execution_run_id,
        execution_context.step_number,
        execution_context.operation_name,
        fs,
    )
    if lineage_edges:
        stamped_edges = [
            edge.model_copy(
                update={"execution_run_id": execution_context.execution_run_id}
            )
            for edge in lineage_edges
        ]
        _stage_artifact_edges(stamped_edges, staging_path, fs)
    execution_edges = build_execution_edges(
        execution_run_id=execution_context.execution_run_id,
        inputs=inputs,
        outputs=passthrough,
    )
    _stage_execution(
        command_recording=command_recording,
        replay_snapshot=replay_snapshot,
        replay_of_execution_run_id=replay_of_execution_run_id,
        execution_run_id=execution_context.execution_run_id,
        execution_spec_id=execution_context.execution_spec_id,
        operation_name=execution_context.operation_name,
        step_number=execution_context.step_number,
        execution_edges=execution_edges,
        staging_path=staging_path,
        fs=fs,
        success=True,
        error=None,
        timestamp_start=execution_context.timestamp_start,
        timestamp_end=timestamp_end,
        worker_id=execution_context.worker_id,
        params=params,
        compute_backend=execution_context.compute_backend,
        result_metadata=result_metadata,
        user_overrides=user_overrides,
        step_run_id=execution_context.step_run_id,
    )
    all_passthrough_ids = [aid for ids in passthrough.values() for aid in ids]
    return StagingResult(
        success=True,
        staging_path=staging_path,
        execution_run_id=execution_context.execution_run_id,
        artifact_ids=all_passthrough_ids,
    )


def _write_failure_log(
    failure_logs_root: str | None,
    execution_run_id: str,
    timestamp_start: datetime,
    operation_name: str,
    step_number: int,
    compute_backend: str,
    error: str,
    tool_output: str | None = None,
) -> None:
    """Write a human-readable failure log file.

    No-op if failure_logs_root is None. Best-effort — never raises.

    Args:
        failure_logs_root: Root directory for failure logs.
        execution_run_id: Immutable execution attempt ID.
        timestamp_start: Timezone-aware source execution start time.
        operation_name: Name of the operation that failed.
        step_number: Pipeline step number.
        compute_backend: Resolved step-runner name.
        error: Full error string (traceback).
        tool_output: Captured tool stdout/stderr.
    """
    if failure_logs_root is None:
        return
    try:
        log_file = os.path.join(
            str(failure_logs_root),
            failure_log_relative_path(execution_run_id, timestamp_start),
        )
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

        sections = [
            "=== Execution Failure Log ===",
            f"Run ID:    {execution_run_id}",
            f"Operation: {operation_name}",
            f"Step:      {step_number}",
            f"Backend:   {compute_backend}",
            f"Time:      {datetime.now(UTC).isoformat()}",
            "",
            "=== Error ===",
            error,
        ]

        if tool_output:
            tail = "\n".join(tool_output.splitlines()[-100:])
            sections.extend(["", "=== Tool Output (last 100 lines) ===", tail])

        with open(log_file, "x") as f:
            f.write("\n".join(sections))
    except FileExistsError:
        return
    except Exception:
        logger.debug(
            "Failed to write failure log for %s", execution_run_id, exc_info=True
        )


def record_execution_failure(
    execution_context: ExecutionContext,
    error: str,
    inputs: dict[str, list[str]],
    timestamp_end: datetime,
    command_recording: CommandRecording,
    replay_snapshot: ReplaySnapshot,
    replay_of_execution_run_id: str | None,
    params: dict[str, Any] | None = None,
    user_overrides: dict[str, Any] | None = None,
    tool_output: str | None = None,
    failure_logs_root: str | None = None,
    error_envelope: dict[str, Any] | None = None,
) -> StagingResult:
    """Stage an execution record for a failed run and write a failure log.

    Double-faults during unsealed staging are folded into the returned result.
    An existing seal cannot be rewritten and raises before staging begins.

    Args:
        execution_context: Immutable context for the current execution.
        error: Formatted error string (typically a traceback).
        inputs: Original input artifact IDs keyed by role.
        timestamp_end: Wall-clock end time of the execution.
        command_recording: Explicit framework-owned subprocess evidence.
        replay_snapshot: Immutable replay evidence captured for this execution.
        replay_of_execution_run_id: Source execution ID for a diagnostic replay,
            or None for an ordinary execution.
        params: Serialized operation parameters.
        user_overrides: User-provided parameter overrides before default merge.
        tool_output: Captured tool stdout/stderr.
        failure_logs_root: Directory for human-readable failure logs.
        error_envelope: Structured ``ArtisanError`` envelope dict for the
            failure (from ``error_envelope_dict``), or None for an
            unstructured failure. Persisted as JSON in the
            ``error_envelope`` column.

    Returns:
        StagingResult with ``success=False``.
    """
    from artisan.execution.recording.parquet_writer import (
        StagingResult,
        _create_staging_path,
        _stage_execution,
    )

    if execution_is_sealed(execution_context):
        msg = f"Execution {execution_context.execution_run_id} is already sealed"
        raise StoreIntegrityError(msg)
    try:
        fs = execution_context.fs
        staging_path = _create_staging_path(
            execution_context.staging_root,
            execution_context.execution_run_id,
            execution_context.step_number,
            execution_context.operation_name,
            fs,
        )
        execution_edges = build_execution_edges(
            execution_run_id=execution_context.execution_run_id,
            inputs=inputs,
            outputs={},
        )
        _stage_execution(
            command_recording=command_recording,
            replay_snapshot=replay_snapshot,
            replay_of_execution_run_id=replay_of_execution_run_id,
            execution_run_id=execution_context.execution_run_id,
            execution_spec_id=execution_context.execution_spec_id,
            operation_name=execution_context.operation_name,
            step_number=execution_context.step_number,
            execution_edges=execution_edges,
            staging_path=staging_path,
            fs=fs,
            success=False,
            error=sanitize_diagnostic(error),
            timestamp_start=execution_context.timestamp_start,
            timestamp_end=timestamp_end,
            worker_id=execution_context.worker_id,
            params=params,
            compute_backend=execution_context.compute_backend,
            user_overrides=user_overrides,
            tool_output=sanitize_diagnostic(tool_output),
            step_run_id=execution_context.step_run_id,
            error_envelope=sanitize_diagnostic(error_envelope),
        )
        _write_failure_log(
            failure_logs_root=failure_logs_root,
            execution_run_id=execution_context.execution_run_id,
            timestamp_start=execution_context.timestamp_start,
            operation_name=execution_context.operation_name,
            step_number=execution_context.step_number,
            compute_backend=execution_context.compute_backend,
            error=sanitize_diagnostic(error),
            tool_output=sanitize_diagnostic(tool_output),
        )
        return StagingResult(
            success=False,
            error=sanitize_diagnostic(error),
            staging_path=staging_path,
            execution_run_id=execution_context.execution_run_id,
            artifact_ids=[],
        )
    except Exception as staging_exc:
        combined = (
            f"{error} | Additionally, staging the failure record failed: "
            f"{type(staging_exc).__name__}: {staging_exc}"
        )
        combined = sanitize_diagnostic(combined)
        logger.error("Double-fault in record_execution_failure: %s", combined)
        return StagingResult(
            success=False,
            error=combined,
            execution_run_id=execution_context.execution_run_id,
            artifact_ids=[],
        )
