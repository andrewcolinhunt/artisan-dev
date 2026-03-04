"""Record execution outcomes by staging artifacts and metadata to Parquet.

Public API:
    build_execution_edges: Create execution-level input/output edge DataFrame.
    record_execution_success: Stage a successful execution run's outputs.
    record_execution_failure: Stage a failed execution run's error record.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl

from artisan.execution.staging.parquet_writer import StagingResult
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge

if TYPE_CHECKING:
    from artisan.schemas.execution.execution_context import ExecutionContext

logger = logging.getLogger(__name__)

_MAX_TOOL_OUTPUT_CHARS = 500_000


def _read_tool_output(log_path: Path | None) -> str | None:
    """Read tool output from a log file.

    Args:
        log_path: Path to the tool output log file.

    Returns:
        File content as string, or None if path is None or file doesn't exist.
        Truncated to last 500K chars if longer.
    """
    if log_path is None or not log_path.exists():
        return None
    try:
        content = log_path.read_text(errors="replace")
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
    return pl.concat(parts)


def record_execution_success(
    execution_context: ExecutionContext,
    artifacts: dict[str, list[Artifact]],
    lineage_edges: list[ArtifactProvenanceEdge],
    inputs: dict[str, list[str]],
    timestamp_end: datetime,
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
        params: Serialized operation parameters.
        result_metadata: Arbitrary metadata to persist with the execution record.
        user_overrides: User-provided parameter overrides before default merge.
        tool_output: Captured tool stdout/stderr.

    Returns:
        StagingResult with ``success=True`` and the staged artifact IDs.
    """
    from artisan.execution.staging.parquet_writer import (
        StagingResult,
        _create_staging_path,
        _stage_artifacts,
        _stage_execution,
    )

    staging_path = _create_staging_path(
        execution_context.staging_root,
        execution_context.execution_run_id,
        execution_context.step_number,
        operation_name=execution_context.operation_name,
    )
    artifact_ids = _stage_artifacts(
        artifacts,
        lineage_edges,
        execution_context.step_number,
        staging_path,
    )
    output_ids = {
        role: [a.artifact_id for a in arts] for role, arts in artifacts.items()
    }
    execution_edges = build_execution_edges(
        execution_run_id=execution_context.execution_run_id,
        inputs=inputs,
        outputs=output_ids,
    )
    _stage_execution(
        execution_run_id=execution_context.execution_run_id,
        execution_spec_id=execution_context.execution_spec_id,
        operation_name=execution_context.operation_name,
        step_number=execution_context.step_number,
        execution_edges=execution_edges,
        staging_path=staging_path,
        success=True,
        error=None,
        timestamp_start=execution_context.timestamp_start,
        timestamp_end=timestamp_end,
        worker_id=execution_context.worker_id,
        params=params,
        compute_backend=execution_context.compute_backend,
        shared_filesystem=execution_context.shared_filesystem,
        result_metadata=result_metadata,
        user_overrides=user_overrides,
        tool_output=tool_output,
    )
    return StagingResult(
        success=True,
        staging_path=staging_path,
        execution_run_id=execution_context.execution_run_id,
        artifact_ids=artifact_ids,
    )


def _write_failure_log(
    failure_logs_root: Path | None,
    execution_run_id: str,
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
        execution_run_id: Execution run ID (used as filename).
        operation_name: Name of the operation that failed.
        step_number: Pipeline step number.
        compute_backend: Compute backend used (local/slurm).
        error: Full error string (traceback).
        tool_output: Captured tool stdout/stderr.
    """
    if failure_logs_root is None:
        return
    try:
        log_dir = failure_logs_root / f"step_{step_number}_{operation_name}"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"{execution_run_id}.log"

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

        log_path.write_text("\n".join(sections))
    except Exception:
        logger.debug(
            "Failed to write failure log for %s", execution_run_id, exc_info=True
        )


def record_execution_failure(
    execution_context: ExecutionContext,
    error: str,
    inputs: dict[str, list[str]],
    timestamp_end: datetime,
    params: dict[str, Any] | None = None,
    user_overrides: dict[str, Any] | None = None,
    tool_output: str | None = None,
    failure_logs_root: Path | None = None,
) -> StagingResult:
    """Stage an execution record for a failed run and write a failure log.

    Double-faults (errors during staging itself) are caught and folded
    into the returned StagingResult so the caller always gets a value.

    Args:
        execution_context: Immutable context for the current execution.
        error: Formatted error string (typically a traceback).
        inputs: Original input artifact IDs keyed by role.
        timestamp_end: Wall-clock end time of the execution.
        params: Serialized operation parameters.
        user_overrides: User-provided parameter overrides before default merge.
        tool_output: Captured tool stdout/stderr.
        failure_logs_root: Directory for human-readable failure logs.

    Returns:
        StagingResult with ``success=False``.
    """
    from artisan.execution.staging.parquet_writer import (
        StagingResult,
        _create_staging_path,
        _stage_execution,
    )

    try:
        staging_path = _create_staging_path(
            execution_context.staging_root,
            execution_context.execution_run_id,
            execution_context.step_number,
            operation_name=execution_context.operation_name,
        )
        execution_edges = build_execution_edges(
            execution_run_id=execution_context.execution_run_id,
            inputs=inputs,
            outputs={},
        )
        _stage_execution(
            execution_run_id=execution_context.execution_run_id,
            execution_spec_id=execution_context.execution_spec_id,
            operation_name=execution_context.operation_name,
            step_number=execution_context.step_number,
            execution_edges=execution_edges,
            staging_path=staging_path,
            success=False,
            error=error,
            timestamp_start=execution_context.timestamp_start,
            timestamp_end=timestamp_end,
            worker_id=execution_context.worker_id,
            params=params,
            compute_backend=execution_context.compute_backend,
            shared_filesystem=execution_context.shared_filesystem,
            user_overrides=user_overrides,
            tool_output=tool_output,
        )
        _write_failure_log(
            failure_logs_root=failure_logs_root,
            execution_run_id=execution_context.execution_run_id,
            operation_name=execution_context.operation_name,
            step_number=execution_context.step_number,
            compute_backend=execution_context.compute_backend,
            error=error,
            tool_output=tool_output,
        )
        return StagingResult(
            success=False,
            error=error,
            staging_path=staging_path,
            execution_run_id=execution_context.execution_run_id,
            artifact_ids=[],
        )
    except Exception as staging_exc:
        combined = (
            f"{error} | Additionally, staging the failure record failed: "
            f"{type(staging_exc).__name__}: {staging_exc}"
        )
        logger.error("Double-fault in record_execution_failure: %s", combined)
        return StagingResult(
            success=False,
            error=combined,
            execution_run_id=execution_context.execution_run_id,
            artifact_ids=[],
        )
