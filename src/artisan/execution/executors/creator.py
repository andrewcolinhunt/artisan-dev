"""Execute creator operations through the preprocess/execute/postprocess lifecycle."""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from artisan.execution.compute.base import ExecuteRouter
from artisan.execution.compute.routing import create_execute_router
from artisan.execution.context.builder import build_creator_execution_context
from artisan.execution.models.artifact_source import ArtifactSource
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.staging.parquet_writer import StagingResult
from artisan.execution.staging.recorder import (
    _read_tool_output,
    record_execution_failure,
    record_execution_success,
)
from artisan.execution.utils import generate_execution_run_id
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.utils.errors import format_error
from artisan.utils.path import cancel_sentinel_path
from artisan.utils.timing import phase_timer

logger = logging.getLogger(__name__)


class _PostprocessFailure(Exception):
    """Raised when postprocess returns success=False with a clean error message."""


class _UploadFailure(_PostprocessFailure):
    """Raised when fs.put / shutil.move fails inside _upload_files_to_root.

    Subclass of _PostprocessFailure so the existing
    ``except (_PostprocessFailure, _ExecuteFailure)`` clause catches it
    and records the failure through ``record_execution_failure``.
    """


class _ExecuteFailure(Exception):
    """Raised when the execute phase throws, carrying the formatted error and tool output."""

    def __init__(self, error: str, tool_output: str | None = None) -> None:
        super().__init__(error)
        self.tool_output = tool_output


@dataclass
class LifecycleResult:
    """Return type of run_creator_lifecycle().

    Attributes:
        input_artifacts: Hydrated input artifacts keyed by role.
        artifacts: Finalized output artifacts keyed by role.
        edges: Provenance edges linking inputs to outputs.
        timings: Phase name to elapsed seconds.
        tool_output: Captured unit log (tool stdout/stderr), read before
            sandbox cleanup. None when no log was written (function ops).
    """

    input_artifacts: dict[str, list[Artifact]]
    artifacts: dict[str, list[Artifact]]
    edges: list[ArtifactProvenanceEdge]
    timings: dict[str, float] = field(default_factory=dict)
    tool_output: str | None = None


def run_creator_lifecycle(
    unit: ExecutionUnit,
    runtime_env: RuntimeEnvironment,
    worker_id: int = 0,
    execution_run_id: str | None = None,
    sources: dict[str, ArtifactSource] | None = None,
    execute_router: ExecuteRouter | None = None,
) -> LifecycleResult:
    """Run one operation through setup → preprocess → execute → postprocess → lineage.

    This is the inner lifecycle extracted from run_creator_flow(). It raises
    on any failure — the caller is responsible for error recording and sandbox
    cleanup.

    Internally delegates to ``prep_unit()`` (setup + preprocess) and
    ``post_unit()`` (postprocess + lineage). Prep splits per artifact
    (honoring the op's ``per_artifact_dispatch``); the execute router
    receives the full per-artifact list and owns the iteration strategy.

    Args:
        unit: Execution unit specifying the operation and its inputs.
        runtime_env: Paths and step_runner configuration for this run.
        worker_id: Numeric worker identifier for concurrency tracking.
        execution_run_id: Pre-generated run ID. Generated if None.
        sources: Optional pre-resolved artifact sources keyed by role.
            When provided, hydrate from sources instead of unit.inputs.
            Used by the composite executor for in-memory artifact passing.
        execute_router: Optional pre-created router for execute-phase dispatch.
            When None, created from the operation's compute_provider config.

    Returns:
        LifecycleResult with artifacts, edges, and timings.

    Raises:
        ValueError: If working_root is not set.
        Exception: Any failure from preprocess/execute/postprocess/lineage.
    """
    from artisan.execution.executors.creator_phases import post_unit, prep_unit

    prepped = prep_unit(unit, runtime_env, worker_id, execution_run_id, sources)

    # --- execute phase ---
    with phase_timer("execute", prepped.timings):
        if execute_router is None:
            config = prepped.operation.compute_provider.current()
            execute_router = create_execute_router(
                config,
                prepped.operation,
                cancel_check=_cancel_check(runtime_env, unit.step_run_id),
            )
        try:
            raw_results = execute_router.route_execute(
                prepped.operation,
                prepped.artifact_execute_inputs,
                prepped.sandbox_path,
            )
        except Exception as exc:
            error = format_error(exc)
            if hasattr(exc, "stdout") and exc.stdout:
                tail = "\n".join(exc.stdout.splitlines()[-30:])
                error += f"\n--- tool stdout (last 30 lines) ---\n{tail}"
            tool_output = _read_tool_output(prepped.log_path)
            raise _ExecuteFailure(error, tool_output=tool_output) from exc
        # Per-artifact failures land as exception entries (the batch
        # contract); surface them here — downstream _reassemble_results
        # silently filters them, which masks the real error as an
        # empty-artifact validation failure.
        failures = [r for r in raw_results if isinstance(r, Exception)]
        if failures:
            msg = (
                f"{len(failures)}/{len(raw_results)} artifact executions "
                f"failed; first: {format_error(failures[0])}"
            )
            raise _ExecuteFailure(msg, tool_output=_read_tool_output(prepped.log_path))

    return post_unit(prepped, raw_results, runtime_env)


def _cancel_check(
    runtime_env: RuntimeEnvironment, step_run_id: str | None
) -> Callable[[], bool] | None:
    """Existence probe for the orchestrator's cancel sentinel.

    The lifecycle router writes the sentinel on the staging filesystem
    when the pipeline cancel event fires; execute routers whose calls
    outlive the orchestrator's threads poll this probe. None when the
    unit carries no ``step_run_id`` (composite-internal lifecycles) or
    no staging root is configured.
    """
    if step_run_id is None or runtime_env.staging_root is None:
        return None
    sentinel = cancel_sentinel_path(runtime_env.staging_root, step_run_id)
    fs = runtime_env.storage.filesystem()
    return lambda: bool(fs.exists(sentinel))


def run_creator_flow(
    unit: ExecutionUnit,
    runtime_env: RuntimeEnvironment,
    worker_id: int = 0,
    execute_router: ExecuteRouter | None = None,
) -> StagingResult:
    """Execute a creator operation through ordered execution phases.

    Phases: setup, preprocess, execute, postprocess, lineage, record.
    Failures at any phase are caught and staged as error records.

    Args:
        unit: Execution unit specifying the operation and its inputs.
        runtime_env: Paths and step_runner configuration for this run.
        worker_id: Numeric worker identifier for concurrency tracking.
        execute_router: Shared router for compute_provider dispatch. When provided,
            the lifecycle skips creating its own router. When ``None``,
            each invocation creates a router from the operation's config.

    Returns:
        StagingResult indicating success or failure with staged paths.
    """
    timings: dict[str, Any] = {}
    timestamp_start = datetime.now(UTC)
    operation = unit.operation
    original_inputs = _extract_inputs(unit)
    user_overrides = unit.user_overrides

    execution_run_id = generate_execution_run_id(
        unit.execution_spec_id,
        timestamp_start,
        worker_id,
    )

    total_start = time.perf_counter()
    execution_context = None
    params_dict = None
    try:
        # Run the lifecycle (setup → lineage)
        lifecycle_result = run_creator_lifecycle(
            unit,
            runtime_env,
            worker_id,
            execution_run_id,
            execute_router=execute_router,
        )
        timings.update(lifecycle_result.timings)

        # Build execution context for the record phase
        execution_context = _build_execution_context(
            execution_run_id,
            unit,
            timestamp_start,
            worker_id,
            runtime_env,
            operation,
        )
        params_dict = _get_params_dict(operation)

        # --- record phase ---
        with phase_timer("record", timings):
            staging_result = record_execution_success(
                execution_context=execution_context,
                artifacts=lifecycle_result.artifacts,
                lineage_edges=lifecycle_result.edges,
                inputs=original_inputs,
                timestamp_end=datetime.now(UTC),
                params=params_dict,
                result_metadata={"timings": timings},
                user_overrides=user_overrides,
                tool_output=lifecycle_result.tool_output,
            )
    except (_PostprocessFailure, _ExecuteFailure) as exc:
        # Lifecycle failures with clean error messages
        if isinstance(exc, _ExecuteFailure):
            error = str(exc)
            tool_output = exc.tool_output
        else:
            error = str(exc)
            tool_output = None
        execution_context = _build_execution_context(
            execution_run_id,
            unit,
            timestamp_start,
            worker_id,
            runtime_env,
            operation,
        )
        params_dict = _get_params_dict(operation)
        staging_result = record_execution_failure(
            execution_context=execution_context,
            error=error,
            inputs=original_inputs,
            timestamp_end=datetime.now(UTC),
            params=params_dict,
            user_overrides=user_overrides,
            tool_output=tool_output,
            failure_logs_root=runtime_env.failure_logs_root,
        )
    except Exception as exc:
        error = format_error(exc)
        execution_context = _try_build_execution_context(
            execution_run_id,
            unit,
            timestamp_start,
            worker_id,
            runtime_env,
            operation,
        )
        if execution_context is None:
            logger.error("Creator setup failed: %s", error)
            staging_result = StagingResult(
                success=False,
                error=error,
                execution_run_id=execution_run_id,
                artifact_ids=[],
            )
        else:
            params_dict = _get_params_dict(operation)
            staging_result = record_execution_failure(
                execution_context=execution_context,
                error=error,
                inputs=original_inputs,
                timestamp_end=datetime.now(UTC),
                params=params_dict,
                user_overrides=user_overrides,
                failure_logs_root=runtime_env.failure_logs_root,
            )

    timings["total"] = round(time.perf_counter() - total_start, 4)
    logger.debug("Execution %s timings: %s", execution_run_id, timings)
    return staging_result


def _get_params_dict(operation: Any) -> dict[str, Any]:
    """Extract serialized params from an operation."""
    from artisan.utils.hashing import serialize_params

    return serialize_params(operation)


def _build_execution_context(
    execution_run_id: str,
    unit: ExecutionUnit,
    timestamp_start: datetime,
    worker_id: int,
    runtime_env: RuntimeEnvironment,
    operation: Any,
) -> Any:
    """Build an execution context, raising if working_root is missing."""
    working_root = runtime_env.working_root
    if working_root is None:
        msg = "RuntimeEnvironment.working_root must be set"
        raise ValueError(msg)
    fs = runtime_env.storage.filesystem()
    storage_options = runtime_env.storage.delta_storage_options()
    return build_creator_execution_context(
        execution_run_id=execution_run_id,
        execution_spec_id=unit.execution_spec_id,
        step_number=unit.step_number,
        timestamp_start=timestamp_start,
        worker_id=worker_id,
        delta_root=runtime_env.delta_root,
        staging_root=runtime_env.staging_root,
        fs=fs,
        storage_options=storage_options,
        operation=operation,
        sandbox_path=os.path.join(working_root, "dummy"),
        compute_backend_name=runtime_env.compute_backend_name,
        shared_filesystem=runtime_env.shared_filesystem,
        step_run_id=unit.step_run_id,
        files_root=runtime_env.files_root,
    )


def _try_build_execution_context(
    execution_run_id: str,
    unit: ExecutionUnit,
    timestamp_start: datetime,
    worker_id: int,
    runtime_env: RuntimeEnvironment,
    operation: Any,
) -> Any | None:
    """Try to build an execution context, returning None on failure."""
    try:
        return _build_execution_context(
            execution_run_id,
            unit,
            timestamp_start,
            worker_id,
            runtime_env,
            operation,
        )
    except (ValueError, Exception):
        return None


def _extract_inputs(unit: ExecutionUnit) -> dict[str, list[str]]:
    """Copy input artifact IDs from the execution unit."""
    if not unit.inputs:
        return {}
    return {role: list(ids) for role, ids in unit.inputs.items()}
