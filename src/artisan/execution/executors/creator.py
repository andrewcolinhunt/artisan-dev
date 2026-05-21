"""Execute creator operations through the preprocess/execute/postprocess lifecycle."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from artisan.errors import ArtisanError, ErrorCode
from artisan.execution.compute.base import ComputeRouter
from artisan.execution.compute.routing import create_router
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


class _ExecuteFailure(ArtisanError):
    """Raised when execute() throws, carrying the formatted error and tool output.

    Subclass of :class:`ArtisanError` so the failure record persists the
    structured envelope (``code=OP_EXECUTE_FAILED``,
    ``recovery_hint=REPORT_TO_USER``) alongside the existing
    ``tool_output`` attribute. The outer
    ``except (_PostprocessFailure, _ExecuteFailure)`` route keeps its
    custom ``tool_output`` extraction; envelope serialization is wired
    separately via ``record_execution_failure(error_envelope=...)``.
    """

    def __init__(
        self,
        error: str,
        *,
        operation_name: str | None = None,
        step_name: str | None = None,
        tool_output: str | None = None,
    ) -> None:
        super().__init__(
            code=ErrorCode.OP_EXECUTE_FAILED,
            error_type="runtime",
            message=error,
            operation_name=operation_name,
            step_name=step_name,
            hint=(
                "Inspect step logs via artisan_get_step_logs; if the "
                "underlying error is transient, retry."
            ),
            recovery_hint="REPORT_TO_USER",
        )
        self.tool_output = tool_output


@dataclass
class LifecycleResult:
    """Return type of run_creator_lifecycle().

    Attributes:
        input_artifacts: Hydrated input artifacts keyed by role.
        artifacts: Finalized output artifacts keyed by role.
        edges: Provenance edges linking inputs to outputs.
        timings: Phase name to elapsed seconds.
    """

    input_artifacts: dict[str, list[Artifact]]
    artifacts: dict[str, list[Artifact]]
    edges: list[ArtifactProvenanceEdge]
    timings: dict[str, float] = field(default_factory=dict)


def run_creator_lifecycle(
    unit: ExecutionUnit,
    runtime_env: RuntimeEnvironment,
    worker_id: int = 0,
    execution_run_id: str | None = None,
    sources: dict[str, ArtifactSource] | None = None,
    compute_router: ComputeRouter | None = None,
) -> LifecycleResult:
    """Run one operation through setup → preprocess → execute → postprocess → lineage.

    This is the inner lifecycle extracted from run_creator_flow(). It raises
    on any failure — the caller is responsible for error recording and sandbox
    cleanup.

    Internally delegates to ``prep_unit()`` (setup + preprocess) and
    ``post_unit()`` (postprocess + lineage). The execute phase runs inline
    here using the first (and only) ExecuteInput from prep_unit — the
    per-artifact fan-out path is wired separately via the batch dispatch
    handle.

    Args:
        unit: Execution unit specifying the operation and its inputs.
        runtime_env: Paths and step_runner configuration for this run.
        worker_id: Numeric worker identifier for concurrency tracking.
        execution_run_id: Pre-generated run ID. Generated if None.
        sources: Optional pre-resolved artifact sources keyed by role.
            When provided, hydrate from sources instead of unit.inputs.
            Used by the composite executor for in-memory artifact passing.
        compute_router: Optional pre-created router for execute() dispatch.
            When None, created from the operation's compute_provider config.

    Returns:
        LifecycleResult with artifacts, edges, and timings.

    Raises:
        ValueError: If working_root is not set.
        Exception: Any failure from preprocess/execute/postprocess/lineage.
    """
    from artisan.execution.executors.creator_phases import post_unit, prep_unit

    prepped = prep_unit(
        unit,
        runtime_env,
        worker_id,
        execution_run_id,
        sources,
        split_per_artifact=False,
    )

    # --- execute phase ---
    with phase_timer("execute", prepped.timings):
        if compute_router is None:
            config = prepped.operation.compute_provider.current()
            compute_router = create_router(
                config,
                compute_resources=prepped.operation.compute_resources,
            )
        try:
            raw_result = compute_router.route_execute(
                prepped.operation,
                prepped.artifact_execute_inputs[0],
                prepped.sandbox_path,
            )
        except ArtisanError:
            # Op raised its own structured error — preserve the inner
            # envelope (its code / recovery_hint is the actionable one;
            # wrapping in OP_EXECUTE_FAILED would flatten CHECK_INPUT to
            # REPORT_TO_USER).
            raise
        except Exception as exc:
            error = format_error(exc)
            if hasattr(exc, "stdout") and exc.stdout:
                tail = "\n".join(exc.stdout.splitlines()[-30:])
                error += f"\n--- tool stdout (last 30 lines) ---\n{tail}"
            tool_output = _read_tool_output(prepped.log_path)
            raise _ExecuteFailure(
                error,
                operation_name=type(prepped.operation).name,
                step_name=str(unit.step_number),
                tool_output=tool_output,
            ) from exc

    return post_unit(prepped, [raw_result], runtime_env)


def run_creator_flow(
    unit: ExecutionUnit,
    runtime_env: RuntimeEnvironment,
    worker_id: int = 0,
    compute_router: ComputeRouter | None = None,
) -> StagingResult:
    """Execute a creator operation through ordered execution phases.

    Phases: setup, preprocess, execute, postprocess, lineage, record.
    Failures at any phase are caught and staged as error records.

    Args:
        unit: Execution unit specifying the operation and its inputs.
        runtime_env: Paths and step_runner configuration for this run.
        worker_id: Numeric worker identifier for concurrency tracking.
        compute_router: Shared router for compute_provider dispatch. When provided,
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
            compute_router=compute_router,
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
        # _ExecuteFailure is an ArtisanError; _PostprocessFailure is not
        # (yet). Persist the envelope where available so the agent gets
        # the structured shape from artisan_get_step_logs.
        envelope = exc.to_dict() if isinstance(exc, ArtisanError) else None
        staging_result = record_execution_failure(
            execution_context=execution_context,
            error=error,
            inputs=original_inputs,
            timestamp_end=datetime.now(UTC),
            params=params_dict,
            user_overrides=user_overrides,
            tool_output=tool_output,
            failure_logs_root=runtime_env.failure_logs_root,
            error_envelope=envelope,
        )
    except ArtisanError as exc:
        # Op raised a structured error directly (e.g. INPUT_TYPE_MISMATCH).
        # Preserve the inner envelope on the failure row.
        error = str(exc)
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
                error_envelope=exc.to_dict(),
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
            # Wrap the unexpected exception in OP_EXECUTE_FAILED so the
            # agent sees a structured shape on the failure row. Inner
            # exception preserved as __cause__.
            wrapped = ArtisanError(
                code=ErrorCode.OP_EXECUTE_FAILED,
                error_type="runtime",
                message=(
                    f"Operation {type(operation).name!r} failed during "
                    f"creator setup or unexpected exception."
                ),
                operation_name=type(operation).name,
                step_name=str(unit.step_number),
                recovery_hint="REPORT_TO_USER",
            )
            wrapped.__cause__ = exc
            staging_result = record_execution_failure(
                execution_context=execution_context,
                error=error,
                inputs=original_inputs,
                timestamp_end=datetime.now(UTC),
                params=params_dict,
                user_overrides=user_overrides,
                failure_logs_root=runtime_env.failure_logs_root,
                error_envelope=wrapped.to_dict(),
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
