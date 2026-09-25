"""Execute curator operations via the single-phase execute_curator lifecycle.

Curator operations receive DataFrames of artifact IDs and return either
an ArtifactResult (new artifacts) or a PassthroughResult (existing IDs).
Routing is decided by ``is_curator_operation``, which checks whether the
operation class overrides ``execute_curator``.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any

import polars as pl

from artisan.execution.context.builder import build_execution_context
from artisan.execution.lineage.builder import build_edges
from artisan.execution.lineage.enrich import (
    build_artifact_edges_from_types,
    require_artifact_type,
)
from artisan.execution.lineage.validation import (
    validate_artifacts_match_specs,
    validate_lineage_completeness,
    validate_lineage_integrity,
)
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording.commands import (
    capture_commands,
    command_snapshot,
    sanitize_diagnostic,
)
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.execution.recording.recorder import (
    error_envelope_dict,
    execution_is_sealed,
    record_execution_failure,
    record_execution_success,
    record_passthrough,
)
from artisan.execution.recording.replay_snapshot import (
    capture_replay,
    replay_recording_fields,
    verify_replay_worker,
)
from artisan.execution.utils import (
    finalize_artifacts,
    generate_execution_run_id,
    validate_passthrough_result,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.curator_result import (
    ArtifactResult,
    PassthroughResult,
)
from artisan.schemas.execution.execution_context import ExecutionContext
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.utils.hashing import serialize_params
from artisan.utils.timing import phase_timer
from artisan.utils.traceback import format_error

logger = logging.getLogger(__name__)


def is_curator_operation(op: type[OperationDefinition] | OperationDefinition) -> bool:
    """Return True if the operation overrides execute_curator."""
    op_class = op if isinstance(op, type) else type(op)
    return (
        hasattr(op_class, "execute_curator")
        and op_class.execute_curator is not OperationDefinition.execute_curator
    )


def _handle_artifact_result(
    result: ArtifactResult,
    operation: OperationDefinition,
    artifact_store: ArtifactStore,
    execution_context: ExecutionContext,
    inputs: dict[str, list[str]],
    timestamp_end: datetime,
    command_recording: CommandRecording,
    replay_snapshot: ReplaySnapshot,
    replay_of_execution_run_id: str | None,
    user_overrides: dict[str, Any] | None = None,
) -> StagingResult:
    """Finalize, validate, and stage new artifacts from a curator result."""
    validate_artifacts_match_specs(
        result.artifacts, operation.outputs, allow_dynamic_outputs=True
    )
    validate_lineage_integrity(
        result.lineage, inputs, result.artifacts, operation.outputs
    )
    validate_lineage_completeness(result.artifacts, operation.outputs, result.lineage)
    finalized = finalize_artifacts(result.artifacts)
    source_ids = {
        mapping.source_artifact_id
        for mappings in result.lineage.values()
        for mapping in mappings
        if mapping.source_artifact_id is not None
    }
    artifact_types = (
        artifact_store.provenance.load_type_map(sorted(source_ids))
        if source_ids
        else {}
    )
    # Identity-neutral outputs must not conceal an absent input in the store.
    for source_id in source_ids:
        require_artifact_type(source_id, artifact_types)
    artifact_types.update(
        {
            artifact.artifact_id: artifact.artifact_type
            for artifacts in finalized.values()
            for artifact in artifacts
            if artifact.artifact_id is not None
        }
    )
    pairs = build_edges(result.lineage, finalized, artifact_types)
    artifact_edges = build_artifact_edges_from_types(
        pairs, execution_context.execution_run_id, artifact_types
    )

    params_dict = serialize_params(operation)
    return record_execution_success(
        replay_snapshot=replay_snapshot,
        replay_of_execution_run_id=replay_of_execution_run_id,
        command_recording=command_recording,
        execution_context=execution_context,
        artifacts=dict(finalized),
        lineage_edges=artifact_edges,
        inputs=inputs,
        timestamp_end=timestamp_end,
        params=params_dict,
        result_metadata=result.metadata if result.metadata else None,
        user_overrides=user_overrides,
    )


def _handle_passthrough_result(
    result: PassthroughResult,
    operation: OperationDefinition,
    execution_context: ExecutionContext,
    inputs: dict[str, Any],
    timestamp_end: datetime,
    command_recording: CommandRecording,
    replay_snapshot: ReplaySnapshot,
    replay_of_execution_run_id: str | None,
    user_overrides: dict[str, Any] | None = None,
) -> StagingResult:
    """Validate and stage a passthrough result (no new artifacts created).

    Validation needs the operation instance (which the recorder never sees),
    so it stays here; the staging body — including edge run-id stamping —
    lives in ``record_passthrough``.
    """
    validate_passthrough_result(result, operation.outputs)
    return record_passthrough(
        replay_snapshot=replay_snapshot,
        replay_of_execution_run_id=replay_of_execution_run_id,
        command_recording=command_recording,
        execution_context=execution_context,
        passthrough=result.passthrough,
        lineage_edges=result.lineage_edges,
        inputs=inputs,
        timestamp_end=timestamp_end,
        params=serialize_params(operation),
        result_metadata=result.metadata if result.metadata else None,
        user_overrides=user_overrides,
    )


def run_curator_flow(
    unit: ExecutionUnit, runtime_env: RuntimeEnvironment
) -> StagingResult:
    """Execute a curator and stage its result with command and replay evidence.

    Args:
        unit: Configured operation and concrete input batch.
        runtime_env: Resolved worker identity, runtime paths and storage.

    Returns:
        Staged success or failure, or an unstaged failure if setup prevents
        recording.
    """
    with (
        capture_commands(unit.operation) as commands,
        capture_replay(unit, runtime_env),
    ):
        commands.add_environment(
            {str(i): value for i, value in enumerate(unit.replay_sensitive_values)}
        )
        return _run_curator_flow(unit, runtime_env)


def _run_curator_flow(
    unit: ExecutionUnit,
    runtime_env: RuntimeEnvironment,
) -> StagingResult:
    """Run setup, execution and recording for one curator unit."""
    timings: dict[str, Any] = {}
    total_start = time.perf_counter()
    operation = unit.operation
    inputs = unit.inputs
    timestamp_start = datetime.now(UTC)
    execution_run_id = generate_execution_run_id(
        unit.execution_spec_id,
        timestamp_start,
        runtime_env.worker_id,
    )
    params_dict = serialize_params(operation)
    user_overrides = unit.user_overrides
    execution_context = None

    try:
        # --- setup phase ---
        with phase_timer("setup", timings):
            execution_context = build_execution_context(
                execution_run_id=execution_run_id,
                execution_spec_id=unit.execution_spec_id,
                step_number=unit.step_number,
                timestamp_start=timestamp_start,
                runtime_env=runtime_env,
                operation=operation,
                step_run_id=unit.step_run_id,
            )
            artifact_store = execution_context.artifact_store
            verify_replay_worker(unit)

            # Build DataFrames with artifact_id column per role
            input_dfs = {
                role: pl.DataFrame({"artifact_id": ids}) for role, ids in inputs.items()
            }

        # --- execute phase ---
        with phase_timer("execute", timings):
            try:
                result = operation.execute_curator(
                    inputs=input_dfs,
                    step_number=unit.step_number,
                    artifact_store=artifact_store,
                )
            except Exception as exc:
                return record_execution_failure(
                    **replay_recording_fields(),
                    command_recording=command_snapshot(),
                    execution_context=execution_context,
                    error=format_error(exc),
                    inputs=inputs,
                    timestamp_end=datetime.now(UTC),
                    params=params_dict,
                    user_overrides=user_overrides,
                    failure_logs_root=runtime_env.failure_logs_root,
                    error_envelope=error_envelope_dict(exc),
                )

        # --- record phase ---
        with phase_timer("record", timings):
            timestamp_end = datetime.now(UTC)
            if not result.success:
                staging_result = record_execution_failure(
                    **replay_recording_fields(),
                    command_recording=command_snapshot(),
                    execution_context=execution_context,
                    error=result.error or "Unknown error",
                    inputs=inputs,
                    timestamp_end=timestamp_end,
                    params=params_dict,
                    user_overrides=user_overrides,
                    failure_logs_root=runtime_env.failure_logs_root,
                )
            else:
                merged_metadata: dict[str, Any] = {"timings": timings}
                if hasattr(result, "metadata") and result.metadata:
                    merged_metadata.update(result.metadata)

                # Inject merged metadata into the result before handling
                result_with_metadata = result.model_copy(
                    update={"metadata": merged_metadata}
                )

                match result_with_metadata:
                    case ArtifactResult():
                        staging_result = _handle_artifact_result(
                            **replay_recording_fields(),
                            command_recording=command_snapshot(),
                            result=result_with_metadata,
                            operation=operation,
                            artifact_store=artifact_store,
                            execution_context=execution_context,
                            inputs=inputs,
                            timestamp_end=timestamp_end,
                            user_overrides=user_overrides,
                        )
                    case PassthroughResult():
                        staging_result = _handle_passthrough_result(
                            **replay_recording_fields(),
                            command_recording=command_snapshot(),
                            result=result_with_metadata,
                            operation=operation,
                            execution_context=execution_context,
                            inputs=inputs,
                            timestamp_end=timestamp_end,
                            user_overrides=user_overrides,
                        )
                    case _:
                        # Defensive branch: mypy narrows to known cases.
                        error = (  # type: ignore[unreachable]
                            f"Unexpected result type from curator operation: "
                            f"{type(result_with_metadata).__name__}. "
                            f"Expected ArtifactResult or PassthroughResult."
                        )
                        staging_result = record_execution_failure(
                            **replay_recording_fields(),
                            command_recording=command_snapshot(),
                            execution_context=execution_context,
                            error=error,
                            inputs=inputs,
                            timestamp_end=datetime.now(UTC),
                            params=params_dict,
                            user_overrides=user_overrides,
                            failure_logs_root=runtime_env.failure_logs_root,
                        )

    except Exception as exc:
        if execution_context is not None and execution_is_sealed(execution_context):
            raise
        error = sanitize_diagnostic(format_error(exc))
        if execution_context is None:
            logger.error("Curator setup failed: %s", error)
            return StagingResult(
                success=False,
                error=error,
                execution_run_id=execution_run_id,
                artifact_ids=[],
            )
        staging_result = record_execution_failure(
            **replay_recording_fields(),
            command_recording=command_snapshot(),
            execution_context=execution_context,
            error=error,
            inputs=inputs,
            timestamp_end=datetime.now(UTC),
            params=params_dict,
            user_overrides=user_overrides,
            failure_logs_root=runtime_env.failure_logs_root,
            error_envelope=error_envelope_dict(exc),
        )

    timings["total"] = round(time.perf_counter() - total_start, 4)
    logger.debug("Execution %s timings: %s", execution_run_id, timings)
    return staging_result
