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

from artisan.execution.context.builder import build_curator_execution_context
from artisan.execution.lineage.builder import build_edges
from artisan.execution.lineage.capture import capture_lineage_metadata
from artisan.execution.lineage.enrich import (
    build_artifact_edges_from_dict,
    build_config_reference_edges,
)
from artisan.execution.lineage.validation import (
    validate_artifacts_match_specs,
    validate_lineage_integrity,
)
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.staging.parquet_writer import StagingResult
from artisan.execution.staging.recorder import (
    build_execution_edges,
    record_execution_failure,
    record_execution_success,
)
from artisan.execution.utils import (
    finalize_artifacts,
    generate_execution_run_id,
    validate_passthrough_result,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.schemas.execution.curator_result import ArtifactResult, PassthroughResult
from artisan.schemas.execution.execution_context import ExecutionContext
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.utils.errors import format_error
from artisan.utils.timing import phase_timer

logger = logging.getLogger(__name__)


def is_curator_operation(op: type[OperationDefinition] | OperationDefinition) -> bool:
    """Return True if the operation overrides execute_curator."""
    op_class = op if isinstance(op, type) else type(op)
    return (
        hasattr(op_class, "execute_curator")
        and op_class.execute_curator is not OperationDefinition.execute_curator
    )


def _get_params(operation: OperationDefinition) -> dict[str, Any]:
    """Safely serialize operation params when available."""
    from artisan.utils.hashing import serialize_params

    return serialize_params(operation)


def _hydrate_inputs_for_lineage(
    inputs: dict[str, list[str]],
    output_specs: dict[str, OutputSpec],
    artifact_store: ArtifactStore,
) -> dict[str, list[Artifact]]:
    """Hydrate only input roles referenced by OutputSpec.infer_lineage_from.

    Lineage capture needs Artifact objects with original_name for stem-matching.
    This helper hydrates only the specific roles needed, avoiding full
    instantiation of all inputs.

    Args:
        inputs: Raw inputs {role: [artifact_id, ...]}.
        output_specs: Operation output specs to check infer_lineage_from.
        artifact_store: Store for artifact hydration.

    Returns:
        {role: [Artifact, ...]} for roles referenced by lineage config.
    """
    needed_roles: set[str] = set()
    for spec in output_specs.values():
        if spec.infer_lineage_from is not None:
            for input_role in spec.infer_lineage_from.get("inputs", []):
                needed_roles.add(input_role)

    result: dict[str, list[Artifact]] = {}
    for role in needed_roles:
        ids = inputs.get(role, [])
        if not ids:
            continue

        # Determine artifact type from the operation's input specs
        type_map = artifact_store.provenance.load_type_map(ids)

        # Group by type for bulk loading
        ids_by_type: dict[str, list[str]] = {}
        for aid in ids:
            atype = type_map.get(aid)
            if atype:
                ids_by_type.setdefault(atype, []).append(aid)

        role_artifacts: dict[str, Artifact] = {}
        for atype, type_ids in ids_by_type.items():
            loaded = artifact_store.get_artifacts_by_type(type_ids, atype)
            role_artifacts.update(loaded)

        # Preserve input order
        result[role] = [role_artifacts[aid] for aid in ids if aid in role_artifacts]

    return result


def _handle_artifact_result(
    result: ArtifactResult,
    operation: OperationDefinition,
    artifact_store: ArtifactStore,
    execution_context: ExecutionContext,
    unit: ExecutionUnit,
    inputs: dict[str, Any],
    input_artifacts: dict[str, list[Artifact]],
    timestamp_end: datetime,
    user_overrides: dict[str, Any] | None = None,
) -> StagingResult:
    """Finalize, validate, and stage new artifacts from a curator result."""
    finalized = finalize_artifacts(result.artifacts)
    validate_artifacts_match_specs(finalized, operation.outputs)
    artifact_edges: list[ArtifactProvenanceEdge] = []

    # Build lookup of all artifacts for lineage edge construction
    built_artifacts: dict[str, Artifact] = {}
    for artifact_list in finalized.values():
        for artifact in artifact_list:
            if artifact.artifact_id is not None:
                built_artifacts[artifact.artifact_id] = artifact
    for artifact_list in input_artifacts.values():
        for artifact in artifact_list:
            if artifact.artifact_id is not None:
                built_artifacts[artifact.artifact_id] = artifact

    # Honor explicit lineage from the curator result, mirroring the creator pattern
    output_specs = getattr(operation, "outputs", {})
    if result.lineage is None:
        lineage = capture_lineage_metadata(
            finalized,
            input_artifacts,
            output_specs,
            group_by=getattr(type(operation), "group_by", None),
            group_ids=unit.group_ids,
        )
    else:
        validate_lineage_integrity(result.lineage, input_artifacts, finalized)
        lineage = result.lineage
    pairs = build_edges(lineage, finalized, input_artifacts, output_specs)
    if pairs:
        artifact_edges.extend(
            build_artifact_edges_from_dict(
                source_target_pairs=pairs,
                execution_run_id=execution_context.execution_run_id,
                built_artifacts=built_artifacts,
            )
        )

    config_artifacts = [
        artifact
        for artifact_list in finalized.values()
        for artifact in artifact_list
        if isinstance(artifact, ExecutionConfigArtifact)
    ]
    if config_artifacts:
        artifact_edges.extend(
            build_config_reference_edges(
                config_artifacts=config_artifacts,
                artifact_store=artifact_store,
                execution_run_id=execution_context.execution_run_id,
            )
        )

    params_dict = _get_params(operation)
    return record_execution_success(
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
    user_overrides: dict[str, Any] | None = None,
) -> StagingResult:
    """Validate and stage a passthrough result (no new artifacts created)."""
    from artisan.execution.staging.parquet_writer import (
        StagingResult,
        _create_staging_path,
        _stage_execution,
    )

    validate_passthrough_result(result, operation.outputs)
    staging_path = _create_staging_path(
        execution_context.staging_root,
        execution_context.execution_run_id,
        execution_context.step_number,
        operation_name=execution_context.operation_name,
    )
    execution_edges = build_execution_edges(
        execution_run_id=execution_context.execution_run_id,
        inputs=inputs,
        outputs=result.passthrough,
    )

    params_dict = _get_params(operation)
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
        params=params_dict,
        compute_backend=execution_context.compute_backend,
        shared_filesystem=execution_context.shared_filesystem,
        result_metadata=result.metadata if result.metadata else None,
        user_overrides=user_overrides,
        step_run_id=execution_context.step_run_id,
    )
    all_passthrough_ids = [aid for ids in result.passthrough.values() for aid in ids]
    return StagingResult(
        success=True,
        staging_path=staging_path,
        execution_run_id=execution_context.execution_run_id,
        artifact_ids=all_passthrough_ids,
    )


def run_curator_flow(
    unit: ExecutionUnit,
    runtime_env: RuntimeEnvironment,
    worker_id: int = 0,
) -> StagingResult:
    """Execute a curator operation through setup, execute, and record phases.

    Args:
        unit: Execution unit specifying the operation and its inputs.
        runtime_env: Paths and backend configuration for this run.
        worker_id: Numeric worker identifier for concurrency tracking.

    Returns:
        StagingResult indicating success or failure with staged paths.
    """
    timings: dict[str, Any] = {}
    total_start = time.perf_counter()
    operation = unit.operation
    inputs = unit.inputs
    timestamp_start = datetime.now(UTC)
    execution_run_id = generate_execution_run_id(
        unit.execution_spec_id,
        timestamp_start,
        worker_id,
    )
    params_dict = _get_params(operation)
    user_overrides = unit.user_overrides
    execution_context = None

    try:
        # --- setup phase ---
        with phase_timer("setup", timings):
            artifact_store = ArtifactStore(
                runtime_env.delta_root_path,
                files_root=runtime_env.files_root_path,
            )

            execution_context = build_curator_execution_context(
                execution_run_id=execution_run_id,
                execution_spec_id=unit.execution_spec_id,
                step_number=unit.step_number,
                timestamp_start=timestamp_start,
                worker_id=worker_id,
                delta_root_path=runtime_env.delta_root_path,
                staging_root_path=runtime_env.staging_root_path,
                operation=operation,
                compute_backend_name=runtime_env.compute_backend_name,
                shared_filesystem=runtime_env.shared_filesystem,
                step_run_id=unit.step_run_id,
                files_root=runtime_env.files_root_path,
            )

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
                    execution_context=execution_context,
                    error=format_error(exc),
                    inputs=inputs,
                    timestamp_end=datetime.now(UTC),
                    params=params_dict,
                    user_overrides=user_overrides,
                    failure_logs_root=runtime_env.failure_logs_root,
                )

        # --- record phase ---
        with phase_timer("record", timings):
            timestamp_end = datetime.now(UTC)
            if not result.success:
                staging_result = record_execution_failure(
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
                        # Hydrate only input roles needed for lineage
                        output_specs = getattr(operation, "outputs", {})
                        input_artifacts = _hydrate_inputs_for_lineage(
                            inputs, output_specs, artifact_store
                        )
                        staging_result = _handle_artifact_result(
                            result=result_with_metadata,
                            operation=operation,
                            artifact_store=artifact_store,
                            execution_context=execution_context,
                            unit=unit,
                            inputs=inputs,
                            input_artifacts=input_artifacts,
                            timestamp_end=timestamp_end,
                            user_overrides=user_overrides,
                        )
                    case PassthroughResult():
                        staging_result = _handle_passthrough_result(
                            result=result_with_metadata,
                            operation=operation,
                            execution_context=execution_context,
                            inputs=inputs,
                            timestamp_end=timestamp_end,
                            user_overrides=user_overrides,
                        )
                    case _:
                        error = (
                            f"Unexpected result type from curator operation: "
                            f"{type(result_with_metadata).__name__}. "
                            f"Expected ArtifactResult or PassthroughResult."
                        )
                        staging_result = record_execution_failure(
                            execution_context=execution_context,
                            error=error,
                            inputs=inputs,
                            timestamp_end=datetime.now(UTC),
                            params=params_dict,
                            user_overrides=user_overrides,
                            failure_logs_root=runtime_env.failure_logs_root,
                        )

    except Exception as exc:
        error = format_error(exc)
        if execution_context is None:
            logger.error("Curator setup failed: %s", error)
            return StagingResult(
                success=False,
                error=error,
                execution_run_id=execution_run_id,
                artifact_ids=[],
            )
        staging_result = record_execution_failure(
            execution_context=execution_context,
            error=error,
            inputs=inputs,
            timestamp_end=datetime.now(UTC),
            params=params_dict,
            user_overrides=user_overrides,
            failure_logs_root=runtime_env.failure_logs_root,
        )

    timings["total"] = round(time.perf_counter() - total_start, 4)
    logger.debug("Execution %s timings: %s", execution_run_id, timings)
    return staging_result
