"""Execute creator operations through the preprocess/execute/postprocess lifecycle."""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from artisan.execution.compute.base import ComputeRouter
from artisan.execution.compute.routing import create_router
from artisan.execution.context.builder import build_creator_execution_context
from artisan.execution.context.sandbox import create_sandbox, output_snapshot
from artisan.execution.inputs.instantiation import instantiate_inputs
from artisan.execution.inputs.materialization import materialize_inputs
from artisan.execution.lineage.builder import build_edges
from artisan.execution.lineage.capture import capture_lineage_metadata
from artisan.execution.lineage.enrich import build_artifact_edges_from_dict
from artisan.execution.lineage.filesystem_match import (
    augment_match_map_from_artifacts,
    build_filesystem_match_map,
)
from artisan.execution.lineage.name_derivation import derive_human_names
from artisan.execution.lineage.validation import (
    validate_artifacts_match_specs,
    validate_lineage_completeness,
    validate_lineage_integrity,
)
from artisan.execution.models.artifact_source import ArtifactSource
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.staging.parquet_writer import StagingResult
from artisan.execution.staging.recorder import (
    _read_tool_output,
    record_execution_failure,
    record_execution_success,
)
from artisan.execution.utils import finalize_artifacts, generate_execution_run_id
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.utils.errors import format_error
from artisan.utils.path import shard_uri
from artisan.utils.timing import phase_timer

logger = logging.getLogger(__name__)


class _PostprocessFailure(Exception):
    """Raised when postprocess returns success=False with a clean error message."""


class _ExecuteFailure(Exception):
    """Raised when execute() throws, carrying the formatted error and tool output."""

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

    Args:
        unit: Execution unit specifying the operation and its inputs.
        runtime_env: Paths and backend configuration for this run.
        worker_id: Numeric worker identifier for concurrency tracking.
        execution_run_id: Pre-generated run ID. Generated if None.
        sources: Optional pre-resolved artifact sources keyed by role.
            When provided, hydrate from sources instead of unit.inputs.
            Used by the composite executor for in-memory artifact passing.
        compute_router: Optional pre-created router for execute() dispatch.
            When None, created from the operation's compute config.

    Returns:
        LifecycleResult with artifacts, edges, and timings.

    Raises:
        ValueError: If working_root is not set.
        Exception: Any failure from preprocess/execute/postprocess/lineage.
    """
    timings: dict[str, Any] = {}
    operation = unit.operation
    operation_class = type(operation)
    original_inputs = _extract_inputs(unit)
    _validate_operation_outputs(operation_class)

    if execution_run_id is None:
        timestamp_start = datetime.now(UTC)
        execution_run_id = generate_execution_run_id(
            unit.execution_spec_id, timestamp_start, worker_id
        )
    else:
        timestamp_start = datetime.now(UTC)

    # --- setup phase ---
    with phase_timer("setup", timings):
        working_root = runtime_env.working_root
        if working_root is None:
            msg = "RuntimeEnvironment.working_root must be set to create a sandbox"
            raise ValueError(msg)

        if working_root == tempfile.gettempdir():
            sandbox_path_str = os.path.join(working_root, execution_run_id)
        else:
            sandbox_path_str = shard_uri(
                working_root,
                execution_run_id,
                unit.step_number,
                operation_name=operation.name,
            )

        sandbox_path_str, preprocess_dir, execute_dir, postprocess_dir = create_sandbox(
            sandbox_path_str
        )

        log_path = os.path.join(sandbox_path_str, "tool_output.log")
        materialized_dir = os.path.join(sandbox_path_str, "materialized_inputs")
        os.makedirs(materialized_dir, exist_ok=True)

        if runtime_env.files_root is not None:
            files_dir: str | None = shard_uri(
                runtime_env.files_root,
                execution_run_id,
                unit.step_number,
                operation_name=operation.name,
            )
            os.makedirs(files_dir, exist_ok=True)
        else:
            files_dir = None

        fs = runtime_env.storage.filesystem()
        storage_options = runtime_env.storage.delta_storage_options()

        execution_context = build_creator_execution_context(
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
            sandbox_path=sandbox_path_str,
            compute_backend_name=runtime_env.compute_backend_name,
            shared_filesystem=runtime_env.shared_filesystem,
            step_run_id=unit.step_run_id,
            files_root=runtime_env.files_root,
        )
        artifact_store = execution_context.artifact_store

        input_specs = getattr(operation_class, "inputs", {})
        default_hydrate = getattr(operation_class, "hydrate_inputs", True)

        if sources is not None:
            # Hydrate from pre-resolved sources (composite executor path)
            input_artifacts: dict[str, list[Artifact]] = {}
            for role, source in sources.items():
                spec = input_specs.get(role, InputSpec())
                input_artifacts[role] = source.hydrate(
                    artifact_store, spec, default_hydrate
                )
            associated: dict[tuple[str, str], list[Artifact]] = {}
        else:
            # Standard path: instantiate from Delta-backed IDs
            input_artifacts, associated = instantiate_inputs(
                original_inputs,
                artifact_store,
                input_specs,
                default_hydrate,
            )
        input_artifacts, materialized_artifact_ids = materialize_inputs(
            input_artifacts,
            input_specs,
            materialized_dir,
            artifact_store,
        )

    # --- preprocess phase ---
    with phase_timer("preprocess", timings):
        preprocess_input = PreprocessInput(
            preprocess_dir=preprocess_dir,
            input_artifacts=input_artifacts,
            _associated=associated,
        )
        prepared_inputs = operation.preprocess(preprocess_input)
        execute_input = ExecuteInput(
            inputs=prepared_inputs,
            execute_dir=execute_dir,
            log_path=log_path,
            files_dir=files_dir,
        )

    # --- execute phase ---
    with phase_timer("execute", timings):
        if compute_router is None:
            config = operation.compute.current()
            compute_router = create_router(config)
        try:
            raw_result = compute_router.route_execute(
                operation, execute_input, sandbox_path_str
            )
        except Exception as exc:
            error = format_error(exc)
            if hasattr(exc, "stdout") and exc.stdout:
                tail = "\n".join(exc.stdout.splitlines()[-30:])
                error += f"\n--- tool stdout (last 30 lines) ---\n{tail}"
            tool_output = _read_tool_output(log_path)
            raise _ExecuteFailure(error, tool_output=tool_output) from exc

    # Capture tool output after execute phase
    tool_output = _read_tool_output(log_path)

    # --- postprocess phase ---
    with phase_timer("postprocess", timings):
        file_outputs = output_snapshot(execute_dir)
        filesystem_match_map = build_filesystem_match_map(
            materialized_artifact_ids, file_outputs
        )

        postprocess_input = PostprocessInput(
            file_outputs=file_outputs,
            memory_outputs=raw_result,
            input_artifacts=_extract_artifacts_from_input(input_artifacts),
            step_number=unit.step_number,
            postprocess_dir=postprocess_dir,
            _associated=associated,
        )
        op_result = operation.postprocess(postprocess_input)

        if not op_result.success:
            raise _PostprocessFailure(op_result.error or "Postprocess failed")

        finalized_artifacts = finalize_artifacts(op_result.artifacts)
        validate_artifacts_match_specs(finalized_artifacts, operation_class.outputs)

        # Augment match map with artifact names from memory-based outputs
        augment_match_map_from_artifacts(
            filesystem_match_map, materialized_artifact_ids, finalized_artifacts
        )

    # --- lineage phase ---
    with phase_timer("lineage", timings):
        flat_input_artifacts = _extract_artifacts_from_input(input_artifacts)
        if op_result.lineage is None:
            lineage = capture_lineage_metadata(
                output_artifacts=finalized_artifacts,
                input_artifacts=flat_input_artifacts,
                output_specs=operation_class.outputs,
                group_by=getattr(operation_class, "group_by", None),
                group_ids=unit.group_ids,
                filesystem_match_map=filesystem_match_map,
            )
        else:
            validate_lineage_integrity(
                op_result.lineage,
                flat_input_artifacts,
                finalized_artifacts,
            )
            lineage = op_result.lineage

        edge_pairs = build_edges(
            lineage=lineage,
            finalized_artifacts=finalized_artifacts,
            input_artifacts=flat_input_artifacts,
            output_specs=operation_class.outputs,
        )

        validate_lineage_completeness(
            finalized_artifacts,
            operation_class.outputs,
            lineage,
        )
        built_artifacts: dict[str, Artifact] = {}
        for artifact_list in finalized_artifacts.values():
            for artifact in artifact_list:
                if artifact.artifact_id is not None:
                    built_artifacts[artifact.artifact_id] = artifact
        for artifact_list in flat_input_artifacts.values():
            for artifact in artifact_list:
                if artifact.artifact_id is not None:
                    built_artifacts[artifact.artifact_id] = artifact

        artifact_edges = build_artifact_edges_from_dict(
            edge_pairs,
            execution_run_id,
            built_artifacts,
        )

    # --- name derivation phase ---
    derive_human_names(
        finalized_artifacts, edge_pairs, flat_input_artifacts, filesystem_match_map
    )

    # Clean up sandbox
    if (
        sandbox_path_str is not None
        and not runtime_env.preserve_working
        and os.path.exists(sandbox_path_str)
    ):
        shutil.rmtree(sandbox_path_str, ignore_errors=True)

    return LifecycleResult(
        input_artifacts=flat_input_artifacts,
        artifacts=finalized_artifacts,
        edges=artifact_edges,
        timings=timings,
    )


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
        runtime_env: Paths and backend configuration for this run.
        worker_id: Numeric worker identifier for concurrency tracking.
        compute_router: Shared router for compute dispatch. When provided,
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


def _validate_operation_outputs(operation_class: type) -> None:
    """Raise ValueError if the operation class has no outputs declared."""
    if getattr(operation_class, "outputs", None) is None:
        msg = (
            f"{operation_class.__name__} must define outputs. "
            "Add a ClassVar like: outputs: ClassVar[dict[str, OutputSpec]] = {}"
        )
        raise ValueError(msg)


def _extract_artifacts_from_input(
    input_artifacts: dict[str, list[Artifact]],
) -> dict[str, list[Artifact]]:
    """Shallow-copy the input artifacts dict to avoid mutation."""
    return {role: list(artifacts) for role, artifacts in input_artifacts.items()}
