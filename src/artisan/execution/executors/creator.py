"""Execute creator operations through the preprocess/execute/postprocess lifecycle."""

from __future__ import annotations

import logging
import shutil
import time
from datetime import UTC, datetime
from typing import Any

from artisan.execution.context.builder import build_creator_execution_context
from artisan.execution.context.sandbox import create_sandbox, output_snapshot
from artisan.execution.inputs.instantiation import instantiate_inputs
from artisan.execution.inputs.materialization import materialize_inputs
from artisan.execution.lineage.builder import build_edges
from artisan.execution.lineage.capture import capture_lineage_metadata
from artisan.execution.lineage.enrich import build_artifact_edges_from_dict
from artisan.execution.lineage.validation import (
    validate_artifacts_match_specs,
    validate_lineage_completeness,
    validate_lineage_integrity,
)
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.staging.parquet_writer import StagingResult
from artisan.execution.staging.recorder import (
    _read_tool_output,
    record_execution_failure,
    record_execution_success,
)
from artisan.execution.utils import finalize_artifacts, generate_execution_run_id
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.utils.errors import format_error
from artisan.utils.timing import phase_timer

logger = logging.getLogger(__name__)


def run_creator_flow(
    unit: ExecutionUnit,
    runtime_env: RuntimeEnvironment,
    worker_id: int = 0,
) -> StagingResult:
    """Execute a creator operation through ordered execution phases.

    Phases: setup, preprocess, execute, postprocess, lineage, record.
    Failures at any phase are caught and staged as error records.

    Args:
        unit: Execution unit specifying the operation and its inputs.
        runtime_env: Paths and backend configuration for this run.
        worker_id: Numeric worker identifier for concurrency tracking.

    Returns:
        StagingResult indicating success or failure with staged paths.
    """
    timings: dict[str, Any] = {}
    timestamp_start = datetime.now(UTC)
    operation = unit.operation
    operation_class = type(operation)
    original_inputs = _extract_inputs(unit)
    user_overrides = unit.user_overrides
    _validate_operation_outputs(operation_class)

    execution_run_id = generate_execution_run_id(
        unit.execution_spec_id,
        timestamp_start,
        worker_id,
    )

    total_start = time.perf_counter()
    sandbox_path = None
    log_path = None
    execution_context = None
    params_dict = None
    try:
        # --- setup phase ---
        with phase_timer("setup", timings):
            working_root = runtime_env.working_root_path
            if working_root is None:
                msg = "RuntimeEnvironment.working_root_path must be set to create a sandbox"
                raise ValueError(msg)
            sandbox_path, preprocess_dir, execute_dir, postprocess_dir = create_sandbox(
                working_root,
                execution_run_id,
                unit.step_number,
                operation_name=operation.name,
            )

            log_path = sandbox_path / "tool_output.log"
            materialized_dir = sandbox_path / "materialized_inputs"
            materialized_dir.mkdir(parents=True, exist_ok=True)

            execution_context = build_creator_execution_context(
                execution_run_id=execution_run_id,
                execution_spec_id=unit.execution_spec_id,
                step_number=unit.step_number,
                timestamp_start=timestamp_start,
                worker_id=worker_id,
                delta_root_path=runtime_env.delta_root_path,
                staging_root_path=runtime_env.staging_root_path,
                operation=operation,
                sandbox_path=sandbox_path,
                compute_backend_name=runtime_env.compute_backend_name,
                shared_filesystem=runtime_env.shared_filesystem,
            )
            artifact_store = execution_context.artifact_store

            params_dict = (
                operation.params.model_dump(mode="json")
                if hasattr(operation, "params")
                else {}
            )

            input_specs = getattr(operation_class, "inputs", {})
            default_hydrate = getattr(operation_class, "hydrate_inputs", True)

            input_artifacts, associated = instantiate_inputs(
                original_inputs,
                artifact_store,
                input_specs,
                default_hydrate,
            )
            input_artifacts = materialize_inputs(
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
            )

        # --- execute phase ---
        with phase_timer("execute", timings):
            try:
                raw_result = operation.execute(execute_input)
            except Exception as exc:
                error = format_error(exc)
                if hasattr(exc, "stdout") and exc.stdout:
                    tail = "\n".join(exc.stdout.splitlines()[-30:])
                    error += f"\n--- tool stdout (last 30 lines) ---\n{tail}"
                return record_execution_failure(
                    execution_context=execution_context,
                    error=error,
                    inputs=original_inputs,
                    timestamp_end=datetime.now(UTC),
                    params=params_dict,
                    user_overrides=user_overrides,
                    tool_output=_read_tool_output(log_path),
                    failure_logs_root=runtime_env.failure_logs_root,
                )

        # Capture tool output after execute phase
        tool_output = _read_tool_output(log_path)

        # --- postprocess phase ---
        with phase_timer("postprocess", timings):
            file_outputs = output_snapshot(execute_dir)
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
                return record_execution_failure(
                    execution_context=execution_context,
                    error=op_result.error or "Postprocess failed",
                    inputs=original_inputs,
                    timestamp_end=datetime.now(UTC),
                    params=params_dict,
                    user_overrides=user_overrides,
                    tool_output=tool_output,
                    failure_logs_root=runtime_env.failure_logs_root,
                )

            finalized_artifacts = finalize_artifacts(op_result.artifacts)
            validate_artifacts_match_specs(finalized_artifacts, operation_class.outputs)

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
                )
            else:
                validate_lineage_integrity(
                    op_result.lineage,
                    flat_input_artifacts,
                    finalized_artifacts,
                )
                lineage = op_result.lineage

            edges = build_edges(
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
                edges,
                execution_run_id,
                built_artifacts,
            )

        # --- record phase ---
        with phase_timer("record", timings):
            staging_result = record_execution_success(
                execution_context=execution_context,
                artifacts=finalized_artifacts,
                lineage_edges=artifact_edges,
                inputs=original_inputs,
                timestamp_end=datetime.now(UTC),
                params=params_dict,
                result_metadata={"timings": timings},
                user_overrides=user_overrides,
                tool_output=tool_output,
            )
    except Exception as exc:
        error = format_error(exc)
        outer_tool_output = _read_tool_output(log_path) if log_path else None
        if execution_context is None:
            logger.error("Creator setup failed: %s", error)
            staging_result = StagingResult(
                success=False,
                error=error,
                execution_run_id=execution_run_id,
                artifact_ids=[],
            )
        else:
            staging_result = record_execution_failure(
                execution_context=execution_context,
                error=error,
                inputs=original_inputs,
                timestamp_end=datetime.now(UTC),
                params=params_dict,
                user_overrides=user_overrides,
                tool_output=outer_tool_output,
                failure_logs_root=runtime_env.failure_logs_root,
            )
    finally:
        if (
            sandbox_path is not None
            and not runtime_env.preserve_working
            and sandbox_path.exists()
        ):
            shutil.rmtree(sandbox_path, ignore_errors=True)

    timings["total"] = round(time.perf_counter() - total_start, 4)
    logger.debug("Execution %s timings: %s", execution_run_id, timings)
    return staging_result


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
