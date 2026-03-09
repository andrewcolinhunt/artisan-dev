"""Main coordinator for step execution.

Ties together the three-phase workflow (dispatch, execute, commit) for
both creator operations (dispatched via Prefect) and curator operations
(executed locally in a subprocess).
"""

from __future__ import annotations

import logging
import multiprocessing
import resource
import shutil
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from artisan.execution.context.builder import build_curator_execution_context
from artisan.execution.executors.curator import (
    _get_params,
    is_curator_operation,
    run_curator_flow,
)
from artisan.execution.inputs.grouping import group_inputs
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.staging.parquet_writer import StagingResult
from artisan.execution.staging.recorder import record_execution_failure
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.backends.base import BackendBase
from artisan.orchestration.engine.batching import (
    generate_execution_unit_batches,
    get_batch_config,
)
from artisan.orchestration.engine.dispatch import _save_units
from artisan.orchestration.engine.inputs import resolve_inputs
from artisan.orchestration.engine.results import (
    aggregate_results,
    extract_execution_run_ids,
)
from artisan.schemas.enums import FailurePolicy, TablePath
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.schemas.orchestration.step_result import StepResult, StepResultBuilder
from artisan.storage.cache.cache_lookup import CacheHit, cache_lookup
from artisan.storage.io.staging_verification import await_staging_files
from artisan.utils.timing import phase_timer

logger = logging.getLogger(__name__)


def instantiate_operation(
    operation_class: type[OperationDefinition],
    params: dict[str, Any] | None,
    resources: dict[str, Any] | None = None,
    execution: dict[str, Any] | None = None,
    environment: str | dict[str, Any] | None = None,
    tool: dict[str, Any] | None = None,
) -> OperationDefinition:
    """Construct an operation instance from class, params, and overrides.

    Args:
        operation_class: The operation class to instantiate.
        params: User-provided parameters (merged into params sub-model or flat fields).
        resources: Optional resource overrides (applied via model_copy).
        execution: Optional execution overrides (applied via model_copy).
        environment: Optional environment override. String selects the active
            environment; dict deep-merges nested EnvironmentSpec fields.
        tool: Optional tool overrides (applied via model_copy on instance.tool).

    Returns:
        Fully configured operation instance.
    """
    from artisan.schemas.operation_config.environment_spec import EnvironmentSpec

    init_kwargs: dict[str, Any] = {}

    if params:
        if "params" in operation_class.model_fields:
            # New-style: wrap user params into the params sub-model
            params_cls = operation_class.model_fields["params"].annotation
            init_kwargs["params"] = params_cls(**params)
        else:
            # Flat fields
            init_kwargs.update(params)

    instance = operation_class(**init_kwargs)

    # Apply overrides via model_copy
    updates: dict[str, Any] = {}
    if resources:
        updates["resources"] = instance.resources.model_copy(update=resources)
    if execution:
        updates["execution"] = instance.execution.model_copy(update=execution)
    if tool and instance.tool is not None:
        updates["tool"] = instance.tool.model_copy(update=tool)
    if environment is not None:
        if isinstance(environment, str):
            updates["environments"] = instance.environments.model_copy(
                update={"active": environment}
            )
        else:
            # Deep-merge nested environment specs so partial overrides
            # (e.g. {"docker": {"image": "v2"}}) don't discard other fields.
            env_updates: dict[str, Any] = {}
            for key, value in environment.items():
                current = getattr(instance.environments, key, None)
                if isinstance(current, EnvironmentSpec) and isinstance(value, dict):
                    env_updates[key] = current.model_copy(update=value)
                else:
                    env_updates[key] = value
            updates["environments"] = instance.environments.model_copy(
                update=env_updates
            )
    if updates:
        instance = instance.model_copy(update=updates)

    return instance


def check_cache_for_batch(
    execution_spec_id: str,
    delta_root: Path,
) -> CacheHit | None:
    """Check if an ExecutionUnit can be skipped due to cache hit.

    Args:
        execution_spec_id: Deterministic hash for the batch.
        delta_root: Root path for Delta Lake tables.

    Returns:
        CacheHit if a successful execution exists, None otherwise.
    """
    executions_path = delta_root / TablePath.EXECUTIONS
    execution_edges_path = delta_root / TablePath.EXECUTION_EDGES
    result = cache_lookup(
        executions_path,
        execution_spec_id,
        execution_edges_path=execution_edges_path,
    )
    return result if isinstance(result, CacheHit) else None


def build_step_result(
    operation: type[OperationDefinition] | OperationDefinition,
    step_number: int,
    succeeded_count: int,
    failed_count: int,
    failure_policy: FailurePolicy,
    metadata: dict[str, Any] | None = None,
) -> StepResult:
    """Build StepResult after step execution completes.

    Args:
        operation: OperationDefinition class or instance.
        step_number: Pipeline step number.
        succeeded_count: Number of items that succeeded.
        failed_count: Number of items that failed.
        failure_policy: Failure handling policy enum.
        metadata: Optional metadata dict (timings, diagnostics, etc.).

    Returns:
        StepResult with execution metadata.
    """
    # Extract output roles and types from operation
    output_roles = {}
    for role, spec in operation.outputs.items():
        output_roles[role] = spec.artifact_type

    builder = StepResultBuilder(
        step_name=operation.name,
        step_number=step_number,
        operation_outputs=output_roles,
    )

    builder.add_success(succeeded_count)
    builder.add_failure(failed_count)

    # With fail_fast, any failure means step failure
    success_override = None
    if failure_policy == FailurePolicy.FAIL_FAST and failed_count > 0:
        success_override = False

    return builder.build(success_override=success_override, metadata=metadata)


def _all_inputs_empty(resolved_inputs: dict[str, list[str]]) -> bool:
    """Return True when every input role resolved to zero artifact IDs.

    An empty dict (generative op with no declared inputs) returns False.
    """
    if not resolved_inputs:
        return False  # {} = generative op, not empty inputs
    return all(len(ids) == 0 for ids in resolved_inputs.values())


def _create_runtime_environment(
    config: PipelineConfig,
    operation: type[OperationDefinition] | OperationDefinition,
    backend: BackendBase | None = None,
) -> RuntimeEnvironment:
    """Build a RuntimeEnvironment from pipeline config and backend traits."""
    # Curator operations don't need a sandbox (no materialization)
    is_curator = is_curator_operation(operation)

    return RuntimeEnvironment(
        delta_root_path=config.delta_root,
        staging_root_path=config.staging_root,
        working_root_path=None if is_curator else config.working_root,
        failure_logs_root=config.delta_root.parent / "logs" / "failures",
        preserve_staging=config.preserve_staging,
        preserve_working=config.preserve_working,
        worker_id_env_var=backend.worker_traits.worker_id_env_var if backend else None,
        shared_filesystem=backend.worker_traits.shared_filesystem if backend else False,
        compute_backend_name=backend.name if backend else "local",
    )


def execute_step(
    operation_class: type[OperationDefinition],
    inputs: Any,
    params: dict[str, Any] | None,
    backend: BackendBase,
    resources: dict[str, Any] | None = None,
    execution: dict[str, Any] | None = None,
    environment: str | dict[str, Any] | None = None,
    tool: dict[str, Any] | None = None,
    step_number: int = 0,
    config: PipelineConfig | None = None,
    failure_policy: FailurePolicy = FailurePolicy.CONTINUE,
    compact: bool = True,
    step_spec_id: str | None = None,
) -> StepResult:
    """Execute a single pipeline step.

    This is the main entry point called by PipelineManager.run().
    It coordinates the three-phase workflow: dispatch, execute, commit.

    For curator operations (MergeOp, FilterOp), a separate execution
    path is used that executes locally without worker dispatch.

    Args:
        operation_class: OperationDefinition subclass to execute.
        inputs: Input specification (see PipelineManager.run() for formats).
        params: Parameter overrides.
        backend: Backend to use for execution.
        resources: Resource overrides (cpus, memory_gb, etc.).
        execution: Batching/scheduling overrides (artifacts_per_unit, etc.).
        environment: Environment override (string or dict).
        tool: Tool overrides (executable, interpreter, etc.).
        step_number: Pipeline step number.
        config: Pipeline configuration.
        failure_policy: "continue" or "fail_fast".
        compact: Whether to run Delta Lake compaction.
        step_spec_id: Pre-computed step spec ID from PipelineManager. When
            provided for curator ops, used directly as execution_spec_id to
            skip the O(N log N) compute_execution_spec_id call.

    Returns:
        StepResult with output references and execution metadata.
    """
    operation = instantiate_operation(
        operation_class, params, resources, execution, environment, tool
    )
    user_overrides = params or {}

    # Merge environment + tool into config_overrides for hashing
    config_overrides = _merge_config_overrides(environment, tool)

    # Check if this is a curator operation
    if is_curator_operation(operation):
        return _execute_curator_step(
            operation=operation,
            inputs=inputs,
            config_overrides=config_overrides,
            step_number=step_number,
            config=config,
            failure_policy=failure_policy,
            compact=compact,
            user_overrides=user_overrides,
            step_spec_id=step_spec_id,
        )

    # Standard creator operation execution
    return _execute_creator_step(
        operation=operation,
        inputs=inputs,
        backend=backend,
        config_overrides=config_overrides,
        step_number=step_number,
        config=config,
        failure_policy=failure_policy,
        compact=compact,
        user_overrides=user_overrides,
    )


def _merge_config_overrides(
    environment: str | dict[str, Any] | None,
    tool: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Merge environment and tool overrides into a single dict for hashing."""
    merged: dict[str, Any] = {}
    if environment is not None:
        if isinstance(environment, str):
            merged["environment"] = environment
        else:
            merged["environment"] = environment
    if tool:
        merged["tool"] = tool
    return merged or None


def _execute_curator_step(
    operation: OperationDefinition,
    inputs: Any,
    config_overrides: dict[str, Any] | None = None,
    step_number: int = 0,
    config: PipelineConfig | None = None,
    failure_policy: FailurePolicy = FailurePolicy.CONTINUE,
    compact: bool = True,
    user_overrides: dict[str, Any] | None = None,
    step_spec_id: str | None = None,
) -> StepResult:
    """Execute a curator operation locally without Prefect dispatch.

    Curator ops produce a single ExecutionUnit and run in a subprocess
    for memory isolation, bypassing Prefect's JSONB size limit.

    Args:
        operation: Fully configured curator operation instance.
        inputs: Input specification.
        config_overrides: Merged environment + tool overrides (for hashing only).
        step_number: Pipeline step number.
        config: Pipeline configuration.
        failure_policy: Continue or fail-fast on errors.
        compact: Whether to run Delta Lake compaction.
        user_overrides: User-provided parameter overrides.
        step_spec_id: Pre-computed step spec ID; when provided, reused as
            execution_spec_id and cache check is skipped.

    Returns:
        StepResult with output references and execution metadata.
    """
    timings: dict[str, Any] = {}
    total_start = time.perf_counter()

    # --- resolve_inputs phase ---
    with phase_timer("resolve_inputs", timings):
        resolved_inputs = resolve_inputs(inputs, config.delta_root)
        total_artifacts = sum(len(ids) for ids in resolved_inputs.values())
        if total_artifacts > 0:
            logger.debug(
                "Step %d (%s): resolved %d input artifacts",
                step_number,
                operation.name,
                total_artifacts,
            )

        # Skip downstream execution when upstream produced no artifacts
        if _all_inputs_empty(resolved_inputs):
            logger.debug(
                "Step %d (%s): all input roles are empty — skipping execution.",
                step_number,
                operation.name,
            )
            return build_step_result(
                operation=operation,
                step_number=step_number,
                succeeded_count=0,
                failed_count=0,
                failure_policy=failure_policy,
                metadata={"skipped": True, "skip_reason": "empty_inputs"},
            )

        # Framework pairing for curator ops with group_by
        if operation.group_by is not None:
            from artisan.storage.core.artifact_store import ArtifactStore

            artifact_store = ArtifactStore(config.delta_root)
            paired_inputs, group_ids = group_inputs(
                resolved_inputs, operation.group_by, artifact_store
            )
        else:
            paired_inputs = resolved_inputs
            group_ids = None

    # --- batch_and_cache phase ---
    with phase_timer("batch_and_cache", timings):
        if step_spec_id is not None:
            # Fast path: step-level cache in PipelineManager already validated
            # inputs via step_spec_id. Reuse it directly as execution_spec_id
            # to skip the O(N log N) compute_execution_spec_id call.
            spec_id = step_spec_id
        else:
            # Fallback: direct calls without PipelineManager (tests, standalone)
            merged_params = (
                operation.params.model_dump(mode="json")
                if hasattr(operation, "params")
                else {}
            )
            from artisan.utils.hashing import compute_execution_spec_id

            spec_id = compute_execution_spec_id(
                operation_name=operation.name,
                inputs=paired_inputs,
                params=merged_params,
                config_overrides=config_overrides,
            )
            cache_result = check_cache_for_batch(spec_id, config.delta_root)
            if cache_result is not None:
                logger.info(
                    "Step %d (%s) CACHED — skipping execution",
                    step_number,
                    operation.name,
                )
                cached_count = sum(len(ids) for ids in paired_inputs.values()) or 1
                return build_step_result(
                    operation=operation,
                    step_number=step_number,
                    succeeded_count=cached_count,
                    failed_count=0,
                    failure_policy=failure_policy,
                )

    # Create single ExecutionUnit with all inputs
    unit = ExecutionUnit(
        operation=operation,
        inputs=paired_inputs,
        execution_spec_id=spec_id,
        step_number=step_number,
        group_ids=group_ids,
        user_overrides=user_overrides,
    )

    # --- execute phase ---
    dispatch_error: str | None = None
    with phase_timer("execute", timings):
        # Create RuntimeEnvironment
        runtime_env = _create_runtime_environment(config, operation)

        # Capture before subprocess spawn — needed for failure record on kill
        timestamp_start = datetime.now(UTC)
        params_dict = _get_params(operation)

        # Execute in subprocess for memory isolation
        try:
            staging_result = _run_curator_in_subprocess(unit, runtime_env)
            results = [
                {
                    "success": staging_result.success,
                    "error": staging_result.error,
                    "item_count": (
                        len(staging_result.artifact_ids)
                        if staging_result.success
                        else 1
                    ),
                    "execution_run_ids": [staging_result.execution_run_id],
                }
            ]
            succeeded, failed = aggregate_results(results, failure_policy)

            # Log filter-specific diagnostics
            if operation.name == "filter":
                total_input = len(paired_inputs.get("passthrough", []))
                logger.info(
                    "Step %d (%s): %d/%d artifacts passed (%d filtered out)",
                    step_number,
                    operation.name,
                    succeeded,
                    total_input,
                    total_input - succeeded,
                )
        except BrokenProcessPool:
            error_msg = _format_subprocess_kill_error(unit)
            logger.error("Step %d (%s): %s", step_number, operation.name, error_msg)

            synthetic_run_id = f"killed-{unit.execution_spec_id[:24]}"
            execution_context = build_curator_execution_context(
                execution_run_id=synthetic_run_id,
                execution_spec_id=unit.execution_spec_id,
                step_number=unit.step_number,
                timestamp_start=timestamp_start,
                worker_id=0,
                delta_root_path=runtime_env.delta_root_path,
                staging_root_path=runtime_env.staging_root_path,
                operation=unit.operation,
                compute_backend_name=runtime_env.compute_backend_name,
                shared_filesystem=runtime_env.shared_filesystem,
            )
            staging_result = record_execution_failure(
                execution_context=execution_context,
                error=error_msg,
                inputs=unit.inputs,
                timestamp_end=datetime.now(UTC),
                params=params_dict,
                user_overrides=user_overrides,
                failure_logs_root=runtime_env.failure_logs_root,
            )
            results = [
                {
                    "success": False,
                    "error": error_msg,
                    "item_count": 1,
                    "execution_run_ids": [synthetic_run_id],
                }
            ]
            succeeded, failed = 0, 1
        except RuntimeError:
            raise  # fail_fast — intentional abort
        except Exception as exc:
            dispatch_error = f"{type(exc).__name__}: {exc}"
            logger.error(
                "Dispatch failed for step %d: %s",
                step_number,
                dispatch_error,
            )
            results = []
            succeeded, failed = 0, 1

    # --- verify_staging phase (no-op: curator runs in-process, no NFS delay) ---
    with phase_timer("verify_staging", timings):
        pass

    # --- commit phase ---
    commit_error = None
    with phase_timer("commit", timings):
        if results:
            try:
                from artisan.storage.io.commit import DeltaCommitter

                committer = DeltaCommitter(config.delta_root, config.staging_root)
                committer.commit_all_tables(
                    cleanup_staging=not runtime_env.preserve_staging,
                    step_number=step_number,
                    operation_name=operation.name,
                )
            except Exception as exc:
                commit_error = f"{type(exc).__name__}: {exc}"
                logger.error("Commit failed for step %d: %s", step_number, commit_error)

    # --- compact phase ---
    with phase_timer("compact", timings):
        if results and compact:
            _compact_step_tables(config.delta_root, config.staging_root)

    timings["total"] = round(time.perf_counter() - total_start, 4)
    logger.debug("Curator step %d timings: %s", step_number, timings)

    metadata: dict[str, Any] = {"timings": timings}
    if commit_error:
        metadata["commit_error"] = commit_error
    if dispatch_error:
        metadata["dispatch_error"] = dispatch_error

    return build_step_result(
        operation=operation,
        step_number=step_number,
        succeeded_count=succeeded,
        failed_count=failed,
        failure_policy=failure_policy,
        metadata=metadata,
    )


def _run_curator_in_subprocess(
    unit: ExecutionUnit,
    runtime_env: RuntimeEnvironment,
) -> StagingResult:
    """Run curator flow in a spawned subprocess for memory isolation."""
    ctx = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=1, mp_context=ctx) as pool:
        future = pool.submit(run_curator_flow, unit, runtime_env, 0)
        return future.result()


def _format_subprocess_kill_error(unit: ExecutionUnit) -> str:
    """Build a diagnostic error message when a curator subprocess is killed."""
    child_rusage = resource.getrusage(resource.RUSAGE_CHILDREN)
    peak_rss_mb = child_rusage.ru_maxrss / 1024  # KB → MB on Linux

    parts = [
        "Curator subprocess killed (likely OOM).",
        f"Child peak RSS: {peak_rss_mb:.0f} MB.",
    ]

    try:
        with open("/proc/meminfo") as f:
            meminfo = {}
            for line in f:
                key, _, value = line.partition(":")
                if key in ("MemTotal", "MemAvailable"):
                    meminfo[key] = int(value.strip().split()[0])  # kB
            if "MemTotal" in meminfo:
                total_gb = meminfo["MemTotal"] / 1024 / 1024
                avail_gb = meminfo.get("MemAvailable", 0) / 1024 / 1024
                parts.append(
                    f"System memory: {avail_gb:.1f}/{total_gb:.1f} GB available."
                )
    except OSError:
        pass

    n_inputs = sum(len(ids) for ids in unit.inputs.values())
    parts.append(f"Input artifacts: {n_inputs}.")
    parts.append("Consider reducing input size or increasing available memory.")
    return " ".join(parts)


def _execute_creator_step(
    operation: OperationDefinition,
    inputs: Any,
    backend: BackendBase,
    config_overrides: dict[str, Any] | None = None,
    step_number: int = 0,
    config: PipelineConfig | None = None,
    failure_policy: FailurePolicy = FailurePolicy.CONTINUE,
    compact: bool = True,
    user_overrides: dict[str, Any] | None = None,
) -> StepResult:
    """Execute a creator operation step by dispatching to workers via Prefect.

    Args:
        operation: Fully configured creator operation instance.
        inputs: Input specification.
        backend: Backend for worker dispatch.
        config_overrides: Merged environment + tool overrides (for hashing only).
        step_number: Pipeline step number.
        config: Pipeline configuration.
        failure_policy: Continue or fail-fast on errors.
        compact: Whether to run Delta Lake compaction.
        user_overrides: User-provided parameter overrides.

    Returns:
        StepResult with output references and execution metadata.
    """
    timings: dict[str, Any] = {}
    total_start = time.perf_counter()

    # =========================================================================
    # PHASE 1: DISPATCH
    # =========================================================================

    # --- resolve_inputs phase ---
    with phase_timer("resolve_inputs", timings):
        # Resolve inputs to artifact IDs
        resolved_inputs = resolve_inputs(inputs, config.delta_root)

        # Skip downstream execution when upstream produced no artifacts
        if _all_inputs_empty(resolved_inputs):
            logger.debug(
                "Step %d (%s): all input roles are empty — skipping execution.",
                step_number,
                operation.name,
            )
            return build_step_result(
                operation=operation,
                step_number=step_number,
                succeeded_count=0,
                failed_count=0,
                failure_policy=failure_policy,
                metadata={"skipped": True, "skip_reason": "empty_inputs"},
            )

        total_artifacts = sum(len(ids) for ids in resolved_inputs.values())
        logger.debug(
            "Step %d (%s): resolved %d input artifacts",
            step_number,
            operation.name,
            total_artifacts,
        )

        # Framework pairing for multi-input creator ops with group_by
        if operation.group_by is not None:
            from artisan.storage.core.artifact_store import ArtifactStore

            artifact_store = ArtifactStore(config.delta_root)
            paired_inputs, group_ids = group_inputs(
                resolved_inputs, operation.group_by, artifact_store
            )
        else:
            paired_inputs = resolved_inputs
            group_ids = None

    # --- batch_and_cache phase ---
    with phase_timer("batch_and_cache", timings):
        # Get batch configuration from the instance
        batch_config = get_batch_config(operation)

        # Get params for spec_id computation
        merged_params = (
            operation.params.model_dump(mode="json")
            if hasattr(operation, "params")
            else {}
        )

        # Import lazily to avoid package import cycles during module initialization.
        from artisan.utils.hashing import compute_execution_spec_id

        # Generate ExecutionUnit batches (Level 1)
        execution_unit_batches = generate_execution_unit_batches(
            paired_inputs, batch_config, group_ids=group_ids
        )

        # Create ExecutionUnits with cache checking
        units_to_dispatch: list[ExecutionUnit] = []
        cached_count = 0
        cached_units = 0

        for execution_unit_inputs, batch_group_ids in execution_unit_batches:
            # Compute spec_id for cache lookup
            spec_id = compute_execution_spec_id(
                operation_name=operation.name,
                inputs=execution_unit_inputs,
                params=merged_params,
                config_overrides=config_overrides,
            )

            # Cache lookup
            cache_result = check_cache_for_batch(spec_id, config.delta_root)

            if cache_result is not None:
                # Cache hit - skip this unit
                cached_count += (
                    sum(len(ids) for ids in execution_unit_inputs.values()) or 1
                )
                cached_units += 1
                continue

            # Cache miss - create ExecutionUnit with operation instance
            unit = ExecutionUnit(
                operation=operation,
                inputs=execution_unit_inputs,
                execution_spec_id=spec_id,
                step_number=step_number,
                group_ids=batch_group_ids,
                user_overrides=user_overrides,
            )
            units_to_dispatch.append(unit)

    total_units = len(units_to_dispatch) + cached_units
    logger.debug(
        "Step %d (%s): %d artifacts -> %d execution units",
        step_number,
        operation.name,
        total_artifacts,
        total_units,
    )
    if cached_units > 0:
        logger.debug(
            "Step %d (%s): %d units cached, %d to dispatch",
            step_number,
            operation.name,
            cached_units,
            len(units_to_dispatch),
        )

    # =========================================================================
    # PHASE 2: EXECUTE (via Prefect)
    # =========================================================================

    dispatch_dir = config.staging_root / "_dispatch"
    try:
        # --- execute phase ---
        dispatch_error: str | None = None
        with phase_timer("execute", timings):
            # Create RuntimeEnvironment with backend traits
            runtime_env = _create_runtime_environment(config, operation, backend)

            succeeded = 0
            failed = 0

            if units_to_dispatch:
                try:
                    backend.validate_operation(operation)
                    units_path = _save_units(
                        units_to_dispatch, config.staging_root, step_number
                    )
                    step_flow = backend.create_flow(
                        operation.resources,
                        operation.execution,
                        step_number,
                        job_name=operation.execution.job_name or operation.name,
                    )
                    results = step_flow(
                        units_path=str(units_path), runtime_env=runtime_env
                    )
                    succeeded, failed = aggregate_results(results, failure_policy)
                except RuntimeError:
                    raise  # fail_fast — intentional abort
                except Exception as exc:
                    dispatch_error = f"{type(exc).__name__}: {exc}"
                    logger.error(
                        "Dispatch failed for step %d: %s",
                        step_number,
                        dispatch_error,
                    )
                    results = []
                    succeeded, failed = 0, len(units_to_dispatch)

        # --- verify_staging phase ---
        with phase_timer("verify_staging", timings):
            if (
                units_to_dispatch
                and backend.orchestrator_traits.needs_staging_verification
            ):
                execution_run_ids = extract_execution_run_ids(results)
                try:
                    await_staging_files(
                        staging_root=config.staging_root,
                        execution_run_ids=execution_run_ids,
                        timeout_seconds=backend.orchestrator_traits.staging_verification_timeout,
                        step_number=step_number,
                        operation_name=operation.name,
                    )
                except TimeoutError:
                    logger.warning(
                        "Staging file verification timed out for step %d (%s). "
                        "Proceeding to commit with available files.",
                        step_number,
                        operation.name,
                    )

        # --- capture_logs phase ---
        with phase_timer("capture_logs", timings):
            if units_to_dispatch:
                backend.capture_logs(
                    results,
                    config.staging_root,
                    runtime_env.failure_logs_root,
                    operation.name,
                )

        # =====================================================================
        # PHASE 3: COMMIT
        # =====================================================================

        # --- commit phase ---
        commit_error = None
        with phase_timer("commit", timings):
            if units_to_dispatch:
                try:
                    from artisan.storage.io.commit import DeltaCommitter

                    committer = DeltaCommitter(config.delta_root, config.staging_root)
                    committer.commit_all_tables(
                        cleanup_staging=not runtime_env.preserve_staging,
                        step_number=step_number,
                        operation_name=operation.name,
                    )
                except Exception as exc:
                    commit_error = f"{type(exc).__name__}: {exc}"
                    logger.error(
                        "Commit failed for step %d: %s", step_number, commit_error
                    )

        # --- compact phase ---
        with phase_timer("compact", timings):
            if units_to_dispatch and compact:
                _compact_step_tables(config.delta_root, config.staging_root)
    finally:
        if dispatch_dir.exists():
            shutil.rmtree(dispatch_dir, ignore_errors=True)

    timings["total"] = round(time.perf_counter() - total_start, 4)
    logger.debug("Creator step %d timings: %s", step_number, timings)

    # Build and return StepResult
    metadata: dict[str, Any] = {"timings": timings}
    if commit_error:
        metadata["commit_error"] = commit_error
    if dispatch_error:
        metadata["dispatch_error"] = dispatch_error

    return build_step_result(
        operation=operation,
        step_number=step_number,
        succeeded_count=succeeded + cached_count,
        failed_count=failed,
        failure_policy=failure_policy,
        metadata=metadata,
    )


def execute_chain_step(
    operations: list[tuple[type, dict[str, Any] | None, dict[str, Any] | None]],
    role_mappings: list[dict[str, str] | None],
    inputs: Any,
    backend: BackendBase,
    chain_resources: Any,  # ResourceConfig
    chain_execution: Any,  # ExecutionConfig
    intermediates: Any,  # ChainIntermediates
    step_number: int = 0,
    config: PipelineConfig | None = None,
    failure_policy: FailurePolicy = FailurePolicy.CONTINUE,
    compact: bool = True,
    final_operation: type | None = None,
) -> StepResult:
    """Execute a chain of creator operations as a single pipeline step.

    Builds an ExecutionChain, dispatches it through the backend, and handles
    commit and compaction — mirroring _execute_creator_step for single ops.

    Args:
        operations: List of (op_class, params, config) tuples.
        role_mappings: Role remappings between adjacent operations.
        inputs: Initial inputs (same formats as execute_step).
        backend: Backend for worker dispatch.
        chain_resources: Chain-level ResourceConfig.
        chain_execution: Chain-level ExecutionConfig.
        intermediates: ChainIntermediates enum value.
        step_number: Pipeline step number.
        config: Pipeline configuration.
        failure_policy: Continue or fail-fast on errors.
        compact: Whether to run Delta Lake compaction.
        final_operation: Last operation class (for output metadata).

    Returns:
        StepResult with output references and execution metadata.
    """
    from artisan.execution.models.execution_chain import ExecutionChain
    from artisan.execution.models.execution_unit import ExecutionUnit
    from artisan.orchestration.engine.inputs import resolve_inputs
    from artisan.utils.hashing import compute_execution_spec_id

    timings: dict[str, Any] = {}
    total_start = time.perf_counter()

    # --- resolve_inputs phase ---
    with phase_timer("resolve_inputs", timings):
        resolved_inputs = resolve_inputs(inputs, config.delta_root)

        if _all_inputs_empty(resolved_inputs):
            logger.debug(
                "Step %d (chain): all input roles are empty — skipping.",
                step_number,
            )
            final_op = final_operation or operations[-1][0]
            return build_step_result(
                operation=instantiate_operation(final_op, None),
                step_number=step_number,
                succeeded_count=0,
                failed_count=0,
                failure_policy=failure_policy,
                metadata={"skipped": True, "skip_reason": "empty_inputs"},
            )

    # --- build_chain phase ---
    with phase_timer("build_chain", timings):
        units: list[ExecutionUnit] = []
        for i, (op_class, params, command) in enumerate(operations):
            operation = instantiate_operation(op_class, params, command=command)
            merged_params = (
                operation.params.model_dump(mode="json")
                if hasattr(operation, "params")
                else {}
            )
            spec_id = compute_execution_spec_id(
                operation_name=operation.name,
                inputs=resolved_inputs if i == 0 else {},
                params=merged_params,
            )
            unit = ExecutionUnit(
                operation=operation,
                inputs=resolved_inputs if i == 0 else {},
                execution_spec_id=spec_id,
                step_number=step_number,
            )
            units.append(unit)

        chain = ExecutionChain(
            operations=units,
            role_mappings=role_mappings,
            resources=chain_resources,
            execution=chain_execution,
            intermediates=intermediates,
        )

    # The chain executor stages using the final operation's name
    final_op_instance = units[-1].operation
    chain_op_name = final_op_instance.name

    # --- execute phase ---
    dispatch_error: str | None = None
    dispatch_dir = config.staging_root / "_dispatch"
    try:
        with phase_timer("execute", timings):
            runtime_env = _create_runtime_environment(
                config, final_op_instance, backend
            )

            succeeded = 0
            failed = 0

            try:
                backend.validate_operation(final_op_instance)
                units_path = _save_units([chain], config.staging_root, step_number)
                step_flow = backend.create_flow(
                    chain_resources,
                    chain_execution,
                    step_number,
                    job_name=chain_execution.job_name or "chain",
                )
                results = step_flow(units_path=str(units_path), runtime_env=runtime_env)
                succeeded, failed = aggregate_results(results, failure_policy)
            except RuntimeError:
                raise
            except Exception as exc:
                dispatch_error = f"{type(exc).__name__}: {exc}"
                logger.error(
                    "Chain dispatch failed for step %d: %s",
                    step_number,
                    dispatch_error,
                )
                results = []
                succeeded, failed = 0, 1

        # --- verify_staging phase ---
        with phase_timer("verify_staging", timings):
            if backend.orchestrator_traits.needs_staging_verification:
                execution_run_ids = extract_execution_run_ids(results)
                try:
                    await_staging_files(
                        staging_root=config.staging_root,
                        execution_run_ids=execution_run_ids,
                        timeout_seconds=backend.orchestrator_traits.staging_verification_timeout,
                        step_number=step_number,
                        operation_name=chain_op_name,
                    )
                except TimeoutError:
                    logger.warning(
                        "Staging verification timed out for chain step %d.",
                        step_number,
                    )

        # --- commit phase ---
        commit_error = None
        with phase_timer("commit", timings):
            try:
                from artisan.storage.io.commit import DeltaCommitter

                committer = DeltaCommitter(config.delta_root, config.staging_root)
                committer.commit_all_tables(
                    cleanup_staging=not runtime_env.preserve_staging,
                    step_number=step_number,
                    operation_name=chain_op_name,
                )
            except Exception as exc:
                commit_error = f"{type(exc).__name__}: {exc}"
                logger.error(
                    "Commit failed for chain step %d: %s", step_number, commit_error
                )

        # --- compact phase ---
        with phase_timer("compact", timings):
            if compact:
                _compact_step_tables(config.delta_root, config.staging_root)
    finally:
        if dispatch_dir.exists():
            shutil.rmtree(dispatch_dir, ignore_errors=True)

    timings["total"] = round(time.perf_counter() - total_start, 4)

    metadata: dict[str, Any] = {"timings": timings}
    if commit_error:
        metadata["commit_error"] = commit_error
    if dispatch_error:
        metadata["dispatch_error"] = dispatch_error

    final_op_instance = units[-1].operation
    return build_step_result(
        operation=final_op_instance,
        step_number=step_number,
        succeeded_count=succeeded,
        failed_count=failed,
        failure_policy=failure_policy,
        metadata=metadata,
    )


def _compact_step_tables(
    delta_root: Path,
    staging_root: Path,
    tables: list[str] | None = None,
) -> None:
    """Compact Delta Lake tables to merge small parquet files.

    Args:
        delta_root: Root path for Delta Lake tables.
        staging_root: Root staging directory (for DeltaCommitter init).
        tables: Specific tables to compact. If None, compact all.
    """
    from artisan.storage.io.commit import DeltaCommitter

    committer = DeltaCommitter(delta_root, staging_root)

    if tables is None:
        from artisan.schemas.artifact.registry import ArtifactTypeDef

        artifact_tables = [td.table_path for td in ArtifactTypeDef.get_all().values()]
        tables = [
            *artifact_tables,
            TablePath.ARTIFACT_INDEX.value,
            TablePath.EXECUTIONS.value,
        ]

    for table in tables:
        table_name = Path(table).name
        try:
            committer.compact_table(table)
        except Exception as exc:
            logger.warning(
                "Compaction failed for table %s: %s: %s",
                table_name,
                type(exc).__name__,
                exc,
            )
