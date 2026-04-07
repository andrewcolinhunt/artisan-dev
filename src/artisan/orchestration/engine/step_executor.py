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
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fsspec import AbstractFileSystem

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
from artisan.orchestration.engine.inputs import resolve_inputs
from artisan.orchestration.engine.results import (
    aggregate_results,
    extract_execution_run_ids,
)
from artisan.schemas.enums import FailurePolicy, TablePath
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.schemas.orchestration.step_result import StepResult, StepResultBuilder
from artisan.storage.cache.cache_lookup import CacheHit, cache_lookup
from artisan.storage.io.staging_verification import await_staging_files
from artisan.utils.hashing import serialize_params
from artisan.utils.spawn import suppress_main_reimport
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
    config: PipelineConfig | None = None,
) -> CacheHit | None:
    """Check if an ExecutionUnit can be skipped due to cache hit.

    Args:
        execution_spec_id: Deterministic hash for the batch.
        delta_root: Root path for Delta Lake tables.
        config: Pipeline config for storage backend. When None,
            uses local filesystem defaults.

    Returns:
        CacheHit if a successful execution exists, None otherwise.
    """
    from artisan.schemas.execution.storage_config import StorageConfig

    storage = config.storage if config is not None else StorageConfig()
    fs = storage.filesystem()
    storage_options = storage.delta_storage_options()

    executions_path = str(delta_root / TablePath.EXECUTIONS)
    execution_edges_path = str(delta_root / TablePath.EXECUTION_EDGES)
    result = cache_lookup(
        executions_path,
        execution_spec_id,
        fs=fs,
        storage_options=storage_options,
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
    step_run_id: str | None = None,
) -> StepResult:
    """Build StepResult after step execution completes.

    Args:
        operation: OperationDefinition class or instance.
        step_number: Pipeline step number.
        succeeded_count: Number of items that succeeded.
        failed_count: Number of items that failed.
        failure_policy: Failure handling policy enum.
        metadata: Optional metadata dict (timings, diagnostics, etc.).
        step_run_id: Unique ID for this step attempt.

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
        step_run_id=step_run_id,
    )

    builder.add_success(succeeded_count)
    builder.add_failure(failed_count)

    # With fail_fast, any failure means step failure
    success_override = None
    if failure_policy == FailurePolicy.FAIL_FAST and failed_count > 0:
        success_override = False

    return builder.build(success_override=success_override, metadata=metadata)


def _cancelled_result(
    operation: type[OperationDefinition] | OperationDefinition,
    step_number: int,
    failure_policy: FailurePolicy,
) -> StepResult:
    """Build a StepResult indicating the step was cancelled before completion."""
    return build_step_result(
        operation=operation,
        step_number=step_number,
        succeeded_count=0,
        failed_count=0,
        failure_policy=failure_policy,
        metadata={"cancelled": True},
    )


def _all_inputs_empty(resolved_inputs: dict[str, list[str]]) -> bool:
    """Return True when every input role resolved to zero artifact IDs.

    An empty dict (generative op with no declared inputs) returns False.
    """
    if not resolved_inputs:
        return False  # {} = generative op, not empty inputs
    return all(len(ids) == 0 for ids in resolved_inputs.values())


def _skip_for_empty_inputs(
    operation: type[OperationDefinition] | OperationDefinition,
    resolved_inputs: dict[str, list[str]],
    step_number: int,
    failure_policy: FailurePolicy,
    *,
    log_label: str = "",
) -> StepResult | None:
    """Return a skip StepResult if all input roles are empty, else None."""
    if not _all_inputs_empty(resolved_inputs):
        return None
    label = log_label or operation.name
    logger.debug(
        "Step %d (%s): all input roles are empty — skipping execution.",
        step_number,
        label,
    )
    return build_step_result(
        operation=operation,
        step_number=step_number,
        succeeded_count=0,
        failed_count=0,
        failure_policy=failure_policy,
        metadata={"skipped": True, "skip_reason": "empty_inputs"},
    )


def _commit_and_compact(
    config: PipelineConfig,
    runtime_env: RuntimeEnvironment,
    step_number: int,
    operation_name: str,
    timings: dict[str, Any],
    *,
    has_work: bool,
    compact: bool,
) -> str | None:
    """Run commit and compact phases, returning any commit error message."""
    commit_error = None
    with phase_timer("commit", timings):
        if has_work:
            try:
                from artisan.storage.io.commit import DeltaCommitter
                from artisan.storage.io.staging import StagingManager

                fs = config.storage.filesystem()
                storage_options = config.storage.delta_storage_options()
                staging_manager = StagingManager(str(config.staging_root), fs)
                committer = DeltaCommitter(
                    str(config.delta_root),
                    staging_manager,
                    fs=fs,
                    storage_options=storage_options,
                )
                committer.commit_all_tables(
                    cleanup_staging=not runtime_env.preserve_staging,
                    step_number=step_number,
                    operation_name=operation_name,
                )
            except Exception as exc:
                commit_error = f"{type(exc).__name__}: {exc}"
                logger.error("Commit failed for step %d: %s", step_number, commit_error)

    with phase_timer("compact", timings):
        if has_work and compact:
            fs = config.storage.filesystem()
            storage_options = config.storage.delta_storage_options()
            _compact_step_tables(
                config.delta_root,
                config.staging_root,
                fs=fs,
                storage_options=storage_options,
            )

    return commit_error


def _build_step_metadata(
    timings: dict[str, Any],
    commit_error: str | None,
    dispatch_error: str | None,
) -> dict[str, Any]:
    """Build the metadata dict for a StepResult."""
    metadata: dict[str, Any] = {"timings": timings}
    if commit_error:
        metadata["commit_error"] = commit_error
    if dispatch_error:
        metadata["dispatch_error"] = dispatch_error
    return metadata


def _handle_dispatch_exception(
    exc: Exception,
    step_number: int,
    *,
    label: str = "Dispatch",
    unit_count: int = 1,
) -> tuple[str, list, int, int]:
    """Handle a generic dispatch exception; returns (error, results, succeeded, failed)."""
    dispatch_error = f"{type(exc).__name__}: {exc}"
    logger.error(
        "%s failed for step %d: %s",
        label,
        step_number,
        dispatch_error,
    )
    return dispatch_error, [], 0, unit_count


def _verify_staging_if_needed(
    backend: BackendBase,
    results: list[UnitResult],
    config: PipelineConfig,
    step_number: int,
    operation_name: str,
    timings: dict[str, Any],
) -> None:
    """Run staging verification when the backend requires it."""
    with phase_timer("verify_staging", timings):
        if backend.orchestrator_traits.needs_staging_verification:
            execution_run_ids = extract_execution_run_ids(results)
            try:
                await_staging_files(
                    staging_root=config.staging_root,
                    execution_run_ids=execution_run_ids,
                    timeout_seconds=backend.orchestrator_traits.staging_verification_timeout,
                    step_number=step_number,
                    operation_name=operation_name,
                )
            except TimeoutError:
                logger.warning(
                    "Staging file verification timed out for step %d (%s). "
                    "Proceeding to commit with available files.",
                    step_number,
                    operation_name,
                )


def _finalize_timings(
    timings: dict[str, Any],
    total_start: float,
    step_number: int,
    label: str,
) -> None:
    """Record total elapsed time and log it."""
    timings["total"] = round(time.perf_counter() - total_start, 4)
    logger.debug("%s step %d timings: %s", label, step_number, timings)


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
        files_root_path=config.files_root,
        failure_logs_root=config.delta_root.parent / "logs" / "failures",
        preserve_staging=config.preserve_staging,
        preserve_working=config.preserve_working,
        worker_id_env_var=backend.worker_traits.worker_id_env_var if backend else None,
        shared_filesystem=backend.worker_traits.shared_filesystem if backend else False,
        compute_backend_name=backend.name if backend else "local",
        storage=config.storage,
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
    cancel_event: threading.Event | None = None,
    skip_cache: bool = False,
    step_run_id: str | None = None,
    step_run_ids: dict[int, str] | None = None,
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
        skip_cache: Bypass execution-level cache lookups.
        step_run_id: Unique ID for this step attempt (for output isolation).
        step_run_ids: Mapping of upstream step_number to step_run_id
            for scoped output resolution.

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
            cancel_event=cancel_event,
            skip_cache=skip_cache,
            step_run_id=step_run_id,
            step_run_ids=step_run_ids,
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
        cancel_event=cancel_event,
        skip_cache=skip_cache,
        step_run_id=step_run_id,
        step_run_ids=step_run_ids,
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
    cancel_event: threading.Event | None = None,
    skip_cache: bool = False,
    step_run_id: str | None = None,
    step_run_ids: dict[int, str] | None = None,
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
        skip_cache: Bypass execution-level cache lookups.
        step_run_id: Unique ID for this step attempt (for output isolation).
        step_run_ids: Upstream step_number to step_run_id mapping.

    Returns:
        StepResult with output references and execution metadata.
    """
    timings: dict[str, Any] = {}
    total_start = time.perf_counter()

    # --- resolve_inputs phase ---
    with phase_timer("resolve_inputs", timings):
        resolved_inputs = resolve_inputs(
            inputs, config.delta_root, step_run_ids=step_run_ids
        )
        total_artifacts = sum(len(ids) for ids in resolved_inputs.values())
        if total_artifacts > 0:
            logger.debug(
                "Step %d (%s): resolved %d input artifacts",
                step_number,
                operation.name,
                total_artifacts,
            )

        skip_result = _skip_for_empty_inputs(
            operation, resolved_inputs, step_number, failure_policy
        )
        if skip_result is not None:
            return skip_result

        # Framework pairing for curator ops with group_by
        if operation.group_by is not None:
            from artisan.storage.core.artifact_store import ArtifactStore

            _fs = config.storage.filesystem()
            _so = config.storage.delta_storage_options()
            artifact_store = ArtifactStore(
                str(config.delta_root),
                fs=_fs,
                storage_options=_so,
                files_root=str(config.files_root) if config.files_root else None,
            )
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
            merged_params = serialize_params(operation)
            from artisan.utils.hashing import compute_execution_spec_id

            spec_id = compute_execution_spec_id(
                operation_name=operation.name,
                inputs=paired_inputs,
                params=merged_params,
                config_overrides=config_overrides,
            )
            if not skip_cache:
                cache_result = check_cache_for_batch(
                    spec_id,
                    config.delta_root,
                    config=config,
                )
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

    # --- cancel check: before execute ---
    if cancel_event is not None and cancel_event.is_set():
        return _cancelled_result(operation, step_number, failure_policy)

    # Create single ExecutionUnit with all inputs
    unit = ExecutionUnit(
        operation=operation,
        inputs=paired_inputs,
        execution_spec_id=spec_id,
        step_number=step_number,
        group_ids=group_ids,
        user_overrides=user_overrides,
        step_run_id=step_run_id,
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
            staging_result = _run_curator_in_subprocess(unit, runtime_env, cancel_event)
            results = [
                UnitResult(
                    success=staging_result.success,
                    error=staging_result.error,
                    item_count=(
                        len(staging_result.artifact_ids)
                        if staging_result.success
                        else 1
                    ),
                    execution_run_ids=[staging_result.execution_run_id],
                )
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
            if cancel_event is not None and cancel_event.is_set():
                error_msg = "Curator subprocess killed during cancellation"
            else:
                error_msg = _format_subprocess_kill_error(unit)
            logger.error("Step %d (%s): %s", step_number, operation.name, error_msg)

            synthetic_run_id = f"killed-{unit.execution_spec_id[:24]}"
            kill_fs = runtime_env.storage.filesystem()
            kill_so = runtime_env.storage.delta_storage_options()
            execution_context = build_curator_execution_context(
                execution_run_id=synthetic_run_id,
                execution_spec_id=unit.execution_spec_id,
                step_number=unit.step_number,
                timestamp_start=timestamp_start,
                worker_id=0,
                delta_root_path=str(runtime_env.delta_root_path),
                staging_root_path=str(runtime_env.staging_root_path),
                fs=kill_fs,
                storage_options=kill_so,
                operation=unit.operation,
                compute_backend_name=runtime_env.compute_backend_name,
                shared_filesystem=runtime_env.shared_filesystem,
                step_run_id=step_run_id,
                files_root=str(runtime_env.files_root_path)
                if runtime_env.files_root_path
                else None,
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
                UnitResult(
                    success=False,
                    error=error_msg,
                    item_count=1,
                    execution_run_ids=[synthetic_run_id],
                )
            ]
            succeeded, failed = 0, 1
        except RuntimeError:
            raise  # fail_fast — intentional abort
        except Exception as exc:
            dispatch_error, results, succeeded, failed = _handle_dispatch_exception(
                exc, step_number
            )

    # --- verify_staging phase (no-op: curator runs in-process, no NFS delay) ---
    with phase_timer("verify_staging", timings):
        pass

    # --- cancel check: before commit ---
    if cancel_event is not None and cancel_event.is_set():
        return _cancelled_result(operation, step_number, failure_policy)

    commit_error = _commit_and_compact(
        config,
        runtime_env,
        step_number,
        operation.name,
        timings,
        has_work=bool(results),
        compact=compact,
    )
    _finalize_timings(timings, total_start, step_number, "Curator")

    return build_step_result(
        operation=operation,
        step_number=step_number,
        succeeded_count=succeeded,
        failed_count=failed,
        failure_policy=failure_policy,
        metadata=_build_step_metadata(timings, commit_error, dispatch_error),
        step_run_id=step_run_id,
    )


def _run_curator_in_subprocess(
    unit: ExecutionUnit,
    runtime_env: RuntimeEnvironment,
    cancel_event: threading.Event | None = None,
) -> StagingResult:
    """Run curator flow in a spawned subprocess for memory isolation."""
    ctx = multiprocessing.get_context("spawn")
    with (
        suppress_main_reimport(),
        ProcessPoolExecutor(max_workers=1, mp_context=ctx) as pool,
    ):
        future = pool.submit(run_curator_flow, unit, runtime_env, 0)
        while True:
            try:
                return future.result(timeout=0.5)
            except TimeoutError as err:
                if cancel_event is not None and cancel_event.is_set():
                    msg = "Curator interrupted by cancellation"
                    raise RuntimeError(msg) from err
                continue


def _format_subprocess_kill_error(unit: ExecutionUnit) -> str:
    """Build a diagnostic error message when a curator subprocess is killed."""
    child_rusage = resource.getrusage(resource.RUSAGE_CHILDREN)
    peak_rss_mb = child_rusage.ru_maxrss / 1024  # KB → MB on Linux

    parts = [
        "Curator subprocess killed (likely OOM).",
        f"Child peak RSS: {peak_rss_mb:.0f} MB.",
    ]

    try:
        with Path("/proc/meminfo").open() as f:
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
    cancel_event: threading.Event | None = None,
    skip_cache: bool = False,
    step_run_id: str | None = None,
    step_run_ids: dict[int, str] | None = None,
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
        skip_cache: Bypass per-batch execution-level cache lookups.
        step_run_id: Unique ID for this step attempt (for output isolation).
        step_run_ids: Upstream step_number to step_run_id mapping.

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
        resolved_inputs = resolve_inputs(
            inputs, config.delta_root, step_run_ids=step_run_ids
        )

        skip_result = _skip_for_empty_inputs(
            operation, resolved_inputs, step_number, failure_policy
        )
        if skip_result is not None:
            return skip_result

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

            _fs = config.storage.filesystem()
            _so = config.storage.delta_storage_options()
            artifact_store = ArtifactStore(
                str(config.delta_root),
                fs=_fs,
                storage_options=_so,
                files_root=str(config.files_root) if config.files_root else None,
            )
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

        merged_params = serialize_params(operation)

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
            cache_result = (
                None
                if skip_cache
                else check_cache_for_batch(spec_id, config.delta_root, config=config)
            )

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
                step_run_id=step_run_id,
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

    # --- cancel check: before execute ---
    if cancel_event is not None and cancel_event.is_set():
        return _cancelled_result(operation, step_number, failure_policy)

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
                    handle = backend.create_dispatch_handle(
                        operation.resources,
                        operation.execution,
                        step_number,
                        job_name=operation.execution.job_name or operation.name,
                        log_folder=config.delta_root.parent / "logs" / "slurm",
                        staging_root=config.staging_root,
                    )
                    results = handle.run(
                        units_to_dispatch, runtime_env, cancel_event=cancel_event
                    )
                    succeeded, failed = aggregate_results(results, failure_policy)
                except BrokenProcessPool:
                    dispatch_error = "Worker process killed (signal or OOM)"
                    logger.warning(
                        "Step %d (%s): %s",
                        step_number,
                        operation.name,
                        dispatch_error,
                    )
                    results = []
                    succeeded, failed = 0, len(units_to_dispatch)
                except RuntimeError:
                    raise  # fail_fast — intentional abort
                except Exception as exc:
                    dispatch_error, results, succeeded, failed = (
                        _handle_dispatch_exception(
                            exc, step_number, unit_count=len(units_to_dispatch)
                        )
                    )

        # --- verify_staging phase ---
        if units_to_dispatch:
            _verify_staging_if_needed(
                backend, results, config, step_number, operation.name, timings
            )
        else:
            with phase_timer("verify_staging", timings):
                pass

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

        # --- cancel check: before commit ---
        if cancel_event is not None and cancel_event.is_set():
            return _cancelled_result(operation, step_number, failure_policy)

        commit_error = _commit_and_compact(
            config,
            runtime_env,
            step_number,
            operation.name,
            timings,
            has_work=bool(units_to_dispatch),
            compact=compact,
        )
    finally:
        if dispatch_dir.exists():
            shutil.rmtree(dispatch_dir, ignore_errors=True)

    _finalize_timings(timings, total_start, step_number, "Creator")

    return build_step_result(
        operation=operation,
        step_number=step_number,
        succeeded_count=succeeded + cached_count,
        failed_count=failed,
        failure_policy=failure_policy,
        metadata=_build_step_metadata(timings, commit_error, dispatch_error),
        step_run_id=step_run_id,
    )


def execute_composite_step(
    composite_class: type,
    inputs: Any,
    params: dict[str, Any] | None,
    backend: BackendBase,
    composite_resources: Any,  # ResourceConfig
    composite_execution: Any,  # ExecutionConfig
    intermediates: Any,  # CompositeIntermediates
    step_number: int = 0,
    config: PipelineConfig | None = None,
    failure_policy: FailurePolicy = FailurePolicy.CONTINUE,
    compact: bool = True,
    step_run_ids: dict[int, str] | None = None,
    step_run_id: str | None = None,
) -> StepResult:
    """Execute a composite operation as a single pipeline step.

    Builds an ExecutionComposite, dispatches it through the backend, and
    handles commit and compaction.

    Args:
        composite_class: CompositeDefinition subclass.
        inputs: Initial inputs (same formats as execute_step).
        params: Parameter overrides.
        backend: Backend for worker dispatch.
        composite_resources: Composite-level ResourceConfig.
        composite_execution: Composite-level ExecutionConfig.
        intermediates: CompositeIntermediates enum value.
        step_number: Pipeline step number.
        config: Pipeline configuration.
        failure_policy: Continue or fail-fast on errors.
        compact: Whether to run Delta Lake compaction.
        step_run_ids: Upstream step_number to step_run_id mapping.
        step_run_id: Unique ID for this step attempt.

    Returns:
        StepResult with output references and execution metadata.
    """
    from artisan.execution.models.execution_composite import ExecutionComposite
    from artisan.utils.hashing import compute_execution_spec_id

    timings: dict[str, Any] = {}
    total_start = time.perf_counter()

    # --- resolve_inputs phase ---
    with phase_timer("resolve_inputs", timings):
        resolved_inputs = resolve_inputs(
            inputs, config.delta_root, step_run_ids=step_run_ids
        )

        instance = instantiate_operation(composite_class, params)
        skip_result = _skip_for_empty_inputs(
            instance,
            resolved_inputs,
            step_number,
            failure_policy,
            log_label="composite",
        )
        if skip_result is not None:
            return skip_result

    # --- build_composite phase ---
    with phase_timer("build_composite", timings):
        merged_params = serialize_params(instance)

        spec_id = compute_execution_spec_id(
            operation_name=instance.name,
            inputs=resolved_inputs,
            params=merged_params,
        )

        composite_transport = ExecutionComposite(
            composite=instance,
            inputs=resolved_inputs,
            step_number=step_number,
            execution_spec_id=spec_id,
            resources=composite_resources,
            execution=composite_execution,
            intermediates=intermediates,
            step_run_id=step_run_id,
        )

    # --- execute phase ---
    dispatch_error: str | None = None
    dispatch_dir = config.staging_root / "_dispatch"
    try:
        with phase_timer("execute", timings):
            runtime_env = _create_runtime_environment(config, instance, backend)

            succeeded = 0
            failed = 0

            try:
                handle = backend.create_dispatch_handle(
                    composite_resources,
                    composite_execution,
                    step_number,
                    job_name=composite_execution.job_name or "composite",
                    log_folder=config.delta_root.parent / "logs" / "slurm",
                    staging_root=config.staging_root,
                )
                results = handle.run([composite_transport], runtime_env)
                succeeded, failed = aggregate_results(results, failure_policy)
            except RuntimeError:
                raise
            except Exception as exc:
                dispatch_error, results, succeeded, failed = _handle_dispatch_exception(
                    exc, step_number, label="Composite dispatch"
                )

        _verify_staging_if_needed(
            backend, results, config, step_number, instance.name, timings
        )

        commit_error = _commit_and_compact(
            config,
            runtime_env,
            step_number,
            instance.name,
            timings,
            has_work=True,
            compact=compact,
        )
    finally:
        if dispatch_dir.exists():
            shutil.rmtree(dispatch_dir, ignore_errors=True)

    _finalize_timings(timings, total_start, step_number, "Composite")

    return build_step_result(
        operation=instance,
        step_number=step_number,
        succeeded_count=succeeded,
        failed_count=failed,
        failure_policy=failure_policy,
        metadata=_build_step_metadata(timings, commit_error, dispatch_error),
        step_run_id=step_run_id,
    )


def _compact_step_tables(
    delta_root: Path,
    staging_root: Path,
    tables: list[str] | None = None,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict[str, str] | None = None,
) -> None:
    """Compact Delta Lake tables to merge small parquet files.

    Args:
        delta_root: Root path for Delta Lake tables.
        staging_root: Root staging directory (for DeltaCommitter init).
        tables: Specific tables to compact. If None, compact all.
        fs: Filesystem implementation. Uses local filesystem if None.
        storage_options: Delta-rs storage options.
    """
    from artisan.storage.io.commit import DeltaCommitter
    from artisan.storage.io.staging import StagingManager

    if fs is None:
        from fsspec.implementations.local import LocalFileSystem

        fs = LocalFileSystem()

    staging_manager = StagingManager(str(staging_root), fs)
    committer = DeltaCommitter(
        str(delta_root),
        staging_manager,
        fs=fs,
        storage_options=storage_options,
    )

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
