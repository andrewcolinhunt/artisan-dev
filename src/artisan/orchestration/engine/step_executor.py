"""Main coordinator for step execution.

Ties together the three-phase workflow (dispatch, execute, commit) for
creator and curator operations.
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import resource
import threading
import time
from concurrent.futures import ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast

from fsspec import AbstractFileSystem
from pydantic import BaseModel

from artisan.execution.context.builder import build_execution_context
from artisan.execution.executors.curator import (
    _get_params,
    is_curator_operation,
    run_curator_flow,
)
from artisan.execution.inputs.grouping import group_inputs
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.execution.recording.recorder import record_execution_failure
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.engine.batching import (
    generate_execution_unit_batches,
    get_batch_config,
)
from artisan.orchestration.engine.inputs import resolve_inputs
from artisan.orchestration.engine.lifecycle_router import LifecycleRouter
from artisan.orchestration.engine.results import (
    aggregate_results,
    extract_execution_run_ids,
    raise_if_fail_fast,
)
from artisan.orchestration.engine.worker_logs import persist_worker_logs
from artisan.orchestration.runners.base import RunnerBase
from artisan.schemas.enums import FailurePolicy, TablePath
from artisan.schemas.execution.cache_result import CacheHit
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.operation_config.compute import ComputeProvider
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.schemas.orchestration.step_overrides import StepOverrides
from artisan.schemas.orchestration.step_result import StepResult, StepResultBuilder
from artisan.storage.cache.cache_lookup import cache_lookup
from artisan.storage.io.staging_verification import await_staging_files
from artisan.utils.hashing import effective_config_payload, serialize_params
from artisan.utils.path import uri_join, uri_parent
from artisan.utils.process_call import execute_process_call, serialize_process_call
from artisan.utils.spawn import suppress_main_reimport
from artisan.utils.timing import phase_timer

logger = logging.getLogger(__name__)


def _deep_merge_model[ModelT: BaseModel](
    base_model: BaseModel,
    override: dict[str, Any],
    model_cls: type[ModelT],
) -> ModelT:
    """Deep-merge a dict override onto a Pydantic model.

    Dumps ``base_model``, shallow-merges each nested dict from ``override``
    (so a partial nested dict keeps its sibling fields), then re-validates
    through ``model_cls`` — this coerces nested dicts into their proper
    sub-models even when the base field was ``None``.

    Args:
        base_model: The operation default to merge onto.
        override: Overrides, whose top-level dict values merge into the base.
        model_cls: Model class to validate the merged mapping through.

    Returns:
        A new ``model_cls`` instance with the override applied.
    """
    base = base_model.model_dump()
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key] = {**base[key], **value}
        else:
            base[key] = value
    return model_cls.model_validate(base)


def instantiate_operation(
    operation_class: type[OperationDefinition],
    ov: StepOverrides,
) -> OperationDefinition:
    """Construct an operation instance from a class and coerced overrides.

    Applies ``ov``'s params and per-step config overrides (runner resources,
    batch strategy, environment, tool, compute provider, compute resources,
    group_by) onto the class default. String overrides select the active
    provider/environment; dicts delta-merge into the class default; typed
    models replace it outright.

    Args:
        operation_class: The operation class to instantiate.
        ov: The coerced per-step overrides.

    Returns:
        Fully configured operation instance.
    """
    params = ov.params
    runner_resources = ov.runner_resources
    batch_strategy = ov.batch_strategy
    environment = ov.environment
    tool = ov.tool
    compute_provider = ov.compute_provider
    compute_resources = ov.compute_resources
    group_by = ov.group_by

    init_kwargs: dict[str, Any] = {}

    if params:
        if "params" in operation_class.model_fields:
            # New-style: wrap user params into the params sub-model
            params_cls = operation_class.model_fields["params"].annotation
            init_kwargs["params"] = params_cls(**params)  # type: ignore[misc]  # pydantic field annotation is non-None at runtime
        else:
            # Flat fields
            init_kwargs.update(params)

    instance = operation_class(**init_kwargs)

    # Apply overrides via model_copy. Each override accepts either a
    # dict (delta-merged into the operation default) or a typed model
    # (replaces the default outright — already validated by construction).
    from artisan.schemas.execution.batch_strategy import BatchStrategy as _BatchStrategy
    from artisan.schemas.operation_config.runner_resources import (
        RunnerResources as _RunnerResources,
    )

    updates: dict[str, Any] = {}
    if runner_resources:
        if isinstance(runner_resources, _RunnerResources):
            updates["runner_resources"] = runner_resources
        else:
            updates["runner_resources"] = instance.runner_resources.model_copy(
                update=runner_resources
            )
    if batch_strategy:
        if isinstance(batch_strategy, _BatchStrategy):
            updates["batch_strategy"] = batch_strategy
        else:
            updates["batch_strategy"] = instance.batch_strategy.model_copy(
                update=batch_strategy
            )
    if tool and instance.tool is not None:
        from artisan.schemas.operation_config.tool_spec import ToolSpec

        if isinstance(tool, ToolSpec):
            updates["tool"] = tool
        else:
            updates["tool"] = instance.tool.model_copy(update=tool)
    if environment is not None:
        if isinstance(environment, str):
            updates["environments"] = instance.environments.model_copy(
                update={"active": environment}
            )
        elif isinstance(environment, Environments):
            updates["environments"] = environment
        else:
            updates["environments"] = _deep_merge_model(
                instance.environments, environment, Environments
            )
    if compute_provider is not None:
        if isinstance(compute_provider, str):
            updates["compute_provider"] = instance.compute_provider.model_copy(
                update={"active": compute_provider}
            )
        elif isinstance(compute_provider, ComputeProvider):
            updates["compute_provider"] = compute_provider
        else:
            updates["compute_provider"] = _deep_merge_model(
                instance.compute_provider, compute_provider, ComputeProvider
            )
    if compute_resources is not None:
        from artisan.schemas.operation_config.compute_resources import ComputeResources

        if isinstance(compute_resources, ComputeResources):
            updates["compute_resources"] = compute_resources
        else:
            updates["compute_resources"] = instance.compute_resources.model_copy(
                update=compute_resources
            )
    if group_by is not None:
        updates["group_by"] = group_by
    if updates:
        instance = instance.model_copy(update=updates)

    return instance


def check_cache_for_batch(
    execution_spec_id: str,
    delta_root: str,
    config: PipelineConfig | None = None,
) -> CacheHit | None:
    """Check if an ExecutionUnit can be skipped due to cache hit.

    Args:
        execution_spec_id: Deterministic hash for the batch.
        delta_root: Root URI for Delta Lake tables.
        config: Pipeline config for the storage backend. When None,
            uses local filesystem defaults.

    Returns:
        CacheHit if a successful execution exists, None otherwise.
    """
    from artisan.schemas.execution.storage_config import StorageConfig

    storage = config.storage if config is not None else StorageConfig()
    fs = storage.filesystem()
    storage_options = storage.delta_storage_options()

    executions_path = uri_join(delta_root, TablePath.EXECUTIONS)
    result = cache_lookup(
        executions_path,
        execution_spec_id,
        fs=fs,
        storage_options=storage_options,
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
    output_roles: dict[str, str | None] = {}
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
                staging_manager = StagingManager(config.staging_root, fs)
                committer = DeltaCommitter(
                    config.delta_root,
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
) -> tuple[str, list[UnitResult], int, int]:
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
    step_runner: RunnerBase,
    results: list[UnitResult],
    config: PipelineConfig,
    step_number: int,
    operation_name: str,
    timings: dict[str, Any],
) -> None:
    """Run staging verification when the step_runner requires it."""
    with phase_timer("verify_staging", timings):
        if step_runner.orchestrator_traits.needs_staging_verification:
            execution_run_ids = extract_execution_run_ids(results)
            try:
                await_staging_files(
                    staging_root=config.staging_root,
                    execution_run_ids=execution_run_ids,
                    timeout_seconds=step_runner.orchestrator_traits.staging_verification_timeout,
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
    step_runner: RunnerBase | None = None,
) -> RuntimeEnvironment:
    """Build a RuntimeEnvironment from pipeline config and step_runner traits."""
    # Curator operations don't need a sandbox (no materialization)
    is_curator = is_curator_operation(operation)

    # failure_logs_root must be local (recorder._write_failure_log uses
    # os.makedirs/open). For local delta_root keep the historical
    # sibling-of-delta layout. For cloud delta_root derive from
    # working_root, which RuntimeEnvironment already declares local.
    if config.storage.is_local:
        failure_logs_root = uri_join(uri_parent(config.delta_root), "logs", "failures")
    else:
        failure_logs_root = os.path.join(config.working_root, "logs", "failures")

    return RuntimeEnvironment(
        delta_root=config.delta_root,
        staging_root=config.staging_root,
        working_root=None if is_curator else config.working_root,
        files_root=config.files_root,
        failure_logs_root=failure_logs_root,
        preserve_staging=config.preserve_staging,
        preserve_working=config.preserve_working,
        worker_id_env_var=step_runner.worker_traits.worker_id_env_var
        if step_runner
        else None,
        shared_filesystem=step_runner.worker_traits.shared_filesystem
        if step_runner
        else False,
        compute_backend_name=step_runner.name if step_runner else "local",
        storage=config.storage,
    )


def execute_step(
    operation_class: type[OperationDefinition],
    inputs: Any,
    ov: StepOverrides,
    step_runner: RunnerBase,
    *,
    step_number: int = 0,
    config: PipelineConfig | None = None,
    step_spec_id: str | None = None,
    cancel_event: threading.Event | None = None,
    step_run_id: str | None = None,
    step_run_ids: dict[int, str] | None = None,
) -> StepResult:
    """Execute a single pipeline step.

    This is the main entry point called by PipelineManager.run().
    It coordinates the three-phase workflow: dispatch, execute, commit.

    For curator operations (Merge, Filter), a separate execution
    path is used that executes locally without worker dispatch.

    Args:
        operation_class: OperationDefinition subclass to execute.
        inputs: Input specification (see PipelineManager.run() for formats).
        ov: Coerced per-step overrides (params + cache/runtime knobs).
        step_runner: Resolved backend to use for execution.
        step_number: Pipeline step number.
        config: Pipeline configuration.
        step_spec_id: Pre-computed step spec ID from PipelineManager. When
            provided for curator ops, used directly as execution_spec_id to
            skip the O(N log N) compute_execution_spec_id call.
        cancel_event: Set to request cooperative cancellation between phases.
        step_run_id: Unique ID for this step attempt (for output isolation).
        step_run_ids: Mapping of upstream step_number to step_run_id
            for scoped output resolution.

    Returns:
        StepResult with output references and execution metadata.
    """
    operation = instantiate_operation(operation_class, ov)
    user_overrides = ov.params or {}

    # Cache-affecting config (environment, tool, compute_provider,
    # compute_resources, group_by, version) read off the instantiated op —
    # class defaults + applied overrides — folded into config_overrides for
    # hashing, symmetric with the merged-params channel.
    config_overrides = effective_config_payload(operation)

    # Resolve runtime knobs against pipeline defaults: ov carries the raw
    # per-step values; an unset one falls back to config.
    failure_policy = (
        ov.failure_policy
        if ov.failure_policy is not None
        else (config.failure_policy if config is not None else FailurePolicy.CONTINUE)
    )
    skip_cache = ov.skip_cache or (config.skip_cache if config is not None else False)

    # Check if this is a curator operation
    if is_curator_operation(operation):
        return _execute_curator_step(
            operation=operation,
            inputs=inputs,
            config_overrides=config_overrides,
            step_number=step_number,
            config=config,
            failure_policy=failure_policy,
            compact=ov.compact,
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
        step_runner=step_runner,
        config_overrides=config_overrides,
        step_number=step_number,
        config=config,
        failure_policy=failure_policy,
        compact=ov.compact,
        user_overrides=user_overrides,
        cancel_event=cancel_event,
        skip_cache=skip_cache,
        step_run_id=step_run_id,
        step_run_ids=step_run_ids,
    )


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
    """Execute a curator operation locally in an isolated subprocess.

    Curator ops produce a single ExecutionUnit and use a subprocess for
    memory isolation.

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
        cancel_event: Set to request cooperative cancellation between phases.
        skip_cache: Bypass execution-level cache lookups.
        step_run_id: Unique ID for this step attempt (for output isolation).
        step_run_ids: Upstream step_number to step_run_id mapping.

    Returns:
        StepResult with output references and execution metadata.
    """
    # All production callers supply config; narrow for type-checker only.
    config = cast(PipelineConfig, config)
    timings: dict[str, Any] = {}
    total_start = time.perf_counter()

    # --- resolve_inputs phase ---
    with phase_timer("resolve_inputs", timings):
        resolved_inputs = resolve_inputs(
            inputs,
            config.delta_root,
            step_run_ids=step_run_ids,
            storage_options=config.storage.delta_storage_options(),
            fs=config.storage.filesystem(),
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
                config.delta_root,
                fs=_fs,
                storage_options=_so,
                files_root=config.files_root,
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
                    execution_run_ids=[staging_result.execution_run_id],  # type: ignore[list-item]  # execution_run_id may be None in failure paths; preserve runtime behavior
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

            run_id = _synthesize_failure_record(
                unit,
                runtime_env,
                error_msg,
                timestamp_start,
                user_overrides,
                step_run_id=step_run_id,
            )
            results = [
                UnitResult(
                    success=False,
                    error=error_msg,
                    item_count=1,
                    execution_run_ids=[run_id] if run_id else [],
                )
            ]
            succeeded, failed = 0, 1
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

    # fail_fast aborts only after the failure record is committed (above).
    raise_if_fail_fast(failure_policy, failed, results, dispatch_error)

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
    call = serialize_process_call(run_curator_flow, unit, runtime_env, 0)
    ctx = multiprocessing.get_context("spawn")
    with (
        suppress_main_reimport(),
        ProcessPoolExecutor(max_workers=1, mp_context=ctx) as pool,
    ):
        future = pool.submit(execute_process_call, call)
        # Poll done() and call result() exactly once after completion. On
        # Python 3.12 concurrent.futures.TimeoutError IS builtins.TimeoutError,
        # so calling result(timeout=) in the loop would swallow a task-raised
        # TimeoutError as a poll timeout and spin forever; polling done()
        # instead lets task exceptions surface as real failures.
        while not future.done():
            if cancel_event is not None and cancel_event.is_set():
                msg = "Curator interrupted by cancellation"
                raise RuntimeError(msg)
            wait([future], timeout=0.5)
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


def _synthesize_failure_record(
    unit: ExecutionUnit,
    runtime_env: RuntimeEnvironment,
    error: str,
    timestamp_start: datetime,
    user_overrides: dict[str, Any] | None,
    *,
    step_run_id: str | None,
) -> str:
    """Stage a synthetic failure record for a unit the worker left unrecorded.

    Covers the two shapes where the worker stages nothing itself: a process
    pool break (``BrokenProcessPool``) that kills the worker mid-run, and a
    pre-try failure (an unimportable or unpicklable op caught in the dispatch
    task) that fails before the executor's own record path. Also writes the
    human failure log. Fully best-effort — any synthesis error is swallowed so
    it never crashes the step.

    Args:
        unit: The execution unit whose failure went unrecorded.
        runtime_env: Runtime paths and storage for the failing step.
        error: Error string to persist (and write to the failure log).
        timestamp_start: Start time captured before dispatch.
        user_overrides: User-provided parameter overrides for the record.
        step_run_id: Owning step run id, or None for composite-internal steps.

    Returns:
        The synthetic ``killed-<spec>`` execution run id, or ``""`` if
        synthesis itself failed.
    """
    synthetic_run_id = f"killed-{unit.execution_spec_id[:24]}"
    try:
        fs = runtime_env.storage.filesystem()
        storage_options = runtime_env.storage.delta_storage_options()
        execution_context = build_execution_context(
            execution_run_id=synthetic_run_id,
            execution_spec_id=unit.execution_spec_id,
            step_number=unit.step_number,
            timestamp_start=timestamp_start,
            worker_id=0,
            delta_root=runtime_env.delta_root,
            staging_root=runtime_env.staging_root,
            fs=fs,
            storage_options=storage_options,
            operation=unit.operation,
            compute_backend_name=runtime_env.compute_backend_name,
            shared_filesystem=runtime_env.shared_filesystem,
            step_run_id=step_run_id,
            files_root=runtime_env.files_root,
        )
        record_execution_failure(
            execution_context=execution_context,
            error=error,
            inputs=unit.inputs,
            timestamp_end=datetime.now(UTC),
            params=_get_params(unit.operation),
            user_overrides=user_overrides,
            failure_logs_root=runtime_env.failure_logs_root,
        )
    except Exception:
        logger.exception(
            "Failed to synthesize failure record for unit %s",
            unit.execution_spec_id,
        )
        return ""
    return synthetic_run_id


def _synthesize_missing_failure_records(
    units: list[ExecutionUnit],
    results: list[UnitResult],
    runtime_env: RuntimeEnvironment,
    timestamp_start: datetime,
    user_overrides: dict[str, Any] | None,
    *,
    step_run_id: str | None,
) -> list[UnitResult]:
    """Backfill records for failed units whose worker recorded nothing.

    A ``UnitResult`` that failed before the executor's try-block staged
    anything (an unimportable or unpicklable op caught in the dispatch task)
    carries an empty ``execution_run_ids``. Synthesize a failure record for
    each so ``inspect_failures`` can see it. Results are paired to units
    positionally; a length mismatch skips the backfill (best-effort).

    Args:
        units: The dispatched units, positionally aligned with ``results``.
        results: Unit results from aggregation.
        runtime_env: Runtime paths and storage for the failing step.
        timestamp_start: Start time captured before dispatch.
        user_overrides: User-provided parameter overrides for the record.
        step_run_id: Owning step run id, or None for composite-internal steps.

    Returns:
        Results with synthetic run ids filled in for the backfilled units.
    """
    if len(results) != len(units):
        return results
    patched: list[UnitResult] = []
    for unit, result in zip(units, results, strict=True):
        if result.success or result.execution_run_ids:
            patched.append(result)
            continue
        error = result.error or (
            "Operation failed before recording (no execution record staged)."
        )
        run_id = _synthesize_failure_record(
            unit,
            runtime_env,
            error,
            timestamp_start,
            user_overrides,
            step_run_id=step_run_id,
        )
        patched.append(replace(result, execution_run_ids=[run_id] if run_id else []))
    return patched


def _execute_creator_step(
    operation: OperationDefinition,
    inputs: Any,
    step_runner: RunnerBase,
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
    """Execute a creator operation step through its lifecycle runner.

    Args:
        operation: Fully configured creator operation instance.
        inputs: Input specification.
        step_runner: Backend for worker dispatch.
        config_overrides: Merged environment + tool overrides (for hashing only).
        step_number: Pipeline step number.
        config: Pipeline configuration.
        failure_policy: Continue or fail-fast on errors.
        compact: Whether to run Delta Lake compaction.
        user_overrides: User-provided parameter overrides.
        cancel_event: Set to request cooperative cancellation between phases.
        skip_cache: Bypass per-batch execution-level cache lookups.
        step_run_id: Unique ID for this step attempt (for output isolation).
        step_run_ids: Upstream step_number to step_run_id mapping.

    Returns:
        StepResult with output references and execution metadata.
    """
    # All production callers supply config; narrow for type-checker only.
    config = cast(PipelineConfig, config)
    timings: dict[str, Any] = {}
    total_start = time.perf_counter()

    # =========================================================================
    # PHASE 1: DISPATCH
    # =========================================================================

    # --- resolve_inputs phase ---
    with phase_timer("resolve_inputs", timings):
        # Resolve inputs to artifact IDs
        resolved_inputs = resolve_inputs(
            inputs,
            config.delta_root,
            step_run_ids=step_run_ids,
            storage_options=config.storage.delta_storage_options(),
            fs=config.storage.filesystem(),
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
                config.delta_root,
                fs=_fs,
                storage_options=_so,
                files_root=config.files_root,
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
    # PHASE 2: EXECUTE
    # =========================================================================

    # --- cancel check: before execute ---
    if cancel_event is not None and cancel_event.is_set():
        return _cancelled_result(operation, step_number, failure_policy)

    dispatch_dir = uri_join(config.staging_root, "_dispatch")
    staging_fs = config.storage.filesystem()
    try:
        # --- execute phase ---
        dispatch_error: str | None = None
        with phase_timer("execute", timings):
            # Create RuntimeEnvironment with step_runner traits
            runtime_env = _create_runtime_environment(config, operation, step_runner)

            succeeded = 0
            failed = 0
            results: list[UnitResult] = []
            # Captured before dispatch — start time for any synthesized record.
            timestamp_start = datetime.now(UTC)

            if units_to_dispatch:
                try:
                    # Axis 1 only: every creator step rides the step_runner's
                    # lifecycle router. The compute provider (axis 2) is
                    # consulted inside the lifecycle, in create_execute_router.
                    step_runner.validate_operation(operation)
                    router: LifecycleRouter = step_runner.create_lifecycle_router(
                        operation.runner_resources,
                        operation.batch_strategy,
                        step_number,
                        job_name=operation.batch_strategy.job_name or operation.name,
                        staging_root=config.staging_root,
                    )

                    results = router.run(
                        units_to_dispatch,
                        runtime_env,
                        cancel_event=cancel_event,
                    )
                    succeeded, failed = aggregate_results(results, failure_policy)
                    # Backfill any pre-try failures the worker never recorded
                    # (unimportable/unpicklable op -> empty execution_run_ids).
                    results = _synthesize_missing_failure_records(
                        units_to_dispatch,
                        results,
                        runtime_env,
                        timestamp_start,
                        user_overrides,
                        step_run_id=step_run_id,
                    )
                except BrokenProcessPool:
                    dispatch_error = "Worker process killed (signal or OOM)"
                    logger.warning(
                        "Step %d (%s): %s",
                        step_number,
                        operation.name,
                        dispatch_error,
                    )
                    # The pool died before workers could stage records —
                    # synthesize one failure record per lost unit (attribution
                    # is imperfect after a crash; be conservative).
                    results = []
                    for lost_unit in units_to_dispatch:
                        run_id = _synthesize_failure_record(
                            lost_unit,
                            runtime_env,
                            dispatch_error,
                            timestamp_start,
                            user_overrides,
                            step_run_id=step_run_id,
                        )
                        results.append(
                            UnitResult(
                                success=False,
                                error=dispatch_error,
                                item_count=1,
                                execution_run_ids=[run_id] if run_id else [],
                            )
                        )
                    succeeded, failed = 0, len(units_to_dispatch)
                except Exception as exc:
                    dispatch_error, results, succeeded, failed = (
                        _handle_dispatch_exception(
                            exc, step_number, unit_count=len(units_to_dispatch)
                        )
                    )

        # --- verify_staging phase ---
        if units_to_dispatch:
            _verify_staging_if_needed(
                step_runner, results, config, step_number, operation.name, timings
            )
        else:
            with phase_timer("verify_staging", timings):
                pass

        # --- capture_logs phase ---
        with phase_timer("capture_logs", timings):
            if units_to_dispatch:
                persist_worker_logs(
                    results,
                    config.staging_root,
                    runtime_env.failure_logs_root,
                    operation.name,
                    step_number,
                    fs=staging_fs,
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
        try:
            if staging_fs.exists(dispatch_dir):
                staging_fs.rm(dispatch_dir, recursive=True)
        except Exception:
            pass

    _finalize_timings(timings, total_start, step_number, "Creator")

    # fail_fast aborts only after the failure records are committed (above).
    raise_if_fail_fast(failure_policy, failed, results, dispatch_error)

    return build_step_result(
        operation=operation,
        step_number=step_number,
        succeeded_count=succeeded + cached_count,
        failed_count=failed,
        failure_policy=failure_policy,
        metadata=_build_step_metadata(timings, commit_error, dispatch_error),
        step_run_id=step_run_id,
    )


def _compact_step_tables(
    delta_root: str,
    staging_root: str,
    tables: list[str] | None = None,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict[str, str] | None = None,
) -> None:
    """Compact Delta Lake tables to merge small parquet files.

    Args:
        delta_root: Root URI for Delta Lake tables.
        staging_root: Root URI for staging directory (for DeltaCommitter init).
        tables: Specific tables to compact. If None, compact all.
        fs: Filesystem implementation. Uses local filesystem if None.
        storage_options: Delta-rs storage options.
    """
    from artisan.storage.io.commit import DeltaCommitter
    from artisan.storage.io.staging import StagingManager

    if fs is None:
        from fsspec.implementations.local import LocalFileSystem

        fs = LocalFileSystem()

    staging_manager = StagingManager(staging_root, fs)
    committer = DeltaCommitter(
        delta_root,
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
        table_name = table.rsplit("/", 1)[-1]
        try:
            committer.compact_table(table)
        except Exception as exc:
            logger.warning(
                "Compaction failed for table %s: %s: %s",
                table_name,
                type(exc).__name__,
                exc,
            )
