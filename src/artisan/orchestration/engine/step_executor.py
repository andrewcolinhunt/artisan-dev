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
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, Never, cast

from fsspec import AbstractFileSystem
from pydantic import BaseModel

from artisan.errors import PersistenceIntegrityError
from artisan.execution.context.builder import build_execution_context
from artisan.execution.executors.curator import (
    is_curator_operation,
    run_curator_flow,
)
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.execution.recording.recorder import record_execution_failure
from artisan.operations.base._param_docs import _params_class
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.engine.batching import (
    generate_execution_unit_batches,
    get_batch_config,
)
from artisan.orchestration.engine.dispatch import failure_results_for_units
from artisan.orchestration.engine.inputs import PreparedInputs, prepare_inputs
from artisan.orchestration.engine.lifecycle_router import LifecycleRouter
from artisan.orchestration.engine.results import (
    aggregate_results,
    classify_step_status,
    extract_execution_run_ids,
)
from artisan.orchestration.engine.worker_logs import persist_worker_logs
from artisan.orchestration.runners.base import RunnerBase
from artisan.schemas.enums import FailurePolicy, TablePath
from artisan.schemas.execution.cache_result import CacheHit
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_overrides import StepOverrides
from artisan.schemas.orchestration.step_result import StepResult, StepResultBuilder
from artisan.storage.cache.cache_lookup import cache_lookup
from artisan.storage.io.staging_verification import await_staging_files
from artisan.utils.hashing import effective_config_payload, serialize_params
from artisan.utils.path import (
    cancel_sentinel_path,
    shard_uri,
    uri_join,
    uri_parent,
)
from artisan.utils.process_call import execute_process_call, serialize_process_call
from artisan.utils.spawn import suppress_main_reimport
from artisan.utils.timing import phase_timer

logger = logging.getLogger(__name__)


def _validated_model_update[ModelT: BaseModel](
    base_model: ModelT,
    patch: dict[str, Any],
) -> ModelT:
    """Deep-merge a detached patch and validate the complete target model."""

    def _merge(base: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
        merged = deepcopy(base)
        for key, value in update.items():
            current = merged.get(key)
            if isinstance(current, dict) and isinstance(value, dict) and value:
                merged[key] = _merge(current, value)
            else:
                merged[key] = deepcopy(value)
        return merged

    model_cls = type(base_model)
    return model_cls.model_validate(_merge(base_model.model_dump(mode="python"), patch))


def _raise_invalid_override(field_name: str, value: object) -> Never:
    """Reject a malformed internal override carrier with a useful error."""
    msg = f"{field_name} override must be a mapping, got {type(value).__name__}"
    raise TypeError(msg)


def instantiate_operation(
    operation_class: type[OperationDefinition],
    ov: StepOverrides,
) -> OperationDefinition:
    """Construct an operation instance from a class and coerced overrides.

    Applies ``ov``'s params and per-step config overrides (runner resources,
    batch strategy, environment, tool, compute provider, compute resources,
    group_by) onto the class default. String selectors compile to ``active``
    patches, and every model-valued patch is recursively merged into the
    operation default and validated by the concrete target model.

    Args:
        operation_class: The operation class to instantiate.
        ov: The coerced per-step overrides.

    Returns:
        Fully configured operation instance.
    """
    params = ov.params
    runner_resources: object = ov.runner_resources
    batch_strategy: object = ov.batch_strategy
    environment: object = ov.environment
    tool: object = ov.tool
    compute_provider: object = ov.compute_provider
    compute_resources: object = ov.compute_resources
    group_by = ov.group_by

    init_kwargs: dict[str, Any] = {}
    params_cls = _params_class(operation_class)
    if params is not None:
        if params_cls is None:
            if params:
                msg = f"Operation {operation_class.name!r} declares no Params"
                raise ValueError(msg)
        else:
            init_kwargs["params"] = params_cls.model_validate(params)

    instance = operation_class(**init_kwargs)

    # ``from_user`` has normalized every model-valued input to a mapping patch.
    updates: dict[str, Any] = {}
    if runner_resources is not None:
        if isinstance(runner_resources, dict):
            updates["runner_resources"] = _validated_model_update(
                instance.runner_resources,
                runner_resources,
            )
        else:
            _raise_invalid_override("runner_resources", runner_resources)
    if batch_strategy is not None:
        if isinstance(batch_strategy, dict):
            updates["batch_strategy"] = _validated_model_update(
                instance.batch_strategy,
                batch_strategy,
            )
        else:
            _raise_invalid_override("batch_strategy", batch_strategy)
    if tool is not None:
        if not isinstance(tool, dict):
            _raise_invalid_override("tool", tool)
        if tool:
            if instance.tool is None:
                msg = f"Operation '{operation_class.name}' has no tool to override"
                raise ValueError(msg)
            updates["tool"] = _validated_model_update(instance.tool, tool)
    if environment is not None:
        environment_patch = (
            {"active": environment} if isinstance(environment, str) else environment
        )
        if not isinstance(environment_patch, dict):
            _raise_invalid_override("environment", environment)
        environments = _validated_model_update(instance.environments, environment_patch)
        environments.current()
        updates["environments"] = environments
    if compute_provider is not None:
        provider_patch = (
            {"active": compute_provider}
            if isinstance(compute_provider, str)
            else compute_provider
        )
        if not isinstance(provider_patch, dict):
            _raise_invalid_override("compute_provider", compute_provider)
        provider = _validated_model_update(instance.compute_provider, provider_patch)
        provider.current()
        updates["compute_provider"] = provider
    if compute_resources is not None:
        if isinstance(compute_resources, dict):
            updates["compute_resources"] = _validated_model_update(
                instance.compute_resources,
                compute_resources,
            )
        else:
            _raise_invalid_override("compute_resources", compute_resources)
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

    result = cache_lookup(
        delta_root,
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
    disposition: StepDisposition | None = StepDisposition.EXECUTED,
    status: StepStatus | None = None,
    cancellation_status: CancellationStatus | None = None,
    error: str | None = None,
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
    classified = status or classify_step_status(
        succeeded_count,
        failed_count,
        failure_policy,
    )

    # Unusable terminal states cannot expose output references.
    output_roles: dict[str, str | None] = {}
    if classified in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}:
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

    if classified in {StepStatus.FAILED, StepStatus.SKIPPED, StepStatus.CANCELLED}:
        disposition = None
    return builder.build(
        classified,
        disposition=disposition,
        cancellation_status=cancellation_status,
        error=error,
        metadata=metadata,
    )


def _cancelled_result(
    operation: type[OperationDefinition] | OperationDefinition,
    step_number: int,
    failure_policy: FailurePolicy,
    step_run_id: str | None = None,
) -> StepResult:
    """Build a StepResult indicating the step was cancelled before completion."""
    return build_step_result(
        operation=operation,
        step_number=step_number,
        succeeded_count=0,
        failed_count=0,
        failure_policy=failure_policy,
        status=StepStatus.CANCELLED,
        cancellation_status=CancellationStatus.CONFIRMED,
        step_run_id=step_run_id,
    )


def _validate_cache_reuse(
    config: PipelineConfig,
    current_step_run_id: str | None,
    cached_execution_run_ids: set[str],
) -> list[str]:
    """Validate the complete execution-cache selection in one bulk pass."""
    if not cached_execution_run_ids or current_step_run_id is None:
        return []
    from artisan.storage.core.run_scope import validate_cached_executions

    return validate_cached_executions(
        config.delta_root,
        current_step_run_id,
        cached_execution_run_ids,
        fs=config.storage.filesystem(),
        storage_options=config.storage.delta_storage_options(),
        files_root=config.files_root,
    )


def _stage_cache_reuse(
    config: PipelineConfig,
    current_step_run_id: str | None,
    cached_execution_run_ids: list[str],
    *,
    step_number: int,
    operation_name: str,
) -> bool:
    """Stage already-validated cache-reuse rows for the current step."""
    if current_step_run_id is None or not cached_execution_run_ids:
        return False
    from artisan.storage.io.staging import StagingManager

    staging = StagingManager(config.staging_root, config.storage.filesystem())
    staging.stage_cache_reuse(
        current_step_run_id,
        cached_execution_run_ids,
        step_number=step_number,
        operation_name=operation_name,
    )
    return True


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
    step_run_id: str | None = None,
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
        status=StepStatus.SKIPPED,
        disposition=None,
        metadata={"skip_reason": "empty_inputs"},
        step_run_id=step_run_id,
    )


def _persist_result(
    result: StepResult,
    execution_run_ids: list[str],
    persist: Callable[[StepResult, tuple[str, ...]], StepResult] | None,
) -> StepResult:
    """Hand one terminal result and its exact dispatch IDs to the manager."""
    if persist is None:
        return result
    return persist(result, tuple(execution_run_ids))


def _require_recorded_execution_ids(
    results: list[UnitResult],
) -> list[str]:
    """Reject dispatched work that produced no committable worker seal identity."""
    missing = [
        index
        for index, result in enumerate(results)
        if not result.execution_run_ids
        or any(not execution_id for execution_id in result.execution_run_ids)
    ]
    if missing:
        msg = f"Dispatched execution results lack sealed staging identities: {missing}"
        raise PersistenceIntegrityError(msg)
    execution_ids = extract_execution_run_ids(results)
    if len(execution_ids) != len(set(execution_ids)):
        msg = "Dispatched execution results contain duplicate staging identities"
        raise PersistenceIntegrityError(msg)
    return execution_ids


def _result_error(results: list[UnitResult], fallback: str | None = None) -> str | None:
    """Return the first unit diagnostic, then an infrastructure fallback."""
    return next(
        (result.error for result in results if not result.success and result.error),
        fallback,
    )


def _aggregate_cancellation(
    results: list[UnitResult],
    router_outcome: CancellationAcknowledgement | None,
) -> CancellationAcknowledgement | None:
    """Combine provider and per-unit cancellation evidence conservatively."""
    outcomes = [
        outcome
        for outcome in [
            router_outcome,
            *(result.cancellation_acknowledgement for result in results),
        ]
        if outcome is not None
    ]
    if not outcomes:
        return None
    for status in (
        CancellationStatus.UNKNOWN,
        CancellationStatus.REJECTED,
        CancellationStatus.CONFIRMED,
        CancellationStatus.REQUESTED,
    ):
        matching = [outcome for outcome in outcomes if outcome.status == status]
        if matching:
            selected = matching[0]
            if status == CancellationStatus.REQUESTED:
                return CancellationAcknowledgement(
                    CancellationStatus.UNKNOWN,
                    selected.message or "Cancellation remained unconfirmed",
                )
            return selected
    return None


def _unknown_cancellation_result(
    operation: type[OperationDefinition] | OperationDefinition,
    step_number: int,
    failure_policy: FailurePolicy,
    outcome: CancellationAcknowledgement,
    *,
    step_run_id: str | None,
) -> StepResult:
    """Build a fail-closed terminal result for indeterminate cancellation."""
    return build_step_result(
        operation=operation,
        step_number=step_number,
        succeeded_count=0,
        failed_count=0,
        failure_policy=failure_policy,
        status=StepStatus.FAILED,
        disposition=None,
        cancellation_status=CancellationStatus.UNKNOWN,
        error=outcome.message or "Cancellation outcome is unknown",
        step_run_id=step_run_id,
    )


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
            await_staging_files(
                staging_root=config.staging_root,
                execution_run_ids=execution_run_ids,
                timeout_seconds=step_runner.orchestrator_traits.staging_verification_timeout,
                step_number=step_number,
                operation_name=operation_name,
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
    # os.makedirs/open). For local delta_root keep the sibling-of-delta
    # layout. For cloud delta_root derive from
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
    operation: OperationDefinition,
    inputs: Any,
    ov: StepOverrides,
    step_runner: RunnerBase,
    *,
    step_number: int = 0,
    config: PipelineConfig | None = None,
    cancel_event: threading.Event | None = None,
    step_run_id: str | None = None,
    step_run_ids: dict[int, str] | None = None,
    persist_result: Callable[[StepResult, tuple[str, ...]], StepResult] | None = None,
) -> StepResult:
    """Execute a single pipeline step.

    This is the main entry point called by PipelineManager.run().
    It coordinates the three-phase workflow: dispatch, execute, commit.

    For curator operations (Merge, Filter), a separate execution
    path is used that executes locally without worker dispatch.

    Args:
        operation: Prepared operation instance to execute.
        inputs: Input specification (see PipelineManager.run() for formats).
        ov: Coerced per-step overrides (params + cache/runtime knobs).
        step_runner: Resolved lifecycle runner to use for execution.
        step_number: Pipeline step number.
        config: Pipeline configuration.
        cancel_event: Set to request cooperative cancellation between phases.
        step_run_id: Unique ID for this step attempt (for output isolation).
        step_run_ids: Mapping of upstream step_number to step_run_id
            for scoped output resolution.

    Returns:
        StepResult with output references and execution metadata.
    """
    user_overrides = ov.params or {}
    config = cast(PipelineConfig, config)
    if not isinstance(inputs, PreparedInputs):
        inputs = prepare_inputs(
            inputs,
            config.delta_root,
            config.storage.filesystem(),
            group_by=operation.group_by,
            step_run_ids=step_run_ids,
            storage_options=config.storage.delta_storage_options(),
            files_root=config.files_root,
        )

    # Cache-affecting config (environment, tool, compute_provider,
    # compute_resources, group_by, version) read off the instantiated op —
    # class defaults + applied overrides — folded into config_overrides for
    # hashing, symmetric with the merged-params channel.
    config_overrides = effective_config_payload(operation)

    # Resolve runtime knobs against pipeline defaults: ov carries the raw
    # per-step values; an unset one falls back to config.
    failure_policy = (
        ov.failure_policy if ov.failure_policy is not None else config.failure_policy
    )
    skip_cache = ov.skip_cache or config.skip_cache

    # Check if this is a curator operation
    if is_curator_operation(operation):
        return _execute_curator_step(
            operation=operation,
            inputs=inputs,
            config_overrides=config_overrides,
            step_number=step_number,
            config=config,
            failure_policy=failure_policy,
            user_overrides=user_overrides,
            cancel_event=cancel_event,
            skip_cache=skip_cache,
            step_run_id=step_run_id,
            step_run_ids=step_run_ids,
            persist_result=persist_result,
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
        user_overrides=user_overrides,
        cancel_event=cancel_event,
        skip_cache=skip_cache,
        step_run_id=step_run_id,
        step_run_ids=step_run_ids,
        persist_result=persist_result,
    )


def _execute_curator_step(
    operation: OperationDefinition,
    inputs: PreparedInputs,
    config_overrides: dict[str, Any] | None = None,
    step_number: int = 0,
    config: PipelineConfig | None = None,
    failure_policy: FailurePolicy = FailurePolicy.CONTINUE,
    user_overrides: dict[str, Any] | None = None,
    cancel_event: threading.Event | None = None,
    skip_cache: bool = False,
    step_run_id: str | None = None,
    step_run_ids: dict[int, str] | None = None,
    persist_result: Callable[[StepResult, tuple[str, ...]], StepResult] | None = None,
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
        user_overrides: User-provided parameter overrides.
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
        paired_inputs = inputs.inputs
        group_ids = inputs.group_ids
        total_artifacts = sum(len(ids) for ids in paired_inputs.values())
        if total_artifacts > 0:
            logger.debug(
                "Step %d (%s): resolved %d input artifacts",
                step_number,
                operation.name,
                total_artifacts,
            )

        skip_result = _skip_for_empty_inputs(
            operation,
            paired_inputs,
            step_number,
            failure_policy,
            step_run_id=step_run_id,
        )
        if skip_result is not None:
            return skip_result

    # --- batch_and_cache phase ---
    with phase_timer("batch_and_cache", timings):
        merged_params = serialize_params(operation)
        from artisan.utils.hashing import compute_execution_spec_id

        spec_id = compute_execution_spec_id(
            operation_name=operation.name,
            inputs=inputs.cache_inputs,
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
                validated_reuse = _validate_cache_reuse(
                    config,
                    step_run_id,
                    {cache_result.execution_run_id},
                )
                if cancel_event is not None and cancel_event.is_set():
                    return _cancelled_result(
                        operation,
                        step_number,
                        failure_policy,
                        step_run_id=step_run_id,
                    )
                _stage_cache_reuse(
                    config,
                    step_run_id,
                    validated_reuse,
                    step_number=step_number,
                    operation_name=operation.name,
                )
                _finalize_timings(timings, total_start, step_number, "Curator")
                result = build_step_result(
                    operation=operation,
                    step_number=step_number,
                    succeeded_count=cached_count,
                    failed_count=0,
                    failure_policy=failure_policy,
                    disposition=StepDisposition.CACHE_HIT,
                    metadata={"timings": timings},
                    step_run_id=step_run_id,
                )
                return _persist_result(result, [], persist_result)

    # --- cancel check: before execute ---
    if cancel_event is not None and cancel_event.is_set():
        return _cancelled_result(
            operation, step_number, failure_policy, step_run_id=step_run_id
        )

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
                        else unit.get_batch_size() or 1
                    ),
                    execution_run_ids=[staging_result.execution_run_id],  # type: ignore[list-item]  # execution_run_id may be None in failure paths; preserve runtime behavior
                )
            ]
            succeeded, failed = aggregate_results(results)

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
                    item_count=unit.get_batch_size() or 1,
                    execution_run_ids=[run_id] if run_id else [],
                )
            ]
            succeeded, failed = 0, unit.get_batch_size() or 1
        except Exception as exc:
            dispatch_error, results, succeeded, failed = _record_dispatch_failure(
                exc,
                [unit],
                runtime_env,
                timestamp_start,
                user_overrides,
                step_number=step_number,
                step_run_id=step_run_id,
            )

    # --- verify_staging phase (no-op: curator runs in-process, no NFS delay) ---
    with phase_timer("verify_staging", timings):
        pass

    # --- cancel check: before commit ---
    if cancel_event is not None and cancel_event.is_set():
        _discard_cancelled_staging(
            results,
            runtime_env,
            operation.name,
            step_number,
        )
        return _cancelled_result(
            operation, step_number, failure_policy, step_run_id=step_run_id
        )

    _finalize_timings(timings, total_start, step_number, "Curator")

    status = classify_step_status(
        succeeded,
        failed,
        failure_policy,
        infrastructure_error=dispatch_error is not None,
    )

    result = build_step_result(
        operation=operation,
        step_number=step_number,
        succeeded_count=succeeded,
        failed_count=failed,
        failure_policy=failure_policy,
        status=status,
        disposition=StepDisposition.EXECUTED,
        error=_result_error(results, dispatch_error),
        metadata={"timings": timings},
        step_run_id=step_run_id,
    )
    execution_ids = extract_execution_run_ids(results)
    if persist_result is not None:
        execution_ids = _require_recorded_execution_ids(results)
    return _persist_result(result, execution_ids, persist_result)


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
            params=serialize_params(unit.operation),
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


def _record_dispatch_failure(
    exc: Exception,
    units: list[ExecutionUnit],
    runtime_env: RuntimeEnvironment,
    timestamp_start: datetime,
    user_overrides: dict[str, Any] | None,
    *,
    step_number: int,
    step_run_id: str | None,
) -> tuple[str, list[UnitResult], int, int]:
    """Create ordered, inspectable failure results for a dispatch exception."""
    dispatch_error = f"{type(exc).__name__}: {exc}"
    logger.error(
        "Dispatch failed for step %d: %s",
        step_number,
        dispatch_error,
    )
    results = failure_results_for_units(units, dispatch_error)
    results = _synthesize_missing_failure_records(
        units,
        results,
        runtime_env,
        timestamp_start,
        user_overrides,
        step_run_id=step_run_id,
    )
    succeeded, failed = aggregate_results(results)
    return dispatch_error, results, succeeded, failed


def _discard_cancelled_staging(
    results: list[UnitResult],
    runtime_env: RuntimeEnvironment,
    operation_name: str,
    step_number: int,
) -> None:
    """Best-effort removal of staged records produced by cancelled work."""
    fs = runtime_env.storage.filesystem()
    for result in results:
        for run_id in result.execution_run_ids:
            if not run_id:
                continue
            staging_path = shard_uri(
                runtime_env.staging_root,
                run_id,
                step_number=step_number,
                operation_name=operation_name,
            )
            try:
                if fs.exists(staging_path):
                    fs.rm(staging_path, recursive=True)
            except Exception as exc:
                logger.warning(
                    "Failed to discard cancelled staging for execution %s: %s",
                    run_id,
                    exc,
                )


def _execute_creator_step(
    operation: OperationDefinition,
    inputs: PreparedInputs,
    step_runner: RunnerBase,
    config_overrides: dict[str, Any] | None = None,
    step_number: int = 0,
    config: PipelineConfig | None = None,
    failure_policy: FailurePolicy = FailurePolicy.CONTINUE,
    user_overrides: dict[str, Any] | None = None,
    cancel_event: threading.Event | None = None,
    skip_cache: bool = False,
    step_run_id: str | None = None,
    step_run_ids: dict[int, str] | None = None,
    persist_result: Callable[[StepResult, tuple[str, ...]], StepResult] | None = None,
) -> StepResult:
    """Execute a creator operation step through its lifecycle runner.

    Args:
        operation: Fully configured creator operation instance.
        inputs: Input specification.
        step_runner: Resolved lifecycle runner for worker dispatch.
        config_overrides: Merged environment + tool overrides (for hashing only).
        step_number: Pipeline step number.
        config: Pipeline configuration.
        failure_policy: Continue or fail-fast on errors.
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
        paired_inputs = inputs.inputs
        group_ids = inputs.group_ids

        skip_result = _skip_for_empty_inputs(
            operation,
            paired_inputs,
            step_number,
            failure_policy,
            step_run_id=step_run_id,
        )
        if skip_result is not None:
            return skip_result

        total_artifacts = sum(len(ids) for ids in paired_inputs.values())
        logger.debug(
            "Step %d (%s): resolved %d input artifacts",
            step_number,
            operation.name,
            total_artifacts,
        )

    # --- batch_and_cache phase ---
    with phase_timer("batch_and_cache", timings):
        # Get batch configuration from the instance
        batch_config = get_batch_config(operation)

        merged_params = serialize_params(operation)

        # Import lazily to avoid package import cycles during module initialization.
        from artisan.utils.hashing import compute_execution_spec_id

        # Generate ExecutionUnit batches (Level 1)
        execution_unit_batches = generate_execution_unit_batches(
            paired_inputs,
            batch_config,
            group_ids=group_ids,
            cache_inputs=inputs.cache_inputs,
        )

        # Create ExecutionUnits with cache checking
        units_to_dispatch: list[ExecutionUnit] = []
        cached_count = 0
        cached_units = 0
        cached_execution_run_ids: set[str] = set()

        for (
            execution_unit_inputs,
            batch_group_ids,
            execution_cache_inputs,
        ) in execution_unit_batches:
            # Compute spec_id for cache lookup
            spec_id = compute_execution_spec_id(
                operation_name=operation.name,
                inputs=execution_cache_inputs,
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
                cached_execution_run_ids.add(cache_result.execution_run_id)
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

        validated_reuse = _validate_cache_reuse(
            config,
            step_run_id,
            cached_execution_run_ids,
        )

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
        return _cancelled_result(
            operation, step_number, failure_policy, step_run_id=step_run_id
        )

    staging_fs = config.storage.filesystem()
    cancellation_outcome: CancellationAcknowledgement | None = None
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
                        cancellation_confirmation_timeout=(
                            step_runner.orchestrator_traits.cancellation_confirmation_timeout
                        ),
                    )
                    cancellation_outcome = _aggregate_cancellation(
                        results,
                        router.cancellation_acknowledgement,
                    )
                    succeeded, failed = aggregate_results(results)
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
                                item_count=lost_unit.get_batch_size() or 1,
                                execution_run_ids=[run_id] if run_id else [],
                            )
                        )
                    succeeded, failed = aggregate_results(results)
                except Exception as exc:
                    dispatch_error, results, succeeded, failed = (
                        _record_dispatch_failure(
                            exc,
                            units_to_dispatch,
                            runtime_env,
                            timestamp_start,
                            user_overrides,
                            step_number=step_number,
                            step_run_id=step_run_id,
                        )
                    )

        # --- verify_staging phase ---
        if units_to_dispatch:
            if persist_result is not None:
                _require_recorded_execution_ids(results)
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
        if (
            cancellation_outcome is not None
            and cancellation_outcome.status == CancellationStatus.UNKNOWN
        ):
            _discard_cancelled_staging(
                results,
                runtime_env,
                operation.name,
                step_number,
            )
            return _unknown_cancellation_result(
                operation,
                step_number,
                failure_policy,
                cancellation_outcome,
                step_run_id=step_run_id,
            )
        if (
            cancel_event is not None
            and cancel_event.is_set()
            and (
                cancellation_outcome is None
                or cancellation_outcome.status == CancellationStatus.CONFIRMED
            )
        ):
            _discard_cancelled_staging(
                results,
                runtime_env,
                operation.name,
                step_number,
            )
            return _cancelled_result(
                operation, step_number, failure_policy, step_run_id=step_run_id
            )

        _stage_cache_reuse(
            config,
            step_run_id,
            validated_reuse,
            step_number=step_number,
            operation_name=operation.name,
        )

    finally:
        if step_run_id is not None:
            sentinel = cancel_sentinel_path(config.staging_root, step_run_id)
            try:
                if staging_fs.exists(sentinel):
                    staging_fs.rm(sentinel)
            except Exception:
                pass

    _finalize_timings(timings, total_start, step_number, "Creator")

    status = classify_step_status(
        succeeded + cached_count,
        failed,
        failure_policy,
        infrastructure_error=dispatch_error is not None,
    )

    result = build_step_result(
        operation=operation,
        step_number=step_number,
        succeeded_count=succeeded + cached_count,
        failed_count=failed,
        failure_policy=failure_policy,
        status=status,
        disposition=(
            StepDisposition.EXECUTED if units_to_dispatch else StepDisposition.CACHE_HIT
        ),
        cancellation_status=(
            CancellationStatus.REJECTED
            if cancellation_outcome is not None
            and cancellation_outcome.status == CancellationStatus.REJECTED
            else None
        ),
        error=(
            _result_error(results, dispatch_error)
            or (
                cancellation_outcome.message
                if cancellation_outcome is not None
                and cancellation_outcome.status == CancellationStatus.REJECTED
                else None
            )
        ),
        metadata={"timings": timings},
        step_run_id=step_run_id,
    )
    return _persist_result(result, extract_execution_run_ids(results), persist_result)


def _compact_step_tables(
    delta_root: str,
    staging_root: str,
    tables: list[str] | None = None,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict[str, str] | None = None,
) -> None:
    """Best-effort compaction after terminal state is authoritative.

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

    try:
        staging_manager = StagingManager(staging_root, fs)
        committer = DeltaCommitter(
            delta_root,
            staging_manager,
            fs=fs,
            storage_options=storage_options,
        )

        if tables is None:
            from artisan.schemas.artifact.registry import ArtifactTypeDef

            artifact_tables = [
                type_def.table_path for type_def in ArtifactTypeDef.get_all().values()
            ]
            tables = [
                *artifact_tables,
                TablePath.ARTIFACT_INDEX.value,
                TablePath.EXECUTIONS.value,
            ]
    except Exception as exc:
        logger.warning(
            "Compaction setup failed after terminalization: %s: %s",
            type(exc).__name__,
            exc,
        )
        return

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
