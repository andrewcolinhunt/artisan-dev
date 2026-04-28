"""Batch dispatch handle with per-artifact fan-out in a child process.

Replaces ``ComputeRoutingDispatchHandle`` for the Modal compute_provider path.
Units process concurrently via a ``ThreadPoolExecutor`` — all threads
share the same warm ``ModalComputeRouter``.
"""

from __future__ import annotations

import logging
import multiprocessing
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

from artisan.execution.compute.base import ComputeRouter
from artisan.execution.compute.routing import create_router
from artisan.execution.executors.creator import (
    _ExecuteFailure,
    _PostprocessFailure,
)
from artisan.execution.executors.creator_phases import (
    _extract_inputs,
    post_unit,
    prep_unit,
)
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.staging.recorder import (
    _read_tool_output,
    record_execution_failure,
    record_execution_success,
)
from artisan.orchestration.engine.dispatch_handle import DispatchHandle, _HandleState
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.operation_config.compute import ComputeConfig
from artisan.schemas.operation_config.compute_resources import ComputeResources
from artisan.utils.errors import format_error
from artisan.utils.spawn import ignore_sigint, suppress_main_reimport
from artisan.utils.timing import phase_timer

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Child process functions
# ---------------------------------------------------------------------------


def _batch_execute_with_shared_router(
    units: list[ExecutionUnit],
    runtime_env: RuntimeEnvironment,
    compute_config: ComputeConfig,
    max_workers: int = 4,
    compute_resources: ComputeResources | None = None,
) -> list[UnitResult]:
    """Process units concurrently with a shared compute provider router.

    Creates a router from the picklable provider config plus hardware
    resources, warms it, and processes units via a thread pool
    (prep/post overlap across units). Closes the router after all
    threads complete.

    Runs inside a spawned child process. Router warm-up time is
    recorded as ``router_init`` on a shared worker timings dict and
    copied onto every unit's ``prepped.timings`` so it appears as a
    column in ``PipelineTimings.execution_stats``.

    Args:
        units: Execution units to process.
        runtime_env: Paths and step_runner configuration.
        compute_config: Picklable config for ``create_router()``.
        max_workers: Thread pool size for cross-unit parallelism.
        compute_resources: Hardware spec (gpu/memory_gb/timeout) for the
            provider; None defers to provider defaults.
    """
    router = create_router(compute_config, compute_resources=compute_resources)
    worker_timings: dict[str, Any] = {}
    try:
        if units:
            with phase_timer("router_init", worker_timings):
                router.warm(units[0].operation.name)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(
                    _process_unit,
                    unit,
                    runtime_env,
                    router,
                    worker_id=i,
                    worker_timings=worker_timings,
                )
                for i, unit in enumerate(units)
            ]
            return [f.result() for f in futures]
    finally:
        if hasattr(router, "close"):
            router.close()


def _process_unit(
    unit: ExecutionUnit,
    runtime_env: RuntimeEnvironment,
    router: ComputeRouter,
    worker_id: int = 0,
    worker_timings: dict[str, Any] | None = None,
) -> UnitResult:
    """Process a single unit: prep → batch execute → post → record.

    ``worker_timings`` carries phase timings shared across threads in
    the worker subprocess (e.g. ``router_init``). Any keys present are
    copied onto this unit's ``prepped.timings`` so they surface as
    columns in :class:`PipelineTimings` stats.
    """
    timings: dict[str, Any] = {}
    total_start = time.perf_counter()
    original_inputs = _extract_inputs(unit)
    operation = unit.operation

    # --- prep ---
    try:
        prepped = prep_unit(unit, runtime_env, worker_id=worker_id)
    except Exception as exc:
        logger.error("Prep failed for unit: %s", format_error(exc))
        return UnitResult(
            success=False,
            error=format_error(exc),
            item_count=unit.get_batch_size() or 1,
            execution_run_ids=[],
        )

    if worker_timings and "router_init" in worker_timings:
        prepped.timings["router_init"] = worker_timings["router_init"]

    # --- execute ---
    try:
        with phase_timer("execute", prepped.timings):
            if getattr(prepped.operation, "per_artifact_dispatch", True):
                handle = router.route_execute_batch(
                    prepped.operation,
                    prepped.artifact_execute_inputs,
                    prepped.sandbox_path,
                    timings=prepped.timings,
                )
                with phase_timer("collect", prepped.timings):
                    raw_results = list(handle)
                _aggregate_container_timings(
                    getattr(handle, "container_timings", []),
                    prepped.timings,
                )
            else:
                raw_results = [
                    router.route_execute(
                        prepped.operation,
                        prepped.artifact_execute_inputs[0],
                        prepped.sandbox_path,
                    )
                ]
    except Exception as exc:
        error = format_error(exc)
        tool_output = _read_tool_output(prepped.log_path)
        params_dict = _get_params_dict(operation)
        record_execution_failure(
            execution_context=prepped.execution_context,
            error=error,
            inputs=original_inputs,
            timestamp_end=datetime.now(UTC),
            params=params_dict,
            user_overrides=unit.user_overrides,
            tool_output=tool_output,
            failure_logs_root=runtime_env.failure_logs_root,
        )
        return UnitResult(
            success=False,
            error=error,
            item_count=unit.get_batch_size() or 1,
            execution_run_ids=[prepped.execution_run_id],
        )

    # --- post ---
    try:
        lifecycle_result = post_unit(prepped, raw_results, runtime_env)
        timings.update(prepped.timings)
    except (_PostprocessFailure, _ExecuteFailure) as exc:
        error = str(exc)
        # Per-container Modal failures are surfaced as exceptions
        # embedded in raw_results (not raised by route_execute_batch),
        # so the line-163 read above doesn't fire. Read the on-disk
        # unit log (written by BatchExecuteHandle.__iter__'s finally
        # block) so partial bytes from failed containers reach the
        # parquet ``tool_output`` column.
        tool_output = getattr(exc, "tool_output", None)
        if tool_output is None:
            tool_output = _read_tool_output(prepped.log_path)
        params_dict = _get_params_dict(operation)
        record_execution_failure(
            execution_context=prepped.execution_context,
            error=error,
            inputs=original_inputs,
            timestamp_end=datetime.now(UTC),
            params=params_dict,
            user_overrides=unit.user_overrides,
            tool_output=tool_output,
            failure_logs_root=runtime_env.failure_logs_root,
        )
        return UnitResult(
            success=False,
            error=error,
            item_count=unit.get_batch_size() or 1,
            execution_run_ids=[prepped.execution_run_id],
        )
    except Exception as exc:
        error = format_error(exc)
        tool_output = _read_tool_output(prepped.log_path)
        params_dict = _get_params_dict(operation)
        record_execution_failure(
            execution_context=prepped.execution_context,
            error=error,
            inputs=original_inputs,
            timestamp_end=datetime.now(UTC),
            params=params_dict,
            user_overrides=unit.user_overrides,
            tool_output=tool_output,
            failure_logs_root=runtime_env.failure_logs_root,
        )
        return UnitResult(
            success=False,
            error=error,
            item_count=unit.get_batch_size() or 1,
            execution_run_ids=[prepped.execution_run_id],
        )

    # --- record success ---
    params_dict = _get_params_dict(operation)
    with phase_timer("record", timings):
        record_execution_success(
            execution_context=prepped.execution_context,
            artifacts=lifecycle_result.artifacts,
            lineage_edges=lifecycle_result.edges,
            inputs=original_inputs,
            timestamp_end=datetime.now(UTC),
            params=params_dict,
            result_metadata={"timings": timings},
            user_overrides=unit.user_overrides,
        )

    timings["total"] = round(time.perf_counter() - total_start, 4)
    return UnitResult(
        success=True,
        error=None,
        item_count=unit.get_batch_size() or 1,
        execution_run_ids=[prepped.execution_run_id],
    )


def _get_params_dict(operation: Any) -> dict[str, Any]:
    """Extract serialized params from an operation."""
    from artisan.utils.hashing import serialize_params

    return serialize_params(operation)


def _aggregate_container_timings(
    cts: list[dict[str, float]],
    timings: dict[str, Any],
) -> None:
    """Fold per-container epoch timestamps into scalar summary phases.

    Writes ``container_cold_start_p50`` (median time from container
    start to execute start), ``container_execute_p50`` (median time
    inside ``operation.execute``), and ``fan_out_span`` (wall-clock
    from the earliest container start to the latest execute end)
    onto ``timings`` as float keys. All values rounded to 4 decimals.
    No-op when ``cts`` is empty so partial-failure batches still work.

    Args:
        cts: Per-artifact epoch dicts from ``_execute_on_modal``.
        timings: Dict to mutate in place.
    """
    if not cts:
        return
    cold = sorted(ct["execute_start_epoch"] - ct["container_start_epoch"] for ct in cts)
    exec_ = sorted(ct["execute_end_epoch"] - ct["execute_start_epoch"] for ct in cts)
    mid = len(cts) // 2
    timings["container_cold_start_p50"] = round(cold[mid], 4)
    timings["container_execute_p50"] = round(exec_[mid], 4)
    timings["fan_out_span"] = round(
        max(ct["execute_end_epoch"] for ct in cts)
        - min(ct["container_start_epoch"] for ct in cts),
        4,
    )


# ---------------------------------------------------------------------------
# Dispatch handle
# ---------------------------------------------------------------------------


class BatchComputeDispatchHandle(DispatchHandle):
    """Batch dispatch with per-artifact fan-out in a child process.

    Spawns a single child process that creates a ``ComputeRouter``,
    then processes units concurrently via a ``ThreadPoolExecutor``.
    Each unit goes through prep → batch execute → post → record.
    All threads share the same warm router for container reuse.

    Args:
        compute_config: Picklable config for ``create_router()``
            inside the child process.
        cancel_event: Pipeline cancel event (threading.Event).
        max_workers: Thread pool size for cross-unit parallelism.
    """

    def __init__(
        self,
        compute_config: ComputeConfig,
        cancel_event: threading.Event | None = None,
        max_workers: int = 4,
        compute_resources: ComputeResources | None = None,
    ) -> None:
        super().__init__()
        self._compute_config = compute_config
        self._compute_resources = compute_resources
        self._cancel_event = cancel_event
        self._max_workers = max_workers

    def dispatch(  # type: ignore[override]  # narrower than base: batch handle only accepts ExecutionUnit, not composites
        self,
        units: list[ExecutionUnit],
        runtime_env: RuntimeEnvironment,
    ) -> None:
        """Spawn a child process and run units with a shared router."""
        self._assert_idle()
        self._state = _HandleState.DISPATCHED

        config = self._compute_config
        compute_resources = self._compute_resources
        cancel = self._cancel_event
        max_workers = self._max_workers
        mp_ctx = multiprocessing.get_context("spawn")

        def _run() -> list[UnitResult]:
            with (
                suppress_main_reimport(),
                ProcessPoolExecutor(
                    max_workers=1,
                    mp_context=mp_ctx,
                    initializer=ignore_sigint,
                ) as pool,
            ):
                future = pool.submit(
                    _batch_execute_with_shared_router,
                    units,
                    runtime_env,
                    config,
                    max_workers,
                    compute_resources,
                )
                while True:
                    try:
                        return future.result(timeout=0.5)
                    except TimeoutError as err:
                        if cancel is not None and cancel.is_set():
                            msg = "Batch compute_provider interrupted"
                            raise RuntimeError(msg) from err
                        continue

        self._start_background(_run)

    def cancel(self) -> None:
        """No-op -- cancellation handled by exiting app.run()."""
