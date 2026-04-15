"""Batch dispatch handle with per-artifact fan-out in a child process.

Replaces ``ComputeRoutingDispatchHandle`` for the Modal compute path.
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
from artisan.execution.executors.creator import _ExecuteFailure, _PostprocessFailure
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
) -> list[UnitResult]:
    """Process units concurrently with a shared compute router.

    Creates a router from the picklable compute config, processes
    units via a thread pool (prep/post overlap across units), and
    closes the router after all threads complete.

    Runs inside a spawned child process.

    Args:
        units: Execution units to process.
        runtime_env: Paths and backend configuration.
        compute_config: Picklable config for ``create_router()``.
        max_workers: Thread pool size for cross-unit parallelism.
    """
    router = create_router(compute_config)
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(
                    _process_unit,
                    unit,
                    runtime_env,
                    router,
                    worker_id=i,
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
) -> UnitResult:
    """Process a single unit: prep → batch execute → post → record."""
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

    # --- execute ---
    try:
        with phase_timer("execute", prepped.timings):
            if getattr(prepped.operation, "per_artifact_dispatch", True):
                raw_results = list(
                    router.route_execute_batch(
                        prepped.operation,
                        prepped.artifact_execute_inputs,
                        prepped.sandbox_path,
                    )
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
        tool_output = getattr(exc, "tool_output", None)
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
        params_dict = _get_params_dict(operation)
        record_execution_failure(
            execution_context=prepped.execution_context,
            error=error,
            inputs=original_inputs,
            timestamp_end=datetime.now(UTC),
            params=params_dict,
            user_overrides=unit.user_overrides,
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
    ) -> None:
        super().__init__()
        self._compute_config = compute_config
        self._cancel_event = cancel_event
        self._max_workers = max_workers

    def dispatch(
        self,
        units: list[ExecutionUnit],
        runtime_env: RuntimeEnvironment,
    ) -> None:
        """Spawn a child process and run units with a shared router."""
        self._assert_idle()
        self._state = _HandleState.DISPATCHED

        config = self._compute_config
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
                )
                while True:
                    try:
                        return future.result(timeout=0.5)
                    except TimeoutError as err:
                        if cancel is not None and cancel.is_set():
                            msg = "Batch compute interrupted"
                            raise RuntimeError(msg) from err
                        continue

        self._start_background(_run)

    def cancel(self) -> None:
        """No-op -- cancellation handled by exiting app.run()."""
