"""Dispatch handle that runs units in a child process with a shared compute router."""

from __future__ import annotations

import multiprocessing
import signal
import threading
from concurrent.futures import ProcessPoolExecutor

from artisan.execution.compute.routing import create_router
from artisan.execution.executors.creator import run_creator_flow
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.orchestration.engine.dispatch_handle import DispatchHandle, _HandleState
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.operation_config.compute import ComputeConfig
from artisan.utils.errors import format_error
from artisan.utils.spawn import suppress_main_reimport


def _ignore_sigint() -> None:
    """Worker initializer: ignore SIGINT so the parent handles cancellation."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def _run_units_with_shared_router(
    units: list[ExecutionUnit],
    runtime_env: RuntimeEnvironment,
    compute_config: ComputeConfig,
    cancel_event: multiprocessing.synchronize.Event | None = None,
) -> list[UnitResult]:
    """Run units sequentially with a shared compute router.

    Creates a router from the picklable compute config, runs all units
    with it, and closes it. Intended to run inside a spawned child
    process -- the router never crosses a process boundary.

    Args:
        units: Execution units to process.
        runtime_env: Paths and backend configuration.
        compute_config: Picklable config for ``create_router()``.
        cancel_event: Cross-process event checked between units.

    Returns:
        One UnitResult per unit. Failed units get error results;
        remaining units get cancellation results if cancel is set.
    """
    router = create_router(compute_config)
    results: list[UnitResult] = []
    try:
        for i, unit in enumerate(units):
            if cancel_event is not None and cancel_event.is_set():
                results.extend(
                    UnitResult(
                        success=False,
                        error="Cancelled",
                        item_count=1,
                        execution_run_ids=[],
                    )
                    for _ in range(len(units) - i)
                )
                break

            try:
                result = run_creator_flow(
                    unit, runtime_env, worker_id=0,
                    compute_router=router,
                )
                results.append(
                    UnitResult(
                        success=result.success,
                        error=result.error,
                        item_count=unit.get_batch_size() or 1,
                        execution_run_ids=[result.execution_run_id],
                    )
                )
            except Exception as exc:
                results.append(
                    UnitResult(
                        success=False,
                        error=format_error(exc),
                        item_count=1,
                        execution_run_ids=[],
                    )
                )
    finally:
        if hasattr(router, "close"):
            router.close()
    return results


class ComputeRoutingDispatchHandle(DispatchHandle):
    """Dispatch units to a child process with a shared compute router.

    Spawns a single child process that creates a ``ComputeRouter`` from
    the picklable ``ComputeConfig``, runs all units sequentially with
    the shared router, and returns results. The router never crosses a
    process boundary -- it is created, used, and closed entirely within
    the child.

    Process isolation ensures a crash in the child (OOM, segfault) does
    not take down the orchestrator.

    Args:
        compute_config: Picklable config passed to ``create_router()``
            inside the child process.
        cancel_event: Pipeline cancel event. Propagated to the child
            via a ``multiprocessing.Event``.
    """

    def __init__(
        self,
        compute_config: ComputeConfig,
        cancel_event: threading.Event | None = None,
    ) -> None:
        super().__init__()
        self._compute_config = compute_config
        self._cancel_event = cancel_event
        self._mp_cancel: multiprocessing.synchronize.Event | None = None

    def dispatch(
        self,
        units: list[ExecutionUnit],
        runtime_env: RuntimeEnvironment,
    ) -> None:
        """Spawn a child process and run all units with a shared router."""
        self._assert_idle()
        self._state = _HandleState.DISPATCHED

        config = self._compute_config
        cancel = self._cancel_event

        mp_ctx = multiprocessing.get_context("spawn")
        mp_cancel = mp_ctx.Event()
        self._mp_cancel = mp_cancel

        def _run() -> list[UnitResult]:
            with (
                suppress_main_reimport(),
                ProcessPoolExecutor(
                    max_workers=1,
                    mp_context=mp_ctx,
                    initializer=_ignore_sigint,
                ) as pool,
            ):
                future = pool.submit(
                    _run_units_with_shared_router,
                    units, runtime_env, config, mp_cancel,
                )
                while True:
                    try:
                        return future.result(timeout=0.5)
                    except TimeoutError:
                        if cancel is not None and cancel.is_set():
                            mp_cancel.set()
                        continue

        self._start_background(_run)

    def cancel(self) -> None:
        """Signal the child process to stop between units."""
        if self._mp_cancel is not None:
            self._mp_cancel.set()
