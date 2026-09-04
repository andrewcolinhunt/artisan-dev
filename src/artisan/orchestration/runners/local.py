"""Native local runner backed by a spawn-context process pool."""

from __future__ import annotations

import multiprocessing
import threading
import warnings
from concurrent.futures import Future, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Any

from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.orchestration.engine.batching import pack_units
from artisan.orchestration.engine.dispatch import (
    execute_unit_batch,
    failure_results_for_units,
    validate_batch_results,
)
from artisan.orchestration.engine.lifecycle_router import LifecycleRouter
from artisan.orchestration.runners.base import (
    OrchestratorTraits,
    RunnerBase,
    WorkerTraits,
)
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.utils.spawn import ignore_sigint, suppress_main_reimport


class LocalLifecycleRouter(LifecycleRouter):
    """Run ordered unit batches in local worker processes.

    Args:
        max_workers: Maximum number of worker processes.
        units_per_worker: Maximum execution units per worker invocation.
    """

    def __init__(self, max_workers: int, units_per_worker: int) -> None:
        super().__init__()
        self._max_workers = max_workers
        self._units_per_worker = units_per_worker
        self._lock = threading.Lock()
        self._executor: ProcessPoolExecutor | None = None
        self._futures: list[Future[list[UnitResult]]] = []
        self._dispatch_started = False
        self._cancel_requested = False

    def _dispatch(
        self,
        units: list[ExecutionUnit],
        runtime_env: RuntimeEnvironment,
    ) -> None:
        """Start native process-pool execution in a background thread."""
        batches = pack_units(units, self._units_per_worker)
        with self._lock:
            self._dispatch_started = True
        self._start_background(lambda: self._run_batches(batches, runtime_env))

    def _run_batches(
        self,
        batches: list[list[ExecutionUnit]],
        runtime_env: RuntimeEnvironment,
    ) -> list[UnitResult]:
        """Own the process pool until all submitted batches are resolved."""
        if not batches:
            return []

        try:
            return self._execute_batches(batches, runtime_env)
        except BrokenProcessPool:
            raise
        except Exception as exc:
            units = [unit for batch in batches for unit in batch]
            return failure_results_for_units(units, exc)

    def _execute_batches(
        self,
        batches: list[list[ExecutionUnit]],
        runtime_env: RuntimeEnvironment,
    ) -> list[UnitResult]:
        """Create the process pool, submit batches, and collect results."""

        mp_context = multiprocessing.get_context("spawn")
        with suppress_main_reimport():
            executor = ProcessPoolExecutor(
                max_workers=self._max_workers,
                mp_context=mp_context,
                initializer=ignore_sigint,
            )
            try:
                futures = self._submit_batches(executor, batches, runtime_env)
                return _collect_batch_futures(batches, futures)
            finally:
                executor.shutdown(
                    wait=True,
                    cancel_futures=self._cancel_requested,
                )
                with self._lock:
                    self._executor = None

    def _submit_batches(
        self,
        executor: ProcessPoolExecutor,
        batches: list[list[ExecutionUnit]],
        runtime_env: RuntimeEnvironment,
    ) -> list[Future[list[UnitResult]]]:
        """Submit all batches unless cancellation arrived before pool startup."""
        with self._lock:
            self._executor = executor
            if self._cancel_requested:
                return []
            self._futures = [
                executor.submit(execute_unit_batch, batch, runtime_env)
                for batch in batches
            ]
            return list(self._futures)

    def cancel(self) -> None:
        """Cancel pending work and stop accepting new submissions.

        Running worker processes cannot be terminated reliably through Python
        3.12's public process-pool API and therefore finish best-effort.
        """
        with self._lock:
            if not self._dispatch_started or self._cancel_requested or self.is_done():
                return
            self._cancel_requested = True
            futures = list(self._futures)
            executor = self._executor
        for future in futures:
            future.cancel()
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)


def _collect_batch_futures(
    batches: list[list[ExecutionUnit]],
    futures: list[Future[list[UnitResult]]],
) -> list[UnitResult]:
    """Collect future results in submission order and validate each batch."""
    if not futures:
        return failure_results_for_units(
            [unit for batch in batches for unit in batch],
            "Local execution cancelled before submission",
        )

    results: list[UnitResult] = []
    for batch, future in zip(batches, futures, strict=True):
        try:
            batch_results = future.result()
        except BrokenProcessPool:
            raise
        except Exception as exc:
            batch_results = failure_results_for_units(batch, exc)
        else:
            batch_results = validate_batch_results(batch, batch_results)
        results.extend(batch_results)
    return results


class LocalRunner(RunnerBase):
    """Process-pool execution on the orchestrator machine.

    Args:
        default_max_workers: Default process pool size. Overridden by
            ``BatchStrategy.max_workers`` when set.
    """

    name = "local"
    worker_traits = WorkerTraits()
    orchestrator_traits = OrchestratorTraits()

    def __init__(self, default_max_workers: int = 4) -> None:
        if default_max_workers < 1:
            msg = "default_max_workers must be at least 1"
            raise ValueError(msg)
        self._default_max_workers = default_max_workers

    def create_lifecycle_router(
        self,
        runner_resources: RunnerResources,
        batch_strategy: BatchStrategy,
        step_number: int,
        job_name: str,
        log_folder: str | None = None,
        staging_root: str | None = None,
    ) -> LifecycleRouter:
        """Build a native local process-pool router.

        GPU operations default to one process to avoid GPU memory contention.
        An explicit ``max_workers`` always takes precedence.
        """
        if batch_strategy.max_workers is not None:
            max_workers = batch_strategy.max_workers
        elif runner_resources.gpus > 0:
            max_workers = 1
        else:
            max_workers = self._default_max_workers
        if max_workers < 1:
            msg = "max_workers must be at least 1"
            raise ValueError(msg)

        return LocalLifecycleRouter(
            max_workers=max_workers,
            units_per_worker=batch_strategy.units_per_worker,
        )

    def validate_operation(self, operation: Any) -> None:
        """Warn on local-runner configuration that is likely a mistake."""
        resources = operation.runner_resources
        if resources.extra:
            warnings.warn(
                f"Operation {operation.name!r} has provider-specific resources "
                f"(extra={resources.extra!r}) but step_runner is 'local'. "
                "These will be ignored.",
                stacklevel=2,
            )
        if resources.gpus > 0 and operation.compute_provider.active == "modal":
            warnings.warn(
                f"Operation {operation.name!r} sets runner_resources.gpus="
                f"{resources.gpus} with compute_provider='modal'. The GPU request "
                "serializes the local lifecycle pool, but execute runs on Modal — "
                "request the container GPU via compute_resources.gpu instead.",
                stacklevel=2,
            )
