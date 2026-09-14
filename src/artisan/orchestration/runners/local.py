"""Native local runner backed by a spawn-context process pool."""

from __future__ import annotations

import multiprocessing
import threading
import time
import warnings
from concurrent.futures import Future, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from contextlib import suppress
from typing import Any

from artisan.execution.compute.routing import routes_to_endpoint
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
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
)
from artisan.utils.process_call import execute_process_call, serialize_process_call
from artisan.utils.spawn import ignore_sigint, suppress_main_reimport

_COOPERATIVE_CANCEL_SECONDS = 1.0


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
        self._cancel_requested_at: float | None = None
        self._cancel_acknowledgement: CancellationAcknowledgement | None = None
        self._requires_remote_cancellation_evidence = False

    def _dispatch(
        self,
        units: list[ExecutionUnit],
        runtime_env: RuntimeEnvironment,
    ) -> None:
        """Start native process-pool execution in a background thread."""
        batches = pack_units(units, self._units_per_worker)
        with self._lock:
            self._dispatch_started = True
            self._requires_remote_cancellation_evidence = any(
                routes_to_endpoint(unit.operation) for unit in units
            )
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
            self._futures = []
            for index, batch in enumerate(batches):
                try:
                    future = executor.submit(
                        execute_process_call,
                        serialize_process_call(execute_unit_batch, batch, runtime_env),
                    )
                except Exception as exc:
                    for unsent_batch in batches[index:]:
                        failed: Future[list[UnitResult]] = Future()
                        failed.set_result(failure_results_for_units(unsent_batch, exc))
                        self._futures.append(failed)
                    break
                self._futures.append(future)
            return list(self._futures)

    def cancel(self) -> CancellationAcknowledgement:
        """Cancel owned work and confirm only after worker exit is proved."""
        with self._lock:
            if self._cancel_acknowledgement is not None:
                return self._cancel_acknowledgement
            if not self._dispatch_started:
                return CancellationAcknowledgement(
                    CancellationStatus.REJECTED,
                    "Local work has not been dispatched",
                )
            if self.is_done():
                acknowledgement = self._completed_cancellation_evidence()
                self._cancel_acknowledgement = acknowledgement
                return acknowledgement
            if not self._cancel_requested:
                self._cancel_requested = True
                self._cancel_requested_at = time.monotonic()
                futures = list(self._futures)
                for future in futures:
                    future.cancel()
                return CancellationAcknowledgement(
                    CancellationStatus.REQUESTED,
                    "Waiting for cooperative worker cancellation",
                )
            assert self._cancel_requested_at is not None
            if (
                time.monotonic() - self._cancel_requested_at
                < _COOPERATIVE_CANCEL_SECONDS
            ):
                return CancellationAcknowledgement(
                    CancellationStatus.REQUESTED,
                    "Waiting for cooperative worker cancellation",
                )
            futures = list(self._futures)
            executor = self._executor
        for future in futures:
            future.cancel()
        confirmed = executor is None or _terminate_process_pool(executor)
        if confirmed and not self._requires_remote_cancellation_evidence:
            acknowledgement = CancellationAcknowledgement(
                CancellationStatus.CONFIRMED,
                "Local worker processes exited",
            )
        else:
            acknowledgement = CancellationAcknowledgement(
                CancellationStatus.UNKNOWN,
                (
                    "Local worker processes exited without remote cancellation evidence"
                    if confirmed
                    else "Could not prove local worker process exit"
                ),
            )
        with self._lock:
            self._cancel_acknowledgement = acknowledgement
        return acknowledgement

    def _completed_cancellation_evidence(self) -> CancellationAcknowledgement:
        """Resolve a completion race without discarding nested endpoint proof."""
        if (
            not self._cancel_requested
            or not self._requires_remote_cancellation_evidence
        ):
            return CancellationAcknowledgement(
                CancellationStatus.REJECTED,
                "Local work completed before cancellation",
            )
        if self._error is not None or self._results is None:
            return CancellationAcknowledgement(
                CancellationStatus.UNKNOWN,
                "Remote work completed without cancellation evidence",
            )
        outcomes = [
            result.cancellation_acknowledgement
            for result in self._results
            if result.cancellation_acknowledgement is not None
        ]
        for status in (
            CancellationStatus.UNKNOWN,
            CancellationStatus.REQUESTED,
            CancellationStatus.REJECTED,
            CancellationStatus.CONFIRMED,
        ):
            matching = [outcome for outcome in outcomes if outcome.status == status]
            if matching:
                outcome = matching[0]
                if status == CancellationStatus.REQUESTED:
                    return CancellationAcknowledgement(
                        CancellationStatus.UNKNOWN,
                        outcome.message or "Remote cancellation remained unconfirmed",
                    )
                return outcome
        if all(result.success for result in self._results):
            return CancellationAcknowledgement(
                CancellationStatus.REJECTED,
                "Remote work completed before cancellation",
            )
        return CancellationAcknowledgement(
            CancellationStatus.UNKNOWN,
            "Remote work failed without cancellation evidence",
        )


def _terminate_process_pool(executor: ProcessPoolExecutor) -> bool:
    """Terminate owned workers and report whether every process exited."""
    processes_by_pid = getattr(executor, "_processes", None)
    processes = () if processes_by_pid is None else tuple(processes_by_pid.values())
    terminate_workers = getattr(type(executor), "terminate_workers", None)
    if callable(terminate_workers):
        terminate_workers(executor)
    else:
        # Python 3.12 has no public termination API. These are the exact processes
        # owned by this executor; shutdown alone cannot stop an in-flight call.
        try:
            for process in processes:
                try:
                    if process.is_alive():
                        process.terminate()
                except (ProcessLookupError, ValueError):
                    pass
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    for process in processes:
        with suppress(AssertionError, ProcessLookupError, ValueError):
            process.join(timeout=1.0)
    try:
        return all(not process.is_alive() for process in processes)
    except (AssertionError, ProcessLookupError, ValueError):
        return False


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

    @property
    def default_max_workers(self) -> int:
        """Default process pool size used when a step does not override it."""
        return self._default_max_workers

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
