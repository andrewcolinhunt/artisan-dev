"""Native local runner backed by a spawn-context process pool."""

from __future__ import annotations

import multiprocessing
import os
import threading
import time
import warnings
from concurrent.futures import Future, ProcessPoolExecutor
from concurrent.futures import wait as wait_futures
from concurrent.futures.process import BrokenProcessPool
from contextlib import suppress
from multiprocessing.connection import wait
from multiprocessing.process import BaseProcess
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
from artisan.utils.command_supervisor import (
    CommandSupervisor,
    OwnershipOutcome,
    initialize_native_worker,
    reject_nested_native_scope,
)
from artisan.utils.external_tools import redact_values
from artisan.utils.process_call import execute_process_call, serialize_process_call
from artisan.utils.spawn import suppress_main_reimport
from artisan.utils.traceback import format_error

_COOPERATIVE_CANCEL_SECONDS = 1.0
_CLEANUP_SECONDS = 10.0


class _NativePoolOwnership:
    """Retain command ownership and exact worker evidence for either pool owner."""

    def __init__(self) -> None:
        reject_nested_native_scope()
        self.supervisor = CommandSupervisor.start()
        self.executor: ProcessPoolExecutor | None = None
        self._workers: dict[int, BaseProcess] = {}
        self._sentinels: dict[int, int] = {}
        self._tracking_valid = True
        self._dispatching = 0
        self._lock = threading.RLock()
        self.deadline: float | None = None
        self.cancelled = False
        self._stop_started_at: float | None = None
        self._termination_started = False
        self._shutdown_started = False
        self._finished = False
        self._command_outcome: OwnershipOutcome | None = None
        self.closed = False
        self.reason: str | None = None

    def capture_workers(self, *, force: bool = False) -> None:
        """Reserve exit sentinels before the executor can discard worker handles."""
        with self._lock:
            if self.executor is None or (self._shutdown_started and not force):
                return
            processes = getattr(self.executor, "_processes", None)
            if processes is None:
                self._tracking_valid = False
                return
            for pid, process in tuple(processes.items()):
                if pid in self._sentinels:
                    continue
                self._workers[pid] = process
                try:
                    self._sentinels[pid] = os.dup(process.sentinel)
                except (OSError, ValueError):
                    self._tracking_valid = False

    def begin_dispatch(self) -> bool:
        """Reserve pending creation before serialization or OS pool startup."""
        with self._lock:
            if (
                self._finished
                or self._shutdown_started
                or self._stop_started_at is not None
            ):
                return False
            self._dispatching += 1
            return True

    def end_dispatch(self) -> None:
        """Capture late-created workers; an expired dispatch can never imply closure."""
        with self._lock:
            self.capture_workers(force=True)
            assert self._dispatching > 0
            self._dispatching -= 1
            expired = self.deadline is not None and time.monotonic() >= self.deadline
            if self._finished or expired:
                if self.executor is not None:
                    _terminate_process_pool(
                        self.executor,
                        deadline=min(
                            self.deadline or time.monotonic(), time.monotonic()
                        ),
                        processes=tuple(self._workers.values()),
                        sentinels=tuple(self._sentinels.values()),
                    )
                    self._shutdown_started = True
                for sentinel in self._sentinels.values():
                    os.close(sentinel)
                self._sentinels.clear()
            else:
                self._terminate_if_due()

    def _workers_exited(self) -> bool:
        with self._lock:
            if (
                self._dispatching
                or not self._tracking_valid
                or len(self._workers) != len(self._sentinels)
            ):
                return False
            if not self._sentinels:
                return True
            try:
                return len(wait(tuple(self._sentinels.values()), timeout=0)) == len(
                    self._sentinels
                )
            except (OSError, ValueError):
                self._tracking_valid = False
                return False

    def request_cancel(self, deadline: float) -> bool:
        """Close admission promptly; collection drives worker termination."""
        with self._lock:
            if self._finished:
                return False
            self.cancelled = True
            self._request_stop(deadline)
            return True

    def request_failure_cleanup(self) -> None:
        """Close a broken pool's work without inventing a cancellation request."""
        with self._lock:
            self._request_stop(time.monotonic() + _CLEANUP_SECONDS)

    def _request_stop(self, deadline: float) -> None:
        previous_deadline = self.deadline
        first_request = self._stop_started_at is None
        self.deadline = min(self.deadline or deadline, deadline)
        if first_request:
            self._stop_started_at = time.monotonic()
        if (
            first_request
            or previous_deadline is None
            or self.deadline < previous_deadline
        ):
            self.supervisor.request_close(self.deadline)

    def advance(self) -> bool:
        """Drive bounded cleanup while collecting; report an expired budget."""
        with self._lock:
            outcome = self.supervisor.poll()
            if outcome.state == "unknown" and self._stop_started_at is None:
                self.reason = outcome.reason or "Managed command ownership is unknown"
                self._request_stop(time.monotonic() + _CLEANUP_SECONDS)
            if self._stop_started_at is None and self._sentinels:
                try:
                    exited = wait(tuple(self._sentinels.values()), timeout=0)
                except (OSError, ValueError):
                    self._tracking_valid = False
                    self.reason = "Native worker exit evidence is unavailable"
                    self._request_stop(time.monotonic() + _CLEANUP_SECONDS)
                    exited = []
                if exited:
                    self.reason = "A native worker exited before collection completed"
                    self._request_stop(time.monotonic() + _CLEANUP_SECONDS)
                    assert self.deadline is not None
                    pid = next(
                        pid
                        for pid, sentinel in self._sentinels.items()
                        if sentinel in exited
                    )
                    self.supervisor.worker_lost(pid, self.deadline)
            self._terminate_if_due()
            return self.deadline is not None and time.monotonic() >= self.deadline

    def _terminate_if_due(self) -> None:
        if (
            self._stop_started_at is None
            or self._termination_started
            or self._dispatching
        ):
            return
        assert self.deadline is not None
        if time.monotonic() < min(
            self._stop_started_at + _COOPERATIVE_CANCEL_SECONDS, self.deadline
        ):
            return
        self._termination_started = True
        if self.executor is not None:
            self.capture_workers()
            _terminate_process_pool(
                self.executor,
                deadline=min(self.deadline, time.monotonic() + 0.01),
                processes=tuple(self._workers.values()),
                sentinels=tuple(self._sentinels.values()),
            )
            self._shutdown_started = True

    def finish(
        self, *, failed: bool = False, cancel_event: threading.Event | None = None
    ) -> bool:
        """Close the pool and supervisor within one budget, retaining uncertainty."""
        with self._lock:
            if self._finished:
                return self.closed
            self.capture_workers()
            if self.deadline is None:
                self.deadline = time.monotonic() + _CLEANUP_SECONDS
            if failed:
                self._request_stop(self.deadline)
            elif (
                self.executor is not None
                and not self._shutdown_started
                and not self._dispatching
            ):
                self.executor.shutdown(wait=False, cancel_futures=self.cancelled)
                self._shutdown_started = True
        try:
            self.supervisor.request_finish(self.deadline)
            while True:
                if (
                    cancel_event is not None
                    and cancel_event.is_set()
                    and not self.cancelled
                ):
                    self.request_cancel(time.monotonic() + _CLEANUP_SECONDS)
                with self._lock:
                    self._terminate_if_due()
                    remaining = self.deadline - time.monotonic()
                self._command_outcome = self.supervisor.poll()
                if (
                    self._command_outcome.state != "pending" and self._workers_exited()
                ) or remaining <= 0:
                    break
                time.sleep(min(0.01, remaining))
            # Final IPC/process cleanup uses only what remains of the same budget.
            self._command_outcome = self.supervisor.finish(self.deadline)
            with self._lock:
                self.closed = (
                    self._workers_exited()
                    and self._command_outcome.state == "closed"
                    and not (self.cancelled and self._command_outcome.external)
                )
                if not self.closed:
                    self.reason = (
                        self._command_outcome.reason
                        or "Could not prove native worker and managed command closure"
                    )
                self._finished = True
                return self.closed
        finally:
            with self._lock:
                self._finished = True
                if not self.closed and self.reason is None:
                    self.reason = "Native ownership cleanup did not complete"
                if (
                    self.executor is not None
                    and not self._shutdown_started
                    and not self._dispatching
                ):
                    self.executor.shutdown(wait=False, cancel_futures=True)
                    self._shutdown_started = True
                for sentinel in self._sentinels.values():
                    os.close(sentinel)
                self._sentinels.clear()

    def acknowledgement(self) -> CancellationAcknowledgement:
        """Return settled proof or a truthful pending/unknown cancellation result."""
        with self._lock:
            if self._finished:
                return CancellationAcknowledgement(
                    CancellationStatus.CONFIRMED
                    if self.closed
                    else CancellationStatus.UNKNOWN,
                    "Native workers and managed commands stopped"
                    if self.closed
                    else self.reason,
                )
            if self.deadline is not None and time.monotonic() >= self.deadline:
                return CancellationAcknowledgement(
                    CancellationStatus.UNKNOWN,
                    "Native cancellation confirmation timed out",
                )
            return CancellationAcknowledgement(
                CancellationStatus.REQUESTED, "Waiting for native ownership closure"
            )


class LocalLifecycleRouter(LifecycleRouter):
    """Run ordered unit batches with independent command and worker ownership."""

    def __init__(self, max_workers: int, units_per_worker: int) -> None:
        super().__init__()
        self._max_workers = max_workers
        self._units_per_worker = units_per_worker
        self._lock = threading.RLock()
        self._executor: ProcessPoolExecutor | None = None
        self._ownership: _NativePoolOwnership | None = None
        self._futures: list[Future[list[UnitResult]]] = []
        self._dispatch_started = False
        self._scope_expected = False
        self._cancel_requested = False
        self._cancel_requested_at: float | None = None
        self._cancel_deadline: float | None = None
        self._cancel_acknowledgement: CancellationAcknowledgement | None = None
        self._remote_indices: list[int] = []

    def _set_cancellation_deadline(self, deadline: float) -> None:
        with self._lock:
            self._cancel_deadline = min(self._cancel_deadline or deadline, deadline)
            if self._cancel_requested and self._ownership is not None:
                self._ownership.request_cancel(self._cancel_deadline)

    def _dispatch(
        self,
        units: list[ExecutionUnit],
        runtime_env: RuntimeEnvironment,
    ) -> None:
        reject_nested_native_scope()
        batches = pack_units(units, self._units_per_worker)
        with self._lock:
            self._dispatch_started = True
            self._scope_expected = bool(batches)
            self._remote_indices = [
                index
                for index, unit in enumerate(units)
                if routes_to_endpoint(unit.operation)
            ]
        self._start_background(lambda: self._run_batches(batches, runtime_env))

    def _run_batches(
        self,
        batches: list[list[ExecutionUnit]],
        runtime_env: RuntimeEnvironment,
    ) -> list[UnitResult]:
        if not batches:
            return []
        try:
            return self._execute_batches(batches, runtime_env)
        except Exception as exc:
            units = [unit for batch in batches for unit in batch]
            return failure_results_for_units(
                units,
                _owner_error(exc, self._ownership)
                if self._ownership is not None
                else exc,
            )

    def _execute_batches(
        self,
        batches: list[list[ExecutionUnit]],
        runtime_env: RuntimeEnvironment,
    ) -> list[UnitResult]:
        ownership = _NativePoolOwnership()
        with self._lock:
            self._ownership = ownership
            if self._cancel_requested:
                assert self._cancel_deadline is not None
                ownership.request_cancel(self._cancel_deadline)
        collected = False
        try:
            with suppress_main_reimport():
                admitted = ownership.begin_dispatch()
                try:
                    with self._lock:
                        cancelled = self._cancel_requested
                    if admitted and not cancelled:
                        executor = ProcessPoolExecutor(
                            max_workers=self._max_workers,
                            mp_context=multiprocessing.get_context("spawn"),
                            initializer=initialize_native_worker,
                            initargs=(ownership.supervisor.attachment,),
                        )
                        with self._lock:
                            self._executor = executor
                            ownership.executor = executor
                finally:
                    if admitted:
                        ownership.end_dispatch()
                futures = self._submit_batches(batches, runtime_env)
                results = _collect_batch_futures(batches, futures, ownership)
                collected = True
        finally:
            closed = ownership.finish(failed=not collected)
            with self._lock:
                self._executor = None
        if not closed:
            self._cancellation_acknowledgement = CancellationAcknowledgement(
                CancellationStatus.UNKNOWN, ownership.reason
            )
        return results

    def _submit_batches(
        self,
        batches: list[list[ExecutionUnit]],
        runtime_env: RuntimeEnvironment,
    ) -> list[Future[list[UnitResult]]]:
        with self._lock:
            executor = self._executor
            ownership = self._ownership
            cancelled = self._cancel_requested
            if cancelled or executor is None:
                return []
            assert ownership is not None
            self._futures = []
        for index, batch in enumerate(batches):
            admitted = ownership.begin_dispatch()
            try:
                if not admitted:
                    message = "Local execution cancelled before submission"
                    raise RuntimeError(message)
                # Serialization and process creation may block. Cancellation must
                # still reach the independent owner while either is in flight.
                call = serialize_process_call(execute_unit_batch, batch, runtime_env)
                with self._lock:
                    cancelled_before_submit = self._cancel_requested
                    if cancelled_before_submit:
                        message = "Local execution cancelled before submission"
                        raise RuntimeError(message)
                future = executor.submit(execute_process_call, call)
            except Exception as exc:
                ownership.request_failure_cleanup()
                error = _owner_error(exc, ownership)
                with self._lock:
                    for unsent_batch in batches[index:]:
                        failed: Future[list[UnitResult]] = Future()
                        failed.set_result(
                            failure_results_for_units(unsent_batch, error)
                        )
                        self._futures.append(failed)
                break
            finally:
                if admitted:
                    ownership.end_dispatch()
            with self._lock:
                self._futures.append(future)
                if self._cancel_requested:
                    future.cancel()
        with self._lock:
            return list(self._futures)

    def cancel(self) -> CancellationAcknowledgement:
        """Initiate cleanup promptly; confirm only from settled ownership proof."""
        with self._lock:
            if self._cancel_acknowledgement is not None:
                return self._cancel_acknowledgement
            if not self._dispatch_started:
                return CancellationAcknowledgement(
                    CancellationStatus.REJECTED, "Local work has not been dispatched"
                )
            if self.is_done():
                acknowledgement = self._completed_cancellation_evidence()
                self._cancel_acknowledgement = acknowledgement
                return acknowledgement
            newly_requested = not self._cancel_requested
            if newly_requested:
                self._cancel_requested = True
                self._cancel_requested_at = time.monotonic()
                self._cancel_deadline = self._cancel_deadline or (
                    self._cancel_requested_at + _CLEANUP_SECONDS
                )
            assert self._cancel_deadline is not None
            if self._ownership is not None:
                accepted = self._ownership.request_cancel(self._cancel_deadline)
                if not accepted and newly_requested:
                    self._cancel_requested = False
                    self._cancel_acknowledgement = (
                        self._completed_cancellation_evidence()
                    )
                    return self._cancel_acknowledgement
            for future in self._futures:
                future.cancel()
            if time.monotonic() >= self._cancel_deadline:
                self._cancel_acknowledgement = CancellationAcknowledgement(
                    CancellationStatus.UNKNOWN,
                    "Native cancellation confirmation timed out",
                )
                return self._cancel_acknowledgement
            return CancellationAcknowledgement(
                CancellationStatus.REQUESTED, "Waiting for native ownership closure"
            )

    def _completed_cancellation_evidence(self) -> CancellationAcknowledgement:
        ownership = self._ownership
        if self._scope_expected and (ownership is None or not ownership.closed):
            return CancellationAcknowledgement(
                CancellationStatus.UNKNOWN,
                ownership.reason
                if ownership is not None
                else "Native owner evidence is missing",
            )
        if not self._cancel_requested:
            return CancellationAcknowledgement(
                CancellationStatus.REJECTED, "Local work completed before cancellation"
            )
        if ownership is None:
            return CancellationAcknowledgement(
                CancellationStatus.CONFIRMED, "Empty local dispatch never started work"
            )
        if self._remote_indices:
            if self._results is None or self._error is not None:
                return CancellationAcknowledgement(
                    CancellationStatus.UNKNOWN,
                    "Remote cancellation evidence is missing",
                )
            for index in self._remote_indices:
                result = self._results[index]
                outcome = result.cancellation_acknowledgement
                if (
                    outcome is not None
                    and outcome.status == CancellationStatus.CONFIRMED
                ):
                    continue
                if result.success and (
                    outcome is None or outcome.status == CancellationStatus.REJECTED
                ):
                    continue
                return CancellationAcknowledgement(
                    CancellationStatus.UNKNOWN,
                    "Local ownership closed without remote cancellation evidence",
                )
        return ownership.acknowledgement()


def _terminate_process_pool(
    executor: ProcessPoolExecutor,
    *,
    deadline: float | None = None,
    processes: tuple[BaseProcess, ...] | None = None,
    sentinels: tuple[int, ...] | None = None,
) -> bool:
    """Terminate owned workers and report whether every process exited."""
    if processes is None:
        processes_by_pid = getattr(executor, "_processes", None)
        if processes_by_pid is None:
            return False
        processes = tuple(processes_by_pid.values())
    manager = getattr(executor, "_executor_manager_thread", None)
    owns_sentinels = sentinels is None
    retained = list(sentinels or ())
    try:
        if owns_sentinels:
            for process in processes:
                with suppress(OSError, ValueError):
                    retained.append(os.dup(process.sentinel))
        terminate_workers = getattr(type(executor), "terminate_workers", None)
        if callable(terminate_workers):
            terminate_workers(executor)
        else:
            # Python 3.12 has no public termination API. Only signal workers
            # owned by this executor; its manager remains responsible for reaping.
            try:
                for process in processes:
                    with suppress(ProcessLookupError, ValueError):
                        process.terminate()
            finally:
                executor.shutdown(wait=False, cancel_futures=True)

        if len(retained) != len(processes):
            return False
        # Concurrent Process.join()/is_alive() can miss another thread's waitpid
        # result before it publishes exitcode. Sentinel readiness cannot be reaped.
        pending = set(retained)
        if deadline is None:
            deadline = time.monotonic() + 1.0
        while pending:
            ready = wait(pending, timeout=max(0.0, deadline - time.monotonic()))
            if not ready:
                return False
            pending.difference_update(ready)
        if manager is not None:
            manager.join(timeout=max(0.0, deadline - time.monotonic()))
        return True
    except (OSError, ValueError):
        return False
    finally:
        if owns_sentinels:
            for sentinel in retained:
                os.close(sentinel)


def _owner_error(error: BaseException, ownership: _NativePoolOwnership) -> str:
    """Protect capability aliases in bootstrap and transport failures."""
    return redact_values(
        format_error(error), (ownership.supervisor.attachment["secret"],)
    )


def _collect_batch_futures(
    batches: list[list[ExecutionUnit]],
    futures: list[Future[list[UnitResult]]],
    ownership: _NativePoolOwnership,
) -> list[UnitResult]:
    """Poll with the owner budget, retaining ordered completed siblings."""
    if not futures:
        return failure_results_for_units(
            [unit for batch in batches for unit in batch],
            "Local execution cancelled before submission",
        )

    inspected: set[Future[list[UnitResult]]] = set()
    while True:
        ready = {future for future in futures if future.done()}
        for future in ready - inspected:
            if not future.cancelled() and isinstance(
                future.exception(), BrokenProcessPool
            ):
                ownership.request_failure_cleanup()
        inspected.update(ready)
        pending = {future for future in futures if not future.done()}
        if not pending or ownership.advance():
            break
        # Recompute done() on the next iteration: a cancelled queued Future need
        # not enter CANCELLED_AND_NOTIFIED in a broken executor manager.
        wait_futures(pending, timeout=0.02)

    results: list[UnitResult] = []
    for batch, future in zip(batches, futures, strict=True):
        if not future.done():
            results.extend(
                failure_results_for_units(batch, "Native cleanup remained unresolved")
            )
            continue
        try:
            batch_results = future.result()
        except Exception as exc:
            batch_results = failure_results_for_units(
                batch, _owner_error(exc, ownership)
            )
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
