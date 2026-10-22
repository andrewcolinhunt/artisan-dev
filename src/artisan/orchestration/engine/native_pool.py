"""Shared native process-pool ownership and bounded cleanup."""

from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from contextlib import suppress
from multiprocessing.connection import wait
from multiprocessing.process import BaseProcess

from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
)
from artisan.utils.command_supervisor import (
    CommandSupervisor,
    OwnershipOutcome,
    reject_nested_native_scope,
)

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
