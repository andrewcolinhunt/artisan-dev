"""LifecycleRouter — places and controls the lifecycle of step_runner work.

The LifecycleRouter places the operation *lifecycle* (prep → execute →
post → record); the ExecuteRouter places the execute phase within it.
"""

from __future__ import annotations

import contextvars
import enum
import logging
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable

from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
)
from artisan.utils.path import cancel_sentinel_path, uri_parent

logger = logging.getLogger(__name__)


class _RouterState(enum.Enum):
    IDLE = "idle"
    DISPATCHED = "dispatched"
    DONE = "done"


class LifecycleRouter(ABC):
    """Places and controls the lifecycle of in-flight step_runner work.

    The manager calls the blocking ``run()`` template in its background step
    thread. Providers can also use ``dispatch()``, ``is_done()``, and
    ``collect()`` to control the lifecycle directly.

    State machine:
        - ``dispatch()`` must be called exactly once (IDLE → DISPATCHED).
        - ``is_done()`` and ``collect()`` are valid after ``dispatch()``.
        - ``cancel()`` is valid in any state and returns cancellation evidence;
          completed work may retain evidence from a nested provider.
        - ``collect()`` is valid only after ``is_done()`` returns True.
    """

    def __init__(self) -> None:
        self._state = _RouterState.IDLE
        self._expected_result_count = 0
        self._thread: threading.Thread | None = None
        self._results: list[UnitResult] | None = None
        self._error: Exception | None = None
        self._done = threading.Event()
        self._cancellation_acknowledgement: CancellationAcknowledgement | None = None

    # ------------------------------------------------------------------
    # Provider hooks
    # ------------------------------------------------------------------

    def dispatch(
        self,
        units: list[ExecutionUnit],
        runtime_env: RuntimeEnvironment,
    ) -> None:
        """Start execution through the provider hook, returning immediately.

        This template owns the router state transition. Providers implement
        :meth:`_dispatch` and never need access to the private state enum.
        """
        self._assert_idle()
        self._expected_result_count = len(units)
        self._state = _RouterState.DISPATCHED
        try:
            self._dispatch(units, runtime_env)
        except Exception as exc:
            self._error = exc
            self._done.set()
            raise

    @abstractmethod
    def _dispatch(
        self,
        units: list[ExecutionUnit],
        runtime_env: RuntimeEnvironment,
    ) -> None:
        """Submit work and arrange for completion state to be populated."""

    @abstractmethod
    def cancel(self) -> CancellationAcknowledgement:
        """Request cancellation and return provider evidence.

        Providers must promptly drive dispatched work to completion after a
        successful cancellation request. Once ``is_done()`` is true,
        ``collect()`` must join any provider-owned collection thread.
        """

    def _set_cancellation_deadline(self, deadline: float) -> None:
        """Supply the absolute cancellation budget to native owners.

        External providers retain their existing cancellation contract. Native
        owners override this hook and may shorten, but never extend, a deadline.
        """
        return

    # ------------------------------------------------------------------
    # Concrete — shared across all handles
    # ------------------------------------------------------------------

    def is_done(self) -> bool:
        """Non-blocking completion check. Thread-safe."""
        return self._done.is_set()

    def collect(self) -> list[UnitResult]:
        """Return results. Valid only after ``is_done()`` returns True.

        Raises:
            RuntimeError: If called before completion.
        """
        if not self._done.is_set():
            msg = "collect() called before completion"
            raise RuntimeError(msg)
        if self._thread is not None:
            self._thread.join()
        self._state = _RouterState.DONE
        if self._error is not None:
            raise self._error
        results = self._results
        if not isinstance(results, list):
            msg = "Lifecycle router completed without a list of UnitResult values"
            raise RuntimeError(msg)
        if len(results) != self._expected_result_count:
            msg = (
                f"Lifecycle router returned {len(results)} results for "
                f"{self._expected_result_count} submitted units"
            )
            raise RuntimeError(msg)
        if not all(isinstance(result, UnitResult) for result in results):
            msg = "Lifecycle router returned a value that is not a UnitResult"
            raise RuntimeError(msg)
        return results

    def run(
        self,
        units: list[ExecutionUnit],
        runtime_env: RuntimeEnvironment,
        cancel_event: threading.Event | None = None,
        cancellation_confirmation_timeout: float = 10.0,
    ) -> list[UnitResult]:
        """Execute the step. Blocks until completion or cancellation.

        Concrete template method: ``dispatch()`` → poll ``is_done()``
        → ``collect()``. Checks *cancel_event* between polls; when it
        fires, writes the cancel sentinel (so worker-held execute calls
        can observe cancellation across the process boundary) and calls
        ``cancel()``.
        """
        self.dispatch(units, runtime_env)
        cancellation_deadline: float | None = None
        cancellation_resolved = False
        while True:
            # Observe completion before cancellation so a final polling boundary
            # cannot discard a request that the provider must reconcile.
            done = self.is_done()
            if (
                cancel_event is not None
                and cancel_event.is_set()
                and not cancellation_resolved
            ):
                if cancellation_deadline is None:
                    cancellation_deadline = (
                        time.monotonic() + cancellation_confirmation_timeout
                    )
                    self._set_cancellation_deadline(cancellation_deadline)
                    self._write_cancel_sentinel(units, runtime_env)
                acknowledgement = self._validated_cancellation_evidence(self.cancel())
                self._cancellation_acknowledgement = acknowledgement
                if acknowledgement.status == CancellationStatus.REQUESTED:
                    if done or time.monotonic() >= cancellation_deadline:
                        self._cancellation_acknowledgement = CancellationAcknowledgement(
                            CancellationStatus.UNKNOWN,
                            (
                                "Lifecycle work completed without final cancellation evidence"
                                if done
                                else "Cancellation confirmation timed out"
                            ),
                        )
                        break
                elif acknowledgement.status == CancellationStatus.UNKNOWN:
                    break
                else:
                    cancellation_resolved = True
            if done:
                break
            remaining = (
                max(0.0, cancellation_deadline - time.monotonic())
                if cancellation_deadline is not None and not cancellation_resolved
                else 0.1
            )
            time.sleep(min(0.1, remaining))
        if (
            self._cancellation_acknowledgement is not None
            and self._cancellation_acknowledgement.status == CancellationStatus.UNKNOWN
            and not self.is_done()
        ):
            acknowledgement = self._cancellation_acknowledgement
            return [
                UnitResult(
                    success=False,
                    error=acknowledgement.message or "Cancellation outcome unknown",
                    item_count=(
                        unit.get_batch_size() or 1
                        if callable(getattr(unit, "get_batch_size", None))
                        else 1
                    ),
                    execution_run_ids=[],
                    cancellation_acknowledgement=acknowledgement,
                )
                for unit in units
            ]
        return self.collect()

    @property
    def cancellation_acknowledgement(
        self,
    ) -> CancellationAcknowledgement | None:
        """Return the final evidence observed by the blocking run template."""
        return self._cancellation_acknowledgement

    @staticmethod
    def _validated_cancellation_evidence(
        evidence: object,
    ) -> CancellationAcknowledgement:
        """Fail closed when a lifecycle provider returns malformed evidence."""
        if isinstance(evidence, CancellationAcknowledgement):
            return evidence
        return CancellationAcknowledgement(
            CancellationStatus.UNKNOWN,
            "Lifecycle runner returned malformed cancellation evidence",
        )

    @staticmethod
    def _write_cancel_sentinel(
        units: list[ExecutionUnit],
        runtime_env: RuntimeEnvironment,
    ) -> None:
        """Best-effort cancel sentinel on the staging filesystem.

        Workers holding remote execute calls cannot see the orchestrator's
        cancel event; they poll for this file instead. A failed write is
        logged, never raised — cancellation must not abort the poll loop.
        """
        step_run_id = next(
            (sid for sid in (getattr(u, "step_run_id", None) for u in units) if sid),
            None,
        )
        if step_run_id is None or runtime_env.staging_root is None:
            return
        try:
            sentinel = cancel_sentinel_path(runtime_env.staging_root, step_run_id)
            fs = runtime_env.storage.filesystem()
            fs.makedirs(uri_parent(sentinel), exist_ok=True)
            fs.touch(sentinel)
        except Exception as exc:
            logger.warning("Failed to write cancel sentinel: %s", exc)

    # ------------------------------------------------------------------
    # Protected helpers for subclasses
    # ------------------------------------------------------------------

    def _assert_idle(self) -> None:
        """Raise if ``dispatch()`` was already called."""
        if self._state is not _RouterState.IDLE:
            msg = "dispatch() already called"
            raise RuntimeError(msg)

    def _start_background(self, fn: Callable[[], list[UnitResult]]) -> None:
        """Run *fn* in a daemon thread, storing results for ``collect()``.

        Copies the current ``contextvars`` context so caller context remains
        available inside provider collection threads.
        """
        ctx = contextvars.copy_context()

        def _run() -> None:
            try:
                self._results = fn()
            except Exception as exc:
                self._error = exc
            finally:
                self._done.set()

        self._thread = threading.Thread(target=lambda: ctx.run(_run), daemon=True)
        self._thread.start()
