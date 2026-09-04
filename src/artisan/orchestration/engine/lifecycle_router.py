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
from artisan.utils.path import cancel_sentinel_path, uri_parent

logger = logging.getLogger(__name__)


class _RouterState(enum.Enum):
    IDLE = "idle"
    DISPATCHED = "dispatched"
    DONE = "done"


class LifecycleRouter(ABC):
    """Places and controls the lifecycle of in-flight step_runner work.

    Provides start, poll, collect, and cancel semantics. Non-streaming
    pipelines use ``run()`` (blocking template method). The streaming
    step scheduler uses the non-blocking ``dispatch()`` / ``is_done()``
    / ``collect()`` methods directly.

    State machine:
        - ``dispatch()`` must be called exactly once (IDLE → DISPATCHED).
        - ``is_done()`` and ``collect()`` are valid after ``dispatch()``.
        - ``cancel()`` is valid in any state (no-op if idle or done).
        - ``collect()`` is valid only after ``is_done()`` returns True.
    """

    def __init__(self) -> None:
        self._state = _RouterState.IDLE
        self._expected_result_count = 0
        self._thread: threading.Thread | None = None
        self._results: list[UnitResult] | None = None
        self._error: Exception | None = None
        self._done = threading.Event()

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
    def cancel(self) -> None:
        """Cancel in-flight work. Thread-safe and idempotent."""

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
    ) -> list[UnitResult]:
        """Execute the step. Blocks until completion or cancellation.

        Concrete template method: ``dispatch()`` → poll ``is_done()``
        → ``collect()``. Checks *cancel_event* between polls; when it
        fires, writes the cancel sentinel (so worker-held execute calls
        can observe cancellation across the process boundary) and calls
        ``cancel()``.
        """
        self.dispatch(units, runtime_env)
        cancelled = False
        while not self.is_done():
            if not cancelled and cancel_event is not None and cancel_event.is_set():
                self._write_cancel_sentinel(units, runtime_env)
                self.cancel()
                cancelled = True
            time.sleep(0.1)
        return self.collect()

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
