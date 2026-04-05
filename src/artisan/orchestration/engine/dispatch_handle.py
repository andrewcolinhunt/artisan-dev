"""DispatchHandle — lifecycle handle for in-flight backend work."""

from __future__ import annotations

import enum
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable

from artisan.execution.models.execution_composite import ExecutionComposite
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.unit_result import UnitResult


class _HandleState(enum.Enum):
    IDLE = "idle"
    DISPATCHED = "dispatched"
    DONE = "done"


class DispatchHandle(ABC):
    """Lifecycle handle for controlling in-flight backend work.

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
        self._state = _HandleState.IDLE
        self._thread: threading.Thread | None = None
        self._results: list[UnitResult] | None = None
        self._error: Exception | None = None
        self._done = threading.Event()

    # ------------------------------------------------------------------
    # Abstract — subclasses implement these
    # ------------------------------------------------------------------

    @abstractmethod
    def dispatch(
        self,
        units: list[ExecutionUnit | ExecutionComposite],
        runtime_env: RuntimeEnvironment,
    ) -> None:
        """Start execution, return immediately.

        The handle owns unit transport — it decides how to deliver
        units to workers. Must be called exactly once.
        """

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
            raise RuntimeError("collect() called before completion")
        if self._thread is not None:
            self._thread.join()
        if self._error is not None:
            raise self._error
        self._state = _HandleState.DONE
        return self._results  # type: ignore[return-value]

    def run(
        self,
        units: list[ExecutionUnit | ExecutionComposite],
        runtime_env: RuntimeEnvironment,
        cancel_event: threading.Event | None = None,
    ) -> list[UnitResult]:
        """Execute the step. Blocks until completion or cancellation.

        Concrete template method: ``dispatch()`` → poll ``is_done()``
        → ``collect()``. Checks *cancel_event* between polls and calls
        ``cancel()`` when set.
        """
        self.dispatch(units, runtime_env)
        cancelled = False
        while not self.is_done():
            if not cancelled and cancel_event is not None and cancel_event.is_set():
                self.cancel()
                cancelled = True
            time.sleep(0.1)
        return self.collect()

    # ------------------------------------------------------------------
    # Protected helpers for subclasses
    # ------------------------------------------------------------------

    def _assert_idle(self) -> None:
        """Raise if ``dispatch()`` was already called."""
        if self._state is not _HandleState.IDLE:
            raise RuntimeError("dispatch() already called")

    def _start_background(self, fn: Callable[[], list[UnitResult]]) -> None:
        """Run *fn* in a daemon thread, storing results for ``collect()``."""

        def _run() -> None:
            try:
                self._results = fn()
            except Exception as exc:
                self._error = exc
            finally:
                self._done.set()

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()
