"""Utilities for safe multiprocessing spawn behavior."""

from __future__ import annotations

import signal
import sys
import threading
from types import ModuleType


class _MainReimportState:
    """Process-wide state shared by overlapping spawn guards."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.guard_count = 0
        self.saved_module: ModuleType | None = None
        self.saved_file: str | None = None
        self.saved_had_file = False


_main_reimport_state = _MainReimportState()


def ignore_sigint() -> None:
    """Worker initializer: ignore SIGINT so the parent handles cancellation."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)


class suppress_main_reimport:
    """Prevent ``multiprocessing.spawn`` from re-importing ``__main__``.

    CPython's spawn bootstrap reads ``__main__.__file__`` and re-executes
    the caller's script in each child process via ``runpy.run_path()``.
    This causes module-level side effects (argument parsing, file I/O,
    print statements, etc.) to run again in every worker.

    This context manager temporarily sets ``__main__.__file__`` to
    ``None``, preventing the spawn bootstrap from finding the script.
    The original value is restored on exit.

    Overlapping contexts share a reference count. The first context suppresses
    re-import and the last restores it, allowing concurrent process pools
    without serializing their lifetimes.

    Use this around any ``ProcessPoolExecutor`` creation that uses the
    ``"spawn"`` multiprocessing context::

        with suppress_main_reimport(), ProcessPoolExecutor(...) as pool:
            pool.submit(work_fn, ...)

    For long-lived pools where workers are spawned lazily, keep the
    context manager open for the pool's entire lifetime.
    """

    def __enter__(self) -> suppress_main_reimport:
        state = _main_reimport_state
        with state.lock:
            if state.guard_count == 0:
                state.saved_module = sys.modules.get("__main__")
                state.saved_had_file = hasattr(state.saved_module, "__file__")
                state.saved_file = getattr(state.saved_module, "__file__", None)
                if state.saved_module is not None:
                    state.saved_module.__file__ = None
            state.guard_count += 1
        self._entered = True
        return self

    def __exit__(self, *args: object) -> None:
        if not getattr(self, "_entered", False):
            return
        state = _main_reimport_state
        with state.lock:
            state.guard_count -= 1
            if state.guard_count == 0:
                _restore_main_file()
            self._entered = False


def _restore_main_file() -> None:
    """Restore the process-wide main module after the final guard exits."""
    state = _main_reimport_state
    if state.saved_module is not None:
        if state.saved_had_file:
            state.saved_module.__file__ = state.saved_file
        else:
            delattr(state.saved_module, "__file__")
    state.saved_module = None
    state.saved_file = None
    state.saved_had_file = False
