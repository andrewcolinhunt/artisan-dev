"""Utilities for safe multiprocessing spawn behavior."""

from __future__ import annotations

import signal
import sys
import threading
from types import ModuleType

_main_reimport_lock = threading.Lock()
_main_reimport_guard_count = 0
_saved_main_module: ModuleType | None = None
_saved_main_file: str | None = None
_saved_main_had_file = False


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
        global _main_reimport_guard_count
        global _saved_main_file
        global _saved_main_had_file
        global _saved_main_module

        with _main_reimport_lock:
            if _main_reimport_guard_count == 0:
                _saved_main_module = sys.modules.get("__main__")
                _saved_main_had_file = hasattr(_saved_main_module, "__file__")
                _saved_main_file = getattr(_saved_main_module, "__file__", None)
                if _saved_main_module is not None:
                    _saved_main_module.__file__ = None
            _main_reimport_guard_count += 1
        self._entered = True
        return self

    def __exit__(self, *args: object) -> None:
        global _main_reimport_guard_count

        if not getattr(self, "_entered", False):
            return
        with _main_reimport_lock:
            _main_reimport_guard_count -= 1
            if _main_reimport_guard_count == 0:
                _restore_main_file()
            self._entered = False


def _restore_main_file() -> None:
    """Restore the process-wide main module after the final guard exits."""
    global _saved_main_file
    global _saved_main_had_file
    global _saved_main_module

    if _saved_main_module is not None:
        if _saved_main_had_file:
            _saved_main_module.__file__ = _saved_main_file
        else:
            delattr(_saved_main_module, "__file__")
    _saved_main_module = None
    _saved_main_file = None
    _saved_main_had_file = False
