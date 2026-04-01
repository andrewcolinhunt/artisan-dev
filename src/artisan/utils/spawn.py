"""Utilities for safe multiprocessing spawn behavior."""

from __future__ import annotations

import sys


class suppress_main_reimport:
    """Prevent ``multiprocessing.spawn`` from re-importing ``__main__``.

    CPython's spawn bootstrap reads ``__main__.__file__`` and re-executes
    the caller's script in each child process via ``runpy.run_path()``.
    This causes module-level side effects (argument parsing, file I/O,
    print statements, etc.) to run again in every worker.

    This context manager temporarily sets ``__main__.__file__`` to
    ``None``, preventing the spawn bootstrap from finding the script.
    The original value is restored on exit.

    Use this around any ``ProcessPoolExecutor`` creation that uses the
    ``"spawn"`` multiprocessing context::

        with suppress_main_reimport(), ProcessPoolExecutor(...) as pool:
            pool.submit(work_fn, ...)

    For long-lived pools where workers are spawned lazily, keep the
    context manager open for the pool's entire lifetime.
    """

    def __enter__(self) -> suppress_main_reimport:
        self._main_mod = sys.modules.get("__main__")
        self._original_file = getattr(self._main_mod, "__file__", None)
        if self._main_mod is not None:
            self._main_mod.__file__ = None  # type: ignore[attr-defined]
        return self

    def __exit__(self, *args: object) -> None:
        if self._main_mod is not None:
            self._main_mod.__file__ = self._original_file  # type: ignore[attr-defined]
