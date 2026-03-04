"""Phase timing utilities for profiling framework overhead."""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any


@contextmanager
def phase_timer(name: str, timings: dict[str, Any]):
    """Record wall-clock seconds for a named phase into timings dict.

    Args:
        name: Phase name (used as dict key).
        timings: Dict to store the elapsed time in.

    Yields:
        None — the timing is recorded on exit.
    """
    start = time.perf_counter()
    yield
    timings[name] = round(time.perf_counter() - start, 4)
