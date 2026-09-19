"""Phase timing utilities for profiling framework overhead."""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any


@contextmanager
def phase_timer(name: str, timings: dict[str, Any]) -> Iterator[None]:
    """Record wall-clock seconds for a phase that exits successfully.

    Args:
        name: Phase name (used as dict key).
        timings: Dict to store the elapsed time in.

    Yields:
        Control to the timed block. Exceptions propagate without recording.
    """
    start = time.perf_counter()
    yield
    timings[name] = round(time.perf_counter() - start, 4)
