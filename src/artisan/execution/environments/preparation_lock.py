"""Serialize explicit preparation of shared environment cache entries."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def preparation_lock(workspace: Path) -> Iterator[None]:
    """Hold one cache workspace's lock until publication or failure cleanup."""
    import fcntl

    workspace.parent.mkdir(parents=True, exist_ok=True)
    lock_path = workspace.with_name(f".{workspace.name}.lock")
    # Keep the inode stable so waiting and newly arriving callers share one lock.
    with lock_path.open("a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        yield
