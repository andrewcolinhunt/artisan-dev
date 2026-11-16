"""Transient worker-owned scratch and preservation for trusted admitted entrypoints."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class _WorkerDirectory:
    path: Path
    preserve_working: bool


_directory: ContextVar[_WorkerDirectory | None] = ContextVar(
    "artisan_worker_directory", default=None
)


@contextmanager
def worker_directory_scope(
    directory: Path, *, preserve_working: bool = False
) -> Iterator[None]:
    """Bind fresh scratch and exact preservation; reset on every exit."""
    if _directory.get() is not None:
        message = "A worker directory scope is already active"
        raise RuntimeError(message)
    token = _directory.set(_WorkerDirectory(directory, preserve_working))
    try:
        yield
    finally:
        _directory.reset(token)


def resolve_worker_directory(
    explicit: Path | None, *, preserve_working: bool = False
) -> Path:
    """Use an explicit path or validate the scoped admitted preservation policy."""
    if explicit is not None:
        return explicit
    directory = _directory.get()
    if directory is None:
        message = "Omitted working_root requires an active worker-owned directory"
        raise RuntimeError(message)
    if directory.preserve_working != preserve_working:
        message = "Worker preservation policy differs from its admitted dispatch"
        raise ValueError(message)
    return directory.path
