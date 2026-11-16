"""Provider cleanup evidence, independent of scientific result availability."""

from __future__ import annotations

from enum import StrEnum
from typing import Protocol


class OwnershipState(StrEnum):
    """Whether the unit and its shared transport have finished required cleanup."""

    PENDING = "pending"
    CLOSED = "closed"
    UNKNOWN = "unknown"


class WorkerOwner(Protocol):
    """Retain cleanup authority for a shared worker, including failed startup."""

    def ownership_state(self) -> OwnershipState: ...

    def request_close(self, deadline: float) -> None:
        """Request whole-worker closure without treating the request as evidence."""
        ...
