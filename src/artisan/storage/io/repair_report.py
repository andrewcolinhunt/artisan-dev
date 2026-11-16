"""Read-only findings shared by store inspection and explicit repair actions."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

RepairClassification = Literal[
    "complete",
    "replayable",
    "conflict",
    "corrupt",
    "unplanned",
    "legacy_unverifiable",
    "abandoned",
    "recoverable",
    "incomplete",
    "ineligible",
    "unknown_owner",
]


class RepairItem(BaseModel):
    """One independently actionable piece of store evidence."""

    evidence_id: str
    classification: RepairClassification
    detail: str

    model_config = ConfigDict(frozen=True)


class RepairReport(BaseModel):
    """Deterministically ordered offline repair report."""

    items: tuple[RepairItem, ...]

    model_config = ConfigDict(frozen=True)

    @property
    def blocking_items(self) -> tuple[RepairItem, ...]:
        """Integrity findings that forbid recovery or new pipeline work."""
        return tuple(
            item
            for item in self.items
            if item.classification in {"corrupt", "conflict", "legacy_unverifiable"}
        )

    @property
    def blocking(self) -> bool:
        """Whether integrity findings forbid recovery or new pipeline work."""
        return bool(self.blocking_items)

    @property
    def unresolved(self) -> bool:
        """Whether the report contains evidence requiring operator action."""
        return any(
            item.classification not in {"complete", "abandoned"} for item in self.items
        )
