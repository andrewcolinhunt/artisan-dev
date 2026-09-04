"""Unit execution result model.

``UnitResult`` represents the outcome of exactly one execution unit.
Lifecycle runners return one result per submitted unit in the same order.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class UnitResult:
    """Result of executing one execution unit.

    Attributes:
        success: Whether execution succeeded.
        error: Error message if execution failed, else None.
        item_count: Number of items processed.
        execution_run_ids: Run IDs produced by this unit.
        worker_log: Optional stdout/stderr captured by the runner provider.
    """

    success: bool
    error: str | None
    item_count: int
    execution_run_ids: list[str]
    worker_log: str | None = None
