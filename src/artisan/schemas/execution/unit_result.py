"""Unit execution result model.

``UnitResult`` represents the outcome of executing one unit (or one
batch of units) in the dispatch layer. Replaces the informal
``list[dict]`` contract between dispatch, result aggregation, and
backend log capture.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class UnitResult:
    """Result of executing one unit (or one batch of units).

    Attributes:
        success: Whether execution succeeded.
        error: Error message if execution failed, else None.
        item_count: Number of items processed.
        execution_run_ids: Run IDs produced by this unit.
        worker_log: Captured worker stdout/stderr (SLURM backends).
    """

    success: bool
    error: str | None
    item_count: int
    execution_run_ids: list[str]
    worker_log: str | None = None
