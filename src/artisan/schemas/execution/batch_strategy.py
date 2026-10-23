"""Batch-strategy configuration for operation execution.

Controls workload slicing — how artifacts are bundled into ExecutionUnits,
how units are distributed to workers, and the parallelism cap.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class BatchStrategy(BaseModel):
    """Control how work is divided into units and distributed to workers.

    ``job_name`` defaults to ``None`` and is resolved to the operation
    name by the orchestrator during instance construction.

    Attributes:
        artifacts_per_unit: Artifacts per ExecutionUnit (level 1 batching).
        max_artifacts_per_unit: Upper bound on artifacts per unit when
            using adaptive batching. None disables the cap.
        units_per_worker: ExecutionUnits per worker (level 2 batching).
        max_workers: Maximum number of parallel workers. None for
            unlimited.
        estimated_seconds: Expected wall-clock time per unit, available to
            runners for scheduling hints.
        job_name: Human-readable job name for logging and scheduler
            labels. Resolved from the operation name if None.
    """

    model_config = ConfigDict(extra="forbid")

    artifacts_per_unit: int = Field(1, ge=1)
    max_artifacts_per_unit: int | None = Field(default=None, ge=1)
    units_per_worker: int = Field(1, ge=1)
    max_workers: int | None = Field(default=None, ge=1)
    estimated_seconds: float | None = Field(default=None, gt=0)
    job_name: str | None = None
