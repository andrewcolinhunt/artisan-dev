"""Schema model for orchestration batching configuration."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class BatchConfig:
    """Configuration for two-level orchestration batching.

    Attributes:
        artifacts_per_unit: Artifacts per ExecutionUnit (level 1 batching).
        units_per_worker: ExecutionUnits per worker (level 2 batching).
    """

    artifacts_per_unit: int = 1
    units_per_worker: int = 1
