"""Schema model for orchestration batching configuration."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class BatchConfig:
    """Configuration for execution-unit input batching.

    Attributes:
        artifacts_per_unit: Artifacts per ExecutionUnit (level 1 batching).
    """

    artifacts_per_unit: int = 1
