"""Portable hardware resource requirements consumed by step runners.

Each step runner (local / SLURM / SLURM intra) translates these to its
native format. The ``extra`` dict is an escape hatch for runner-specific
settings (e.g. SLURM partition, gres flags). For Modal compute, see
``ComputeResources`` in ``compute_resources.py``.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class RunnerResources(BaseModel):
    """Portable hardware resource requirements for the step runner.

    Read by SLURM / SLURM-intra / local runners at dispatch time. Modal
    has its own hardware-spec schema (``ComputeResources``).

    Attributes:
        cpus: Number of CPU cores per task.
        memory_gb: Memory in gigabytes per task.
        gpus: Number of GPUs requested.
        time_limit: Wall-clock time limit (HH:MM:SS format).
        extra: Runner-specific settings (e.g. {"partition": "gpu"}).
    """

    cpus: int = Field(1, ge=1)
    memory_gb: int = Field(4, ge=1)
    gpus: int = Field(0, ge=0)
    time_limit: str = "01:00:00"
    extra: dict[str, Any] = Field(default_factory=dict)
