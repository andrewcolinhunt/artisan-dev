"""Hardware resource allocation for SLURM jobs."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ResourceConfig(BaseModel):
    """Hardware resource allocation for SLURM jobs.

    Attributes:
        partition: SLURM partition name.
        gres: Generic resource request (e.g. "gpu:1"). None for none.
        cpus_per_task: CPU cores per task.
        mem_gb: Memory in gigabytes per task.
        time_limit: Wall-clock time limit (HH:MM:SS format).
        extra_slurm_kwargs: Additional ``sbatch`` flags passed through.
    """

    partition: str = "cpu"
    gres: str | None = None
    cpus_per_task: int = Field(1, ge=1)
    mem_gb: int = Field(4, ge=1)
    time_limit: str = "01:00:00"
    extra_slurm_kwargs: dict[str, Any] = Field(default_factory=dict)
