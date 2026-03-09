"""Portable hardware resource requirements.

Each backend translates these to its native format.
The extra dict is an escape hatch for backend-specific settings.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ResourceConfig(BaseModel):
    """Portable hardware resource requirements.

    Attributes:
        cpus: Number of CPU cores per task.
        memory_gb: Memory in gigabytes per task.
        gpus: Number of GPUs requested.
        time_limit: Wall-clock time limit (HH:MM:SS format).
        extra: Backend-specific settings (e.g. {"partition": "gpu"}).
    """

    cpus: int = Field(1, ge=1)
    memory_gb: int = Field(4, ge=1)
    gpus: int = Field(0, ge=0)
    time_limit: str = "01:00:00"
    extra: dict[str, Any] = Field(default_factory=dict)
