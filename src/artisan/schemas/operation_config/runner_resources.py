"""Portable hardware resource requirements consumed by step runners.

Each step runner translates these to its native format. The ``extra`` dict is
an escape hatch for provider-specific settings such as a queue or accelerator
constraint. Providers reject options they cannot apply.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class RunnerResources(BaseModel):
    """Portable hardware resource requirements for the step runner.

    Read by the selected placement runner when resolving and launching work.

    Attributes:
        cpus: Number of CPU cores per task.
        memory_gb: Memory in gigabytes per task.
        gpus: Number of GPUs requested.
        time_limit: Wall-clock time limit (HH:MM:SS format).
        extra: Runner-specific settings (e.g. {"queue": "gpu"}).
    """

    model_config = ConfigDict(extra="forbid")

    cpus: int = Field(1, ge=1)
    memory_gb: int = Field(4, ge=1)
    gpus: int = Field(0, ge=0)
    time_limit: str = "01:00:00"
    extra: dict[str, Any] = Field(default_factory=dict)

    @field_validator("time_limit")
    @classmethod
    def _validate_time_limit(cls, value: str) -> str:
        """Require a positive wall-clock duration in HH:MM:SS format."""
        match = re.fullmatch(r"(\d{2}):([0-5]\d):([0-5]\d)", value)
        if match is None or not any(int(part) for part in match.groups()):
            msg = "time_limit must be a positive duration in HH:MM:SS format"
            raise ValueError(msg)
        return value


def merge_runner_resources(
    defaults: RunnerResources,
    override: RunnerResources | Mapping[str, object] | None,
) -> RunnerResources:
    """Apply supplied resource fields while detaching all caller-owned values."""
    if override is None:
        patch = {}
    elif isinstance(override, RunnerResources):
        patch = override.model_dump(exclude_unset=True)
    else:
        patch = dict(override)
    return RunnerResources.model_validate(deepcopy({**defaults.model_dump(), **patch}))
