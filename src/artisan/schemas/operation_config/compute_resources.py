"""Hardware resources requested from a compute provider (e.g. Modal).

Distinct from ``RunnerResources`` — runner resources govern the SLURM
job / local process; compute resources govern the per-call container
allocation when ``compute_provider`` routes ``execute()`` to a remote
provider. Modal's API translates these directly into ``modal.App.function``
arguments. ``None`` defers to the provider default.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class ComputeResources(BaseModel):
    """Hardware resources for compute providers (currently Modal).

    Attributes:
        gpu: GPU type string (e.g. ``"A10G"``, ``"A100"``, ``"H100"``).
            None defers to the provider default.
        cpu: Fractional CPU cores. None defers to the provider default
            (Modal: 0.125 cores — often too low for non-trivial CPU
            work; set explicitly for pandas / numpy / shell-invoked
            ops). Fractional values like ``0.5`` and ``2.5`` are
            allowed.
        memory_gb: Container memory in GB. None defers to the provider
            default.
        timeout: Per-call timeout in seconds. None defers to the
            provider default.
    """

    # Reject unknown keys so ``_validate_compute_resources`` (which calls
    # ``model_validate``) actually catches typos at the public boundary.
    model_config = ConfigDict(extra="forbid")

    gpu: str | None = Field(default=None)
    cpu: float | None = Field(default=None, gt=0)
    memory_gb: int | None = Field(default=None, ge=1)
    timeout: int | None = Field(default=None, ge=1)
