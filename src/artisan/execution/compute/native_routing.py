"""Construct command routing for an already selected native environment."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from artisan.execution.compute.prepared import PreparedExecuteRouter
from artisan.execution.environments.observed_worker import (
    ObservedCommandEnvironment,
    ObservedWorker,
)
from artisan.execution.local_software import LocalSoftware
from artisan.schemas.execution.command_witness import PreparedCommandWitness
from artisan.schemas.execution.gpu import NativeGPUAllocation
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment


def native_router(
    target: PreparedNativeEnvironment | ObservedWorker | None,
    *,
    pixi: Path,
    system_env: Path,
    runtime_env: Mapping[str, str],
    witness: PreparedCommandWitness | None = None,
    sensitive_values: tuple[str, ...] = (),
    gpu: NativeGPUAllocation | None = None,
    local_software: LocalSoftware | None = None,
    call_context: dict[str, Any] | None = None,
) -> PreparedExecuteRouter:
    """Preserve native launch settings and redact observed credentials."""
    observed = target if isinstance(target, ObservedWorker) else None
    return PreparedExecuteRouter(
        None if isinstance(target, ObservedWorker) else target,
        pixi=pixi,
        system_env=system_env,
        runtime_env=runtime_env,
        witness=witness,
        sensitive_values=(
            *sensitive_values,
            *(observed.sensitive_values if observed else ()),
        ),
        gpu=gpu,
        local_software=local_software,
        call_context=call_context,
        command_environment=(
            lambda root, invocation: ObservedCommandEnvironment(
                observed,
                root / "command-runtime",
                runtime_env,
                invocation,
                gpu,
                local_software,
            )
        )
        if observed is not None
        else None,
    )
