"""Transient prepared bindings for configuration-selected operation calls."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict

from artisan.schemas.composites.plan import CalledOperationContract
from artisan.schemas.execution.command_witness import PreparedCommandWitness
from artisan.schemas.execution.gpu import NativeGPUAllocation
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.execution.worker_transport import RuntimeCompatibility


class PreparedCalledOperation(BaseModel):
    """Bind one selected definition to existing command software when declared."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    contract: CalledOperationContract
    environment_key: tuple[str, str] | None
    target: PreparedNativeEnvironment | None = None


class ConfiguredCallScope(BaseModel):
    """Inert command handoff; definition names never authorize imports."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    operations: tuple[PreparedCalledOperation, ...]
    caller_prefix: Path
    caller_compatibility: RuntimeCompatibility
    caller_target: PreparedNativeEnvironment | None = None
    pixi: Path
    system_env: Path
    witness: PreparedCommandWitness | None = None
    gpu: NativeGPUAllocation | None = None
    runtime_env: dict[str, str]
