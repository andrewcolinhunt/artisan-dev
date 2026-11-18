"""Execute admitted native work and commands in verified prepared prefixes."""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, ClassVar, Literal, Protocol

from artisan.execution.compute.base import ExecuteRouter
from artisan.execution.compute.invoke import tool_command_inputs
from artisan.execution.environments.command_witness import WitnessInvocation
from artisan.execution.environments.native_gpu import (
    native_gpu_command,
    native_gpu_environment,
)
from artisan.execution.environments.native_target import native_executable
from artisan.execution.local_software import (
    LOCAL_SOFTWARE_ENV,
    LocalSoftware,
    local_software_scope,
)
from artisan.execution.recording.commands import invocation_scope, reserve_invocations
from artisan.execution.scientific_failure import scientific_phase
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.execution.command_witness import PreparedCommandWitness
from artisan.schemas.execution.gpu import NativeGPUAllocation
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.specs.input_models import ExecuteInput
from artisan.utils.configuration_secrets import COMMAND_ATTACHMENT_ENV
from artisan.utils.external_tools import ExternalToolError, run_command

_GPU_ENVIRONMENT = (
    "CUDA_VISIBLE_DEVICES",
    "NVIDIA_VISIBLE_DEVICES",
    "NVIDIA_DRIVER_CAPABILITIES",
)


class CommandEnvironment(Protocol):
    """A preselected command launcher consumed by the common lifecycle router."""

    def wrap_command(self, command: list[str], cwd: str | None = None) -> list[str]: ...

    def prepare_env(self) -> dict[str, str]: ...


def freeze_runtime_injections(environment: Mapping[str, str]) -> Mapping[str, str]:
    """Copy explicit runtime values after rejecting software/ownership overrides."""
    injections = dict(environment)
    reserved = {
        "PATH",
        "HOME",
        "TMPDIR",
        "VIRTUAL_ENV",
        LOCAL_SOFTWARE_ENV,
        *COMMAND_ATTACHMENT_ENV,
    }
    if reserved & injections.keys() or any(
        key.startswith(("PYTHON", "LD_", "DYLD_", "CONDA_", "PIXI_"))
        for key in injections
    ):
        msg = (
            "Runtime injections cannot replace software, scratch or ownership controls"
        )
        raise ValueError(msg)
    if any(
        re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) is None or "\0" in value
        for key, value in injections.items()
    ):
        msg = "Runtime injections must contain valid literal environment entries"
        raise ValueError(msg)
    return MappingProxyType(injections)


@dataclass(frozen=True)
class PreparedCommandEnvironment:
    """Activate an admitted prefix with explicit runtime values applied last.

    Ambient Python, loader, executable and credential paths are never copied.
    The command supervisor injects its transient ownership capability separately.
    """

    target: PreparedNativeEnvironment
    pixi: Path
    system_env: Path
    scratch: Path
    env: Mapping[str, str] = field(default_factory=dict)
    witness: WitnessInvocation | None = None
    gpu: NativeGPUAllocation | None = None
    local_software: LocalSoftware | None = None
    public_env: ClassVar[tuple[str, ...]] = ()
    cancellation_scope: ClassVar[Literal["process_group"]] = "process_group"

    def __post_init__(self) -> None:
        object.__setattr__(self, "env", freeze_runtime_injections(self.env))
        native_gpu_environment(self.gpu, self.env)

    def wrap_command(self, command: list[str], _cwd: str | None = None) -> list[str]:
        """Resolve the first executable inside the exact prepared target only."""
        if not command:
            msg = "A prepared command requires an executable"
            raise ValueError(msg)
        executable = native_executable(self.target, command[0])
        resolved = [str(executable), *command[1:]]
        if self.witness is not None:
            resolved = self.witness.wrap(resolved)
        resolved = native_gpu_command(self.gpu, resolved, self.scratch)
        overrides = {
            "HOME": str(self.scratch / "home"),
            "TMPDIR": str(self.scratch / "tmp"),
            **{key: os.environ[key] for key in _GPU_ENVIRONMENT if key in os.environ},
            **self.env,
            **native_gpu_environment(self.gpu, self.env),
            **(self.local_software.environment() if self.local_software else {}),
        }
        return [
            str(self.pixi),
            "run",
            "--no-config",
            "--offline",
            "--as-is",
            "--executable",
            "--manifest-path",
            str(self.target.workspace / self.target.source.manifest_name),
            "-e",
            self.target.source.name,
            str(self.system_env),
            *[f"{key}={value}" for key, value in overrides.items()],
            *resolved,
        ]

    def prepare_env(self) -> dict[str, str]:
        """Build the launcher environment without inheriting worker activation."""
        home, temporary = self.scratch / "home", self.scratch / "tmp"
        home.mkdir(parents=True, exist_ok=True)
        temporary.mkdir(parents=True, exist_ok=True)
        return {
            "PATH": os.defpath,
            "HOME": str(home),
            "TMPDIR": str(temporary),
            "LANG": "C.UTF-8",
            **{key: os.environ[key] for key in _GPU_ENVIRONMENT if key in os.environ},
            **native_gpu_environment(self.gpu, self.env),
            **(self.local_software.environment() if self.local_software else {}),
        }


class PreparedExecuteRouter(ExecuteRouter):
    """Run native phases here; commands use one previously selected target."""

    def __init__(
        self,
        target: PreparedNativeEnvironment | None,
        *,
        pixi: Path,
        system_env: Path,
        runtime_env: Mapping[str, str],
        command_environment: Callable[
            [Path, WitnessInvocation | None], CommandEnvironment
        ]
        | None = None,
        witness: PreparedCommandWitness | None = None,
        sensitive_values: tuple[str, ...] = (),
        gpu: NativeGPUAllocation | None = None,
        local_software: LocalSoftware | None = None,
    ) -> None:
        self.target = target
        self.pixi = pixi
        self.system_env = system_env
        self.runtime_env = freeze_runtime_injections(runtime_env)
        native_gpu_environment(gpu, self.runtime_env)
        self.gpu = gpu
        self.command_environment = command_environment
        self.witness = witness
        self.sensitive_values = sensitive_values
        self.local_software = local_software

    def context(self) -> AbstractContextManager[None]:
        """Expose selected local software while all native phases run."""
        return (
            local_software_scope(self.local_software)
            if self.local_software is not None
            else nullcontext()
        )

    def route_execute(
        self,
        operation: OperationDefinition,
        execute_inputs: list[ExecuteInput],
        sandbox_root: str | None,
    ) -> list[Any]:
        slots = reserve_invocations(len(execute_inputs))
        results = []
        for slot, inputs in zip(slots, execute_inputs, strict=True):
            with invocation_scope(operation, slot):
                if not operation.is_command_op():
                    with scientific_phase("execute"):
                        results.append(operation.execute_function(inputs))
                    continue
                if sandbox_root is None:
                    msg = "Command execution requires a working directory"
                    raise ValueError(msg)
                witness = (
                    WitnessInvocation(
                        self.witness, Path(sandbox_root) / "command-runtime" / "exits"
                    )
                    if self.witness is not None
                    else None
                )
                if self.command_environment is not None:
                    environment = self.command_environment(Path(sandbox_root), witness)
                else:
                    assert self.target is not None
                    environment = PreparedCommandEnvironment(
                        self.target,
                        self.pixi,
                        self.system_env,
                        Path(sandbox_root) / "command-runtime",
                        self.runtime_env,
                        witness,
                        self.gpu,
                        self.local_software,
                    )
                prepared = (
                    inputs.inputs
                    if operation.execute_as_tool
                    else tool_command_inputs(inputs.inputs)
                )
                with scientific_phase("execute_command"):
                    command = operation.execute_command(prepared)
                try:
                    result = run_command(
                        environment,
                        command,
                        cwd=inputs.execute_dir,
                        log_path=inputs.log_path,
                        log_mode="a",
                        sensitive_values=self.sensitive_values,
                    )
                except ExternalToolError as error:
                    if witness is not None and witness.verify_exit(error.return_code):
                        with scientific_phase("execute"):
                            raise
                    raise
                if witness is not None:
                    witness.verify_exit(result.returncode)
                results.append(None)
        return results
