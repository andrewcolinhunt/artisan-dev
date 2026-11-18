"""Run one admitted local assignment with explicit software and sealing seams."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

from artisan.composites.base.compiled import CompiledPlan
from artisan.execution.assignment import Assignment
from artisan.execution.composition import ExecuteAdapter
from artisan.execution.compute.prepared import (
    CommandEnvironment,
    PreparedCommandEnvironment,
    PreparedExecuteRouter,
)
from artisan.execution.environments.command_witness import (
    lookup_command_witness,
    verify_command_witness,
)
from artisan.execution.environments.container_launch import (
    PreparedContainerCommandEnvironment,
    validate_container_bind_path,
)
from artisan.execution.environments.container_runtime import (
    ApptainerRuntime,
    DockerRuntime,
    capture_gpu_driver,
)
from artisan.execution.environments.native import lookup_native_environment
from artisan.execution.environments.native_gpu import (
    resolve_container_gpu_devices,
    verify_native_gpu,
    verify_native_gpu_loader,
)
from artisan.execution.environments.native_target import verify_native_environment
from artisan.execution.environments.observed_worker import (
    ObservedCommandEnvironment,
    ObservedWorker,
)
from artisan.execution.environments.prepared_oci import (
    lookup_oci_environment,
    verify_oci_environment,
)
from artisan.execution.environments.prepared_sif import (
    lookup_sif_source_environment,
    verify_sif_environment,
)
from artisan.execution.environments.requirements import select_tool_environments
from artisan.execution.executor import Executor
from artisan.execution.local_profile import LocalLauncherIdentity, LocalWorkerProfile
from artisan.execution.local_software import LocalSoftware
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.execution.command_witness import PreparedCommandWitness
from artisan.schemas.execution.container_environment import (
    ContainerLauncherIdentity,
    PreparedOCIEnvironment,
    PreparedSIFEnvironment,
)
from artisan.schemas.execution.gpu import GPUDriverIdentity, NativeGPUAllocation
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.execution.worker_transport import RuntimeCompatibility
from artisan.schemas.operation_config.environment import Environment
from artisan.utils.application_modules import validate_application_modules
from artisan.utils.external_tools import run_command
from artisan.utils.hashing import canonical_json_bytes


@dataclass(frozen=True)
class PreparedToolBinding:
    """One command call path bound before admission to a prepared target."""

    call_path: tuple[str, ...]
    target: (
        PreparedNativeEnvironment
        | PreparedOCIEnvironment
        | PreparedSIFEnvironment
        | ObservedWorker
    )


@dataclass(frozen=True)
class PreparedLocalBindings:
    """Closed worker/tool selection; no authored dependency recipe or fallback."""

    worker: PreparedNativeEnvironment | ObservedWorker
    tools: tuple[PreparedToolBinding, ...]
    pixi: Path
    system_env: Path
    launcher: LocalLauncherIdentity | None
    container_runtime: DockerRuntime | ApptainerRuntime | None = None
    container_launcher: ContainerLauncherIdentity | None = None
    gpu_driver: GPUDriverIdentity | None = None
    command_witness: PreparedCommandWitness | None = None
    native_gpu: NativeGPUAllocation | None = None
    application_modules: tuple[str, ...] = ()
    cache_root: Path | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "application_modules",
            validate_application_modules(self.application_modules),
        )
        paths = [tool.call_path for tool in self.tools]
        if paths != sorted(set(paths)):
            msg = "Prepared command bindings must have unique sorted call paths"
            raise ValueError(msg)
        if self.worker.compatibility is None:
            msg = (
                "Prepared worker requires qualified live-object transport compatibility"
            )
            raise ValueError(msg)
        native_activation = isinstance(self.worker, PreparedNativeEnvironment) or any(
            isinstance(tool.target, PreparedNativeEnvironment) for tool in self.tools
        )
        if native_activation != (self.launcher is not None):
            msg = (
                "Prepared native activation requires its exact local launcher identity"
            )
            raise ValueError(msg)
        has_containers = any(
            not isinstance(tool.target, (PreparedNativeEnvironment, ObservedWorker))
            for tool in self.tools
        )
        if has_containers != (
            self.container_runtime is not None and self.container_launcher is not None
        ):
            msg = "Prepared container tools require their exact qualified launcher"
            raise ValueError(msg)
        if (self.gpu_driver is not None) != bool(
            self.container_runtime is not None and self.container_runtime.gpu_devices
        ):
            msg = "Allocated container GPUs require qualified driver evidence"
            raise ValueError(msg)

    @property
    def software_cacheable(self) -> bool:
        """Observed ambient software has no complete canonical dependency closure."""
        return not self.worker_is_observed

    @property
    def worker_is_observed(self) -> bool:
        """Select direct interpreter bootstrap without claiming Pixi preparation."""
        return isinstance(self.worker, ObservedWorker)

    @property
    def diagnostic_sensitive_values(self) -> tuple[str, ...]:
        """Include observed credentials in transport diagnostics before writing them."""
        return (
            self.worker.sensitive_values
            if isinstance(self.worker, ObservedWorker)
            else ()
        )

    @property
    def worker_profile(self) -> LocalWorkerProfile:
        """Separate node-local launch capability from scientific tool selection."""
        return LocalWorkerProfile(
            self.worker,
            self.pixi,
            self.system_env,
            self.launcher
            if isinstance(self.worker, PreparedNativeEnvironment)
            else None,
            self.native_gpu,
        )

    def worker_environment(
        self, scratch: Path, runtime_env: Mapping[str, str]
    ) -> CommandEnvironment:
        """Launch one selected worker profile without provisioning or substitution."""
        return self.worker_profile.environment(scratch, runtime_env)

    @property
    def compatibility(self) -> RuntimeCompatibility:
        """Return the selected worker's mandatory transport qualification."""
        assert self.worker.compatibility is not None
        return self.worker.compatibility

    @property
    def software_digest(self) -> str:
        """Bind full selected targets, transport code and actual local launchers."""
        descriptor = {
            "worker": self.worker.digest,
            "tools": [
                {"call_path": item.call_path, "target": item.target.digest}
                for item in self.tools
            ],
            "compatibility": self.compatibility.model_dump(mode="json"),
            "application_modules": self.application_modules,
            "launcher": self.launcher.descriptor()
            if self.launcher is not None
            else {"kind": "observed-direct"},
        }
        if self.container_launcher is not None:
            descriptor["container_launcher"] = {
                "identity": self.container_launcher.model_dump(mode="json"),
                "profile": "docker-owned-v1"
                if self.container_launcher.kind == "docker"
                else "apptainer-unsquash-v1",
            }
        if self.gpu_driver is not None:
            descriptor["gpu_driver"] = self.gpu_driver.model_dump(mode="json")
        if self.command_witness is not None:
            descriptor["command_witness"] = self.command_witness.digest
        if self.native_gpu is not None:
            descriptor["native_gpu"] = self.native_gpu.software_descriptor()
        return hashlib.sha256(
            b"artisan.local-software-v1\0" + canonical_json_bytes(descriptor)
        ).hexdigest()

    def verify(self, plan: CompiledPlan) -> None:
        """Check selected prepared targets and launchers without provisioning."""
        expected = _command_paths(plan)
        if {binding.call_path for binding in self.tools} != expected:
            msg = "Prepared tool selection does not exactly match the compiled commands"
            raise ValueError(msg)
        self._verify_payloads()

    def verify_worker(self) -> None:
        """Verify a worker-only call without inventing compiled command paths."""
        if (
            self.tools
            or self.container_runtime is not None
            or self.container_launcher is not None
            or self.gpu_driver is not None
            or self.command_witness is not None
        ):
            msg = "Worker-only software cannot contain command tool bindings"
            raise ValueError(msg)
        self._verify_payloads()

    def _verify_payloads(self) -> None:
        """Check prepared identities and launch capability without payload scans."""
        if self.native_gpu is not None:
            verify_native_gpu(self.native_gpu)
        if (
            self.launcher is not None
            and LocalLauncherIdentity.capture(self.pixi, self.system_env)
            != self.launcher
        ):
            msg = "Qualified local launcher identity changed"
            raise ValueError(msg)
        targets: dict[Path, PreparedNativeEnvironment] = {}
        unique_targets = {
            target.digest: target
            for target in (self.worker, *(tool.target for tool in self.tools))
        }
        for target in unique_targets.values():
            if isinstance(target, ObservedWorker):
                target.verify()
                continue
            if isinstance(target, PreparedOCIEnvironment):
                if not isinstance(self.container_runtime, DockerRuntime):
                    msg = "Prepared OCI tools require the admitted Docker launcher"
                    raise ValueError(msg)
                verify_oci_environment(target, docker=self.container_runtime)
                continue
            if isinstance(target, PreparedSIFEnvironment):
                if not isinstance(self.container_runtime, ApptainerRuntime):
                    msg = "Prepared SIF tools require the admitted Apptainer launcher"
                    raise ValueError(msg)
                verify_sif_environment(target, apptainer=self.container_runtime)
                continue
            if (
                target.prefix in targets
                and targets[target.prefix].digest != target.digest
            ):
                msg = "One native prefix cannot have conflicting prepared identities"
                raise ValueError(msg)
            targets[target.prefix] = target
        if bool(self.tools) != (self.command_witness is not None):
            msg = "Command assignments require exact prepared exit-witness coverage"
            raise ValueError(msg)
        if self.command_witness is not None:
            verify_command_witness(self.command_witness)
        for target in targets.values():
            verify_native_environment(target)
        if (
            self.container_runtime is not None
            and self.container_runtime.identity() != self.container_launcher
        ):
            msg = "Qualified container launcher identity changed"
            raise ValueError(msg)
        if (
            self.container_runtime is not None
            and capture_gpu_driver(self.container_runtime.gpu_devices)
            != self.gpu_driver
        ):
            msg = "Qualified container GPU driver identity changed"
            raise ValueError(msg)

    def create_adapter(
        self,
        assignment: Assignment,
        *,
        working_root: Path,
        runtime_env: Mapping[str, str],
    ) -> ExecuteAdapter:
        """Validate local carriers/binds once and freeze this assignment's routing."""
        if self.native_gpu is not None:
            verify_native_gpu_loader(self.native_gpu)
        input_files = (
            container_input_files(
                (
                    *[
                        artifact
                        for values in assignment.inputs.values()
                        for artifact in values
                    ],
                    *assignment.context_artifacts.values(),
                )
            )
            if self.container_runtime is not None
            else ()
        )
        if self.container_runtime is not None:
            for path in (working_root, *input_files):
                validate_container_bind_path(path, self.container_runtime)
        return lambda operation, path: self.adapter(
            operation, path, runtime_env=runtime_env, input_files=input_files
        )

    def adapter(
        self,
        operation: OperationDefinition,
        path: tuple[str, ...],
        *,
        runtime_env: Mapping[str, str],
        input_files: tuple[Path, ...] = (),
    ) -> PreparedExecuteRouter:
        """Select the admitted call binding without rereading environment defaults."""
        target: (
            PreparedNativeEnvironment
            | PreparedOCIEnvironment
            | PreparedSIFEnvironment
            | ObservedWorker
        ) = self.worker
        if operation.is_command_op():
            matches = [item.target for item in self.tools if item.call_path == path]
            if not matches:
                msg = "Command has no admitted prepared target"
                raise ValueError(msg)
            target = matches[0]
        local = (
            LocalSoftware(self.cache_root, self.pixi, self.system_env, runtime_env)
            if self.cache_root is not None
            else None
        )
        if isinstance(target, ObservedWorker):
            return PreparedExecuteRouter(
                None,
                pixi=self.pixi,
                system_env=self.system_env,
                runtime_env=runtime_env,
                witness=self.command_witness,
                sensitive_values=self.diagnostic_sensitive_values,
                gpu=self.native_gpu,
                local_software=local,
                command_environment=lambda sandbox, witness: ObservedCommandEnvironment(
                    target,
                    sandbox / "command-runtime",
                    runtime_env,
                    witness,
                    self.native_gpu,
                    local,
                ),
            )
        if not isinstance(target, PreparedNativeEnvironment):
            runtime = self.container_runtime
            assert runtime is not None
            return PreparedExecuteRouter(
                None,
                pixi=self.pixi,
                system_env=self.system_env,
                runtime_env=runtime_env,
                sensitive_values=self.diagnostic_sensitive_values,
                gpu=self.native_gpu,
                command_environment=lambda sandbox,
                witness: PreparedContainerCommandEnvironment(
                    target,
                    runtime,
                    sandbox,
                    runtime_env,
                    input_files,
                    witness,
                ),
                witness=self.command_witness,
            )
        return PreparedExecuteRouter(
            target,
            pixi=self.pixi,
            system_env=self.system_env,
            runtime_env=runtime_env,
            witness=self.command_witness,
            sensitive_values=self.diagnostic_sensitive_values,
            gpu=self.native_gpu,
            local_software=local,
        )


def qualify_local_worker(
    target: PreparedNativeEnvironment,
    *,
    pixi: Path,
    system_env: Path,
) -> PreparedNativeEnvironment:
    """Capture all installed package metadata once for the prepared worker."""
    LocalLauncherIdentity.capture(pixi, system_env)
    verify_native_environment(target)
    program = (
        "import sys; "
        "from pathlib import Path; "
        "from artisan.execution.runtime_compatibility import capture_runtime_compatibility; "
        "Path(sys.argv[1]).write_text(capture_runtime_compatibility().model_dump_json())"
    )
    with Executor() as executor, executor.invocation() as paths:
        assert paths.sandbox is not None
        assert paths.postprocess is not None
        environment = PreparedCommandEnvironment(
            target, pixi, system_env, Path(paths.sandbox)
        )
        measured = Path(paths.postprocess) / "runtime.json"
        run_command(environment, ["python", "-I", "-c", program, str(measured)])
        compatibility = RuntimeCompatibility.model_validate_json(measured.read_bytes())
    return target.model_copy(update={"compatibility": compatibility})


def bind_local_worker(
    worker: PreparedNativeEnvironment | ObservedWorker,
    *,
    pixi: Path,
    system_env: Path,
    native_gpu: NativeGPUAllocation | None = None,
    application_modules: tuple[str, ...] = (),
) -> PreparedLocalBindings:
    """Bind a measured worker without a creator plan or command-only targets."""
    bindings = PreparedLocalBindings(
        worker=worker,
        tools=(),
        pixi=pixi,
        system_env=system_env,
        launcher=LocalLauncherIdentity.capture(pixi, system_env)
        if isinstance(worker, PreparedNativeEnvironment)
        else None,
        native_gpu=native_gpu,
        application_modules=application_modules,
    )
    bindings.verify_worker()
    return bindings


def bind_local_software(
    plan: CompiledPlan,
    worker: PreparedNativeEnvironment | ObservedWorker,
    *,
    cache_root: Path,
    pixi: Path,
    system_env: Path,
    worker_environment: Environment | None = None,
    tool_backend: Literal["pixi", "docker", "apptainer"] = "pixi",
    docker: DockerRuntime | None = None,
    apptainer: ApptainerRuntime | None = None,
    native_gpu: NativeGPUAllocation | None = None,
    application_modules: tuple[str, ...] = (),
) -> PreparedLocalBindings:
    """Resolve the complete selected command software before cache or dispatch."""
    resolved: dict[
        Environment,
        PreparedNativeEnvironment
        | PreparedOCIEnvironment
        | PreparedSIFEnvironment
        | ObservedWorker,
    ] = {}
    if tool_backend == "pixi" and worker_environment is not None:
        resolved[worker_environment] = worker
    selections = sorted(select_tool_environments(plan), key=lambda item: item.call_path)
    for selection in selections:
        declaration = selection.declaration
        if declaration is not None and declaration not in resolved:
            resolved[declaration] = _lookup_tool(
                declaration, cache_root, tool_backend, docker, apptainer
            )
    tools = tuple(
        PreparedToolBinding(
            selection.call_path,
            worker
            if selection.declaration is None
            else resolved[selection.declaration],
        )
        for selection in selections
    )
    has_containers = any(
        not isinstance(tool.target, (PreparedNativeEnvironment, ObservedWorker))
        for tool in tools
    )
    runtime = (
        (docker if tool_backend == "docker" else apptainer) if has_containers else None
    )
    if runtime is not None and native_gpu is not None:
        selected = (
            native_gpu.devices
            if runtime.gpu_devices is None
            else resolve_container_gpu_devices(runtime.gpu_devices)
        )
        if not set(selected) <= set(native_gpu.devices):
            msg = "Container tools cannot expand the native GPU allocation"
            raise ValueError(msg)
        runtime = replace(runtime, gpu_devices=selected)
    bindings = PreparedLocalBindings(
        worker,
        tools,
        pixi,
        system_env,
        LocalLauncherIdentity.capture(pixi, system_env)
        if isinstance(worker, PreparedNativeEnvironment)
        or any(isinstance(tool.target, PreparedNativeEnvironment) for tool in tools)
        else None,
        runtime,
        runtime.identity() if runtime is not None else None,
        capture_gpu_driver(runtime.gpu_devices) if runtime is not None else None,
        lookup_command_witness(cache_root) if tools else None,
        native_gpu,
        application_modules,
        cache_root,
    )
    bindings.verify(plan)
    return bindings


def _lookup_tool(
    declaration: Environment,
    cache_root: Path,
    backend: str,
    docker: DockerRuntime | None,
    apptainer: ApptainerRuntime | None,
) -> PreparedNativeEnvironment | PreparedOCIEnvironment | PreparedSIFEnvironment:
    if backend == "pixi":
        return lookup_native_environment(declaration, cache_root=cache_root)
    if backend == "docker" and docker is not None:
        return lookup_oci_environment(declaration, cache_root=cache_root, docker=docker)
    if backend == "apptainer" and apptainer is not None:
        return lookup_sif_source_environment(
            declaration, cache_root=cache_root, apptainer=apptainer
        )
    msg = "Selected tool backend requires its explicit qualified local launcher"
    raise ValueError(msg)


def container_input_files(artifacts: Sequence[Artifact]) -> tuple[Path, ...]:
    """Validate the already selected local carrier inventory for container binds."""
    files = set()
    for artifact in artifacts:
        if artifact.EXTERNALLY_BACKED:
            locator = getattr(artifact, next(iter(artifact.LOCATOR_FIELDS)))
            path = Path(locator)
            if not path.is_absolute() or not path.is_file() or path.resolve() != path:
                msg = "Container input carriers require admitted local regular files"
                raise ValueError(msg)
            files.add(path)
    return tuple(sorted(files))


def _command_paths(
    plan: CompiledPlan, path: tuple[str, ...] = ()
) -> set[tuple[str, ...]]:
    if plan.recipe.execution.mode in {"command", "python_adapter"}:
        return {path}
    return {
        nested
        for call in plan.calls
        for nested in _command_paths(call.plan, (*path, call.name))
    }
