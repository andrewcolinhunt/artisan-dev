"""Local preparation and exact admitted assignment and curator dispatch."""

from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar, Literal, cast

from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.compiler import Definition, Parameters, compile_definition
from artisan.execution.application_bootstrap import validate_worker_applications
from artisan.execution.assignment import verify_assignment
from artisan.execution.assignment_transport import (
    AdmittedAssignment,
    AssignmentDispatch,
)
from artisan.execution.curator_definition import capture_curator_definition
from artisan.execution.curator_worker import AdmittedCurator, LocalCuratorHandle
from artisan.execution.environments.command_witness import prepare_command_witness
from artisan.execution.environments.container_runtime import (
    ApptainerRuntime,
    DockerRuntime,
)
from artisan.execution.environments.curator_software import resolve_curator_software
from artisan.execution.environments.native import (
    lookup_native_environment,
    prepare_native_environment,
)
from artisan.execution.environments.native_gpu import capture_native_gpu
from artisan.execution.environments.observed_worker import (
    ObservedWorker,
    qualify_observed_worker,
)
from artisan.execution.environments.prepared_oci import prepare_oci_environment
from artisan.execution.environments.prepared_sif import prepare_sif_environment
from artisan.execution.environments.requirements import select_tool_environments
from artisan.execution.environments.worker_qualification import (
    controller_definition_sources,
    controller_worker_sources,
    lookup_worker_qualification,
    prepare_worker_qualification,
)
from artisan.execution.local_assignment import (
    PreparedLocalBindings,
    bind_local_software,
)
from artisan.execution.local_call import LocalCallStartupError
from artisan.execution.local_packing import (
    LocalAssignmentRequest,
    LocalPackStartupError,
    launch_local_assignments,
)
from artisan.execution.local_worker import LocalAssignmentHandle
from artisan.execution.runtime_compatibility import verify_runtime_compatibility
from artisan.execution.worker_requirements import (
    observed_controller_sources,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.runners.assignment import (
    AssignmentHandle,
    AssignmentLaunchError,
    AssignmentSoftware,
)
from artisan.orchestration.runners.curator import CuratorHandle, CuratorLaunchError
from artisan.registry import bootstrap_applications
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.operation_config.environment import Environment
from artisan.schemas.operation_config.runner_resources import (
    RunnerResources,
    merge_runner_resources,
)
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
)
from artisan.utils.application_modules import validate_application_modules


@dataclass(frozen=True)
class LocalRunner:
    """Store explicit software/launcher declarations without provisioning.

    This local runner does not reserve CPU or memory. Worker GPU qualification
    remains separate from observed-current-interpreter qualification.
    """

    environment: Environment | None = None
    cache_root: Path = field(
        default_factory=lambda: Path(
            os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")
        )
        / "artisan/environments"
    )
    tool_backend: Literal["pixi", "docker", "apptainer"] = "pixi"
    pixi: Path = Path.home() / ".pixi/bin/pixi"
    system_env: Path = Path("/usr/bin/env")
    docker: DockerRuntime | None = None
    apptainer: ApptainerRuntime | None = None
    base_image: str | None = None
    name: ClassVar[str] = "local"

    def __post_init__(self) -> None:
        if (
            not self.cache_root.is_absolute()
            or not self.pixi.is_absolute()
            or not self.system_env.is_absolute()
        ):
            msg = "Local runner configuration requires explicit absolute paths"
            raise ValueError(msg)
        if self.tool_backend not in {"pixi", "docker", "apptainer"}:
            msg = "Unsupported local tool backend"
            raise ValueError(msg)

    def worker_code_roots(self) -> tuple[type, ...]:
        """Select trusted provider code, including SDK entry types used by subclasses.

        These live types extend measured import coverage; they are not authored
        package requirements or recorded names to import on a worker.
        """
        return (type(self),)

    def _trusted_code_roots(self) -> tuple[type, ...]:
        # An SDK extension cannot remove the actual selected provider's code.
        return tuple(dict.fromkeys((type(self), *self.worker_code_roots())))

    def prepare(
        self,
        definition: Definition,
        *,
        params: Parameters = None,
        runner_resources: RunnerResources | None = None,
        application_modules: tuple[str, ...] = (),
    ) -> None:
        """Compile and explicitly provision unique worker/tool software requirements."""
        application_modules = validate_application_modules(application_modules)
        bootstrap_applications(application_modules, purpose="worker preparation")
        plan = compile_definition(definition, params=params)
        resources = merge_runner_resources(
            plan.instantiate().runner_resources, runner_resources
        )
        _validate_resources(resources)
        capture_native_gpu(resources.gpus)
        selections = select_tool_environments(plan)
        declarations: list[Environment] = []
        for selection in selections:
            declaration = selection.declaration
            if declaration is not None and declaration not in declarations:
                declarations.append(declaration)
        if declarations and self.tool_backend != "pixi":
            self._validate_container_preparation()
        if selections:
            prepare_command_witness(self.cache_root)
        if self.environment is not None:
            worker = prepare_native_environment(
                self.environment, cache_root=self.cache_root
            )
            names, _ = controller_worker_sources(
                plan,
                self._trusted_code_roots(),
                self.environment,
                worker,
                application_modules=application_modules,
            )
            prepare_worker_qualification(
                worker,
                names,
                cache_root=self.cache_root,
                pixi=self.pixi,
                system_env=self.system_env,
            )
        for declaration in declarations:
            self._prepare_tool(declaration)
        self.resolve(plan, resources, application_modules=application_modules)

    def prepare_curator(
        self,
        operation: OperationDefinition,
        *,
        runner_resources: RunnerResources | None = None,
        application_modules: tuple[str, ...] = (),
    ) -> None:
        """Explicitly prepare only the worker needed by one trusted curator."""
        resources = merge_runner_resources(operation.runner_resources, runner_resources)
        _validate_resources(resources)
        application_modules = validate_application_modules(application_modules)
        bootstrap_applications(
            application_modules, purpose="curator worker preparation"
        )
        capture_curator_definition(operation)
        capture_native_gpu(resources.gpus)
        if self.environment is not None:
            worker = prepare_native_environment(
                self.environment, cache_root=self.cache_root
            )
            names, _ = controller_definition_sources(
                (type(operation),),
                self._trusted_code_roots(),
                self.environment,
                worker,
                application_modules=application_modules,
            )
            prepare_worker_qualification(
                worker,
                names,
                cache_root=self.cache_root,
                pixi=self.pixi,
                system_env=self.system_env,
            )
        self.resolve_curator(
            operation, resources, application_modules=application_modules
        )

    def resolve_curator(
        self,
        operation: OperationDefinition,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> AssignmentSoftware:
        """Resolve curator worker coverage without compiling a creator plan."""
        _validate_resources(runner_resources)
        return resolve_curator_software(
            operation,
            self._trusted_code_roots(),
            environment=self.environment,
            cache_root=self.cache_root,
            pixi=self.pixi,
            system_env=self.system_env,
            native_gpu=capture_native_gpu(runner_resources.gpus),
            application_modules=validate_application_modules(application_modules),
        )

    def launch_curator(
        self,
        curator: AdmittedCurator,
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> CuratorHandle:
        """Launch one whole curator stream without fabricating a creator attempt."""
        if not isinstance(software, PreparedLocalBindings):
            msg = "Local curator dispatch requires resolved local software"
            raise ValueError(msg)
        verify_runtime_compatibility(software.compatibility)
        validate_worker_applications(
            dispatch.application_modules,
            qualified_modules=software.application_modules,
        )
        try:
            return LocalCuratorHandle(curator, software, dispatch)
        except LocalCallStartupError as error:
            handle = cast(LocalCuratorHandle, error.handle)
            msg = "Local curator startup failed"
            raise CuratorLaunchError(
                msg,
                handles=(handle,),
                cancellation=CancellationAcknowledgement(
                    CancellationStatus.CONFIRMED
                    if error.cleanup_outcome.state == "closed"
                    else CancellationStatus.UNKNOWN,
                    error.cleanup_outcome.reason,
                ),
            ) from error

    def _validate_container_preparation(self) -> None:
        if self.docker is None or self.base_image is None:
            msg = "Container preparation requires an explicit local Docker builder and existing base image"
            raise ValueError(msg)
        if self.tool_backend == "apptainer" and self.apptainer is None:
            msg = "SIF preparation requires an explicit Apptainer launcher"
            raise ValueError(msg)

    def _prepare_tool(self, declaration: Environment) -> None:
        if self.tool_backend == "pixi":
            if declaration != self.environment:
                prepare_native_environment(declaration, cache_root=self.cache_root)
            return
        assert self.docker is not None
        assert self.base_image is not None
        oci = prepare_oci_environment(
            declaration,
            cache_root=self.cache_root,
            docker=self.docker,
            base_image=self.base_image,
        )
        if self.tool_backend == "apptainer":
            assert self.apptainer is not None
            prepare_sif_environment(
                oci,
                cache_root=self.cache_root,
                docker=self.docker,
                apptainer=self.apptainer,
            )

    def resolve(
        self,
        plan: CompiledPlan,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> AssignmentSoftware:
        """Resolve prepared targets and compatibility without provisioning software."""
        _validate_resources(runner_resources)
        application_modules = validate_application_modules(application_modules)
        allocation = capture_native_gpu(runner_resources.gpus)
        worker: PreparedNativeEnvironment | ObservedWorker
        if self.environment is None:
            names, _ = observed_controller_sources(
                plan,
                self._trusted_code_roots(),
                application_modules=application_modules,
            )
            worker = qualify_observed_worker(names)
        else:
            worker = lookup_native_environment(
                self.environment, cache_root=self.cache_root
            )
            names, _ = controller_worker_sources(
                plan,
                self._trusted_code_roots(),
                self.environment,
                worker,
                application_modules=application_modules,
            )
            worker = lookup_worker_qualification(
                worker, names, cache_root=self.cache_root
            )
        return bind_local_software(
            plan,
            worker,
            cache_root=self.cache_root,
            pixi=self.pixi,
            system_env=self.system_env,
            worker_environment=self.environment,
            tool_backend=self.tool_backend,
            docker=self.docker,
            apptainer=self.apptainer,
            native_gpu=allocation,
            application_modules=application_modules,
        )

    def launch(
        self,
        assignments: Sequence[AdmittedAssignment],
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> tuple[AssignmentHandle, ...]:
        """Launch exactly one ordered pack and preserve any partially started handles."""
        if not isinstance(software, PreparedLocalBindings):
            msg = "Local dispatch requires resolved local software"
            raise ValueError(msg)
        for assignment in assignments:
            verify_assignment(assignment, software)
        verify_runtime_compatibility(software.compatibility)
        validate_worker_applications(
            dispatch.application_modules,
            qualified_modules=software.application_modules,
        )
        if not assignments:
            return ()
        requests = tuple(
            LocalAssignmentRequest(
                assignment, software, None, dispatch.runtime_env, dispatch
            )
            for assignment in assignments
        )
        try:
            return launch_local_assignments(
                requests,
                working_root=dispatch.working_root,
                units_per_worker=len(requests),
            )
        except LocalPackStartupError as error:
            message = "Local assignment pack startup failed"
            raise AssignmentLaunchError(
                message,
                handles=cast(tuple[LocalAssignmentHandle, ...], error.handles),
                cancellation=_startup_cancellation(error),
                worker_owners=error.worker_owners,
            ) from error


def _validate_resources(resources: RunnerResources) -> None:
    if resources.extra:
        msg = "This local adapter does not support provider-specific resource options"
        raise ValueError(msg)


def _startup_cancellation[ResultT](
    error: LocalPackStartupError[ResultT],
) -> CancellationAcknowledgement:
    settled = True
    for handle in error.handles:
        try:
            confirmed = handle.done() and handle.cancel().status in {
                CancellationStatus.CONFIRMED,
                CancellationStatus.REJECTED,
            }
        except Exception:
            confirmed = False
        settled = settled and confirmed
    closed = all(outcome.state == "closed" for outcome in error.transport_outcomes)
    return CancellationAcknowledgement(
        CancellationStatus.CONFIRMED
        if settled and closed
        else CancellationStatus.UNKNOWN,
        "Local startup ownership settled"
        if settled and closed
        else "Local startup ownership could not be confirmed",
    )
