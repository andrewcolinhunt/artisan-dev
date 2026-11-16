"""Explicitly prepared full-assignment Modal workers with no run-time provisioning."""

from __future__ import annotations

import hashlib
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar
from uuid import uuid4

from artisan.orchestration.runner_api import (
    AdmittedAssignment,
    AdmittedCurator,
    AssignmentDispatch,
    AssignmentHandle,
    AssignmentLaunchError,
    AssignmentSoftware,
    CancellationAcknowledgement,
    CancellationStatus,
    CompiledPlan,
    CuratorHandle,
    CuratorLaunchError,
    Definition,
    Environment,
    NativeGPUSoftware,
    OperationDefinition,
    OwnershipState,
    Parameters,
    RunnerResources,
    compile_definition,
    merge_runner_resources,
)
from artisan.registry import bootstrap_applications
from artisan_modal.bindings import ModalFunctionRuntime, PreparedModalImage
from artisan_modal.gpu import GPUProbe
from artisan_modal.gpu_owner import GPUProbeOwner
from artisan_modal.handles import ModalAssignmentHandle, ModalCuratorHandle
from artisan_modal.image_store import MissingPreparedImage, lookup_image, publish_image
from artisan_modal.pack_control import ModalPack
from artisan_modal.pack_owner import ModalFunction, ModalPackOwner
from artisan_modal.pack_worker import invoke_pack
from artisan_modal.preparation import BASE_IMAGE, _require_sdk, prepare_image
from artisan_modal.resolution import (
    bind_software,
    controller_compatibility,
    function_profile,
    requirements,
)
from artisan_modal.software import ModalSoftware
from artisan_modal.submission import prepare_call, sensitive_values, validate_dispatch


def _deployment_name(
    app_name: str, image: PreparedModalImage, profile: ModalFunctionRuntime
) -> str:
    descriptor = image.digest + "\0" + profile.model_dump_json()
    suffix = hashlib.sha256(descriptor.encode()).hexdigest()[:24]
    return f"{app_name}-{suffix}"


def _function(app: object, profile: ModalFunctionRuntime) -> ModalFunction:
    import modal

    if not isinstance(app, modal.App):
        message = "Expected an explicitly owned Modal application"
        raise TypeError(message)
    return app.function(
        name="assignment",
        image=modal.Image.from_id(profile.image_id),
        cpu=profile.milli_cpu / 1000,
        memory=profile.memory_mb,
        gpu=f"{profile.gpu_type}:{profile.gpu_count}" if profile.gpu_count else None,
        timeout=profile.timeout_seconds,
        startup_timeout=300,
        max_containers=1,
        retries=0,
        include_source=False,
    )(invoke_pack)


@dataclass(frozen=True)
class ModalRunner:
    """Declare an explicit canonical worker and optional provider placement.

    Construction does no observation or provisioning. Explicit preparation builds
    and deploys the assignment entrypoint. Resolution reads existing evidence;
    launch only invokes a previously prepared endpoint. Ordered units share one
    SDK input while retaining independent command cancellation and result handles.
    """

    environment: Environment
    app_name: str
    cache_root: Path = field(
        default_factory=lambda: Path.home() / ".cache/artisan/environments"
    )
    base_image: str = BASE_IMAGE
    name: ClassVar[str] = "modal"

    def __post_init__(self) -> None:
        if not self.cache_root.is_absolute():
            message = "Modal environment cache requires an absolute path"
            raise ValueError(message)
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,38}", self.app_name) is None:
            message = "Modal application prefix must contain 1-39 portable characters"
            raise ValueError(message)

    def _providers(self) -> tuple[type, ...]:
        import modal

        _require_sdk()
        return type(self), modal.Function

    def prepare(
        self,
        definition: Definition,
        *,
        params: Parameters = None,
        runner_resources: RunnerResources | None = None,
        application_modules: tuple[str, ...] = (),
    ) -> None:
        bootstrap_applications(application_modules, purpose="Modal worker preparation")
        plan = compile_definition(definition, params=params)
        self._prepare(
            plan,
            merge_runner_resources(
                plan.instantiate().runner_resources, runner_resources
            ),
            application_modules,
        )

    def prepare_curator(
        self,
        operation: OperationDefinition,
        *,
        runner_resources: RunnerResources | None = None,
        application_modules: tuple[str, ...] = (),
    ) -> None:
        bootstrap_applications(application_modules, purpose="Modal curator preparation")
        self._prepare(
            operation,
            merge_runner_resources(operation.runner_resources, runner_resources),
            application_modules,
        )

    def _prepare(
        self,
        subject: CompiledPlan | OperationDefinition,
        resources: RunnerResources,
        applications: tuple[str, ...],
    ) -> None:
        import modal

        function_profile("im-pending", resources)
        providers = self._providers()
        names, tools = requirements(subject, providers, applications)
        try:
            image = lookup_image(
                self.environment,
                tools,
                package_names=names,
                cache_root=self.cache_root,
                base_image=self.base_image,
            ).image
        except MissingPreparedImage:
            image = prepare_image(
                self.environment,
                tools,
                package_names=names,
                cache_root=self.cache_root,
                base_image=self.base_image,
            )
            controller_compatibility(
                subject, providers, self.environment, image, applications
            )
            publish_image(image, cache_root=self.cache_root)
        else:
            controller_compatibility(
                subject, providers, self.environment, image, applications
            )
        profile = function_profile(image.image_id, resources)
        if self._deployment_exists(image, profile):
            return
        app = modal.App()
        _function(app, profile)
        app.deploy(name=_deployment_name(self.app_name, image, profile))

    def _deployment_exists(
        self, image: PreparedModalImage, profile: ModalFunctionRuntime
    ) -> bool:
        """Distinguish an absent deployment from an inaccessible Modal service."""
        import modal

        try:
            modal.Function.from_name(
                _deployment_name(self.app_name, image, profile), "assignment"
            ).hydrate()
        except modal.exception.NotFoundError:
            return False
        return True

    def resolve(
        self,
        plan: CompiledPlan,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> AssignmentSoftware:
        return self._resolve(plan, runner_resources, application_modules)

    def resolve_curator(
        self,
        operation: OperationDefinition,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> AssignmentSoftware:
        return self._resolve(operation, runner_resources, application_modules)

    def _resolve(
        self,
        subject: CompiledPlan | OperationDefinition,
        resources: RunnerResources,
        applications: tuple[str, ...],
    ) -> ModalSoftware:
        function_profile("im-pending", resources)
        providers = self._providers()
        names, tools = requirements(subject, providers, applications)
        selected = lookup_image(
            self.environment,
            tools,
            package_names=names,
            cache_root=self.cache_root,
            base_image=self.base_image,
        )
        image = selected.image
        compatibility = controller_compatibility(
            subject, providers, self.environment, image, applications
        )
        profile = function_profile(image.image_id, resources)
        gpu = self._measure_gpu(
            image, profile, curator=not isinstance(subject, CompiledPlan)
        )
        return bind_software(
            subject, selected, compatibility, resources, applications, gpu
        )

    def _measure_gpu(
        self,
        image: PreparedModalImage,
        profile: ModalFunctionRuntime,
        *,
        curator: bool,
    ) -> NativeGPUSoftware | None:
        if not profile.gpu_count:
            return None
        compatibility = image.startup.worker.compatibility
        assert compatibility is not None
        request = GPUProbe(
            nonce=uuid4(),
            function=profile,
            runtime=image.startup.runtime,
            compatibility=compatibility,
        )
        owner = GPUProbeOwner(self._lookup_profile(image, profile), request)
        deadline = time.monotonic() + min(profile.timeout_seconds, 60)
        try:
            owner.start()
            result = owner.result(max(0.0, deadline - time.monotonic()))
            if owner.ownership_state() != OwnershipState.CLOSED:
                message = "GPU measurement owner did not close"
                raise RuntimeError(message)
            return result
        except BaseException as error:
            owner.request_close(deadline)
            failure = CuratorLaunchError if curator else AssignmentLaunchError
            message = "Modal GPU qualification failed"
            raise failure(
                message,
                handles=(),
                worker_owners=(owner,),
                cancellation=CancellationAcknowledgement(
                    CancellationStatus.CONFIRMED
                    if owner.ownership_state() == OwnershipState.CLOSED
                    else CancellationStatus.UNKNOWN
                ),
            ) from error

    def _lookup_profile(
        self, image: PreparedModalImage, profile: ModalFunctionRuntime
    ) -> ModalFunction:
        import modal

        _require_sdk()
        return modal.Function.from_name(
            _deployment_name(self.app_name, image, profile), "assignment"
        )

    def _lookup_function(self, software: ModalSoftware) -> ModalFunction:
        return self._lookup_profile(software.image, software.function)

    def _owner(
        self,
        admitted: tuple[AdmittedAssignment | AdmittedCurator, ...],
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> ModalPackOwner:
        if not isinstance(software, ModalSoftware):
            message = "Modal launch requires exact resolved Modal software"
            raise TypeError(message)
        validate_dispatch(dispatch, software)
        function = self._lookup_function(software)

        def prepare() -> ModalPack:
            return ModalPack(
                pack_id=uuid4(),
                staging_root=dispatch.staging_root,
                storage=dispatch.storage,
                calls=tuple(
                    prepare_call(item, software, dispatch) for item in admitted
                ),
            )

        return ModalPackOwner(
            function,
            prepare,
            count=len(admitted),
            sensitive_values=sensitive_values(dispatch),
        )

    def launch(
        self,
        assignments: Sequence[AdmittedAssignment],
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> tuple[AssignmentHandle, ...]:
        if not assignments:
            msg = "Modal launch requires at least one assignment"
            raise ValueError(msg)
        owner = self._owner(tuple(assignments), software, dispatch)
        handles = tuple(
            ModalAssignmentHandle(
                item.attempt,
                owner,
                index=index,
                sensitive_values=sensitive_values(dispatch),
            )
            for index, item in enumerate(assignments)
        )
        try:
            owner.start()
        except Exception as error:
            msg = "Modal pack startup failed"
            raise AssignmentLaunchError(
                msg,
                handles=handles,
                worker_owners=(owner,),
                cancellation=CancellationAcknowledgement(CancellationStatus.UNKNOWN),
            ) from error
        return handles

    def launch_curator(
        self,
        curator: AdmittedCurator,
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> CuratorHandle:
        owner = self._owner((curator,), software, dispatch)
        handle = ModalCuratorHandle(
            curator.admission, owner, sensitive_values=sensitive_values(dispatch)
        )
        try:
            owner.start()
        except Exception as error:
            message = "Modal curator startup failed"
            raise CuratorLaunchError(
                message,
                handles=(handle,),
                worker_owners=(owner,),
                cancellation=CancellationAcknowledgement(CancellationStatus.UNKNOWN),
            ) from error
        return handle
