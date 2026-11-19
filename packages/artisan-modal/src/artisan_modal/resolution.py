"""Resolve canonical image requirements without installing or contacting workers."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from artisan.orchestration.runner_api import (
    CompiledPlan,
    Environment,
    NativeGPUSoftware,
    OperationDefinition,
    PreparedExternalFiles,
    PreparedLocalBindings,
    PreparedToolBinding,
    RunnerResources,
    RuntimeCompatibility,
    capture_curator_definition,
    controller_definition_sources,
    controller_worker_sources,
    observed_controller_sources,
    observed_definition_sources,
    select_runtime_compatibility,
    select_tool_environments,
    verify_runtime_compatibility,
)
from artisan_modal.bindings import (
    PIXI,
    ModalFunctionProfile,
    ModalFunctionRuntime,
    ModalFunctionRuntimeWithVolumes,
    PreparedModalImage,
)
from artisan_modal.image_store import ImageSelection
from artisan_modal.software import ModalSoftware
from artisan_modal.volumes import ModalVolumeMount


def requirements(
    subject: CompiledPlan | OperationDefinition,
    providers: tuple[type, ...],
    applications: tuple[str, ...],
) -> tuple[tuple[str, ...], tuple[Environment, ...]]:
    """Use the trusted collector and compiled command selections as the sole recipe."""
    if isinstance(subject, CompiledPlan):
        names, _ = observed_controller_sources(
            subject, providers, application_modules=applications
        )
        declarations = tuple(
            dict.fromkeys(
                selected.declaration
                for selected in select_tool_environments(subject)
                if selected.declaration is not None
            )
        )
        return names, declarations
    capture_curator_definition(subject)
    names, _ = observed_definition_sources(
        (type(subject),), providers, application_modules=applications
    )
    return names, ()


def function_profile(
    image_id: str,
    resources: RunnerResources,
    *,
    volumes: tuple[ModalVolumeMount, ...] = (),
) -> ModalFunctionProfile:
    """Translate explicit portable resources without choosing an accelerator fallback."""
    import re

    gpu_type = resources.extra.get("gpu_type", "")
    if set(resources.extra) - {"gpu_type"} or not isinstance(gpu_type, str):
        message = "Modal accepts only the gpu_type provider resource option"
        raise ValueError(message)
    if (resources.gpus > 0) != bool(gpu_type) or (
        gpu_type and re.fullmatch(r"[A-Z][A-Z0-9-]{0,31}", gpu_type) is None
    ):
        message = "Modal GPU resources require an explicit canonical gpu_type"
        raise ValueError(message)
    hours, minutes, seconds = (int(value) for value in resources.time_limit.split(":"))
    profile = ModalFunctionRuntime(
        image_id=image_id,
        milli_cpu=resources.cpus * 1000,
        memory_mb=resources.memory_gb * 1024,
        gpu_count=resources.gpus,
        gpu_type=gpu_type,
        max_inputs=1,
        timeout_seconds=hours * 3600 + minutes * 60 + seconds,
    )
    if volumes:
        return ModalFunctionRuntimeWithVolumes(**profile.model_dump(), volumes=volumes)
    return profile


def controller_compatibility(
    subject: CompiledPlan | OperationDefinition,
    providers: tuple[type, ...],
    worker: Environment,
    image: PreparedModalImage,
    applications: tuple[str, ...],
) -> RuntimeCompatibility:
    """Select compatible controller packages from the prepared worker coverage."""
    if isinstance(subject, CompiledPlan):
        names, _sources = controller_worker_sources(
            subject,
            providers,
            worker,
            image.startup.worker,
            application_modules=applications,
        )
    else:
        names, _sources = controller_definition_sources(
            (type(subject),),
            providers,
            worker,
            image.startup.worker,
            application_modules=applications,
        )
    compatibility = image.startup.worker.compatibility
    assert compatibility is not None
    selected = select_runtime_compatibility(compatibility, names)
    verify_runtime_compatibility(selected)
    return selected


def bind_software(
    subject: CompiledPlan | OperationDefinition,
    selected: ImageSelection,
    compatibility: RuntimeCompatibility,
    resources: RunnerResources,
    applications: tuple[str, ...],
    gpu: NativeGPUSoftware | None = None,
    *,
    external_files: Mapping[str, PreparedExternalFiles] | None = None,
    volumes: tuple[ModalVolumeMount, ...] = (),
) -> ModalSoftware:
    """Bind call paths to already resolved targets without reopening source files."""
    image = selected.image
    worker = image.startup.worker.model_copy(update={"compatibility": compatibility})
    tools = (
        ()
        if not isinstance(subject, CompiledPlan)
        else tuple(
            PreparedToolBinding(
                selection.call_path,
                worker
                if selection.declaration is None
                or selected.targets[selection.declaration].digest == worker.digest
                else selected.targets[selection.declaration],
            )
            for selection in select_tool_environments(subject)
        )
    )
    local = PreparedLocalBindings(
        worker=worker,
        tools=tools,
        pixi=PIXI,
        system_env=Path("/usr/bin/env"),
        launcher=image.startup.launcher,
        command_witness=image.startup.layout.witness if tools else None,
        application_modules=applications,
        external_files=external_files or {},
    )
    return ModalSoftware(
        image, local, function_profile(image.image_id, resources, volumes=volumes), gpu
    )
