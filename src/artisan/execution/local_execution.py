"""Execute local calls without owning files, history, or durable storage."""

from __future__ import annotations

import os
import shutil
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Literal, cast

from fsspec.implementations.local import LocalFileSystem

from artisan.composites.base.compiled import CompiledPlan
from artisan.execution.composition import (
    ExecuteAdapter,
    OperationInvoker,
    execute_local_plan,
    validate_local_inputs,
)
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.environments.container_launch import validate_container_bind_path
from artisan.execution.environments.container_runtime import (
    ApptainerRuntime,
    DockerRuntime,
)
from artisan.execution.environments.observed_worker import observe_current_worker
from artisan.execution.environments.requirements import select_tool_environments
from artisan.execution.external_access import bind_external_files, select_external_files
from artisan.execution.inputs.materialization import requires_materialization
from artisan.execution.inputs.partitioning import partition_local_inputs
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.execution.local_assignment import (
    bind_local_software,
    container_external_files,
    container_input_files,
)
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.external_descriptor import (
    PreparedExternalFiles,
    expected_receipt_digest,
)
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.worker_transport import TRANSPORT_PACKAGES
from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.schemas.provenance.projection import (
    FrozenBoundaryContext,
)
from artisan.schemas.specs.input_defaults import capture_input_defaults


def local_inputs(
    plan: CompiledPlan, inputs: Mapping[str, Sequence[Artifact]]
) -> dict[str, tuple[Artifact, ...]]:
    """Apply fixed defaults and validate inputs and declarations before lookup."""
    selected = {role: tuple(values) for role, values in inputs.items()}
    selected.update(capture_input_defaults(plan.definition.inputs, selected.keys()))
    validate_local_inputs(plan, selected)
    select_tool_environments(plan)
    return selected


def execute_local_call(
    plan: CompiledPlan,
    inputs: Mapping[str, Sequence[Artifact]],
    invoke: OperationInvoker,
    root: Path | None,
    *,
    identity: str,
    context: FrozenBoundaryContext,
    captured: Mapping[OccurrenceAddress, Artifact],
    external_files: Mapping[str, PreparedExternalFiles] | None = None,
    adapter_factory: Callable[[], ExecuteAdapter] | None = None,
    tool_backend: Literal["pixi", "docker", "apptainer"] = "pixi",
    cache_root: Path | None = None,
) -> ArtifactResult:
    """Execute a validated local boundary using caller-owned paths and context."""
    if next(partition_local_inputs(inputs, plan.recipe.policy), None) is None:
        return ArtifactResult(
            artifacts={spec.role: [] for spec in plan.recipe.outputs},
            lineage={spec.role: [] for spec in plan.recipe.outputs},
        )
    inputs, captured = _clone_external_artifacts(inputs, captured)
    inventory = [artifact for values in inputs.values() for artifact in values]
    inventory.extend(captured.values())
    _validate_local_carriers(inventory)
    selected = select_external_files(inventory, external_files or {})
    bind_external_files(inventory, selected)
    boundary = {
        OccurrenceAddress(invocation_id=identity, role=role, index=index): artifact
        for role, values in inputs.items()
        for index, artifact in enumerate(values)
    }
    graph = OccurrenceGraph(boundary_invocation=identity)
    for role, values in inputs.items():
        graph.declare_input_role(role)
        for artifact in values:
            graph.add_input(role, artifact)
    for address, artifact in captured.items():
        graph.add_context_input(address, artifact)
    graph.validate_boundary_context(context)
    references = _materialization_references(context, {**boundary, **captured})
    if root is None and _requires_files(plan, inputs, references):
        msg = "Operation commands or materialization require work_dir"
        raise ValueError(msg)
    adapter = (
        adapter_factory()
        if adapter_factory is not None
        else select_local_software(
            plan, inventory, root, tool_backend=tool_backend, cache_root=cache_root
        )
    )
    execution = execute_local_plan(
        plan,
        inputs,
        invoke,
        invocation_id=identity,
        execute_adapter=adapter,
        context_artifacts=captured,
        frozen_context=context,
    )
    return execution.result


def select_local_software(
    plan: CompiledPlan,
    inventory: Sequence[Artifact],
    root: Path | None,
    *,
    tool_backend: Literal["pixi", "docker", "apptainer"] = "pixi",
    cache_root: Path | None = None,
) -> ExecuteAdapter:
    selections = select_tool_environments(plan)
    if not selections:
        return lambda _operation, _path: LocalExecuteRouter()
    assert root is not None
    from artisan.execution.local_software import local_software

    software = local_software()
    worker = observe_current_worker(TRANSPORT_PACKAGES)
    separate = any(item.declaration is not None for item in selections)
    docker = None
    apptainer = None
    if separate and tool_backend != "pixi":
        executable = shutil.which(
            tool_backend, path=worker.environment.get("PATH", os.defpath)
        )
        if executable is None:
            msg = "Selected executor tool backend is unavailable; prepare it explicitly"
            raise ValueError(msg)
        if tool_backend == "docker":
            if (
                worker.environment.get("DOCKER_HOST", "unix:///var/run/docker.sock")
                != "unix:///var/run/docker.sock"
                or worker.environment.get("DOCKER_CONTEXT", "default") != "default"
            ):
                msg = "Standalone Docker requires the default local Unix-socket engine"
                raise ValueError(msg)
            docker = DockerRuntime(Path(executable).absolute())
        else:
            apptainer = ApptainerRuntime(
                Path(executable).absolute(), temporary_root=root / "apptainer"
            )
    external_files = {}
    for artifact in inventory:
        if artifact.EXTERNAL_REFERENCE:
            assert artifact.artifact_id is not None
            descriptor = artifact.external_descriptor()
            external_files[artifact.artifact_id] = PreparedExternalFiles(
                root=artifact.external_root,
                descriptor=descriptor,
                receipt_digest=expected_receipt_digest(descriptor),
            )
    bindings = bind_local_software(
        plan,
        worker,
        cache_root=cache_root or software.cache_root,
        pixi=software.pixi,
        system_env=software.system_env,
        tool_backend=tool_backend,
        docker=docker,
        apptainer=apptainer,
        external_files=external_files,
    )
    files: tuple[Path, ...] = ()
    installations: tuple[PreparedExternalFiles, ...] = ()
    if bindings.container_runtime is not None:
        files = container_input_files(inventory)
        installations = container_external_files(inventory, external_files)
        if installations and not isinstance(bindings.container_runtime, DockerRuntime):
            msg = "External artifact mounts are supported only by the Docker backend"
            raise ValueError(msg)
        for path in (root, *files, *(item.root for item in installations)):
            validate_container_bind_path(path, bindings.container_runtime)
    return lambda operation, path: bindings.adapter(
        operation,
        path,
        runtime_env=software.runtime_env,
        input_files=files,
        external_files=installations,
    )


def _clone_external_artifacts(
    inputs: Mapping[str, Sequence[Artifact]],
    captured: Mapping[OccurrenceAddress, Artifact],
) -> tuple[dict[str, tuple[Artifact, ...]], dict[OccurrenceAddress, Artifact]]:
    clones: dict[int, Artifact] = {}

    def clone(artifact: Artifact) -> Artifact:
        if not artifact.EXTERNAL_REFERENCE:
            return artifact
        key = id(artifact)
        if key not in clones:
            clones[key] = artifact.model_copy(deep=True)
        return clones[key]

    return (
        {
            role: tuple(clone(artifact) for artifact in values)
            for role, values in inputs.items()
        },
        {address: clone(artifact) for address, artifact in captured.items()},
    )


def _materialization_references(
    context: FrozenBoundaryContext,
    inventory: Mapping[OccurrenceAddress, Artifact],
) -> dict[str, dict[str, Artifact]]:
    references = {}
    for capture in context.references:
        source = inventory[capture.source]
        assert source.artifact_id is not None
        members = {}
        for address in capture.members:
            artifact = inventory[address]
            assert artifact.artifact_id is not None
            members[artifact.artifact_id] = artifact
        references[source.artifact_id] = members
    return references


def _requires_files(
    plan: CompiledPlan,
    inputs: Mapping[str, Sequence[Artifact]] | None = None,
    references: Mapping[str, Mapping[str, Artifact]] | None = None,
) -> bool:
    if plan.recipe.definition.kind != "creator":
        return any(_requires_files(call.plan) for call in plan.calls)
    if plan.recipe.execution.mode != "native":
        return True
    if inputs is not None:
        return requires_materialization(
            inputs, {spec.role: spec for spec in plan.recipe.inputs}, references or {}
        )
    return any(
        spec.resolve_references
        or (
            spec.materialize
            and (
                spec.artifact_type == ArtifactTypes.ANY
                or not cast(
                    type[Artifact], ArtifactTypeDef.get(spec.artifact_type).model
                ).EXTERNAL_REFERENCE
            )
        )
        for spec in plan.recipe.inputs
    )


def _validate_local_carriers(artifacts: Sequence[Artifact]) -> None:
    for artifact in artifacts:
        artifact.finalize()
        if artifact.EXTERNALLY_BACKED:
            locator = getattr(artifact, next(iter(artifact.LOCATOR_FIELDS)))
            if not Path(locator).is_file():
                msg = "Standalone inputs require available local artifact carriers"
                raise ValueError(msg)
            artifact.verify_external_content(fs=LocalFileSystem())
