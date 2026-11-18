"""Retain exact local call context and resolve already prepared command targets."""

from __future__ import annotations

import os
import shutil
from collections import deque
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Literal, cast
from uuid import uuid4

from fsspec.implementations.local import LocalFileSystem

from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.compiler import Definition, Parameters, compile_definition
from artisan.execution.composition import (
    ExecuteAdapter,
    OperationInvoker,
    PlanExecution,
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
from artisan.execution.inputs.local_context import (
    boundary_queries,
    capture_local_context,
)
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
from artisan.schemas.provenance.occurrence import OccurrenceAddress, ReferenceContext
from artisan.schemas.provenance.projection import (
    FrozenAssociation,
    FrozenBoundaryContext,
    FrozenReferences,
)
from artisan.schemas.specs.input_models import FrozenInputContext


class ExecutorRuntime:
    """Keep only completed boundaries, exports and required support until closure."""

    def __init__(self, tool_backend: Literal["pixi", "docker", "apptainer"]) -> None:
        self.tool_backend = tool_backend
        self.cache_root: Path | None = None
        self.artifacts: dict[OccurrenceAddress, Artifact] = {}
        self.objects: dict[int, tuple[Artifact, set[OccurrenceAddress]]] = {}
        self.produced: set[OccurrenceAddress] = set()
        self.relations: set[tuple[OccurrenceAddress, OccurrenceAddress]] = set()
        self.references: dict[
            OccurrenceAddress,
            tuple[tuple[OccurrenceAddress, ...], tuple[OccurrenceAddress, ...]],
        ] = {}

    def run(
        self,
        definition: Definition,
        inputs: Mapping[str, Sequence[Artifact]],
        params: Parameters,
        invoke: OperationInvoker,
        root: Path | None,
        *,
        explicit_context: FrozenInputContext | None = None,
        external_files: Mapping[str, PreparedExternalFiles] | None = None,
    ) -> ArtifactResult:
        """Validate all captured inputs before selecting software or invoking science."""
        inputs = {role: tuple(values) for role, values in inputs.items()}
        plan = compile_definition(definition, params=params)
        validate_local_inputs(plan, inputs)
        select_tool_environments(plan)
        identity = explicit_context.invocation_id if explicit_context else uuid4().hex
        if explicit_context is None:
            positions, context, captured = self._capture(plan, inputs, identity)
        else:
            context, captured = capture_local_context(plan, explicit_context)
            positions = {}
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
        # Validate policy captures before software lookup and any authored phase.
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
        adapter = self._software(plan, inventory, root)
        execution = execute_local_plan(
            plan,
            inputs,
            invoke,
            invocation_id=identity,
            execute_adapter=adapter,
            context_artifacts=captured,
            frozen_context=context,
        )
        if explicit_context is None:
            self._remember(execution, positions, {**boundary, **captured})
        return execution.result

    def _source(
        self, artifact: Artifact, address: OccurrenceAddress
    ) -> OccurrenceAddress:
        found = self.objects.get(id(artifact))
        if found is None:
            return address
        if len(found[1]) != 1:
            msg = "Returned artifact object has ambiguous producer occurrences"
            raise ValueError(msg)
        return next(iter(found[1]))

    def _capture(
        self,
        plan: CompiledPlan,
        inputs: Mapping[str, Sequence[Artifact]],
        identity: str,
    ) -> tuple[
        dict[OccurrenceAddress, OccurrenceAddress],
        FrozenBoundaryContext,
        dict[OccurrenceAddress, Artifact],
    ]:
        positions = {}
        supplied = {}
        captures = []
        requested = boundary_queries(plan)
        for role, values in inputs.items():
            for index, artifact in enumerate(values):
                address = OccurrenceAddress(
                    invocation_id=identity, role=role, index=index
                )
                source = self._source(artifact, address)
                positions[address] = source
                supplied[source] = artifact
                for kind in sorted(requested.get(role, ())):
                    if source not in self.produced:
                        msg = "Associated input query has no captured executor producer context"
                        raise ValueError(msg)
                    members = tuple(
                        sorted(
                            (
                                target
                                for parent, target in self.relations
                                if parent == source
                                and self.artifacts[target].artifact_type == kind
                            ),
                            key=lambda item: item.key,
                        )
                    )
                    captures.append((address, kind, members))
        representatives: dict[OccurrenceAddress, OccurrenceAddress] = {}
        for address, source in positions.items():
            representatives.setdefault(source, address)
        required = dict.fromkeys(representatives)
        for _, _, members in captures:
            required.update(dict.fromkeys(members))
        pending = deque(required)
        references = {}
        while pending:
            source = pending.popleft()
            artifact = (
                supplied[source] if source in supplied else self.artifacts[source]
            )
            policy = ArtifactTypeDef.get(artifact.artifact_type)
            if policy.reference_policy() is None:
                continue
            if source in self.references:
                members, parents = self.references[source]
            else:
                members = tuple(
                    item.address
                    for item in policy.resolve_required_references(
                        artifact, ReferenceContext(supplied)
                    )
                )
                parents = ()
            references[source] = members, parents
            for member in members:
                if member not in required:
                    required[member] = None
                    pending.append(member)
        captured = {}
        for source in required:
            if source not in representatives:
                address = OccurrenceAddress(
                    invocation_id=f"{identity}/context",
                    role="captured",
                    index=len(positions),
                )
                representatives[source] = address
                positions[address] = source
                captured[address] = self.artifacts[source]
        context = FrozenBoundaryContext(
            associations=tuple(
                FrozenAssociation(
                    primary=address,
                    artifact_type=kind,
                    members=tuple(representatives[member] for member in members),
                )
                for address, kind, members in captures
            ),
            references=tuple(
                FrozenReferences(
                    source=address,
                    members=tuple(
                        representatives[member] for member in references[source][0]
                    ),
                    direct_parents=tuple(
                        representatives[parent]
                        for parent in references[source][1]
                        if parent in required
                    ),
                )
                for address, source in positions.items()
                if source in references
            ),
        )
        return positions, context, captured

    def _remember(
        self,
        execution: PlanExecution,
        sources: Mapping[OccurrenceAddress, OccurrenceAddress],
        boundary: Mapping[OccurrenceAddress, Artifact],
    ) -> None:
        projection = execution.projection
        inventory = {**boundary, **execution.retained_artifacts}
        canonical = {address: sources.get(address, address) for address in inventory}
        for address, artifact in inventory.items():
            self.artifacts[canonical[address]] = artifact
        unscoped_objects = {
            id(artifact)
            for address, artifact in boundary.items()
            if canonical[address] not in self.produced
        }
        self.produced.update(
            canonical[address]
            for address, artifact in execution.retained_artifacts.items()
            if id(artifact) not in unscoped_objects
        )
        self.relations.update(
            (canonical[edge.source], canonical[edge.target])
            for edge in projection.associations
            if canonical[edge.source] != canonical[edge.target]
        )
        frozen = {item.source: item for item in projection.frozen_context.references}
        for address, artifact in inventory.items():
            if ArtifactTypeDef.get(artifact.artifact_type).reference_policy() is None:
                continue
            if address in frozen:
                members, parents = (
                    frozen[address].members,
                    frozen[address].direct_parents,
                )
            else:
                members = tuple(
                    edge.target
                    for edge in projection.references
                    if edge.source == address
                )
                parents = tuple(
                    edge.source
                    for edge in projection.associations
                    if edge.target == address
                )
            self.references.setdefault(
                canonical[address],
                (
                    tuple(canonical[member] for member in members),
                    tuple(canonical[parent] for parent in parents),
                ),
            )
        for role in projection.exports:
            for address in role.occurrences:
                artifact = inventory[address]
                _, occurrences = self.objects.setdefault(
                    id(artifact), (artifact, set())
                )
                occurrences.add(canonical[address])

    def _software(
        self, plan: CompiledPlan, inventory: Sequence[Artifact], root: Path | None
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
        if separate and self.tool_backend != "pixi":
            executable = shutil.which(
                self.tool_backend, path=worker.environment.get("PATH", os.defpath)
            )
            if executable is None:
                msg = "Selected executor tool backend is unavailable; prepare it explicitly"
                raise ValueError(msg)
            if self.tool_backend == "docker":
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
            cache_root=self.cache_root or software.cache_root,
            pixi=software.pixi,
            system_env=software.system_env,
            tool_backend=self.tool_backend,
            docker=docker,
            apptainer=apptainer,
            external_files=external_files,
        )
        files: tuple[Path, ...] = ()
        installations: tuple[PreparedExternalFiles, ...] = ()
        if bindings.container_runtime is not None:
            files = container_input_files(inventory)
            installations = container_external_files(inventory, external_files)
            if installations and not isinstance(
                bindings.container_runtime, DockerRuntime
            ):
                msg = (
                    "External artifact mounts are supported only by the Docker backend"
                )
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
