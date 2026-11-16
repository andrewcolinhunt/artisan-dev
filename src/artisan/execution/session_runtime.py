"""Retain exact local call context and resolve already prepared command targets."""

from __future__ import annotations

import os
import shutil
from collections import deque
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Literal
from uuid import uuid4

from fsspec.implementations.local import LocalFileSystem

from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.compiler import Definition, Parameters, compile_definition
from artisan.execution.composition import (
    ExecuteAdapter,
    PlanExecution,
    execute_local_plan,
    validate_local_inputs,
)
from artisan.execution.environments.container_launch import validate_container_bind_path
from artisan.execution.environments.container_runtime import (
    ApptainerRuntime,
    DockerRuntime,
)
from artisan.execution.environments.observed_worker import observe_current_worker
from artisan.execution.environments.requirements import select_tool_environments
from artisan.execution.inputs.partitioning import partition_local_inputs
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.execution.local_assignment import (
    bind_local_software,
    container_input_files,
)
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.worker_transport import TRANSPORT_PACKAGES
from artisan.schemas.provenance.occurrence import OccurrenceAddress, ReferenceContext
from artisan.schemas.provenance.projection import (
    FrozenAssociation,
    FrozenBoundaryContext,
    FrozenReferences,
)

if TYPE_CHECKING:
    from artisan.execution.session import ExecutionSession


class SessionRuntime:
    """Keep only completed boundaries, exports and required support until closure."""

    def __init__(self, tool_backend: Literal["pixi", "docker", "apptainer"]) -> None:
        self.tool_backend = tool_backend
        self.cache_root = (
            Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
            / "artisan/environments"
        )
        if not self.cache_root.is_absolute():
            msg = "Session environment cache requires an absolute path"
            raise ValueError(msg)
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
        session: ExecutionSession,
        root: Path,
    ) -> ArtifactResult:
        """Validate all captured inputs before selecting software or invoking science."""
        inputs = {role: tuple(values) for role, values in inputs.items()}
        plan = compile_definition(definition, params=params)
        validate_local_inputs(plan, inputs)
        select_tool_environments(plan)
        identity = uuid4().hex
        positions, context, captured = self._capture(plan, inputs, identity)
        if next(partition_local_inputs(inputs, plan.recipe.policy), None) is None:
            return ArtifactResult(
                artifacts={spec.role: [] for spec in plan.recipe.outputs},
                lineage={spec.role: [] for spec in plan.recipe.outputs},
            )
        inventory = [artifact for values in inputs.values() for artifact in values]
        inventory.extend(captured.values())
        _validate_local_carriers(inventory)
        # Validate policy captures before software lookup and any authored phase.
        graph = OccurrenceGraph(boundary_invocation=identity)
        for role, values in inputs.items():
            graph.declare_input_role(role)
            for artifact in values:
                graph.add_input(role, artifact)
        for address, artifact in captured.items():
            graph.add_context_input(address, artifact)
        graph.validate_boundary_context(context)
        adapter = self._software(plan, inventory, root)
        execution = execute_local_plan(
            plan,
            inputs,
            session,
            invocation_id=identity,
            execute_adapter=adapter,
            context_artifacts=captured,
            frozen_context=context,
        )
        boundary = {
            OccurrenceAddress(invocation_id=identity, role=role, index=index): artifact
            for role, values in inputs.items()
            for index, artifact in enumerate(values)
        }
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
        requested = _boundary_queries(plan)
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
                        msg = "Associated input query has no captured session producer context"
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
        self, plan: CompiledPlan, inventory: Sequence[Artifact], root: Path
    ) -> ExecuteAdapter:
        worker = observe_current_worker(TRANSPORT_PACKAGES)
        separate = any(
            item.declaration is not None for item in select_tool_environments(plan)
        )
        docker = None
        apptainer = None
        if separate and self.tool_backend != "pixi":
            executable = shutil.which(
                self.tool_backend, path=worker.environment.get("PATH", os.defpath)
            )
            if executable is None:
                msg = "Selected session tool backend is unavailable; prepare it explicitly"
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
        bindings = bind_local_software(
            plan,
            worker,
            cache_root=self.cache_root,
            pixi=Path.home() / ".pixi/bin/pixi",
            system_env=Path("/usr/bin/env"),
            tool_backend=self.tool_backend,
            docker=docker,
            apptainer=apptainer,
        )
        files: tuple[Path, ...] = ()
        if bindings.container_runtime is not None:
            files = container_input_files(inventory)
            for path in (root, *files):
                validate_container_bind_path(path, bindings.container_runtime)
        return lambda operation, path: bindings.adapter(
            operation, path, runtime_env={}, input_files=files
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


def _boundary_queries(plan: CompiledPlan) -> dict[str, set[str]]:
    """Follow only static boundary wiring; produced values get graph-local queries."""
    queries: dict[str, set[str]] = {}

    def visit(
        node: CompiledPlan, origins: Mapping[str, set[str]]
    ) -> dict[str, set[str]]:
        for spec in node.recipe.inputs:
            for role in origins.get(spec.role, ()):
                queries.setdefault(role, set()).update(spec.with_associated)
        calls: dict[str, dict[str, set[str]]] = {}
        for compiled, recorded in zip(node.calls, node.recipe.calls, strict=True):
            child = {
                binding.role: set().union(
                    *(
                        origins.get(source.role, set())
                        if source.call is None
                        else calls[source.call].get(source.role, set())
                        for source in binding.sources
                    )
                )
                for binding in recorded.inputs
            }
            calls[compiled.name] = visit(compiled.plan, child)
        return {
            binding.role: set().union(
                *(
                    origins.get(source.role, set())
                    if source.call is None
                    else calls[source.call].get(source.role, set())
                    for source in binding.sources
                )
            )
            for binding in node.recipe.exports
        }

    visit(plan, {spec.role: {spec.role} for spec in plan.recipe.inputs})
    return queries
