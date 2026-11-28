"""Retain exact artifact context across calls in an explicit executor session."""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Literal
from uuid import uuid4

from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.compiler import Definition, Parameters, compile_definition
from artisan.execution.composition import (
    OperationInvoker,
)
from artisan.execution.inputs.local_context import (
    boundary_queries,
)
from artisan.execution.local_execution import execute_local_call, local_inputs
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.external_descriptor import (
    PreparedExternalFiles,
)
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.result_support import ArtifactResultSupport
from artisan.schemas.provenance.occurrence import OccurrenceAddress, ReferenceContext
from artisan.schemas.provenance.projection import (
    FrozenAssociation,
    FrozenBoundaryContext,
    FrozenConstant,
    FrozenReferences,
)


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
        external_files: Mapping[str, PreparedExternalFiles] | None = None,
    ) -> ArtifactResult:
        """Execute a local call and retain its exact completed context."""
        plan = compile_definition(definition, params=params)
        inputs = local_inputs(plan, inputs)
        identity = uuid4().hex
        positions, context, captured = self._capture(plan, inputs, identity)
        result = execute_local_call(
            plan,
            inputs,
            invoke,
            root,
            identity=identity,
            context=context,
            captured=captured,
            external_files=external_files,
            tool_backend=self.tool_backend,
            cache_root=self.cache_root,
        )
        if result.support is not None:
            self._remember(result.support, positions)
        return result

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
        captured: dict[OccurrenceAddress, Artifact] = {}
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
        constants = []
        for binding in plan.constants:
            addresses = []
            for artifact in binding.artifacts:
                address = OccurrenceAddress(
                    invocation_id=f"{identity}/context",
                    role="constant",
                    index=len(captured),
                )
                positions[address] = address
                supplied[address] = artifact
                captured[address] = artifact
                addresses.append(address)
            constants.append(
                FrozenConstant(
                    call_path=binding.call_path,
                    role=binding.role,
                    occurrences=tuple(addresses),
                )
            )
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
            schema_version=2 if constants else 1,
            constants=tuple(constants),
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
        support: ArtifactResultSupport,
        sources: Mapping[OccurrenceAddress, OccurrenceAddress],
    ) -> None:
        projection = support.projection
        inventory = support.artifacts
        boundary = projection.boundary_addresses()
        canonical = {address: sources.get(address, address) for address in inventory}
        for address, artifact in inventory.items():
            self.artifacts[canonical[address]] = artifact
        unscoped_objects = {
            id(artifact)
            for address, artifact in inventory.items()
            if address in boundary and canonical[address] not in self.produced
        }
        self.produced.update(
            canonical[address]
            for address, artifact in inventory.items()
            if address not in boundary and id(artifact) not in unscoped_objects
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
