"""Exact transient derivation and retention without storage or orchestration."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
from types import MappingProxyType
from typing import cast

from artisan.execution.lineage.validation import (
    resolve_input_index,
    validate_artifacts_match_specs,
    validate_lineage_completeness,
    validate_lineage_integrity,
)
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.provenance.lineage_mapping import LineageMapping
from artisan.schemas.provenance.occurrence import (
    ArtifactOccurrence,
    OccurrenceAddress,
    ReferenceContext,
    RequiredReference,
)
from artisan.schemas.provenance.projection import (
    BoundaryInputAttribution,
    BoundaryProjection,
    FrozenBoundaryContext,
    OccurrenceRelation,
    ProjectedOccurrence,
    RoleOccurrences,
)
from artisan.schemas.specs.output_spec import OutputSpec


@dataclass(frozen=True)
class OccurrenceNode:
    """A producer occurrence, its direct declared parents, and an explicit alias."""

    address: OccurrenceAddress
    artifact: Artifact
    parents: tuple[OccurrenceAddress, ...] = ()
    alias_of: OccurrenceAddress | None = None


class OccurrenceGraph:
    """Record finalized invocation results and project only the selected boundary."""

    def __init__(self, *, boundary_invocation: str = "boundary") -> None:
        if not boundary_invocation:
            msg = "Boundary invocation identity must be nonempty"
            raise ValueError(msg)
        self._boundary_invocation = boundary_invocation
        self._nodes: dict[OccurrenceAddress, OccurrenceNode] = {}
        self._inputs: dict[str, tuple[OccurrenceAddress, ...]] = {}
        self._external: set[OccurrenceAddress] = set()
        self._invocations: set[str] = set()
        self._alias_invocations: set[str] = set()

    @property
    def nodes(self) -> Mapping[OccurrenceAddress, OccurrenceNode]:
        """Expose read-only membership; artifact locators remain session-owned."""
        return MappingProxyType(self._nodes)

    def declare_input_role(self, role: str) -> None:
        """Preserve a supplied role even when its input sequence is empty."""
        RoleOccurrences(role=role, occurrences=())
        self._inputs.setdefault(role, ())

    def add_input(
        self, role: str, artifact: Artifact, *, address: OccurrenceAddress | None = None
    ) -> OccurrenceAddress:
        """Bind a finalized input, preserving an outer producer address when supplied."""
        RoleOccurrences(role=role, occurrences=())
        values = self._inputs.get(role, ())
        selected = address or OccurrenceAddress(
            invocation_id=self._boundary_invocation, role=role, index=len(values)
        )
        self._admit_external(selected, artifact)
        self._inputs[role] = (*values, selected)
        return selected

    def add_context_input(
        self, address: OccurrenceAddress, artifact: Artifact
    ) -> OccurrenceAddress:
        """Admit an external dependency whose membership must be explicitly captured."""
        self._admit_external(address, artifact)
        return address

    def _admit_external(self, address: OccurrenceAddress, artifact: Artifact) -> None:
        if address.invocation_id in self._invocations | self._alias_invocations:
            msg = "External inputs cannot reuse a local invocation identity"
            raise ValueError(msg)
        self._validate_artifact(address, artifact)
        existing = self._nodes.get(address)
        if existing is not None and existing.artifact.to_row() != artifact.to_row():
            msg = "One producer occurrence cannot name conflicting artifact values"
            raise ValueError(msg)
        self._nodes.setdefault(address, OccurrenceNode(address, artifact))
        self._external.add(address)

    def add_alias(
        self, invocation_id: str, role: str, source: OccurrenceAddress
    ) -> OccurrenceAddress:
        """Record explicit identity routing without fabricating another producer."""
        if invocation_id in self._invocations or any(
            key.invocation_id == invocation_id for key in self._external
        ):
            msg = "Alias invocation identity is already owned"
            raise ValueError(msg)
        original = self.original(source)
        index = sum(
            key.invocation_id == invocation_id and key.role == role
            for key in self._nodes
        )
        address = OccurrenceAddress(invocation_id=invocation_id, role=role, index=index)
        self._nodes[address] = OccurrenceNode(
            address, self._nodes[original].artifact, (source,), alias_of=original
        )
        self._alias_invocations.add(invocation_id)
        return address

    def original(self, address: OccurrenceAddress) -> OccurrenceAddress:
        """Resolve only an explicitly recorded alias; equal content proves nothing."""
        node = self._nodes.get(address)
        if node is None:
            msg = "Occurrence is not admitted to this graph"
            raise ValueError(msg)
        return node.alias_of or address

    def record_result(
        self,
        invocation_id: str,
        inputs: Mapping[str, Sequence[OccurrenceAddress]],
        result: ArtifactResult,
        output_specs: dict[str, OutputSpec],
    ) -> Mapping[str, tuple[OccurrenceAddress, ...]]:
        """Map consuming input positions to actual producers and record one result.

        The invocation kernel supplies finalized artifacts. The graph never
        invents an origin or infers derivation from Python object identity.
        Failed validation leaves graph membership unchanged.
        """
        if (
            not invocation_id
            or invocation_id in self._invocations | self._alias_invocations
            or any(key.invocation_id == invocation_id for key in self._external)
        ):
            msg = "Invocation identity must be nonempty and uniquely owned"
            raise ValueError(msg)
        if not result.success:
            msg = "Cannot record a failed operation result"
            raise ValueError(msg)
        input_ids = self._input_identities(inputs)
        validate_artifacts_match_specs(result.artifacts, output_specs)
        validate_lineage_completeness(result.artifacts, output_specs, result.lineage)
        validate_lineage_integrity(
            result.lineage,
            input_ids,
            result.artifacts,
            output_specs,
            invocation_id=invocation_id,
        )
        outputs = {
            role: tuple(
                OccurrenceAddress(invocation_id=invocation_id, role=role, index=i)
                for i in range(len(artifacts))
            )
            for role, artifacts in result.artifacts.items()
        }
        parents = self._resolve_parents(
            invocation_id, inputs, input_ids, outputs, result
        )
        pending = {
            address: OccurrenceNode(address, artifact, tuple(parents.get(address, ())))
            for role, artifacts in result.artifacts.items()
            for address, artifact in zip(outputs[role], artifacts, strict=True)
        }
        self._validate_pending(pending)
        self._nodes.update(pending)
        self._invocations.add(invocation_id)
        return MappingProxyType(outputs)

    def _input_identities(
        self, inputs: Mapping[str, Sequence[OccurrenceAddress]]
    ) -> dict[str, list[str]]:
        identities: dict[str, list[str]] = {}
        for role, addresses in inputs.items():
            identities[role] = []
            for address in addresses:
                node = self._nodes[self.original(address)]
                identities[role].append(
                    self._validate_artifact(node.address, node.artifact)
                )
        return identities

    def _resolve_parents(
        self,
        invocation_id: str,
        inputs: Mapping[str, Sequence[OccurrenceAddress]],
        input_ids: dict[str, list[str]],
        outputs: Mapping[str, tuple[OccurrenceAddress, ...]],
        result: ArtifactResult,
    ) -> dict[OccurrenceAddress, list[OccurrenceAddress]]:
        parents: dict[OccurrenceAddress, list[OccurrenceAddress]] = {}
        for role, mappings in result.lineage.items():
            for mapping in mappings:
                target = outputs[role][mapping.draft_index]
                if mapping.source_output_index is not None:
                    source = outputs[mapping.source_role][mapping.source_output_index]
                else:
                    index = resolve_input_index(
                        mapping, input_ids, invocation_id=invocation_id
                    )
                    source = self.original(inputs[mapping.source_role][index])
                values = parents.setdefault(target, [])
                if source not in values:
                    values.append(source)
        return parents

    @staticmethod
    def _validate_artifact(address: OccurrenceAddress, artifact: Artifact) -> str:
        ArtifactOccurrence(address, artifact)
        ArtifactTypeDef.get(artifact.artifact_type).reference_policy()
        return cast(str, artifact.artifact_id)

    def _validate_pending(
        self, pending: Mapping[OccurrenceAddress, OccurrenceNode]
    ) -> None:
        if self._nodes.keys() & pending.keys():
            msg = "Producer occurrence identities must be unique"
            raise ValueError(msg)
        for node in pending.values():
            self._validate_artifact(node.address, node.artifact)
        parents = {
            key: node.parents for key, node in (self._nodes | dict(pending)).items()
        }
        if any(
            parent not in parents for values in parents.values() for parent in values
        ):
            msg = "A direct parent is not an admitted occurrence"
            raise ValueError(msg)
        try:
            tuple(TopologicalSorter(parents).static_order())
        except CycleError as error:
            msg = "Original derivation must be acyclic"
            raise ValueError(msg) from error

    def project(
        self,
        exports: Mapping[str, Sequence[OccurrenceAddress]],
        output_specs: dict[str, OutputSpec],
        *,
        frozen_context: FrozenBoundaryContext | None = None,
        input_attribution: tuple[BoundaryInputAttribution, ...] | None = None,
    ) -> BoundaryProjection:
        """Retain exports and required support; contract only actual parent paths."""
        context = frozen_context or FrozenBoundaryContext()
        boundary = {address for values in self._inputs.values() for address in values}
        boundary.update(context.occurrence_addresses())
        self._validate_context(context, boundary)
        attribution = self._input_attribution(boundary, input_attribution)
        for node in self._nodes.values():
            self._validate_artifact(node.address, node.artifact)
        selected = {
            role: tuple(self.original(key) for key in values)
            for role, values in exports.items()
        }
        artifacts = {
            role: [self._nodes[key].artifact for key in values]
            for role, values in selected.items()
        }
        validate_artifacts_match_specs(artifacts, output_specs)
        retained, references = self._retain(selected, boundary, context)
        kept = boundary | retained
        public = {key for values in selected.values() for key in values}
        return BoundaryProjection(
            boundary_inputs=tuple(
                RoleOccurrences(role=role, occurrences=values)
                for role, values in self._inputs.items()
            ),
            input_attribution=attribution,
            occurrences=tuple(
                ProjectedOccurrence(
                    address=key,
                    artifact_id=cast(str, node.artifact.artifact_id),
                    artifact_type=node.artifact.artifact_type,
                )
                for key, node in self._nodes.items()
                if key in kept
            ),
            exports=tuple(
                RoleOccurrences(role=role, occurrences=values)
                for role, values in selected.items()
            ),
            support=tuple(
                key for key in self._nodes if key in retained - public - boundary
            ),
            parents=self._project_parents(retained, kept),
            associations=self._direct_associations(kept),
            references=references,
            frozen_context=context,
        )

    def visible_result(
        self,
        invocation_id: str,
        inputs: Mapping[str, Sequence[OccurrenceAddress]],
        exports: Mapping[str, Sequence[OccurrenceAddress]],
        output_specs: dict[str, OutputSpec],
    ) -> ArtifactResult:
        """Rebase lineage to uniquely addressed visible inputs and public outputs.

        Repeated public exports use their first public membership as the
        canonical sibling reference. Each target membership receives its own
        mappings. Visible input occurrences must remain distinct; adopting an
        ambiguous repeated producer cannot reconstruct a selected consuming slot.
        """
        if not invocation_id:
            msg = "Visible result requires an invocation identity"
            raise ValueError(msg)
        input_positions: dict[OccurrenceAddress, tuple[str, int]] = {}
        for role, values in inputs.items():
            for index, address in enumerate(values):
                original = self.original(address)
                self._validate_artifact(original, self._nodes[original].artifact)
                if original in input_positions:
                    msg = "Visible input producer is ambiguous; distinct input occurrences are required"
                    raise ValueError(msg)
                input_positions[original] = (role, index)
        selected = {
            role: tuple(self.original(key) for key in values)
            for role, values in exports.items()
        }
        artifacts = {
            role: [self._nodes[key].artifact for key in values]
            for role, values in selected.items()
        }
        validate_artifacts_match_specs(artifacts, output_specs)
        for values in selected.values():
            for address in values:
                self._validate_artifact(address, self._nodes[address].artifact)
        output_positions: dict[OccurrenceAddress, tuple[str, int]] = {}
        for role, values in selected.items():
            for index, address in enumerate(values):
                output_positions.setdefault(address, (role, index))
        lineage: dict[str, list[LineageMapping]] = {role: [] for role in selected}
        for role, values in selected.items():
            for index, target in enumerate(values):
                sources = (
                    (target,)
                    if target in input_positions
                    else self._parent_frontier(
                        target,
                        set(input_positions) | (set(output_positions) - {target}),
                    )
                )
                for source in sources:
                    if source in output_positions and source != target:
                        source_role, source_index = output_positions[source]
                        mapping = LineageMapping(
                            draft_index=index,
                            source_role=source_role,
                            source_output_index=source_index,
                        )
                    else:
                        source_role, source_index = input_positions[source]
                        mapping = LineageMapping(
                            draft_index=index,
                            source_role=source_role,
                            source_artifact_id=self._nodes[source].artifact.artifact_id,
                            source_input_index=source_index,
                            source_invocation=invocation_id,
                        )
                    lineage[role].append(mapping)
        return ArtifactResult(artifacts=artifacts, lineage=lineage)

    def _parent_frontier(
        self, target: OccurrenceAddress, stops: set[OccurrenceAddress]
    ) -> tuple[OccurrenceAddress, ...]:
        pending = list(reversed(self._nodes[target].parents))
        visited: set[OccurrenceAddress] = set()
        sources = []
        while pending:
            parent = self.original(pending.pop())
            if parent in visited:
                continue
            visited.add(parent)
            if parent in stops:
                sources.append(parent)
            else:
                pending.extend(reversed(self._nodes[parent].parents))
        return tuple(sources)

    def required_references(
        self,
        address: OccurrenceAddress,
        *,
        frozen_context: FrozenBoundaryContext | None = None,
    ) -> tuple[RequiredReference, ...]:
        """Resolve a local artifact's declared exact dependencies without I/O.

        Boundary callers still require explicit captured context when a child
        input requests typed dependencies or materialization, including an empty
        capture. This resolver does not grant that author-facing capability.
        """
        context = frozen_context or FrozenBoundaryContext()
        boundary = {key for values in self._inputs.values() for key in values}
        boundary.update(context.occurrence_addresses())
        if not boundary <= self._external:
            msg = "Frozen boundary context must contain admitted external occurrences"
            raise ValueError(msg)
        references = self._required_references(
            self.original(address), boundary, context
        )
        if any(
            reference.address in self._external and reference.address not in boundary
            for reference in references
        ):
            msg = "External required referent is outside captured input closure"
            raise ValueError(msg)
        return references

    def validate_boundary_context(
        self,
        context: FrozenBoundaryContext,
        *,
        input_attribution: tuple[BoundaryInputAttribution, ...] | None = None,
    ) -> None:
        """Validate exact frozen dependencies before any scientific invocation."""
        boundary = {key for values in self._inputs.values() for key in values}
        boundary.update(context.occurrence_addresses())
        self._validate_context(context, boundary)
        self._input_attribution(boundary, input_attribution)

    def _input_attribution(
        self,
        boundary: set[OccurrenceAddress],
        supplied: tuple[BoundaryInputAttribution, ...] | None,
    ) -> tuple[BoundaryInputAttribution, ...]:
        if supplied is None:
            return tuple(
                BoundaryInputAttribution(boundary=address, producer=None)
                for address in self._nodes
                if address in boundary
            )
        attribution = tuple(
            BoundaryInputAttribution.model_validate(item) for item in supplied
        )
        addresses = [item.boundary for item in attribution]
        if len(addresses) != len(set(addresses)) or set(addresses) != boundary:
            msg = "Input attribution must name every boundary occurrence exactly once"
            raise ValueError(msg)
        return attribution

    def _validate_context(
        self, context: FrozenBoundaryContext, boundary: set[OccurrenceAddress]
    ) -> None:
        if not boundary <= self._external:
            msg = "Frozen boundary context must contain admitted external occurrences"
            raise ValueError(msg)
        for address in boundary:
            self._validate_artifact(address, self._nodes[address].artifact)
        for association in context.associations:
            if any(
                self._nodes[member].artifact.artifact_type != association.artifact_type
                for member in association.members
            ):
                msg = "Captured association has the wrong artifact type"
                raise ValueError(msg)
        for capture in context.references:
            if not set(capture.direct_parents) <= boundary:
                msg = (
                    "Captured direct parents must stay within the frozen input closure"
                )
                raise ValueError(msg)
            if (
                ArtifactTypeDef.get(
                    self._nodes[capture.source].artifact.artifact_type
                ).reference_policy()
                is None
            ):
                msg = "Reference captures require a declared artifact reference policy"
                raise ValueError(msg)
        for address in boundary:
            for reference in self._required_references(address, boundary, context):
                if self.original(reference.address) not in boundary:
                    msg = "Boundary required referent is outside frozen input closure"
                    raise ValueError(msg)

    def _required_references(
        self,
        address: OccurrenceAddress,
        boundary: set[OccurrenceAddress],
        context: FrozenBoundaryContext,
    ) -> tuple[RequiredReference, ...]:
        node = self._nodes[address]
        type_def = ArtifactTypeDef.get(node.artifact.artifact_type)
        if type_def.reference_policy() is None:
            return ()
        candidates = set(self._nodes)
        direct = tuple(self.original(parent) for parent in node.parents)
        capture = None
        if address in boundary:
            capture = context.reference_capture(address)
            candidates = set(capture.members) | set(capture.direct_parents)
            direct = capture.direct_parents
            if not candidates <= boundary:
                msg = "Boundary required referent is outside frozen input closure"
                raise ValueError(msg)
        canonical = {
            self.original(key): self._nodes[self.original(key)].artifact
            for key in candidates
        }
        references = type_def.resolve_required_references(
            node.artifact, ReferenceContext(canonical, direct_parents=direct)
        )
        if capture is not None and {item.address for item in references} != set(
            capture.members
        ):
            msg = "Captured semantic references must match exact type dependencies"
            raise ValueError(msg)
        return references

    def _retain(
        self,
        exports: Mapping[str, tuple[OccurrenceAddress, ...]],
        boundary: set[OccurrenceAddress],
        context: FrozenBoundaryContext,
    ) -> tuple[set[OccurrenceAddress], tuple[OccurrenceRelation, ...]]:
        retained = {key for values in exports.values() for key in values}
        pending = list(retained)
        visited: set[OccurrenceAddress] = set()
        references: set[tuple[OccurrenceAddress, OccurrenceAddress]] = set()
        while pending:
            key = pending.pop()
            if key in visited:
                continue
            visited.add(key)
            if key in self._external and key not in boundary:
                msg = "External required referent is outside captured input closure"
                raise ValueError(msg)
            for reference in self._required_references(key, boundary, context):
                target = self.original(reference.address)
                references.add((key, target))
                if target not in boundary:
                    retained.add(target)
                    pending.append(target)
        return retained, self._relations(references)

    def _project_parents(
        self, retained: set[OccurrenceAddress], kept: set[OccurrenceAddress]
    ) -> tuple[OccurrenceRelation, ...]:
        return self._relations(
            {
                (parent, target)
                for target in retained
                for parent in self._parent_frontier(target, kept)
            }
        )

    def _direct_associations(
        self, kept: set[OccurrenceAddress]
    ) -> tuple[OccurrenceRelation, ...]:
        return self._relations(
            {
                (self.original(parent), key)
                for key, node in self._nodes.items()
                if key in kept and node.alias_of is None
                for parent in node.parents
                if self.original(parent) in kept
            }
        )

    @staticmethod
    def _relations(
        pairs: set[tuple[OccurrenceAddress, OccurrenceAddress]],
    ) -> tuple[OccurrenceRelation, ...]:
        return tuple(
            OccurrenceRelation(source=source, target=target)
            for source, target in sorted(
                pairs, key=lambda pair: (pair[0].key, pair[1].key)
            )
        )
