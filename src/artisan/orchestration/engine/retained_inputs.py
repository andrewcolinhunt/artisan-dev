"""Freeze exact selected inputs and partition them before controller admission."""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType

from artisan.execution.inputs.occurrence_grouping import (
    InputSelection,
    LineageSnapshot,
    SelectionKey,
    partition_occurrence_inputs,
)
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.composites.plan import ExecutionRecipe
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.assignment import AssignmentIdentity, SealedDelivery
from artisan.schemas.provenance.occurrence import (
    ArtifactOccurrence,
    OccurrenceAddress,
    ReferenceContext,
)
from artisan.schemas.provenance.projection import (
    AcceptedCuratorOccurrence,
    AcceptedOccurrence,
    AcceptedProducer,
    BoundaryInputAttribution,
    BoundaryProjection,
    FrozenAssociation,
    FrozenBoundaryContext,
    FrozenReferences,
)
from artisan.storage.core.curator_reader import AcceptedCuratorResult, CuratorReader
from artisan.storage.core.retained_reader import (
    AcceptedArtifact,
    AcceptedResult,
    AssociationQuery,
    RetainedReader,
)


@dataclass(frozen=True)
class PreparedAssignmentInputs:
    """A scientific partition; publishing admission and choosing software are separate."""

    assignment: AssignmentIdentity
    boundary: BoundaryProjection
    artifacts: Mapping[OccurrenceAddress, Artifact]


def prepare_step_inputs(
    recipe: ExecutionRecipe,
    inputs: Mapping[str, Sequence[AcceptedArtifact | Artifact]],
    *,
    step_run_id: str,
    reader: RetainedReader,
) -> tuple[PreparedAssignmentInputs, ...]:
    """Capture finite inputs once without publishing, caching or dispatching work.

    Qualified producers preserve exact accepted-owner occurrence attribution. Unscoped
    artifacts never acquire producer identity through content equality; their
    reference policies can use only the explicitly supplied artifact inventory.
    """
    recipe = ExecutionRecipe.model_validate(recipe)
    sources = _InputSources(reader)
    selected = sources.select(inputs)
    _validate_inputs(recipe, selected)
    associations = sources.associated(recipe, selected)
    lineage = (
        sources.lineage(selected)
        if recipe.policy.group_by == GroupByStrategy.LINEAGE and any(selected.values())
        else None
    )
    return tuple(
        _freeze_partition(
            partition,
            AssignmentIdentity(step_run_id=step_run_id, unit_index=index),
            sources,
            associations,
            recipe,
        )
        for index, partition in enumerate(
            partition_occurrence_inputs(selected, recipe.policy, lineage=lineage)
        )
    )


def _validate_inputs(
    recipe: ExecutionRecipe, inputs: Mapping[str, Sequence[InputSelection]]
) -> None:
    specs = {spec.role: spec for spec in recipe.inputs}
    if (
        inputs.keys() - specs.keys()
        or {role for role, spec in specs.items() if spec.required} - inputs.keys()
    ):
        msg = "Input admission has missing required or undeclared roles"
        raise ValueError(msg)
    for role, selections in inputs.items():
        for item in selections:
            if not ArtifactTypes.matches(
                specs[role].artifact_type, item.artifact.artifact_type
            ):
                msg = f"Input role {role!r} has an incompatible artifact type"
                raise ValueError(msg)
            if specs[role].hydrate and not item.artifact.is_hydrated:
                msg = f"Input role {role!r} requires hydrated artifacts"
                raise ValueError(msg)


def _freeze_partition(
    inputs: Mapping[str, Sequence[InputSelection]],
    assignment: AssignmentIdentity,
    sources: _InputSources,
    associations: Mapping[AssociationQuery, tuple[AcceptedProducer, ...]],
    recipe: ExecutionRecipe,
) -> PreparedAssignmentInputs:
    graph = OccurrenceGraph(boundary_invocation=assignment.assignment_id)
    representatives: dict[SelectionKey, OccurrenceAddress] = {}
    positions: dict[OccurrenceAddress, SelectionKey] = {}
    attribution: list[BoundaryInputAttribution] = []
    captures = []
    specs = {spec.role: spec for spec in recipe.inputs}
    for role, selections in inputs.items():
        graph.declare_input_role(role)
        for item in selections:
            address = graph.add_input(role, item.artifact)
            key = sources.canonical_key(item.key)
            positions[address] = key
            # Context aliases share a representative; selected positions and their
            # exact producer attribution remain distinct admission evidence.
            representatives.setdefault(key, address)
            attribution.append(
                BoundaryInputAttribution(boundary=address, producer=item.producer)
            )
            for kind in specs[role].with_associated:
                assert item.producer is not None
                members = associations[AssociationQuery(item.producer, kind)]
                captures.append(
                    (
                        address,
                        kind,
                        tuple(
                            dict.fromkeys(
                                sources.canonical(member) for member in members
                            )
                        ),
                    )
                )
    required = dict.fromkeys(representatives)
    for _, _, members in captures:
        required.update(dict.fromkeys(members))
    pending = deque(required)
    references = {}
    while pending:
        key = pending.popleft()
        if not sources.has_policy(key):
            continue
        referents, parents = sources.references(key)
        referents = tuple(
            dict.fromkeys(sources.canonical_key(member) for member in referents)
        )
        parents = tuple(
            dict.fromkeys(sources.canonical_key(parent) for parent in parents)
        )
        references[key] = (referents, parents)
        for member in referents:
            if member not in required:
                required[member] = None
                pending.append(member)
    for key in required:
        if key not in representatives:
            address = OccurrenceAddress(
                invocation_id=f"{assignment.assignment_id}/context",
                role="captured",
                index=len(positions),
            )
            graph.add_context_input(address, sources.artifact(key))
            representatives[key] = address
            positions[address] = key
            attribution.append(
                BoundaryInputAttribution(
                    boundary=address,
                    producer=None if isinstance(key, OccurrenceAddress) else key,
                )
            )
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
                members=tuple(representatives[member] for member in references[key][0]),
                direct_parents=tuple(
                    representatives[parent]
                    for parent in references[key][1]
                    if parent in required
                ),
            )
            for address, key in positions.items()
            if key in references
        ),
    )
    boundary = graph.project(
        {}, {}, frozen_context=context, input_attribution=tuple(attribution)
    )
    return PreparedAssignmentInputs(
        assignment,
        boundary,
        MappingProxyType(
            {address: node.artifact for address, node in graph.nodes.items()}
        ),
    )


class _InputSources:
    """Reuse verified accepted evidence only within this one admission snapshot."""

    def __init__(self, reader: RetainedReader) -> None:
        self.reader = reader
        self._receipts: dict[str, SealedDelivery] | None = None
        self._steps: dict[str, int] = {}
        self._results: dict[str, AcceptedResult] = {}
        self._artifacts: dict[SelectionKey, Artifact] = {}
        self._inventory: dict[OccurrenceAddress, SelectionKey] = {}
        self._canonical: dict[AcceptedProducer, AcceptedProducer] = {}
        self._curator_reader = CuratorReader(reader.committer)
        self._curators: dict[tuple[str, str], AcceptedCuratorResult] = {}

    def select(
        self, inputs: Mapping[str, Sequence[AcceptedArtifact | Artifact]]
    ) -> dict[str, tuple[InputSelection, ...]]:
        selected = {}
        inventory_keys = set()
        for role, values in inputs.items():
            selections = []
            for index, value in enumerate(values):
                address = OccurrenceAddress(
                    invocation_id="supplied", role=role, index=index
                )
                if isinstance(value, AcceptedArtifact):
                    artifact = self.artifact(value.reference)
                    value.artifact.finalize()
                    if value.artifact.to_row() != artifact.to_row():
                        msg = "Selected artifact differs from its accepted producer content"
                        raise ValueError(msg)
                    key: SelectionKey = value.reference
                else:
                    ArtifactOccurrence(address, value)
                    artifact, key = value, address
                    self._artifacts[key] = artifact
                ArtifactTypeDef.get(artifact.artifact_type).reference_policy()
                selections.append(InputSelection(artifact, key))
                if key not in inventory_keys:
                    self._inventory[address] = key
                    inventory_keys.add(key)
            selected[role] = tuple(selections)
        return selected

    def result(self, reference: AcceptedOccurrence) -> AcceptedResult:
        if self._receipts is None:
            plans = self.reader.completed_plans(ordinary=False)
            self._receipts = {
                plan.receipt.delivery.delivery_id: plan.receipt for plan in plans
            }
            self._steps = {
                plan.receipt.delivery.delivery_id: plan.step_number for plan in plans
            }
        receipt = self._receipts.get(reference.delivery_id)
        if receipt is None or receipt.seal_digest != reference.seal_digest:
            msg = "Selected input has no matching completed producer seal"
            raise ValueError(msg)
        if reference.delivery_id not in self._results:
            self._results[reference.delivery_id] = self.reader.read(receipt)
        result = self._results[reference.delivery_id]
        if reference.address not in result.artifacts:
            msg = "Selected input occurrence is absent from its accepted result"
            raise ValueError(msg)
        return result

    def curator_result(
        self, reference: AcceptedCuratorOccurrence
    ) -> AcceptedCuratorResult:
        owner = (reference.step_run_id, reference.plan_digest)
        if owner not in self._curators:
            self._curators[owner] = self._curator_reader.read_result(*owner)
        result = self._curators[owner]
        if (
            reference.address not in result.artifacts
            or result.reference(reference.address) != reference
        ):
            msg = "Selected curator input is absent from its completed membership"
            raise ValueError(msg)
        return result

    def parents(self, reference: AcceptedProducer) -> tuple[AcceptedProducer, ...]:
        if isinstance(reference, AcceptedCuratorOccurrence):
            projection = self.curator_result(reference).projection
            member = next(
                item
                for item in projection.occurrences
                if item.occurrence.address == reference.address
            )
            return (
                *member.sources,
                *(
                    _reference(reference, edge.source)
                    for edge in projection.lineage
                    if edge.target == reference.address
                ),
            )
        return tuple(
            _reference(reference, edge.source)
            for edge in self.result(reference).projection.parents
            if edge.target == reference.address
        )

    def artifact(self, key: SelectionKey) -> Artifact:
        if key not in self._artifacts:
            if isinstance(key, OccurrenceAddress):
                msg = "Unscoped referent is outside the supplied inventory"
                raise ValueError(msg)
            result = (
                self.curator_result(key)
                if isinstance(key, AcceptedCuratorOccurrence)
                else self.result(key)
            )
            self._artifacts[key] = result.artifacts[key.address]
        return self._artifacts[key]

    def has_policy(self, key: SelectionKey) -> bool:
        return (
            ArtifactTypeDef.get(self.artifact(key).artifact_type).reference_policy()
            is not None
        )

    def associated(
        self, recipe: ExecutionRecipe, inputs: Mapping[str, Sequence[InputSelection]]
    ) -> dict[AssociationQuery, tuple[AcceptedProducer, ...]]:
        specs = {spec.role: spec for spec in recipe.inputs}
        queries: dict[AssociationQuery, None] = {}
        for role, selections in inputs.items():
            for item in selections:
                for kind in specs[role].with_associated:
                    if item.producer is None:
                        msg = "Associated input queries require accepted producer scope"
                        raise ValueError(msg)
                    queries[AssociationQuery(item.producer, kind)] = None
        found = self.reader.associated_many(tuple(queries))
        return {
            query: tuple(item.reference for item in members)
            for query, members in zip(queries, found, strict=True)
        }

    def references(
        self, key: SelectionKey
    ) -> tuple[tuple[SelectionKey, ...], tuple[SelectionKey, ...]]:
        if isinstance(key, OccurrenceAddress):
            context = ReferenceContext(
                {
                    address: self.artifact(member)
                    for address, member in self._inventory.items()
                }
            )
            artifact = self.artifact(key)
            required = ArtifactTypeDef.get(
                artifact.artifact_type
            ).resolve_required_references(artifact, context)
            return tuple(self._inventory[item.address] for item in required), ()
        if isinstance(key, AcceptedCuratorOccurrence):
            capture = next(
                item
                for item in self.curator_result(key).projection.references
                if item.source == key.address
            )
            return (
                tuple(_reference(key, address) for address in capture.members),
                tuple(_reference(key, address) for address in capture.direct_parents),
            )
        projection = self.result(key).projection
        if key.address in projection.boundary_addresses():
            captured = projection.frozen_context.reference_capture(key.address)
            members, parents = captured.members, captured.direct_parents
        else:
            members = tuple(
                edge.target
                for edge in projection.references
                if edge.source == key.address
            )
            parents = tuple(
                edge.source
                for edge in projection.associations
                if edge.target == key.address
            )
        return (
            tuple(_reference(key, address) for address in members),
            tuple(_reference(key, address) for address in parents),
        )

    def canonical_key(self, key: SelectionKey) -> SelectionKey:
        return key if isinstance(key, OccurrenceAddress) else self.canonical(key)

    def canonical(self, reference: AcceptedProducer) -> AcceptedProducer:
        chain = []
        current = reference
        while current not in self._canonical:
            if current in chain:
                msg = "Selected producer attribution contains a cycle"
                raise ValueError(msg)
            chain.append(current)
            if isinstance(current, AcceptedCuratorOccurrence):
                self.curator_result(current)
                self._canonical[current] = current
                break
            projection = self.result(current).projection
            producer = next(
                (
                    item.producer
                    for item in projection.input_attribution
                    if item.boundary == current.address
                ),
                None,
            )
            if producer is None:
                self._canonical[current] = current
                break
            actual, previous = self.artifact(current), self.artifact(producer)
            if (actual.artifact_id, actual.artifact_type) != (
                previous.artifact_id,
                previous.artifact_type,
            ):
                msg = "Selected producer alias changed artifact identity"
                raise ValueError(msg)
            current = producer
        target = self._canonical[current]
        self._canonical.update(dict.fromkeys(chain, target))
        return target

    def lineage(
        self, inputs: Mapping[str, Sequence[InputSelection]]
    ) -> LineageSnapshot:
        roots = []
        for selections in inputs.values():
            for item in selections:
                if item.producer is None:
                    msg = "LINEAGE grouping requires accepted producer scope for every input"
                    raise ValueError(msg)
                roots.append(self.canonical(item.producer))
        parents = {}
        pending = list(roots)
        while pending:
            reference = pending.pop()
            if reference in parents:
                continue
            parents[reference] = tuple(
                dict.fromkeys(
                    self.canonical(parent) for parent in self.parents(reference)
                )
            )
            pending.extend(parents[reference])
        return LineageSnapshot(
            MappingProxyType(dict(self._canonical)),
            MappingProxyType(parents),
            MappingProxyType(
                {
                    item: self.curator_result(item).result.step_number
                    if isinstance(item, AcceptedCuratorOccurrence)
                    else self._steps[item.delivery_id]
                    for item in parents
                }
            ),
        )


def _reference(
    source: AcceptedProducer, address: OccurrenceAddress
) -> AcceptedProducer:
    return source.model_copy(update={"address": address})
