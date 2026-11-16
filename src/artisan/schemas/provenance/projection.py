"""Closed immutable occurrence evidence retained at an execution boundary."""

from __future__ import annotations

from collections.abc import Mapping
from graphlib import CycleError, TopologicalSorter
from types import MappingProxyType
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


class _ProjectionModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )


class RoleOccurrences(_ProjectionModel):
    """One ordered role membership, preserving repetitions and explicit empties."""

    role: str = Field(min_length=1, strict=True)
    occurrences: tuple[OccurrenceAddress, ...]


class ProjectedOccurrence(_ProjectionModel):
    """A retained producer occurrence and its finalized content identity."""

    address: OccurrenceAddress
    artifact_id: str = Field(pattern=r"^[0-9a-fA-F]{32}$", strict=True)
    artifact_type: str = Field(min_length=1, strict=True)


class AcceptedOccurrence(_ProjectionModel):
    """A selected prior result occurrence; admission must verify its acceptance.

    These fields are provenance claims, not proof of a winner decision. The
    controller checks the complete seal and exact artifact membership before
    allowing the reference to enter a new assignment.
    """

    delivery_id: str = Field(pattern=r"^[0-9a-f]{32}$", strict=True)
    seal_digest: str = Field(pattern=r"^[0-9a-f]{32}$", strict=True)
    address: OccurrenceAddress


class AcceptedCuratorOccurrence(_ProjectionModel):
    """One curator's accepted content membership, not an assignment delivery.

    The completed curator plan proves membership and its captured query context.
    Selecting existing content does not claim that the curator created new bytes
    or selected one particular upstream producer among equal-content inputs.
    """

    step_run_id: str = Field(pattern=r"^[0-9a-f]{32}$", strict=True)
    plan_digest: str = Field(pattern=r"^[0-9a-f]{32}$", strict=True)
    address: OccurrenceAddress


type AcceptedProducer = AcceptedOccurrence | AcceptedCuratorOccurrence


class BoundaryInputAttribution(_ProjectionModel):
    """Bind one fresh input slot to an exact producer, or explicitly no producer."""

    boundary: OccurrenceAddress
    producer: AcceptedProducer | None


class OccurrenceRelation(_ProjectionModel):
    """One exact directed relation, independent of content-ID equality."""

    source: OccurrenceAddress
    target: OccurrenceAddress


class FrozenAssociation(_ProjectionModel):
    """Captured direct association membership for an exact primary occurrence."""

    primary: OccurrenceAddress
    artifact_type: str = Field(min_length=1, strict=True)
    members: tuple[OccurrenceAddress, ...]


class FrozenReferences(_ProjectionModel):
    """Exact type-declared referents and original direct-parent evidence.

    Direct parents are restricted to the independently selected input closure;
    they neither expand retention nor manufacture semantic associations.
    """

    source: OccurrenceAddress
    members: tuple[OccurrenceAddress, ...]
    direct_parents: tuple[OccurrenceAddress, ...] = ()


class FrozenBoundaryContext(_ProjectionModel):
    """Explicit dependency snapshots; no missing capture implies an empty query."""

    associations: tuple[FrozenAssociation, ...] = ()
    references: tuple[FrozenReferences, ...] = ()

    @model_validator(mode="after")
    def _validate_captures(self) -> FrozenBoundaryContext:
        associations = [
            (item.primary, item.artifact_type) for item in self.associations
        ]
        references = [item.source for item in self.references]
        if len(associations) != len(set(associations)) or len(references) != len(
            set(references)
        ):
            msg = "Duplicate frozen context capture"
            raise ValueError(msg)
        for item in self.references:
            if (
                len(item.members) != len(set(item.members))
                or item.source in item.members
                or len(item.direct_parents) != len(set(item.direct_parents))
                or item.source in item.direct_parents
            ):
                msg = "Captured references must name distinct non-self occurrences"
                raise ValueError(msg)
        for association in self.associations:
            if association.primary in association.members:
                msg = "Captured associations cannot reference their primary occurrence"
                raise ValueError(msg)
        try:
            tuple(
                TopologicalSorter(
                    {item.source: item.direct_parents for item in self.references}
                ).static_order()
            )
        except CycleError as error:
            msg = "Captured original derivation must be acyclic"
            raise ValueError(msg) from error
        return self

    def associated(
        self, primary: OccurrenceAddress, artifact_type: str
    ) -> tuple[OccurrenceAddress, ...]:
        """Return captured membership, including captured-empty; reject absence."""
        for capture in self.associations:
            if (capture.primary, capture.artifact_type) == (primary, artifact_type):
                return capture.members
        msg = "Association membership was not captured"
        raise ValueError(msg)

    def referenced(self, source: OccurrenceAddress) -> tuple[OccurrenceAddress, ...]:
        """Return captured semantic dependencies without refreshing them."""
        return self.reference_capture(source).members

    def reference_capture(self, source: OccurrenceAddress) -> FrozenReferences:
        """Require the explicit type-policy snapshot, including captured-empty."""
        for capture in self.references:
            if capture.source == source:
                return capture
        msg = "Semantic references were not captured"
        raise ValueError(msg)

    def occurrence_addresses(self) -> frozenset[OccurrenceAddress]:
        """Return the exact input closure named by these snapshots."""
        addresses = set()
        for association in self.associations:
            addresses.add(association.primary)
            addresses.update(association.members)
        for reference in self.references:
            addresses.add(reference.source)
            addresses.update(reference.members)
        return frozenset(addresses)


class BoundaryProjection(_ProjectionModel):
    """Authoritative retained projection; all nested membership is immutable.

    The version identifies this evidence contract, not an activated store format.
    Associations preserve original direct relationships; parents may include
    shortcuts through discarded occurrences. Required references alone justify
    support retention. Content-ID edges are only a derived lossy view.
    """

    domain: Literal["artisan.boundary-projection"] = "artisan.boundary-projection"
    schema_version: Literal[1] = 1
    boundary_inputs: tuple[RoleOccurrences, ...]
    input_attribution: tuple[BoundaryInputAttribution, ...]
    occurrences: tuple[ProjectedOccurrence, ...]
    exports: tuple[RoleOccurrences, ...]
    support: tuple[OccurrenceAddress, ...] = ()
    parents: tuple[OccurrenceRelation, ...] = ()
    associations: tuple[OccurrenceRelation, ...] = ()
    references: tuple[OccurrenceRelation, ...] = ()
    frozen_context: FrozenBoundaryContext = Field(default_factory=FrozenBoundaryContext)

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        if type(value) is not int:
            msg = "Projection schema version must be an integer"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _validate_projection(self) -> BoundaryProjection:
        for groups in (self.boundary_inputs, self.exports):
            roles = [group.role for group in groups]
            if len(roles) != len(set(roles)):
                msg = "Projection role membership must have unique role names"
                raise ValueError(msg)
        addresses = [item.address for item in self.occurrences]
        if len(addresses) != len(set(addresses)):
            msg = "Duplicate producer occurrence"
            raise ValueError(msg)
        boundary = self.boundary_addresses()
        attribution = [item.boundary for item in self.input_attribution]
        if len(attribution) != len(set(attribution)) or set(attribution) != boundary:
            msg = "Input attribution must name every boundary occurrence exactly once"
            raise ValueError(msg)
        exports = {address for group in self.exports for address in group.occurrences}
        support = set(self.support)
        if len(support) != len(self.support) or support & (boundary | exports):
            msg = "Support must be distinct from boundary inputs and public exports"
            raise ValueError(msg)
        if set(addresses) != boundary | exports | support:
            msg = "Projection membership is incomplete or contains an orphan"
            raise ValueError(msg)
        for relations in (self.parents, self.associations, self.references):
            self._validate_relations(relations, set(addresses))
        self._validate_identity_and_context()
        self._validate_derivation()
        self._validate_support(exports)
        return self

    @staticmethod
    def _validate_relations(
        relations: tuple[OccurrenceRelation, ...], addresses: set[OccurrenceAddress]
    ) -> None:
        pairs = [(item.source, item.target) for item in relations]
        if len(pairs) != len(set(pairs)):
            msg = "Duplicate occurrence relation"
            raise ValueError(msg)
        if any(
            source not in addresses or target not in addresses
            for source, target in pairs
        ):
            msg = "Relation points outside the retained boundary"
            raise ValueError(msg)
        if any(source == target for source, target in pairs):
            msg = "Occurrence self-relations are invalid"
            raise ValueError(msg)

    def _validate_identity_and_context(self) -> None:
        content_types: dict[str, str] = {}
        types = {item.address: item.artifact_type for item in self.occurrences}
        for occurrence in self.occurrences:
            previous = content_types.setdefault(
                occurrence.artifact_id, occurrence.artifact_type
            )
            if previous != occurrence.artifact_type:
                msg = "One content identity cannot have conflicting artifact types"
                raise ValueError(msg)
        for association in self.frozen_context.associations:
            if any(
                types[member] != association.artifact_type
                for member in association.members
            ):
                msg = "Captured association has the wrong artifact type"
                raise ValueError(msg)
        if any(
            not set(item.direct_parents) <= self.boundary_addresses()
            for item in self.frozen_context.references
        ):
            msg = "Captured direct parents must stay within the frozen input closure"
            raise ValueError(msg)

    def _validate_derivation(self) -> None:
        if not set(self.associations) <= set(self.parents):
            msg = "Original direct associations must also be projected parents"
            raise ValueError(msg)
        parents: dict[OccurrenceAddress, set[OccurrenceAddress]] = {
            item.address: set() for item in self.occurrences
        }
        for relation in self.parents:
            parents[relation.target].add(relation.source)
        try:
            tuple(TopologicalSorter(parents).static_order())
        except CycleError as error:
            msg = "Projected derivation must be acyclic"
            raise ValueError(msg) from error

    def _validate_support(self, exports: set[OccurrenceAddress]) -> None:
        reachable = set(exports)
        pending = list(exports)
        references: dict[OccurrenceAddress, list[OccurrenceAddress]] = {}
        for relation in self.references:
            references.setdefault(relation.source, []).append(relation.target)
        while pending:
            for target in references.get(pending.pop(), ()):
                if target not in reachable:
                    reachable.add(target)
                    pending.append(target)
        if not set(self.support) <= reachable:
            msg = "Support must be reachable through required references from exports"
            raise ValueError(msg)

    def boundary_addresses(self) -> frozenset[OccurrenceAddress]:
        """Include captured dependencies in the trusted input closure."""
        return (
            frozenset(
                address
                for group in self.boundary_inputs
                for address in group.occurrences
            )
            | self.frozen_context.occurrence_addresses()
        )

    @property
    def input_context_digest(self) -> str:
        """Bind the exact admission snapshot, including selected prior producers.

        Current consuming addresses remain in this identity. Cache keys that
        compare independent assignments must normalize those addresses separately.
        """
        boundary = self.boundary_addresses()
        snapshot = {
            "boundary_inputs": [
                item.model_dump(mode="json") for item in self.boundary_inputs
            ],
            "occurrences": [
                item.model_dump(mode="json")
                for item in sorted(self.occurrences, key=lambda item: item.address.key)
                if item.address in boundary
            ],
            "input_attribution": [
                item.model_dump(mode="json") for item in self.input_attribution
            ],
            "frozen_context": self.frozen_context.model_dump(mode="json"),
        }
        return compute_content_digest(
            b"artisan.input-context-v1\0" + canonical_json_bytes(snapshot)
        )

    def export_roles(self) -> Mapping[str, tuple[OccurrenceAddress, ...]]:
        """Return a read-only role lookup without changing tuple authority."""
        return MappingProxyType(
            {group.role: group.occurrences for group in self.exports}
        )

    def input_roles(self) -> Mapping[str, tuple[OccurrenceAddress, ...]]:
        """Return a read-only view of ordered boundary input roles."""
        return MappingProxyType(
            {group.role: group.occurrences for group in self.boundary_inputs}
        )

    def content_edges(self) -> frozenset[tuple[str, str]]:
        """Return a lossy content union, suppressing content-identity self-edges."""
        ids = {item.address: item.artifact_id for item in self.occurrences}
        return frozenset(
            (ids[edge.source], ids[edge.target])
            for edge in self.parents
            if ids[edge.source] != ids[edge.target]
        )
