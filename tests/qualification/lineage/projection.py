"""Closed candidate boundary evidence shared by semantic and storage probes.

Content identity and occurrence identity are separate. Export lists retain their
order and repetitions; a content table can still store each artifact once.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator


class FrozenModel(BaseModel):
    """Reject unknown fields in the candidate transport contract."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class Occurrence(FrozenModel):
    """One retained or admitted occurrence of finalized artifact content."""

    key: str = Field(min_length=1)
    artifact_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    artifact_type: str = Field(min_length=1)


class Relation(FrozenModel):
    """One exact directed relationship between occurrence keys."""

    source: str = Field(min_length=1)
    target: str = Field(min_length=1)


class FrozenAssociation(FrozenModel):
    """Captured membership, including a deliberately empty member sequence."""

    primary: str = Field(min_length=1)
    artifact_type: str = Field(min_length=1)
    members: tuple[str, ...]


class FrozenReferences(FrozenModel):
    """Captured explicit dependencies of one configuration occurrence."""

    config: str = Field(min_length=1)
    members: tuple[str, ...]


class FrozenContext(FrozenModel):
    """Association and configuration snapshots without ambient store access."""

    associations: tuple[FrozenAssociation, ...] = ()
    references: tuple[FrozenReferences, ...] = ()

    @model_validator(mode="after")
    def unique_captures(self) -> FrozenContext:
        """Reject conflicting snapshots rather than choosing an arbitrary one."""
        pairs = [(item.primary, item.artifact_type) for item in self.associations]
        configs = [item.config for item in self.references]
        if len(pairs) != len(set(pairs)) or len(configs) != len(set(configs)):
            msg = "Duplicate frozen context capture"
            raise ValueError(msg)
        return self

    def associated(self, primary: str, artifact_type: str) -> tuple[str, ...]:
        """Return captured membership; absence is an error, not an empty query."""
        for capture in self.associations:
            if (capture.primary, capture.artifact_type) == (primary, artifact_type):
                return capture.members
        msg = "Association membership was not captured"
        raise ValueError(msg)

    def referenced(self, config: str) -> tuple[str, ...]:
        """Return captured configuration dependencies without refreshing them."""
        for capture in self.references:
            if capture.config == config:
                return capture.members
        msg = "Configuration references were not captured"
        raise ValueError(msg)

    def occurrence_keys(self) -> set[str]:
        """Return exact dependencies admitted through this context."""
        keys: set[str] = set()
        for association in self.associations:
            keys.add(association.primary)
            keys.update(association.members)
        for reference in self.references:
            keys.add(reference.config)
            keys.update(reference.members)
        return keys


class BoundaryProjection(FrozenModel):
    """Candidate retained boundary, independent of its delivery/seal wrapper."""

    boundary_inputs: dict[str, tuple[str, ...]]
    occurrences: tuple[Occurrence, ...]
    exports: dict[str, tuple[str, ...]]
    support: tuple[str, ...] = ()
    parents: tuple[Relation, ...] = ()
    associations: tuple[Relation, ...] = ()
    references: tuple[Relation, ...] = ()
    frozen_context: FrozenContext = Field(default_factory=FrozenContext)

    @model_validator(mode="after")
    def validate_membership(self) -> BoundaryProjection:
        """Require every relation and retained occurrence to have real membership."""
        keys = [item.key for item in self.occurrences]
        if len(keys) != len(set(keys)):
            msg = "Duplicate occurrence key"
            raise ValueError(msg)
        boundary = self.boundary_keys()
        exports = {key for values in self.exports.values() for key in values}
        support = set(self.support)
        if len(support) != len(self.support) or support & (boundary | exports):
            msg = "Support must be distinct from inputs and public exports"
            raise ValueError(msg)
        if set(keys) != boundary | exports | support:
            msg = "Occurrence membership is incomplete or contains an orphan"
            raise ValueError(msg)
        for relations in (self.parents, self.associations, self.references):
            self._validate_relations(relations, set(keys))
        self._validate_identities()
        self._validate_parent_dag()
        self._validate_support(exports)
        self._validate_context_types()
        return self

    def _validate_identities(self) -> None:
        types: dict[str, str] = {}
        for occurrence in self.occurrences:
            previous = types.setdefault(
                occurrence.artifact_id, occurrence.artifact_type
            )
            if previous != occurrence.artifact_type:
                msg = "One content identity cannot have conflicting artifact types"
                raise ValueError(msg)

    def _validate_parent_dag(self) -> None:
        parents: dict[str, set[str]] = {item.key: set() for item in self.occurrences}
        for relation in self.parents:
            parents[relation.target].add(relation.source)
        while parents:
            roots = {key for key, values in parents.items() if not values}
            if not roots:
                msg = "Projected derivation must be acyclic"
                raise ValueError(msg)
            parents = {
                key: values - roots
                for key, values in parents.items()
                if key not in roots
            }

    def _validate_support(self, exports: set[str]) -> None:
        reachable = set(exports)
        while True:
            additions = {
                edge.target for edge in self.references if edge.source in reachable
            }
            if additions <= reachable:
                break
            reachable.update(additions)
        if not set(self.support) <= reachable:
            msg = "Support must be reachable through required references from exports"
            raise ValueError(msg)

    def boundary_keys(self) -> set[str]:
        """Include explicit context dependencies in the admitted input closure."""
        return {
            key for values in self.boundary_inputs.values() for key in values
        } | self.frozen_context.occurrence_keys()

    @staticmethod
    def _validate_relations(relations: tuple[Relation, ...], keys: set[str]) -> None:
        pairs = [(item.source, item.target) for item in relations]
        if len(pairs) != len(set(pairs)):
            msg = "Duplicate occurrence relation"
            raise ValueError(msg)
        if any(source not in keys or target not in keys for source, target in pairs):
            msg = "Relation references an occurrence outside the accepted boundary"
            raise ValueError(msg)
        if any(source == target for source, target in pairs):
            msg = "Occurrence self-relations are invalid"
            raise ValueError(msg)

    def _validate_context_types(self) -> None:
        types = {item.key: item.artifact_type for item in self.occurrences}
        for capture in self.frozen_context.associations:
            if any(types[key] != capture.artifact_type for key in capture.members):
                msg = "Frozen association member has the wrong artifact type"
                raise ValueError(msg)

    def content_edges(self) -> set[tuple[str, str]]:
        """Return a lossy content union for comparison, never occurrence authority."""
        ids = {item.key: item.artifact_id for item in self.occurrences}
        return {
            (ids[edge.source], ids[edge.target])
            for edge in self.parents
            if ids[edge.source] != ids[edge.target]
        }
