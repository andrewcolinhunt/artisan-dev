"""Accepted content-level curator selections and their frozen query evidence."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.schemas.provenance.projection import (
    AcceptedProducer,
    FrozenReferences,
    OccurrenceRelation,
    ProjectedOccurrence,
    RoleOccurrences,
)


class _CuratorProjectionModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )


class CuratorContentOccurrence(_CuratorProjectionModel):
    """One curator-owned content membership with its complete matching sources.

    Sources preserve content-level aggregation. They do not assert an exact alias
    or add derivation edges, and equal-content prior owners remain distinct.
    """

    occurrence: ProjectedOccurrence
    sources: tuple[AcceptedProducer, ...] = ()

    @model_validator(mode="after")
    def _validate_sources(self) -> CuratorContentOccurrence:
        if len(self.sources) != len(set(self.sources)):
            msg = "Curator content sources must not repeat exact accepted owners"
            raise ValueError(msg)
        return self


class CuratorQueryMembership(_CuratorProjectionModel):
    """A frozen content-level association query, distinct from authored edges."""

    primary: OccurrenceAddress
    artifact_type: str = Field(min_length=1, strict=True)
    members: tuple[OccurrenceAddress, ...]


class CuratorProjection(_CuratorProjectionModel):
    """Existing deduplicated curator output membership plus required context.

    Output and query memberships use content IDs as the curator API does. Exact
    source sets and the completed curator owner make that aggregation explicit;
    they never turn into original-direct scientific associations.
    """

    domain: Literal["artisan.curator-projection"] = "artisan.curator-projection"
    occurrences: tuple[CuratorContentOccurrence, ...]
    outputs: tuple[RoleOccurrences, ...]
    queries: tuple[CuratorQueryMembership, ...] = ()
    references: tuple[FrozenReferences, ...] = ()
    lineage: tuple[OccurrenceRelation, ...] = ()

    @model_validator(mode="after")
    def _validate_projection(self) -> CuratorProjection:
        inventory = {
            item.occurrence.address: item.occurrence for item in self.occurrences
        }
        if len(inventory) != len(self.occurrences):
            msg = "Curator projection repeats an occurrence"
            raise ValueError(msg)
        content = [item.occurrence.artifact_id for item in self.occurrences]
        if content != sorted(set(content)):
            msg = "Curator content inventory must be unique and canonically ordered"
            raise ValueError(msg)
        roles = [group.role for group in self.outputs]
        if roles != sorted(set(roles)):
            msg = "Curator output roles require canonical unique membership"
            raise ValueError(msg)
        self._validate_addresses(set(inventory))
        used: set[OccurrenceAddress] = set()
        for group in self.outputs:
            ids = [inventory[address].artifact_id for address in group.occurrences]
            if ids != sorted(set(ids)):
                msg = "Curator output IDs must preserve canonical content deduplication"
                raise ValueError(msg)
            used.update(group.occurrences)
        captures = [(item.primary, item.artifact_type) for item in self.queries]
        if len(captures) != len(set(captures)):
            msg = "Curator projection repeats a captured association query"
            raise ValueError(msg)
        for query in self.queries:
            used.add(query.primary)
            used.update(query.members)
            if any(
                inventory[address].artifact_type != query.artifact_type
                for address in query.members
            ):
                msg = "Curator query membership has another artifact type"
                raise ValueError(msg)
            ids = [inventory[address].artifact_id for address in query.members]
            if ids != sorted(set(ids)):
                msg = "Curator query results require canonical content deduplication"
                raise ValueError(msg)
        sources = [item.source for item in self.references]
        if len(sources) != len(set(sources)):
            msg = "Curator projection repeats required-reference evidence"
            raise ValueError(msg)
        for capture in self.references:
            used.add(capture.source)
            used.update(capture.members)
            if not set(capture.direct_parents) <= set(inventory):
                msg = "Curator direct-parent evidence escapes selected membership"
                raise ValueError(msg)
        pairs = [(edge.source, edge.target) for edge in self.lineage]
        if len(pairs) != len(set(pairs)) or any(
            source == target for source, target in pairs
        ):
            msg = "Curator lineage must contain unique distinct content memberships"
            raise ValueError(msg)
        for source, target in pairs:
            used.update((source, target))
        if used != set(inventory):
            msg = "Curator projection contains missing or orphaned content membership"
            raise ValueError(msg)
        return self

    def _validate_addresses(self, inventory: set[OccurrenceAddress]) -> None:
        mentioned = {address for group in self.outputs for address in group.occurrences}
        for capture in self.queries:
            mentioned.add(capture.primary)
            mentioned.update(capture.members)
        for reference in self.references:
            mentioned.add(reference.source)
            mentioned.update(reference.members)
            mentioned.update(reference.direct_parents)
        for edge in self.lineage:
            mentioned.update((edge.source, edge.target))
        if not mentioned <= inventory:
            msg = "Curator projection references missing content membership"
            raise ValueError(msg)
