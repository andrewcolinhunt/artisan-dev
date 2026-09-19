"""Lightweight source-target pair for lineage inference.

Created by ``LineageBuilder`` during execution, later enriched with
execution context and artifact types to produce full
``ArtifactProvenanceEdge`` records.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceTargetPair:
    """Lightweight derivation edge before enrichment.

    Represents a single source-to-target relationship without execution
    context or artifact types, which are added when converting to
    ``ArtifactProvenanceEdge``.

    Attributes:
        source: Artifact ID of the derivation source.
        target: Artifact ID of the derived artifact.
        source_role: Role name of the source artifact.
        target_role: Role name of the target artifact.
        group_id: Hash linking co-input edges. None for single-input.
    """

    source: str
    target: str
    source_role: str
    target_role: str
    group_id: str | None = None
