"""Lightweight source-target pair resolved from explicit lineage declarations.

Created by the lineage builder during execution, later enriched with
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
        group_id: Hash of the jointly declared parent set; None for one parent.
    """

    source: str
    target: str
    source_role: str
    target_role: str
    group_id: str | None = None
