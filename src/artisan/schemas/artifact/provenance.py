"""Artifact provenance model for source-to-target derivation edges.

Each ``ArtifactProvenanceEdge`` represents a directed edge in the
artifact provenance graph, capturing "wasDerivedFrom" relationships.
Uses source/target (graph-centric) terminology, distinct from
inputs/outputs (operation-centric) used in execution provenance.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class ArtifactProvenanceEdge(BaseModel):
    """Directed source-to-target artifact derivation edge.

    Stored in the artifact_edges Delta Lake table. Enables provenance
    queries such as "what are the sources of artifact X?" and "what
    metrics derive from artifact Y?".

    For multi-input operations, edges sharing the same ``group_id``
    and ``target_artifact_id`` were co-inputs to a single derivation.
    Independent (single-input) derivations have ``group_id=None``.

    Attributes:
        execution_run_id: Execution that established this edge.
        source_artifact_id: Content-addressed ID of the source.
        target_artifact_id: Content-addressed ID of the target.
        source_artifact_type: Type key of the source (denormalized).
        target_artifact_type: Type key of the target (denormalized).
        source_role: Role name of the source artifact.
        target_role: Role name of the target artifact.
        group_id: Hash linking co-input edges. None for single-input.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    execution_run_id: str = Field(
        ...,
        min_length=32,
        max_length=32,
        description="Which execution established this edge",
    )
    source_artifact_id: str = Field(
        ...,
        min_length=32,
        max_length=32,
        description="Source artifact ID (content hash)",
    )
    target_artifact_id: str = Field(
        ...,
        min_length=32,
        max_length=32,
        description="Target artifact ID (content hash)",
    )
    source_artifact_type: str = Field(
        ...,
        description="Artifact type of source (denormalized). Should match registered "
        "artifact type keys (e.g. 'data', 'metric'); 'UNKNOWN' is acceptable as fallback.",
    )
    target_artifact_type: str = Field(
        ...,
        description="Artifact type of target (denormalized). Should match registered "
        "artifact type keys (e.g. 'data', 'metric'); 'UNKNOWN' is acceptable as fallback.",
    )
    source_role: str = Field(
        ...,
        description="Role name of source artifact",
    )
    target_role: str = Field(
        ...,
        description="Role name of target artifact",
    )
    group_id: str | None = Field(
        default=None,
        description="Deterministic hash linking jointly-necessary input edges. "
        "Edges sharing the same group_id and target_artifact_id were co-inputs "
        "to a single derivation. None for independent (single-input) derivation.",
    )

    def __hash__(self) -> int:
        """Make ArtifactProvenanceEdge hashable."""
        return hash(
            (
                self.execution_run_id,
                self.source_artifact_id,
                self.target_artifact_id,
                self.source_artifact_type,
                self.target_artifact_type,
                self.source_role,
                self.target_role,
                self.group_id,
            )
        )
