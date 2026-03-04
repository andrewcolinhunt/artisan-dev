"""Explicit artifact-to-artifact lineage declarations.

``LineageMapping`` allows operations to declare explicit parent-child
relationships between input artifacts and output drafts, enabling
custom lineage beyond the default filename-matching inference.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class LineageMapping(BaseModel):
    """Maps a draft artifact to its source artifact (1:1 mapping).

    Used in ArtifactResult.lineage to declare explicit parent-child
    relationships between input artifacts and output drafts.

    Attributes:
        draft_original_name: original_name of the draft artifact being created.
            Must match an artifact in ArtifactResult.artifacts.
        source_artifact_id: artifact_id of the source (parent) artifact.
            Must be a valid 32-character hex string.
        source_role: The role name where the source artifact came from
            (e.g., "data", "reference", "score").
        group_id: Deterministic hash linking jointly-necessary input edges.
            When multiple source artifacts are co-inputs to a derivation,
            all edges for the same output share the same group_id.
            None for independent (single-input) derivation.

    Example:
        # Declare that output "sample_001_processed.dat" derives from input artifact
        LineageMapping(
            draft_original_name="sample_001_processed.dat",
            source_artifact_id="abc123def456ghijklmnopqrstuvwxyz",
            source_role="data"
        )
    """

    model_config = ConfigDict(frozen=True)

    draft_original_name: str = Field(
        ...,
        min_length=1,
        description="original_name of the draft artifact",
    )
    source_artifact_id: str = Field(
        ...,
        min_length=32,
        max_length=32,
        description="artifact_id of source artifact",
    )
    source_role: str = Field(
        ...,
        min_length=1,
        description="role name of the source artifact",
    )
    group_id: str | None = Field(
        default=None,
        description="Deterministic hash linking jointly-necessary input edges. "
        "None for independent (single-input) derivation.",
    )
