"""Explicit artifact-to-artifact lineage declarations.

``LineageMapping`` allows operations to declare explicit parent-child
relationships between input artifacts and output drafts, enabling
custom lineage beyond the default filename-matching inference.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator


class LineageMapping(BaseModel):
    """Maps a draft artifact to its source artifact (1:1 mapping).

    Used in ArtifactResult.lineage to declare explicit parent-child
    relationships between input artifacts and output drafts. The source
    can be referenced two ways; exactly one must be provided:

    - ``source_artifact_id``: 32-char content-addressed ID. Use when the
      source is an input artifact (its ID is known at postprocess time).
    - ``source_original_name``: filename-derived name. Use when the source
      is a co-produced output (its ID is not assigned until finalization).
      Resolved against finalized outputs in the role named by
      ``source_role``.

    Attributes:
        draft_original_name: original_name of the draft artifact being created.
            Must match an artifact in ArtifactResult.artifacts.
        source_artifact_id: artifact_id of the source (parent) artifact.
            Must be a valid 32-character hex string. Mutually exclusive
            with ``source_original_name``.
        source_original_name: original_name of the source (parent) artifact,
            for referencing co-produced outputs whose IDs are not yet
            assigned. Resolved against finalized outputs in
            ``source_role``. Mutually exclusive with ``source_artifact_id``.
        source_role: The role name where the source artifact came from
            (e.g., "data", "reference", "score").
        group_id: Deterministic hash linking jointly-necessary input edges.
            When multiple source artifacts are co-inputs to a derivation,
            all edges for the same output share the same group_id.
            None for independent (single-input) derivation.

    Example:
        # Input source: known artifact_id
        LineageMapping(
            draft_original_name="sample_001_processed.dat",
            source_artifact_id="abc123def456ghijklmnopqrstuvwxyz",
            source_role="data",
        )

        # Co-produced output source: filename reference
        LineageMapping(
            draft_original_name="1abc_out_energy",
            source_original_name="1abc_out",
            source_role="structures",
        )
    """

    model_config = ConfigDict(frozen=True)

    draft_original_name: str = Field(
        ...,
        min_length=1,
        description="original_name of the draft artifact",
    )
    source_artifact_id: str | None = Field(
        default=None,
        min_length=32,
        max_length=32,
        description="artifact_id of source artifact (for input sources)",
    )
    source_original_name: str | None = Field(
        default=None,
        min_length=1,
        description=(
            "original_name of source artifact (for co-produced output sources)"
        ),
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

    @model_validator(mode="after")
    def _require_one_source_ref(self) -> LineageMapping:
        """Require exactly one of source_artifact_id or source_original_name."""
        has_id = self.source_artifact_id is not None
        has_name = self.source_original_name is not None
        if has_id == has_name:
            msg = "Provide exactly one of source_artifact_id or source_original_name"
            raise ValueError(msg)
        return self
