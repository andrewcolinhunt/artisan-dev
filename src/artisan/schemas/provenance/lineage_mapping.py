"""Operation-authored references between input artifacts and output occurrences."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator


class LineageMapping(BaseModel):
    """Declare one parent of an output occurrence in an ``ArtifactResult``.

    The lineage dictionary key names the target output role. Indices address
    that result's artifact lists, independently of filenames or artifact IDs.
    Exactly one source reference is required.

    Attributes:
        draft_index: Position in the target role's output list.
        source_role: Input or sibling-output role containing the parent.
        source_artifact_id: Exact input artifact ID in ``source_role``.
        source_output_index: Position in the sibling output role's list.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    draft_index: int = Field(ge=0, strict=True)
    source_role: str = Field(min_length=1)
    source_artifact_id: str | None = Field(default=None, pattern=r"^[0-9a-fA-F]{32}$")
    source_output_index: int | None = Field(default=None, ge=0, strict=True)

    @model_validator(mode="after")
    def _require_one_source_ref(self) -> LineageMapping:
        """Require exactly one input ID or sibling-output index."""
        if (self.source_artifact_id is None) == (self.source_output_index is None):
            msg = "Provide exactly one of source_artifact_id or source_output_index"
            raise ValueError(msg)
        return self
