"""Result models returned by curator operations.

Curator operations return one of two shapes:
- ``ArtifactResult`` -- creates new draft artifacts.
- ``PassthroughResult`` -- routes existing artifact IDs.

The ``CuratorResult`` union captures both for typing and validation.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.schemas.provenance.lineage_mapping import LineageMapping


class ArtifactResult(BaseModel):
    """Result from operations that create new artifacts.

    These operations produce draft artifacts from their inputs. Drafts have
    ``artifact_id=None`` and are finalized by the framework before staging.

    Attributes:
        success: Whether execution completed successfully.
        error: Error message if success is False.
        artifacts: Output role -> draft artifact list.
            e.g. {"data": [DataArtifact(...), ...]}
        lineage: Explicit parent declarations keyed by output role. Every emitted
            role must be present; root roles explicitly contain an empty list.
        metadata: Extensibility escape hatch for additional data.

    Example:
        >>> result = ArtifactResult(
        ...     artifacts={"data": [data1, data2]},
        ...     lineage={"data": [LineageMapping(...)]},
        ... )
    """

    model_config = ConfigDict(
        extra="forbid",
        arbitrary_types_allowed=True,
    )

    success: bool = True
    error: str | None = None
    artifacts: dict[str, list[Artifact]] = Field(default_factory=dict)
    lineage: dict[str, list[LineageMapping]] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    def add_artifact(
        self,
        role: str,
        artifact: Artifact,
        *,
        sources: dict[str, list[str | int]],
    ) -> int:
        """Append an artifact and its declared parents atomically.

        Args:
            role: Output role receiving the artifact.
            artifact: Artifact to append without finalization or renaming.
            sources: Parent role to input IDs (strings) or sibling-output
                indices (integers). All references must use the same kind.
                An empty dictionary explicitly declares a root.

        Returns:
            The artifact's index within its output role.

        Raises:
            ValueError: If a role, source list, or reference is invalid.
            TypeError: If the artifact is not an Artifact.
        """
        if not isinstance(role, str) or not role:
            msg = "Output role must be a nonempty string"
            raise ValueError(msg)
        if not isinstance(artifact, Artifact):
            msg = "artifact must be an Artifact"
            raise TypeError(msg)
        index = len(self.artifacts.get(role, []))
        mappings: list[LineageMapping] = []
        kinds: set[type] = set()
        for source_role, references in sources.items():
            if not references:
                msg = f"Source role {source_role!r} has no references"
                raise ValueError(msg)
            for reference in references:
                if type(reference) not in (str, int):
                    msg = "Sources must be input IDs or integer output indices"
                    raise ValueError(msg)
                kinds.add(type(reference))
                mappings.append(
                    LineageMapping(
                        draft_index=index,
                        source_role=source_role,
                        source_artifact_id=reference
                        if isinstance(reference, str)
                        else None,
                        source_output_index=reference
                        if isinstance(reference, int)
                        else None,
                    )
                )
        if len(kinds) > 1:
            msg = "Cannot mix input IDs and sibling-output indices"
            raise ValueError(msg)
        self.artifacts.setdefault(role, []).append(artifact)
        self.lineage.setdefault(role, []).extend(mappings)
        return index


class PassthroughResult(BaseModel):
    """Result from curator operations that pass through existing artifacts.

    These operations route existing artifacts without creating new ones.
    They return artifact IDs, not draft artifact objects.

    Attributes:
        success: Whether execution completed successfully.
        error: Error message if success is False.
        passthrough: Output role -> artifact ID list.
            e.g. {"filtered": ["abc123...", "def456..."]}
        lineage_edges: Optional list of directed provenance edges to
            stage alongside the passthrough. Used by operations like
            ``DeclareLineage`` that emit edges without producing new
            artifacts. When None or empty, no edges are staged.
        metadata: Extensibility escape hatch for additional data.

    Example:
        >>> result = PassthroughResult(
        ...     passthrough={"filtered": ["abc123def456...", "789xyz..."]}
        ... )
    """

    model_config = ConfigDict(frozen=True)

    success: bool = True
    error: str | None = None
    passthrough: dict[str, list[str]] = Field(default_factory=dict)
    lineage_edges: list[ArtifactProvenanceEdge] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


# Union of supported curator-operation result models.
CuratorResult = ArtifactResult | PassthroughResult
