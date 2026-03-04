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
from artisan.schemas.provenance.lineage_mapping import LineageMapping


class ArtifactResult(BaseModel):
    """Result from curator operations that create new artifacts.

    These operations produce draft artifacts from their inputs. Drafts have
    ``artifact_id=None`` and are finalized by the framework before staging.

    Attributes:
        success: Whether execution completed successfully.
        error: Error message if success is False.
        artifacts: Output role -> draft artifact list.
            e.g. {"data": [DataArtifact(...), ...]}
        lineage: Optional explicit lineage declarations per role.
            If None, framework infers lineage from output specs.
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
    lineage: dict[str, list[LineageMapping]] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class PassthroughResult(BaseModel):
    """Result from curator operations that pass through existing artifacts.

    These operations route existing artifacts without creating new ones.
    They return artifact IDs, not draft artifact objects.

    Attributes:
        success: Whether execution completed successfully.
        error: Error message if success is False.
        passthrough: Output role -> artifact ID list.
            e.g. {"filtered": ["abc123...", "def456..."]}
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
    metadata: dict[str, Any] = Field(default_factory=dict)


# Union of supported curator-operation result models.
CuratorResult = ArtifactResult | PassthroughResult
