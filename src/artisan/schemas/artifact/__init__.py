"""Artisan artifact schema package."""

from __future__ import annotations

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.execution_config import (
    ExecutionConfigArtifact,
    find_artifact_references,
    substitute_references,
)
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes

__all__ = [
    "Artifact",
    "ArtifactProvenanceEdge",
    "ArtifactTypeDef",
    "ArtifactTypes",
    "DataArtifact",
    "ExecutionConfigArtifact",
    "FileRefArtifact",
    "MetricArtifact",
    "find_artifact_references",
    "substitute_references",
]
