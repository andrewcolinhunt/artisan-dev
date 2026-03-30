"""Artisan artifact schema package."""

from __future__ import annotations

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.schemas.artifact.types import ArtifactTypes

__all__ = [
    "Artifact",
    "ArtifactProvenanceEdge",
    "ArtifactTypes",
    "DataArtifact",
    "ExecutionConfigArtifact",
    "FileRefArtifact",
    "MetricArtifact",
]
