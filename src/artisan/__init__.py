"""Artisan: framework for artifact-centric computational pipelines."""

from __future__ import annotations

from artisan._version import __version__, __version_tuple__
from artisan.operations.base.per_artifact import PerArtifact

__all__ = ["PerArtifact", "__version__", "__version_tuple__"]
