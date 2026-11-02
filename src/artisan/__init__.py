"""Artisan: framework for artifact-centric computational pipelines."""

from __future__ import annotations

from importlib.metadata import version as _distribution_version

__version__ = _distribution_version("dexterity-artisan")

__all__ = ["__version__"]
