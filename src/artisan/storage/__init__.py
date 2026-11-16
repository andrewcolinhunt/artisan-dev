"""Artisan storage layer for Delta Lake persistence."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from artisan.storage.core.artifact_store import ArtifactStore
    from artisan.storage.publication import PublicationExport, export_publication

__all__ = [
    "ArtifactStore",
    "PublicationExport",
    "export_publication",
]


def __getattr__(name: str) -> object:
    # Storage contracts are imported before lifecycle and publication readers.
    if name == "ArtifactStore":
        from artisan.storage.core.artifact_store import ArtifactStore

        return ArtifactStore
    if name in {"PublicationExport", "export_publication"}:
        from artisan.storage import publication

        return getattr(publication, name)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
