"""Create complete supported stores for test inputs and empty-reader cases."""

from __future__ import annotations

from fsspec import AbstractFileSystem

from artisan.storage.io.retained_store import create_retained_store
from artisan.utils.path import uri_join, uri_parent


def publish_test_store(
    delta_root: str,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
) -> None:
    """Initialize all required tables through the same path as pipeline creation."""
    create_retained_store(
        delta_root,
        uri_join(uri_parent(delta_root), "files"),
        fs=fs,
        storage_options=storage_options,
    )
