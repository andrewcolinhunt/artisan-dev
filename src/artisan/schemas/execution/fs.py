"""Resolve an fsspec filesystem for a path, honoring StorageConfig.

The two-step rule lets pipeline-storage credentials apply to inputs on
the same protocol (so MinIO-backed test fixtures work without env-var
leakage) while still supporting cross-protocol inputs (e.g. a local
pipeline ingesting from S3) via fsspec's standard ambient credential
discovery.
"""

from __future__ import annotations

import fsspec
from fsspec import AbstractFileSystem
from fsspec.utils import get_protocol

from artisan.schemas.execution.storage_config import StorageConfig


def resolve_fs(
    path: str,
    storage: StorageConfig | None = None,
) -> tuple[AbstractFileSystem, str]:
    """Resolve an fsspec filesystem and stripped path for a URI.

    Two-step rule:

    1. If ``storage`` is provided and the path's protocol matches
       ``storage.protocol``, return ``storage.filesystem()`` so
       configured options/credentials apply (used by tests pointing
       at MinIO and by production pipelines with explicit creds).
    2. Otherwise fall back to :func:`fsspec.core.url_to_fs`, which
       uses ambient credential discovery (env vars, IAM roles,
       ``~/.aws/credentials``).

    Args:
        path: URI (e.g. ``"s3://bucket/key"``) or local path.
        storage: Pipeline ``StorageConfig`` if available.

    Returns:
        ``(filesystem, stripped_path)`` — ``stripped_path`` has the
        protocol prefix removed (matching ``url_to_fs`` semantics) so
        callers can pass it directly to ``fs.exists``, ``fs.open``, etc.
    """
    proto = get_protocol(path)  # "file" for bare local paths
    if storage is not None and proto == storage.protocol:
        fs = storage.filesystem()
        stripped = path.split("://", 1)[1] if "://" in path else path
        return fs, stripped
    return fsspec.core.url_to_fs(path)
