"""Shared fixtures for storage-layer tests.

The ``backend_fs`` fixture parametrizes over ``["local", "s3"]`` so the
same test body runs against both storage backends. S3 params skip cleanly when MinIO is unavailable (the
session-scoped ``minio_endpoint`` fixture in ``tests/conftest.py``
yields None on boot failure).
"""

from __future__ import annotations

import pytest
from fixtures.store_format import publish_test_store
from fsspec.implementations.local import LocalFileSystem

from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_store import open_retained_store
from artisan.utils.path import uri_join, uri_parent


@pytest.fixture(
    params=[
        pytest.param("local"),
        pytest.param("s3", marks=pytest.mark.s3),
    ]
)
def backend_fs(request, tmp_path):
    """Yield ``(fs, storage_config, uri_prefix)`` for both backends.

    Tests that use this fixture run twice — once against
    ``LocalFileSystem`` rooted at ``tmp_path``, once against the
    per-test S3 bucket on MinIO.

    The ``s3`` param carries the ``s3`` resource marker so
    ``pixi run -e dev test-unit`` (``-m '... and not s3'``) collects
    only the local branch and never boots MinIO; the s3 branch runs
    under ``test-s3``. The marker is explicit on the param (not
    auto-derived) because ``s3_fs`` is resolved lazily via
    ``request.getfixturevalue`` — the local-only run never
    instantiates MinIO via testcontainers, and lazy resolution is
    invisible to the fixture closure the auto-marker inspects.
    """
    if request.param == "local":
        result = LocalFileSystem(), StorageConfig(), str(tmp_path)
    else:
        result = request.getfixturevalue("s3_fs")
    fs, storage, root = result
    publish_test_store(root, fs, storage.delta_storage_options())
    return result


@pytest.fixture
def backend_committer(backend_fs) -> RetainedCommitter:
    """Use the exact filesystem and managed-root binding of the initialized backend."""
    fs, storage, root = backend_fs
    return open_retained_store(
        root,
        uri_join(uri_parent(root), "files"),
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )
