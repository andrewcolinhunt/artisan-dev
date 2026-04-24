"""Shared fixtures for storage-layer tests.

The ``backend_fs`` fixture parametrizes over ``["local", "s3"]`` so the
same test body runs against both backends without ``if backend == "s3":``
branching. S3 params skip cleanly when MinIO is unavailable (the
session-scoped ``minio_endpoint`` fixture in ``tests/conftest.py``
yields None on boot failure).
"""

from __future__ import annotations

import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.schemas.execution.storage_config import StorageConfig


@pytest.fixture(
    params=[
        pytest.param("local"),
        pytest.param("s3", marks=pytest.mark.integration),
    ]
)
def backend_fs(request, tmp_path, s3_fs):
    """Yield ``(fs, storage_config, uri_prefix)`` for both backends.

    Tests that use this fixture run twice — once against
    ``LocalFileSystem`` rooted at ``tmp_path``, once against the
    per-test S3 bucket on MinIO.

    The ``s3`` param carries the ``integration`` marker so
    ``pixi run -e dev test-unit`` (``pytest -m 'not integration'``)
    collects only the local branch and never boots MinIO. The s3
    branch runs under ``test-integration``.
    """
    if request.param == "local":
        return LocalFileSystem(), StorageConfig(), str(tmp_path)
    return s3_fs  # already (fs, storage, uri_prefix)
