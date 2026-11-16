"""Shared local/S3 fixtures with explicit store initialization.

``raw_backend_fs`` provides an isolated filesystem location. ``backend_fs``
initializes a complete store there for tests that need one. Tests creating their
own stores use the raw location to avoid initializing an unused store first.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.store_format import publish_test_store
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_store import open_retained_store
from artisan.utils.path import uri_join, uri_parent

type Backend = tuple[AbstractFileSystem, StorageConfig, str]


@pytest.fixture(
    params=[
        pytest.param("local"),
        pytest.param("s3", marks=pytest.mark.s3),
    ]
)
def raw_backend_fs(request: pytest.FixtureRequest, tmp_path: Path) -> Backend:
    """Provide an isolated backend location without creating a store.

    The S3 parameter is explicitly marked because lazy fixture resolution is
    invisible to the fixture-closure marker hook. Unit selection therefore
    collects only the local parameter and never starts MinIO.
    """
    result: Backend
    if request.param == "local":
        result = LocalFileSystem(), StorageConfig(), str(tmp_path)
    else:
        result = request.getfixturevalue("s3_fs")
    return result


@pytest.fixture
def backend_fs(raw_backend_fs: Backend) -> Backend:
    """Initialize the backend root as a complete supported store."""
    fs, storage, root = raw_backend_fs
    publish_test_store(root, fs, storage.delta_storage_options())
    return raw_backend_fs


@pytest.fixture
def backend_committer(backend_fs: Backend) -> RetainedCommitter:
    """Use the exact filesystem and managed-root binding of the initialized backend."""
    fs, storage, root = backend_fs
    return open_retained_store(
        root,
        uri_join(uri_parent(root), "files"),
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )
