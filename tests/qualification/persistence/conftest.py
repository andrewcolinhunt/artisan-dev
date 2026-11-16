"""Use the repository's real MinIO fixture with isolated candidate stores."""

from __future__ import annotations

from pathlib import Path

import pytest
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.schemas.execution.storage_config import StorageConfig


@pytest.fixture(params=["local", pytest.param("s3", marks=pytest.mark.s3)])
def persistence_backend(
    request: pytest.FixtureRequest, tmp_path: Path
) -> tuple[AbstractFileSystem, StorageConfig, str]:
    """Run identical storage proofs against local files and actual MinIO objects."""
    if request.param == "local":
        return LocalFileSystem(), StorageConfig(), str(tmp_path / "backend")
    return request.getfixturevalue("s3_fs")
