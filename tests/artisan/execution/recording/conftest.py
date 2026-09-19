"""Filesystem fixtures for execution recording tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem


@pytest.fixture(
    params=[
        pytest.param("local"),
        pytest.param("s3", marks=pytest.mark.s3),
    ]
)
def backend_fs(
    request: pytest.FixtureRequest, tmp_path: Path
) -> tuple[AbstractFileSystem, str]:
    """Return a filesystem and root; local-only tests never initialize MinIO.

    Explicit S3 marking is required because lazy fixture lookup is invisible
    to the marker hook's fixture-closure inspection.
    """
    if request.param == "local":
        return LocalFileSystem(), str(tmp_path)
    fs, _, uri_prefix = request.getfixturevalue("s3_fs")
    return fs, uri_prefix
