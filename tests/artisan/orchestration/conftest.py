"""Shared release-format fixtures for orchestration tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.storage.core.store_format import publish_store_manifest


@pytest.fixture(autouse=True)
def _format_common_delta_root(tmp_path: Path) -> None:
    """Initialize both conventional orchestration-test roots as format 2."""
    fs = LocalFileSystem()
    publish_store_manifest(str(tmp_path), fs)
    publish_store_manifest(str(tmp_path / "delta"), fs)
