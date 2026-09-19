"""Shared release-format fixtures for orchestration tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.store_format import publish_test_store
from fsspec.implementations.local import LocalFileSystem


@pytest.fixture(autouse=True)
def _format_common_delta_root(tmp_path: Path) -> None:
    """Initialize both conventional orchestration-test roots with the supported format."""
    fs = LocalFileSystem()
    publish_test_store(str(tmp_path), fs)
    publish_test_store(str(tmp_path / "delta"), fs)
