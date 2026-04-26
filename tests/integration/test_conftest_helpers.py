"""Contract tests for read_table (tests/integration/conftest.py helpers).

Covers the URI-vs-path branch so the silent-empty landmine doesn't
regress: cloud URIs must go through ``fs.exists`` + ``fs``-aware
``pl.read_delta``, never ``os.path.exists`` on the URI string.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from fsspec.implementations.local import LocalFileSystem

from .conftest import read_table


class TestReadTableLocal:
    """Local path behavior — must stay byte-identical to pre-PR-8a."""

    def test_returns_rows_when_table_exists(self, tmp_path: Path) -> None:
        delta_root = str(tmp_path)
        df = pl.DataFrame({"id": ["a", "b"], "step": [0, 1]})
        df.write_delta(f"{delta_root}/my_table")

        result = read_table(delta_root, "my_table")

        assert result.shape[0] == 2
        assert set(result["id"].to_list()) == {"a", "b"}

    def test_returns_empty_when_table_missing(self, tmp_path: Path) -> None:
        result = read_table(str(tmp_path), "does_not_exist")
        assert result.is_empty()

    def test_fs_kwarg_accepted_on_local(self, tmp_path: Path) -> None:
        """Passing fs is harmless for local paths (URI detection is ``://``)."""
        delta_root = str(tmp_path)
        pl.DataFrame({"id": ["a"]}).write_delta(f"{delta_root}/t")

        result = read_table(delta_root, "t", fs=LocalFileSystem())

        assert result.shape[0] == 1


class TestReadTableCloudContract:
    """URI-path contract — matters even without a live cloud step_runner."""

    def test_raises_without_fs_on_cloud_uri(self) -> None:
        with pytest.raises(ValueError, match="fs required for cloud"):
            read_table("s3://bucket/delta", "my_table")

    def test_error_includes_delta_root(self) -> None:
        with pytest.raises(ValueError, match="s3://bucket/delta"):
            read_table("s3://bucket/delta", "my_table")


@pytest.mark.integration
class TestReadTableCloud:
    """Round-trip against the MinIO-backed s3_fs fixture."""

    def test_returns_rows_on_cloud_when_table_exists(self, s3_fs) -> None:
        fs, storage, uri_prefix = s3_fs
        delta_root = f"{uri_prefix}/delta"

        df = pl.DataFrame({"id": ["a", "b"], "step": [0, 1]})
        df.write_delta(
            f"{delta_root}/my_table",
            storage_options=storage.delta_storage_options(),
        )

        result = read_table(
            delta_root,
            "my_table",
            fs=fs,
            storage_options=storage.delta_storage_options(),
        )

        assert result.shape[0] == 2
        assert set(result["id"].to_list()) == {"a", "b"}

    def test_returns_empty_on_cloud_when_table_missing(self, s3_fs) -> None:
        fs, storage, uri_prefix = s3_fs
        delta_root = f"{uri_prefix}/delta"

        result = read_table(
            delta_root,
            "does_not_exist",
            fs=fs,
            storage_options=storage.delta_storage_options(),
        )

        assert result.is_empty()
