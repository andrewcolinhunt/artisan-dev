"""Tests for staging.py — parametrized over [local, s3] backends.

The ``backend_fs`` fixture (from ``tests/artisan/storage/conftest.py``)
yields ``(fs, storage_config, uri_prefix)`` for each backend so the
test body is backend-agnostic. S3 params skip cleanly when MinIO is
unavailable.
"""

from __future__ import annotations

import polars as pl
import pytest

from artisan.schemas.artifact.metric import MetricArtifact
from artisan.storage.io.staging import StagingArea, StagingManager

METRICS_SCHEMA = MetricArtifact.POLARS_SCHEMA


def _row(artifact_id: str = "a" * 32, name: str = "test") -> dict:
    """Reusable single-row payload matching METRICS_SCHEMA."""
    return {
        "artifact_id": [artifact_id],
        "origin_step_number": [0],
        "content": [b'{"score": 0.5}'],
        "original_name": [name],
        "extension": [".json"],
        "metadata": ["{}"],
        "external_path": [None],
    }


class TestStagingArea:
    """Tests for StagingArea worker operations on both local and s3."""

    def test_create_staging_area(self, backend_fs):
        """Create staging area creates batch directory.

        S3 has no concept of empty directories — a prefix only "exists"
        once an object has been written under it. The post-write
        existence check is exercised by ``test_stage_dataframe`` instead.
        """
        from fsspec.implementations.local import LocalFileSystem

        fs, _, root = backend_fs
        staging = StagingArea(root, fs, batch_id="test_batch")

        assert staging.batch_id == "test_batch"
        assert staging.batch_dir == f"{root}/test_batch"
        if isinstance(fs, LocalFileSystem):
            assert fs.exists(staging.batch_dir)

    def test_auto_generate_batch_id(self, backend_fs):
        """Batch ID is auto-generated when not provided."""
        fs, _, root = backend_fs
        staging = StagingArea(root, fs, worker_id=5)

        assert staging.batch_id.startswith("w5_")
        assert len(staging.batch_id) > 3

    def test_stage_dataframe(self, backend_fs):
        """Stage DataFrame writes Parquet file."""
        fs, _, root = backend_fs
        staging = StagingArea(root, fs, batch_id="test")

        df = pl.DataFrame(_row(), schema=METRICS_SCHEMA)
        uri = staging.stage_dataframe(df, "metrics")

        assert fs.exists(uri)
        assert "metrics" in staging.list_staged_tables()

        # Read-back with polars across both fs flavors. For s3, polars
        # reads via fsspec when given an s3:// URI directly.
        with fs.open(uri, "rb") as f:
            read_back = pl.read_parquet(f)
        assert read_back.shape == (1, 7)

    def test_stage_dataframe_append(self, backend_fs):
        """Staging same table twice appends data."""
        fs, _, root = backend_fs
        staging = StagingArea(root, fs, batch_id="test")

        staging.stage_dataframe(
            pl.DataFrame(_row("a" * 32, "a"), schema=METRICS_SCHEMA),
            "metrics",
        )
        staging.stage_dataframe(
            pl.DataFrame(_row("b" * 32, "b"), schema=METRICS_SCHEMA),
            "metrics",
        )

        uri = staging.get_staged_file("metrics")
        with fs.open(uri, "rb") as f:
            read_back = pl.read_parquet(f)
        assert read_back.shape[0] == 2

    def test_stage_empty_dataframe(self, backend_fs):
        """Staging empty DataFrame doesn't write file."""
        fs, _, root = backend_fs
        staging = StagingArea(root, fs, batch_id="test")

        df = pl.DataFrame(schema=METRICS_SCHEMA)
        staging.stage_dataframe(df, "metrics")

        staged_file = staging.get_staged_file("metrics")
        assert staged_file is None

    def test_stage_artifacts(self, backend_fs):
        """Stage multiple artifact tables at once."""
        fs, _, root = backend_fs
        staging = StagingArea(root, fs, batch_id="test")

        metrics_df = pl.DataFrame(_row(), schema=METRICS_SCHEMA)

        staging.stage_artifacts(
            {
                "metrics": metrics_df,
                "results": pl.DataFrame(),  # Empty
            }
        )

        assert "metrics" in staging.list_staged_tables()

    def test_cleanup(self, backend_fs):
        """Cleanup removes batch directory."""
        fs, _, root = backend_fs
        staging = StagingArea(root, fs, batch_id="test")

        staging.stage_dataframe(pl.DataFrame(_row(), schema=METRICS_SCHEMA), "metrics")
        assert fs.exists(staging.batch_dir)

        staging.cleanup()
        assert not fs.exists(staging.batch_dir)

    def test_context_manager(self, backend_fs):
        """StagingArea works as context manager.

        S3 has no concept of empty directories; the post-init existence
        check only applies to LocalFileSystem.
        """
        from fsspec.implementations.local import LocalFileSystem

        fs, _, root = backend_fs
        with StagingArea(root, fs, batch_id="context_test") as staging:
            assert staging.batch_dir == f"{root}/context_test"
            if isinstance(fs, LocalFileSystem):
                assert fs.exists(staging.batch_dir)

    def test_batch_dir_returns_str(self, backend_fs):
        """batch_dir returns str, not Path."""
        fs, _, root = backend_fs
        staging = StagingArea(root, fs, batch_id="test")
        assert isinstance(staging.batch_dir, str)

    def test_stage_dataframe_returns_str(self, backend_fs):
        """stage_dataframe returns str URI."""
        fs, _, root = backend_fs
        staging = StagingArea(root, fs, batch_id="test")
        result = staging.stage_dataframe(
            pl.DataFrame(_row(), schema=METRICS_SCHEMA), "metrics"
        )
        assert isinstance(result, str)


class TestStagingManager:
    """Tests for StagingManager orchestrator operations on both backends."""

    @pytest.fixture
    def populated_staging(self, backend_fs):
        """Create staging directory with multiple batches on the parametrized backend."""
        fs, _, root = backend_fs

        batch1 = StagingArea(root, fs, batch_id="batch1", worker_id=0)
        batch1.stage_dataframe(
            pl.DataFrame(_row("a" * 32, "a"), schema=METRICS_SCHEMA),
            "metrics",
        )

        batch2 = StagingArea(root, fs, batch_id="batch2", worker_id=1)
        batch2.stage_dataframe(
            pl.DataFrame(_row("b" * 32, "b"), schema=METRICS_SCHEMA),
            "metrics",
        )

        return StagingManager(root, fs)

    def test_list_batch_ids(self, populated_staging):
        """List all batch IDs."""
        batch_ids = populated_staging.list_batch_ids()
        assert set(batch_ids) == {"batch1", "batch2"}

    def test_get_staged_files_for_table(self, populated_staging):
        """Get all staged Parquet files for a table."""
        files = populated_staging.get_staged_files_for_table("metrics")
        assert len(files) == 2

    def test_get_staged_files_returns_str(self, populated_staging):
        """get_staged_files_for_table returns list of str."""
        files = populated_staging.get_staged_files_for_table("metrics")
        for f in files:
            assert isinstance(f, str)

    def test_read_all_staged_for_table(self, populated_staging):
        """Read all staged files into one DataFrame."""
        df = populated_staging.read_all_staged_for_table("metrics")

        assert df is not None
        assert df.shape[0] == 2
        assert set(df["artifact_id"].to_list()) == {"a" * 32, "b" * 32}

    def test_read_all_staged_no_files(self, backend_fs):
        """Returns None when no staged files exist."""
        fs, _, root = backend_fs
        manager = StagingManager(root, fs)
        result = manager.read_all_staged_for_table("metrics")
        assert result is None

    def test_cleanup_batch(self, populated_staging):
        """Cleanup specific batch."""
        populated_staging.cleanup_batch("batch1")

        remaining = populated_staging.list_batch_ids()
        assert "batch1" not in remaining
        assert "batch2" in remaining

    def test_cleanup_all(self, populated_staging):
        """Cleanup all batches."""
        populated_staging.cleanup_all()
        assert populated_staging.list_batch_ids() == []
