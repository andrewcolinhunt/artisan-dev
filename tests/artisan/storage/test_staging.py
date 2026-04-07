"""Tests for staging.py"""

from __future__ import annotations

import polars as pl
import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.schemas.artifact.metric import MetricArtifact
from artisan.storage.io.staging import StagingArea, StagingManager

METRICS_SCHEMA = MetricArtifact.POLARS_SCHEMA


@pytest.fixture
def local_fs():
    """Local filesystem for staging tests."""
    return LocalFileSystem()


class TestStagingArea:
    """Tests for StagingArea worker operations."""

    def test_create_staging_area(self, tmp_path, local_fs):
        """Create staging area creates batch directory."""
        root = str(tmp_path)
        staging = StagingArea(root, local_fs, batch_id="test_batch")

        assert staging.batch_id == "test_batch"
        assert local_fs.exists(staging.batch_dir)
        assert staging.batch_dir == f"{root}/test_batch"

    def test_auto_generate_batch_id(self, tmp_path, local_fs):
        """Batch ID is auto-generated when not provided."""
        staging = StagingArea(str(tmp_path), local_fs, worker_id=5)

        assert staging.batch_id.startswith("w5_")
        assert len(staging.batch_id) > 3

    def test_stage_dataframe(self, tmp_path, local_fs):
        """Stage DataFrame writes Parquet file."""
        staging = StagingArea(str(tmp_path), local_fs, batch_id="test")

        df = pl.DataFrame(
            {
                "artifact_id": ["a" * 32],
                "origin_step_number": [0],
                "content": [b'{"score": 0.5}'],
                "original_name": ["test"],
                "extension": [".json"],
                "metadata": ["{}"],
                "external_path": [None],
            },
            schema=METRICS_SCHEMA,
        )

        uri = staging.stage_dataframe(df, "metrics")

        assert local_fs.exists(uri)
        assert "metrics" in staging.list_staged_tables()

        read_back = pl.read_parquet(uri)
        assert read_back.shape == (1, 7)

    def test_stage_dataframe_append(self, tmp_path, local_fs):
        """Staging same table twice appends data."""
        staging = StagingArea(str(tmp_path), local_fs, batch_id="test")

        df1 = pl.DataFrame(
            {
                "artifact_id": ["a" * 32],
                "origin_step_number": [0],
                "content": [b'{"score": 0.5}'],
                "original_name": ["a"],
                "extension": [".json"],
                "metadata": ["{}"],
                "external_path": [None],
            },
            schema=METRICS_SCHEMA,
        )

        df2 = pl.DataFrame(
            {
                "artifact_id": ["b" * 32],
                "origin_step_number": [0],
                "content": [b'{"score": 0.5}'],
                "original_name": ["b"],
                "extension": [".json"],
                "metadata": ["{}"],
                "external_path": [None],
            },
            schema=METRICS_SCHEMA,
        )

        staging.stage_dataframe(df1, "metrics")
        staging.stage_dataframe(df2, "metrics")

        uri = staging.get_staged_file("metrics")
        read_back = pl.read_parquet(uri)
        assert read_back.shape[0] == 2

    def test_stage_empty_dataframe(self, tmp_path, local_fs):
        """Staging empty DataFrame doesn't write file."""
        staging = StagingArea(str(tmp_path), local_fs, batch_id="test")

        df = pl.DataFrame(schema=METRICS_SCHEMA)
        staging.stage_dataframe(df, "metrics")

        staged_file = staging.get_staged_file("metrics")
        assert staged_file is None

    def test_stage_artifacts(self, tmp_path, local_fs):
        """Stage multiple artifact tables at once."""
        staging = StagingArea(str(tmp_path), local_fs, batch_id="test")

        metrics_df = pl.DataFrame(
            {
                "artifact_id": ["a" * 32],
                "origin_step_number": [0],
                "content": [b'{"score": 0.5}'],
                "original_name": ["test"],
                "extension": [".json"],
                "metadata": ["{}"],
                "external_path": [None],
            },
            schema=METRICS_SCHEMA,
        )

        staging.stage_artifacts(
            {
                "metrics": metrics_df,
                "results": pl.DataFrame(),  # Empty
            }
        )

        assert "metrics" in staging.list_staged_tables()

    def test_cleanup(self, tmp_path, local_fs):
        """Cleanup removes batch directory."""
        staging = StagingArea(str(tmp_path), local_fs, batch_id="test")

        df = pl.DataFrame(
            {
                "artifact_id": ["a" * 32],
                "origin_step_number": [0],
                "content": [b'{"score": 0.5}'],
                "original_name": ["test"],
                "extension": [".json"],
                "metadata": ["{}"],
                "external_path": [None],
            },
            schema=METRICS_SCHEMA,
        )

        staging.stage_dataframe(df, "metrics")
        assert local_fs.exists(staging.batch_dir)

        staging.cleanup()
        assert not local_fs.exists(staging.batch_dir)

    def test_context_manager(self, tmp_path, local_fs):
        """StagingArea works as context manager."""
        with StagingArea(str(tmp_path), local_fs, batch_id="context_test") as staging:
            assert local_fs.exists(staging.batch_dir)

    def test_batch_dir_returns_str(self, tmp_path, local_fs):
        """batch_dir returns str, not Path."""
        staging = StagingArea(str(tmp_path), local_fs, batch_id="test")
        assert isinstance(staging.batch_dir, str)

    def test_stage_dataframe_returns_str(self, tmp_path, local_fs):
        """stage_dataframe returns str URI."""
        staging = StagingArea(str(tmp_path), local_fs, batch_id="test")
        df = pl.DataFrame(
            {
                "artifact_id": ["a" * 32],
                "origin_step_number": [0],
                "content": [b'{"score": 0.5}'],
                "original_name": ["test"],
                "extension": [".json"],
                "metadata": ["{}"],
                "external_path": [None],
            },
            schema=METRICS_SCHEMA,
        )
        result = staging.stage_dataframe(df, "metrics")
        assert isinstance(result, str)


class TestStagingManager:
    """Tests for StagingManager orchestrator operations."""

    @pytest.fixture
    def populated_staging(self, tmp_path, local_fs):
        """Create staging directory with multiple batches."""
        root = str(tmp_path)

        batch1 = StagingArea(root, local_fs, batch_id="batch1", worker_id=0)
        batch1.stage_dataframe(
            pl.DataFrame(
                {
                    "artifact_id": ["a" * 32],
                    "origin_step_number": [0],
                    "content": [b'{"score": 0.5}'],
                    "original_name": ["a"],
                    "extension": [".json"],
                    "metadata": ["{}"],
                    "external_path": [None],
                },
                schema=METRICS_SCHEMA,
            ),
            "metrics",
        )

        batch2 = StagingArea(root, local_fs, batch_id="batch2", worker_id=1)
        batch2.stage_dataframe(
            pl.DataFrame(
                {
                    "artifact_id": ["b" * 32],
                    "origin_step_number": [0],
                    "content": [b'{"score": 0.5}'],
                    "original_name": ["b"],
                    "extension": [".json"],
                    "metadata": ["{}"],
                    "external_path": [None],
                },
                schema=METRICS_SCHEMA,
            ),
            "metrics",
        )

        return StagingManager(root, local_fs)

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

    def test_read_all_staged_no_files(self, tmp_path, local_fs):
        """Returns None when no staged files exist."""
        manager = StagingManager(str(tmp_path), local_fs)
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
