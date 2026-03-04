"""Tests for staging.py"""

from __future__ import annotations

import polars as pl
import pytest

from artisan.schemas.artifact.metric import MetricArtifact
from artisan.storage.io.staging import StagingArea, StagingManager

METRICS_SCHEMA = MetricArtifact.POLARS_SCHEMA


class TestStagingArea:
    """Tests for StagingArea worker operations."""

    def test_create_staging_area(self, tmp_path):
        """Create staging area creates batch directory."""
        staging = StagingArea(tmp_path, batch_id="test_batch")

        assert staging.batch_id == "test_batch"
        assert staging.batch_dir.exists()
        assert staging.batch_dir == tmp_path / "test_batch"

    def test_auto_generate_batch_id(self, tmp_path):
        """Batch ID is auto-generated when not provided."""
        staging = StagingArea(tmp_path, worker_id=5)

        assert staging.batch_id.startswith("w5_")
        assert len(staging.batch_id) > 3

    def test_stage_dataframe(self, tmp_path):
        """Stage DataFrame writes Parquet file."""
        staging = StagingArea(tmp_path, batch_id="test")

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

        path = staging.stage_dataframe(df, "metrics")

        assert path.exists()
        assert "metrics" in staging.list_staged_tables()

        # Verify content
        read_back = pl.read_parquet(path)
        assert read_back.shape == (1, 7)

    def test_stage_dataframe_append(self, tmp_path):
        """Staging same table twice appends data."""
        staging = StagingArea(tmp_path, batch_id="test")

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

        read_back = pl.read_parquet(staging.get_staged_file("metrics"))
        assert read_back.shape[0] == 2

    def test_stage_empty_dataframe(self, tmp_path):
        """Staging empty DataFrame doesn't write file."""
        staging = StagingArea(tmp_path, batch_id="test")

        df = pl.DataFrame(schema=METRICS_SCHEMA)
        staging.stage_dataframe(df, "metrics")

        # Empty df doesn't add to staged tables
        staged_file = staging.get_staged_file("metrics")
        assert staged_file is None or not staged_file.exists()

    def test_stage_artifacts(self, tmp_path):
        """Stage multiple artifact tables at once."""
        staging = StagingArea(tmp_path, batch_id="test")

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
        # results not staged because empty

    def test_cleanup(self, tmp_path):
        """Cleanup removes batch directory."""
        staging = StagingArea(tmp_path, batch_id="test")

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
        assert staging.batch_dir.exists()

        staging.cleanup()
        assert not staging.batch_dir.exists()

    def test_context_manager(self, tmp_path):
        """StagingArea works as context manager."""
        with StagingArea(tmp_path, batch_id="context_test") as staging:
            assert staging.batch_dir.exists()


class TestStagingManager:
    """Tests for StagingManager orchestrator operations."""

    @pytest.fixture
    def populated_staging(self, tmp_path):
        """Create staging directory with multiple batches."""
        # Batch 1
        batch1 = StagingArea(tmp_path, batch_id="batch1", worker_id=0)
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

        # Batch 2
        batch2 = StagingArea(tmp_path, batch_id="batch2", worker_id=1)
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

        return StagingManager(tmp_path)

    def test_list_batch_ids(self, populated_staging):
        """List all batch IDs."""
        batch_ids = populated_staging.list_batch_ids()
        assert set(batch_ids) == {"batch1", "batch2"}

    def test_get_staged_files_for_table(self, populated_staging):
        """Get all staged Parquet files for a table."""
        files = populated_staging.get_staged_files_for_table("metrics")
        assert len(files) == 2

    def test_read_all_staged_for_table(self, populated_staging):
        """Read all staged files into one DataFrame."""
        df = populated_staging.read_all_staged_for_table("metrics")

        assert df is not None
        assert df.shape[0] == 2
        assert set(df["artifact_id"].to_list()) == {"a" * 32, "b" * 32}

    def test_read_all_staged_no_files(self, tmp_path):
        """Returns None when no staged files exist."""
        manager = StagingManager(tmp_path)
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
