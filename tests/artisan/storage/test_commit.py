"""Tests for commit.py"""

from __future__ import annotations

import polars as pl
import pytest

from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    ARTIFACT_INDEX_SCHEMA,
)
from artisan.storage.io.commit import DeltaCommitter

METRICS_SCHEMA = MetricArtifact.POLARS_SCHEMA
from artisan.storage.io.staging import StagingArea


class TestDeltaCommitter:
    """Tests for DeltaCommitter."""

    @pytest.fixture
    def setup_paths(self, tmp_path):
        """Create delta and staging directories."""
        delta_path = tmp_path / "delta"
        staging_path = tmp_path / "staging"
        delta_path.mkdir()
        staging_path.mkdir()
        return delta_path, staging_path

    @pytest.fixture
    def committer(self, setup_paths):
        """Create a DeltaCommitter instance."""
        delta_path, staging_path = setup_paths
        return DeltaCommitter(delta_path, staging_path)

    def test_commit_table_creates_new_table(self, setup_paths, committer):
        """Commit to non-existent table creates it."""
        delta_path, staging_path = setup_paths

        # Stage some data
        staging = StagingArea(staging_path, batch_id="test")
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

        # Commit
        rows = committer.commit_table("artifacts/metrics")

        assert rows == 1
        assert (delta_path / "artifacts/metrics").exists()

    def test_commit_table_appends_to_existing(self, setup_paths, committer):
        """Commit to existing table appends data."""
        delta_path, staging_path = setup_paths

        # Create initial table
        initial_df = pl.DataFrame(
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
        initial_df.write_delta(str(delta_path / "artifacts/metrics"))

        # Stage new data
        staging = StagingArea(staging_path, batch_id="test")
        new_df = pl.DataFrame(
            {
                "artifact_id": ["b" * 32],
                "origin_step_number": [0],
                "content": [b'{"score": 0.8}'],
                "original_name": ["b"],
                "extension": [".json"],
                "metadata": ["{}"],
                "external_path": [None],
            },
            schema=METRICS_SCHEMA,
        )
        staging.stage_dataframe(new_df, "metrics")

        # Commit
        rows = committer.commit_table("artifacts/metrics")

        assert rows == 1

        # Verify total
        result = pl.read_delta(str(delta_path / "artifacts/metrics"))
        assert result.shape[0] == 2

    def test_commit_table_deduplicates(self, setup_paths, committer):
        """Commit skips artifacts with existing IDs."""
        delta_path, staging_path = setup_paths

        # Create initial table
        initial_df = pl.DataFrame(
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
        initial_df.write_delta(str(delta_path / "artifacts/metrics"))

        # Stage duplicate
        staging = StagingArea(staging_path, batch_id="test")
        duplicate_df = pl.DataFrame(
            {
                "artifact_id": ["a" * 32],  # Same ID
                "origin_step_number": [0],
                "content": [b'{"score": 0.5}'],
                "original_name": ["a"],
                "extension": [".json"],
                "metadata": ["{}"],
                "external_path": [None],
            },
            schema=METRICS_SCHEMA,
        )
        staging.stage_dataframe(duplicate_df, "metrics")

        # Commit
        rows = committer.commit_table("artifacts/metrics")

        assert rows == 0  # Duplicate skipped

    def test_commit_all_tables(self, setup_paths, committer):
        """Commit all staged tables at once."""
        delta_path, staging_path = setup_paths

        # Stage multiple tables
        staging = StagingArea(staging_path, batch_id="test")

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

        index_df = pl.DataFrame(
            {
                "artifact_id": ["a" * 32],
                "artifact_type": ["data"],
                "origin_step_number": [0],
                "metadata": ["{}"],
            },
            schema=ARTIFACT_INDEX_SCHEMA,
        )

        staging.stage_dataframe(metrics_df, "metrics")
        staging.stage_dataframe(index_df, "index")

        # Commit all
        results = committer.commit_all_tables(cleanup_staging=False)

        assert "metrics" in results
        assert "index" in results
        assert results["metrics"] == 1
        assert results["index"] == 1

    def test_commit_all_tables_cleanup(self, setup_paths, committer):
        """Commit all cleans up staging by default."""
        delta_path, staging_path = setup_paths

        # Stage data
        staging = StagingArea(staging_path, batch_id="test")
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

        # Commit with cleanup
        committer.commit_all_tables(cleanup_staging=True)

        # Staging should be empty
        assert committer.staging_manager.list_batch_ids() == []

    def test_commit_batch_specific(self, setup_paths, committer):
        """Commit only a specific batch."""
        delta_path, staging_path = setup_paths

        # Stage multiple batches
        staging1 = StagingArea(staging_path, batch_id="batch1")
        staging2 = StagingArea(staging_path, batch_id="batch2")

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
                "content": [b'{"score": 0.8}'],
                "original_name": ["b"],
                "extension": [".json"],
                "metadata": ["{}"],
                "external_path": [None],
            },
            schema=METRICS_SCHEMA,
        )

        staging1.stage_dataframe(df1, "metrics")
        staging2.stage_dataframe(df2, "metrics")

        # Commit only batch1
        results = committer.commit_batch("batch1", cleanup_after=True)

        assert results["metrics"] == 1

        # batch1 cleaned up, batch2 remains
        assert "batch1" not in committer.staging_manager.list_batch_ids()
        assert "batch2" in committer.staging_manager.list_batch_ids()

    def test_initialize_tables(self, setup_paths, committer):
        """Initialize creates empty tables with schemas."""
        delta_path, staging_path = setup_paths

        committer.initialize_tables()

        # Check tables exist
        assert (delta_path / "artifacts/data").exists()
        assert (delta_path / "artifacts/metrics").exists()
        assert (delta_path / "artifacts/file_refs").exists()
        assert (delta_path / "orchestration/executions").exists()
        assert (delta_path / "artifacts/index").exists()
        assert (delta_path / "provenance/artifact_edges").exists()

    def test_compact_table_no_error_on_missing(self, setup_paths, committer):
        """Compact table doesn't error when table doesn't exist."""
        # This should not raise and returns empty stats
        stats = committer.compact_table("artifacts/metrics")
        assert stats == {"files_added": 0, "files_removed": 0}

    def test_vacuum_table_no_error_on_missing(self, setup_paths, committer):
        """Vacuum table doesn't error when table doesn't exist."""
        # This should not raise
        committer.vacuum_table("artifacts/metrics")

    def test_compact_table_returns_stats(self, setup_paths, committer):
        """Compact table returns compaction statistics."""
        delta_path, staging_path = setup_paths

        # Create table with some data
        staging = StagingArea(staging_path, batch_id="test")
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
        committer.commit_table("artifacts/metrics")

        # Compact
        stats = committer.compact_table("artifacts/metrics")

        # Should return dict with stats keys
        assert "files_added" in stats
        assert "files_removed" in stats

    def test_compact_table_with_zorder(self, setup_paths, committer):
        """Compact table with Z-ORDER clusters data by specified column."""
        delta_path, staging_path = setup_paths

        # Create table with multiple rows to make Z-ORDER meaningful
        rows = []
        for i in range(10):
            artifact_id = f"{chr(ord('a') + i % 3)}" * 32  # 3 distinct IDs
            rows.append(
                {
                    "artifact_id": artifact_id,
                    "origin_step_number": i % 2,
                    "content": f'{{"score": {i}}}'.encode(),
                    "original_name": f"test{i}",
                    "extension": ".json",
                    "metadata": "{}",
                    "external_path": None,
                }
            )

        df = pl.DataFrame(rows, schema=METRICS_SCHEMA)
        df.write_delta(str(delta_path / "artifacts/metrics"), mode="overwrite")

        # Compact with Z-ORDER on artifact_id
        stats = committer.compact_table(
            "artifacts/metrics", z_order_columns=["artifact_id"]
        )

        # Should return stats (may or may not compact depending on file count)
        assert "files_added" in stats
        assert "files_removed" in stats

        # Verify data is still correct after Z-ORDER
        result = pl.read_delta(str(delta_path / "artifacts/metrics"))
        assert result.shape[0] == 10
        assert result["artifact_id"].n_unique() == 3

    def test_compact_all_tables_with_zorder(self, setup_paths, committer):
        """Compact all tables applies correct Z-ORDER columns."""
        delta_path, staging_path = setup_paths

        # Initialize tables to have something to compact
        committer.initialize_tables()

        # Add some data to metrics table
        staging = StagingArea(staging_path, batch_id="test")
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
        committer.commit_table("artifacts/metrics")

        # Compact all with Z-ORDER
        results = committer.compact_all_tables(z_order=True)

        # Should return dict (may be empty if no files needed compaction)
        assert isinstance(results, dict)

    def test_compact_all_tables_without_zorder(self, setup_paths, committer):
        """Compact all tables without Z-ORDER performs standard compaction."""
        delta_path, staging_path = setup_paths

        # Initialize tables
        committer.initialize_tables()

        # Compact without Z-ORDER
        results = committer.compact_all_tables(z_order=False)

        # Should return dict
        assert isinstance(results, dict)

    def test_compact_table_by_step(self, setup_paths, committer):
        """Compact table with step filter only compacts that partition."""
        delta_path, staging_path = setup_paths

        # Create table with data in multiple steps
        rows = []
        for step in [0, 1, 2]:
            for i in range(3):
                artifact_id = f"{chr(ord('a') + step * 3 + i)}" * 32
                rows.append(
                    {
                        "artifact_id": artifact_id,
                        "origin_step_number": step,
                        "content": f'{{"score": {step * 3 + i}}}'.encode(),
                        "original_name": f"test_s{step}_{i}",
                        "extension": ".json",
                        "metadata": "{}",
                        "external_path": None,
                    }
                )

        df = pl.DataFrame(rows, schema=METRICS_SCHEMA)
        df.write_delta(
            str(delta_path / "artifacts/metrics"),
            mode="overwrite",
            delta_write_options={"partition_by": ["origin_step_number"]},
        )

        # Compact only step 1
        stats = committer.compact_table(
            "artifacts/metrics",
            z_order_columns=["artifact_id"],
            step_number=1,
        )

        # Should return stats
        assert "files_added" in stats
        assert "files_removed" in stats

        # Verify all data still present
        result = pl.read_delta(str(delta_path / "artifacts/metrics"))
        assert result.shape[0] == 9
        assert set(result["origin_step_number"].unique().to_list()) == {0, 1, 2}

    def test_compact_all_tables_by_step(self, setup_paths, committer):
        """Compact all tables with step filter."""
        delta_path, staging_path = setup_paths

        # Initialize tables
        committer.initialize_tables()

        # Add data with specific step
        staging = StagingArea(staging_path, batch_id="test")
        df = pl.DataFrame(
            {
                "artifact_id": ["a" * 32],
                "origin_step_number": [5],  # specific step
                "content": [b'{"score": 0.5}'],
                "original_name": ["test"],
                "extension": [".json"],
                "metadata": ["{}"],
                "external_path": [None],
            },
            schema=METRICS_SCHEMA,
        )
        staging.stage_dataframe(df, "metrics")
        committer.commit_table("artifacts/metrics")

        # Compact all tables for step 5 only
        results = committer.compact_all_tables(z_order=True, step_number=5)

        # Should return dict
        assert isinstance(results, dict)

    def test_compact_table_step_filter_ignored_for_artifact_index(
        self, setup_paths, committer
    ):
        """Step filter is ignored for artifact_index (not partitioned by origin_step_number)."""
        delta_path, staging_path = setup_paths

        # Create artifact_index with data
        df = pl.DataFrame(
            {
                "artifact_id": ["a" * 32, "b" * 32],
                "artifact_type": ["data", "data"],
                "origin_step_number": [0, 1],
                "metadata": ["{}", "{}"],
            },
            schema=ARTIFACT_INDEX_SCHEMA,
        )
        df.write_delta(str(delta_path / "artifacts/index"), mode="overwrite")

        # Compact with step_number filter - should not fail, step_number is ignored
        stats = committer.compact_table(
            TablePath.ARTIFACT_INDEX,
            z_order_columns=["artifact_id"],
            step_number=0,  # This should be ignored
        )

        # Should return stats without error
        assert "files_added" in stats
        assert "files_removed" in stats

        # All data should still be present
        result = pl.read_delta(str(delta_path / "artifacts/index"))
        assert result.shape[0] == 2


class TestArtifactEdgesCommit:
    """Tests for artifact_edges table commits."""

    @pytest.fixture
    def setup_paths(self, tmp_path):
        """Create delta and staging directories."""
        delta_path = tmp_path / "delta"
        staging_path = tmp_path / "staging"
        delta_path.mkdir()
        staging_path.mkdir()
        return delta_path, staging_path

    @pytest.fixture
    def committer(self, setup_paths):
        """Create a DeltaCommitter instance."""
        delta_path, staging_path = setup_paths
        return DeltaCommitter(delta_path, staging_path)

    def test_artifact_edges_in_commit_order(self, setup_paths, committer):
        """artifact_edges is committed in correct order."""
        delta_path, staging_path = setup_paths

        # Stage artifact_edges data
        staging = StagingArea(staging_path, batch_id="test")
        prov_df = pl.DataFrame(
            {
                "execution_run_id": ["e" * 32],
                "source_artifact_id": ["s" * 32],
                "target_artifact_id": ["t" * 32],
                "source_artifact_type": ["data"],
                "target_artifact_type": ["metric"],
                "source_role": ["data"],
                "target_role": ["score"],
                "group_id": [None],
                "step_boundary": [True],
            },
            schema=ARTIFACT_EDGES_SCHEMA,
        )
        staging.stage_dataframe(prov_df, "artifact_edges")

        # Commit
        results = committer.commit_all_tables(cleanup_staging=False)

        assert "artifact_edges" in results
        assert results["artifact_edges"] == 1
        assert (delta_path / "provenance/artifact_edges").exists()

    def test_artifact_edges_not_partitioned(self, setup_paths, committer):
        """artifact_edges is NOT partitioned by step number."""
        delta_path, staging_path = setup_paths

        # Stage artifact_edges data
        staging = StagingArea(staging_path, batch_id="test")
        prov_df = pl.DataFrame(
            {
                "execution_run_id": ["e" * 32],
                "source_artifact_id": ["s" * 32],
                "target_artifact_id": ["t" * 32],
                "source_artifact_type": ["data"],
                "target_artifact_type": ["metric"],
                "source_role": ["data"],
                "target_role": ["score"],
                "group_id": [None],
                "step_boundary": [True],
            },
            schema=ARTIFACT_EDGES_SCHEMA,
        )
        staging.stage_dataframe(prov_df, "artifact_edges")

        committer.commit_all_tables(cleanup_staging=False)

        # Check that table exists without partition subdirectories
        table_path = delta_path / "provenance/artifact_edges"
        assert table_path.exists()

        # If partitioned, there would be origin_step_number=X directories
        partition_dirs = list(table_path.glob("origin_step_number=*"))
        assert len(partition_dirs) == 0, "artifact_edges should not be partitioned"

    def test_artifact_edges_in_commit_batch(self, setup_paths, committer):
        """artifact_edges works with commit_batch."""
        delta_path, staging_path = setup_paths

        # Stage artifact_edges data via StagingArea
        staging = StagingArea(staging_path, batch_id="test_batch")
        prov_df = pl.DataFrame(
            {
                "execution_run_id": ["e" * 32],
                "source_artifact_id": ["s" * 32],
                "target_artifact_id": ["t" * 32],
                "source_artifact_type": ["data"],
                "target_artifact_type": ["metric"],
                "source_role": ["data"],
                "target_role": ["score"],
                "group_id": [None],
                "step_boundary": [True],
            },
            schema=ARTIFACT_EDGES_SCHEMA,
        )
        staging.stage_dataframe(prov_df, "artifact_edges")

        # Commit specific batch
        results = committer.commit_batch("test_batch", cleanup_after=False)

        assert "artifact_edges" in results
        assert results["artifact_edges"] == 1

    def test_compact_step_filter_ignored_for_artifact_edges(
        self, setup_paths, committer
    ):
        """Step filter is ignored for artifact_edges (not partitioned)."""
        delta_path, staging_path = setup_paths

        # Create artifact_edges with data
        prov_df = pl.DataFrame(
            {
                "execution_run_id": ["e" * 32, "f" * 32],
                "source_artifact_id": ["s" * 32, "s2" + "x" * 30],
                "target_artifact_id": ["t" * 32, "t2" + "y" * 30],
                "source_artifact_type": ["data", "data"],
                "target_artifact_type": ["metric", "metric"],
                "source_role": ["data", "data"],
                "target_role": ["score", "accuracy"],
                "group_id": [None, None],
                "step_boundary": [True, True],
            },
            schema=ARTIFACT_EDGES_SCHEMA,
        )
        prov_df.write_delta(
            str(delta_path / "provenance/artifact_edges"), mode="overwrite"
        )

        # Compact with step_number filter - should not fail, step_number is ignored
        stats = committer.compact_table(
            TablePath.ARTIFACT_EDGES,
            z_order_columns=["source_artifact_id", "target_artifact_id"],
            step_number=0,  # This should be ignored
        )

        # Should return stats without error
        assert "files_added" in stats
        assert "files_removed" in stats

        # All data should still be present
        result = pl.read_delta(str(delta_path / "provenance/artifact_edges"))
        assert result.shape[0] == 2

    def test_artifact_edges_zorder_config(self, setup_paths, committer):
        """artifact_edges uses source_artifact_id and target_artifact_id for Z-ORDER."""
        delta_path, staging_path = setup_paths

        # Initialize tables
        committer.initialize_tables()

        # Stage artifact_edges data
        staging = StagingArea(staging_path, batch_id="test")
        prov_df = pl.DataFrame(
            {
                "execution_run_id": ["e" * 32],
                "source_artifact_id": ["s" * 32],
                "target_artifact_id": ["t" * 32],
                "source_artifact_type": ["data"],
                "target_artifact_type": ["metric"],
                "source_role": ["data"],
                "target_role": ["score"],
                "group_id": [None],
                "step_boundary": [True],
            },
            schema=ARTIFACT_EDGES_SCHEMA,
        )
        staging.stage_dataframe(prov_df, "artifact_edges")
        committer.commit_table(TablePath.ARTIFACT_EDGES)

        # Compact all tables with Z-ORDER
        results = committer.compact_all_tables(z_order=True)

        # Should run without error (Z-ORDER config includes artifact_edges)
        assert isinstance(results, dict)


class TestRecoverStaged:
    """Tests for DeltaCommitter.recover_staged()."""

    @pytest.fixture
    def setup_paths(self, tmp_path):
        """Create delta and staging directories."""
        delta_path = tmp_path / "delta"
        staging_path = tmp_path / "staging"
        delta_path.mkdir()
        staging_path.mkdir()
        return delta_path, staging_path

    @pytest.fixture
    def committer(self, setup_paths):
        """Create a DeltaCommitter instance."""
        delta_path, staging_path = setup_paths
        return DeltaCommitter(delta_path, staging_path)

    def _stage_mock_execution(
        self, staging_path, batch_id="crashed_worker", artifact_id="a" * 32
    ):
        """Stage mock execution + artifact data simulating a crashed run."""
        from artisan.storage.core.table_schemas import (
            EXECUTION_EDGES_SCHEMA,
            EXECUTIONS_SCHEMA,
        )

        staging = StagingArea(staging_path, batch_id=batch_id)

        # Stage an execution record
        exec_df = pl.DataFrame(
            {
                "execution_run_id": ["exec_001"],
                "execution_spec_id": ["spec_001"],
                "origin_step_number": [0],
                "operation_name": ["TestOp"],
                "params": ["{}"],
                "user_overrides": ["{}"],
                "timestamp_start": [None],
                "timestamp_end": [None],
                "source_worker": [0],
                "compute_backend": ["local"],
                "success": [True],
                "error": [None],
                "tool_output": [None],
                "worker_log": [None],
                "metadata": ["{}"],
            },
            schema=EXECUTIONS_SCHEMA,
        )
        staging.stage_dataframe(exec_df, "executions")

        # Stage execution edges
        edges_df = pl.DataFrame(
            {
                "execution_run_id": ["exec_001"],
                "direction": ["output"],
                "role": ["metric"],
                "artifact_id": [artifact_id],
            },
            schema=EXECUTION_EDGES_SCHEMA,
        )
        staging.stage_dataframe(edges_df, "execution_edges")

        # Stage a metric artifact
        metrics_df = pl.DataFrame(
            {
                "artifact_id": [artifact_id],
                "origin_step_number": [0],
                "content": [b'{"score": 0.5}'],
                "original_name": ["test"],
                "extension": [".json"],
                "metadata": ["{}"],
                "external_path": [None],
            },
            schema=METRICS_SCHEMA,
        )
        staging.stage_dataframe(metrics_df, "metrics")

        # Stage artifact index
        index_df = pl.DataFrame(
            {
                "artifact_id": [artifact_id],
                "artifact_type": ["metric"],
                "origin_step_number": [0],
                "metadata": ["{}"],
            },
            schema=ARTIFACT_INDEX_SCHEMA,
        )
        staging.stage_dataframe(index_df, "index")

        return staging

    def test_recover_staged_no_staging_dir(self, tmp_path):
        """Returns {} when staging dir doesn't exist."""
        delta_path = tmp_path / "delta"
        delta_path.mkdir()
        nonexistent = tmp_path / "no_such_staging"

        committer = DeltaCommitter(delta_path, nonexistent)
        result = committer.recover_staged()

        assert result == {}

    def test_recover_staged_empty_staging(self, setup_paths, committer):
        """Returns {} when staging dir exists but has no files."""
        result = committer.recover_staged()

        assert result == {}

    def test_recover_staged_commits_leftover_files(self, setup_paths, committer):
        """Stages mock data, calls recover_staged, verifies rows in Delta."""
        delta_path, staging_path = setup_paths
        self._stage_mock_execution(staging_path)

        results = committer.recover_staged(preserve_staging=True)

        assert results.get("executions") == 1
        assert results.get("execution_edges") == 1
        assert results.get("metrics") == 1
        assert results.get("index") == 1

        # Verify data is actually in Delta
        exec_table = pl.read_delta(str(delta_path / "orchestration/executions"))
        assert exec_table.shape[0] == 1
        assert exec_table["execution_run_id"][0] == "exec_001"

        metrics_table = pl.read_delta(str(delta_path / "artifacts/metrics"))
        assert metrics_table.shape[0] == 1

    def test_recover_staged_idempotent(self, setup_paths, committer):
        """Calling recover_staged twice with preserve_staging produces no duplicates."""
        delta_path, staging_path = setup_paths
        self._stage_mock_execution(staging_path)

        results1 = committer.recover_staged(preserve_staging=True)
        results2 = committer.recover_staged(preserve_staging=True)

        # First call commits rows
        assert results1.get("metrics") == 1

        # Second call: metrics deduplicated (artifact_id), so 0 new rows
        assert results2.get("metrics", 0) == 0

        # Verify no duplicates in Delta
        metrics_table = pl.read_delta(str(delta_path / "artifacts/metrics"))
        assert metrics_table.shape[0] == 1

    def test_recover_staged_cleans_up_staging(self, setup_paths, committer):
        """Staging files removed after recovery (default behavior)."""
        delta_path, staging_path = setup_paths
        self._stage_mock_execution(staging_path)

        committer.recover_staged()

        # Staging dir should have been cleaned
        remaining = list(staging_path.rglob("*.parquet"))
        assert remaining == []

    def test_recover_staged_preserves_staging(self, setup_paths, committer):
        """With preserve_staging=True, staging files remain after recovery."""
        delta_path, staging_path = setup_paths
        self._stage_mock_execution(staging_path)

        committer.recover_staged(preserve_staging=True)

        # Staging files should still exist
        remaining = list(staging_path.rglob("*.parquet"))
        assert len(remaining) > 0
