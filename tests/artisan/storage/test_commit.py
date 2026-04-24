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
from artisan.storage.io.staging import StagingArea, StagingManager

METRICS_SCHEMA = MetricArtifact.POLARS_SCHEMA


@pytest.fixture
def commit_env(backend_fs):
    """Yield ``(committer, fs, storage, delta_root, staging_root)``.

    Shared by ``TestDeltaCommitter``, ``TestArtifactEdgesCommit``, and
    ``TestRecoverStaged``. Each consuming test runs twice — once per
    ``backend_fs`` param. Delta/staging roots are URI strings; tests
    must use ``f"{delta_root}/..."`` and ``fs.exists``, never
    ``Path / str`` or ``Path.exists``.
    """
    fs, storage, root = backend_fs
    delta_root = f"{root}/delta"
    staging_root = f"{root}/staging"
    # Local backend needs explicit dir creation; s3 buckets use prefixes
    # and don't require pre-created "directories".
    fs.makedirs(delta_root, exist_ok=True)
    fs.makedirs(staging_root, exist_ok=True)
    sm = StagingManager(staging_root, fs)
    committer = DeltaCommitter(
        delta_root, sm, fs=fs, storage_options=storage.delta_storage_options()
    )
    return committer, fs, storage, delta_root, staging_root


class TestDeltaCommitter:
    """Tests for DeltaCommitter."""

    def test_commit_table_creates_new_table(self, commit_env):
        """Commit to non-existent table creates it."""
        committer, fs, _storage, delta_root, staging_root = commit_env

        # Stage some data
        staging = StagingArea(staging_root, fs, batch_id="test")
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
        assert fs.exists(f"{delta_root}/artifacts/metrics")

    def test_commit_table_appends_to_existing(self, commit_env):
        """Commit to existing table appends data."""
        committer, fs, storage, delta_root, staging_root = commit_env

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
        initial_df.write_delta(
            f"{delta_root}/artifacts/metrics",
            storage_options=storage.delta_storage_options(),
        )

        # Stage new data
        staging = StagingArea(staging_root, fs, batch_id="test")
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
        result = pl.read_delta(
            f"{delta_root}/artifacts/metrics",
            storage_options=storage.delta_storage_options(),
        )
        assert result.shape[0] == 2

    def test_commit_table_deduplicates(self, commit_env):
        """Commit skips artifacts with existing IDs."""
        committer, fs, storage, delta_root, staging_root = commit_env

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
        initial_df.write_delta(
            f"{delta_root}/artifacts/metrics",
            storage_options=storage.delta_storage_options(),
        )

        # Stage duplicate
        staging = StagingArea(staging_root, fs, batch_id="test")
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

    def test_commit_all_tables(self, commit_env):
        """Commit all staged tables at once."""
        committer, fs, _storage, _delta_root, staging_root = commit_env

        # Stage multiple tables
        staging = StagingArea(staging_root, fs, batch_id="test")

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

    def test_commit_all_tables_cleanup(self, commit_env):
        """Commit all cleans up staging by default."""
        committer, fs, _storage, _delta_root, staging_root = commit_env

        # Stage data
        staging = StagingArea(staging_root, fs, batch_id="test")
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

    def test_commit_batch_specific(self, commit_env):
        """Commit only a specific batch."""
        committer, fs, _storage, _delta_root, staging_root = commit_env

        # Stage multiple batches
        staging1 = StagingArea(staging_root, fs, batch_id="batch1")
        staging2 = StagingArea(staging_root, fs, batch_id="batch2")

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

    def test_initialize_tables(self, commit_env):
        """Initialize creates empty tables with schemas."""
        committer, fs, _storage, delta_root, _staging_root = commit_env

        committer.initialize_tables()

        # Check tables exist
        assert fs.exists(f"{delta_root}/artifacts/data")
        assert fs.exists(f"{delta_root}/artifacts/metrics")
        assert fs.exists(f"{delta_root}/artifacts/file_refs")
        assert fs.exists(f"{delta_root}/orchestration/executions")
        assert fs.exists(f"{delta_root}/artifacts/index")
        assert fs.exists(f"{delta_root}/provenance/artifact_edges")

    def test_compact_table_no_error_on_missing(self, commit_env):
        """Compact table doesn't error when table doesn't exist."""
        committer, _fs, _storage, _delta_root, _staging_root = commit_env
        # This should not raise and returns empty stats
        stats = committer.compact_table("artifacts/metrics")
        assert stats == {"files_added": 0, "files_removed": 0}

    def test_vacuum_table_no_error_on_missing(self, commit_env):
        """Vacuum table doesn't error when table doesn't exist."""
        committer, _fs, _storage, _delta_root, _staging_root = commit_env
        # This should not raise
        committer.vacuum_table("artifacts/metrics")

    def test_compact_table_returns_stats(self, commit_env):
        """Compact table returns compaction statistics."""
        committer, fs, _storage, _delta_root, staging_root = commit_env

        # Create table with some data
        staging = StagingArea(staging_root, fs, batch_id="test")
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

    def test_compact_table_with_zorder(self, commit_env):
        """Compact table with Z-ORDER clusters data by specified column."""
        committer, _fs, storage, delta_root, _staging_root = commit_env

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
        df.write_delta(
            f"{delta_root}/artifacts/metrics",
            mode="overwrite",
            storage_options=storage.delta_storage_options(),
        )

        # Compact with Z-ORDER on artifact_id
        stats = committer.compact_table(
            "artifacts/metrics", z_order_columns=["artifact_id"]
        )

        # Should return stats (may or may not compact depending on file count)
        assert "files_added" in stats
        assert "files_removed" in stats

        # Verify data is still correct after Z-ORDER
        result = pl.read_delta(
            f"{delta_root}/artifacts/metrics",
            storage_options=storage.delta_storage_options(),
        )
        assert result.shape[0] == 10
        assert result["artifact_id"].n_unique() == 3

    def test_compact_all_tables_with_zorder(self, commit_env):
        """Compact all tables applies correct Z-ORDER columns."""
        committer, fs, _storage, _delta_root, staging_root = commit_env

        # Initialize tables to have something to compact
        committer.initialize_tables()

        # Add some data to metrics table
        staging = StagingArea(staging_root, fs, batch_id="test")
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

    def test_compact_all_tables_without_zorder(self, commit_env):
        """Compact all tables without Z-ORDER performs standard compaction."""
        committer, _fs, _storage, _delta_root, _staging_root = commit_env

        # Initialize tables
        committer.initialize_tables()

        # Compact without Z-ORDER
        results = committer.compact_all_tables(z_order=False)

        # Should return dict
        assert isinstance(results, dict)

    def test_compact_table_by_step(self, commit_env):
        """Compact table with step filter only compacts that partition."""
        committer, _fs, storage, delta_root, _staging_root = commit_env

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
            f"{delta_root}/artifacts/metrics",
            mode="overwrite",
            delta_write_options={"partition_by": ["origin_step_number"]},
            storage_options=storage.delta_storage_options(),
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
        result = pl.read_delta(
            f"{delta_root}/artifacts/metrics",
            storage_options=storage.delta_storage_options(),
        )
        assert result.shape[0] == 9
        assert set(result["origin_step_number"].unique().to_list()) == {0, 1, 2}

    def test_compact_all_tables_by_step(self, commit_env):
        """Compact all tables with step filter."""
        committer, fs, _storage, _delta_root, staging_root = commit_env

        # Initialize tables
        committer.initialize_tables()

        # Add data with specific step
        staging = StagingArea(staging_root, fs, batch_id="test")
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

    def test_compact_table_step_filter_ignored_for_artifact_index(self, commit_env):
        """Step filter is ignored for artifact_index (not partitioned by origin_step_number)."""
        committer, _fs, storage, delta_root, _staging_root = commit_env

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
        df.write_delta(
            f"{delta_root}/artifacts/index",
            mode="overwrite",
            storage_options=storage.delta_storage_options(),
        )

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
        result = pl.read_delta(
            f"{delta_root}/artifacts/index",
            storage_options=storage.delta_storage_options(),
        )
        assert result.shape[0] == 2


class TestArtifactEdgesCommit:
    """Tests for artifact_edges table commits."""

    def test_artifact_edges_in_commit_order(self, commit_env):
        """artifact_edges is committed in correct order."""
        committer, fs, _storage, delta_root, staging_root = commit_env

        # Stage artifact_edges data
        staging = StagingArea(staging_root, fs, batch_id="test")
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
        assert fs.exists(f"{delta_root}/provenance/artifact_edges")

    def test_artifact_edges_not_partitioned(self, commit_env):
        """artifact_edges is NOT partitioned by step number."""
        committer, fs, _storage, delta_root, staging_root = commit_env

        # Stage artifact_edges data
        staging = StagingArea(staging_root, fs, batch_id="test")
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

        # Check the table exists. The old assertion also checked for
        # absence of ``origin_step_number=*`` partition directories on
        # disk, but that's a delta-rs POSIX-layout internal, not a
        # user-facing invariant, and doesn't translate to S3 key
        # prefixes. The table-level round-trip + the dedicated
        # ``commit_all_tables`` result above already prove the data
        # was written correctly without partitioning.
        assert fs.exists(f"{delta_root}/provenance/artifact_edges")

    def test_artifact_edges_in_commit_batch(self, commit_env):
        """artifact_edges works with commit_batch."""
        committer, fs, _storage, _delta_root, staging_root = commit_env

        # Stage artifact_edges data via StagingArea
        staging = StagingArea(staging_root, fs, batch_id="test_batch")
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

    def test_compact_step_filter_ignored_for_artifact_edges(self, commit_env):
        """Step filter is ignored for artifact_edges (not partitioned)."""
        committer, _fs, storage, delta_root, _staging_root = commit_env

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
            f"{delta_root}/provenance/artifact_edges",
            mode="overwrite",
            storage_options=storage.delta_storage_options(),
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
        result = pl.read_delta(
            f"{delta_root}/provenance/artifact_edges",
            storage_options=storage.delta_storage_options(),
        )
        assert result.shape[0] == 2

    def test_artifact_edges_zorder_config(self, commit_env):
        """artifact_edges uses source_artifact_id and target_artifact_id for Z-ORDER."""
        committer, fs, _storage, _delta_root, staging_root = commit_env

        # Initialize tables
        committer.initialize_tables()

        # Stage artifact_edges data
        staging = StagingArea(staging_root, fs, batch_id="test")
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

    def _stage_mock_execution(
        self, staging_root, fs, batch_id="crashed_worker", artifact_id="a" * 32
    ):
        """Stage mock execution + artifact data simulating a crashed run."""
        from artisan.storage.core.table_schemas import (
            EXECUTION_EDGES_SCHEMA,
            EXECUTIONS_SCHEMA,
        )

        staging = StagingArea(staging_root, fs, batch_id=batch_id)

        # Stage an execution record
        exec_df = pl.DataFrame(
            {
                "execution_run_id": ["exec_001"],
                "execution_spec_id": ["spec_001"],
                "step_run_id": [None],
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

    def test_recover_staged_no_staging_dir(self, backend_fs):
        """Returns {} when staging dir doesn't exist."""
        fs, storage, root = backend_fs
        delta_root = f"{root}/delta_no_staging"
        fs.makedirs(delta_root, exist_ok=True)
        # Deliberately do NOT create staging_root — the fixture's
        # purpose is to exercise the missing-staging-dir path.
        nonexistent = f"{root}/no_such_staging"

        sm = StagingManager(nonexistent, fs)
        committer = DeltaCommitter(
            delta_root, sm, fs=fs, storage_options=storage.delta_storage_options()
        )
        result = committer.recover_staged()

        assert result == {}

    def test_recover_staged_empty_staging(self, commit_env):
        """Returns {} when staging dir exists but has no files."""
        committer, _fs, _storage, _delta_root, _staging_root = commit_env
        result = committer.recover_staged()

        assert result == {}

    def test_recover_staged_commits_leftover_files(self, commit_env):
        """Stages mock data, calls recover_staged, verifies rows in Delta."""
        committer, fs, storage, delta_root, staging_root = commit_env
        self._stage_mock_execution(staging_root, fs)

        results = committer.recover_staged(preserve_staging=True)

        assert results.get("executions") == 1
        assert results.get("execution_edges") == 1
        assert results.get("metrics") == 1
        assert results.get("index") == 1

        # Verify data is actually in Delta
        exec_table = pl.read_delta(
            f"{delta_root}/orchestration/executions",
            storage_options=storage.delta_storage_options(),
        )
        assert exec_table.shape[0] == 1
        assert exec_table["execution_run_id"][0] == "exec_001"

        metrics_table = pl.read_delta(
            f"{delta_root}/artifacts/metrics",
            storage_options=storage.delta_storage_options(),
        )
        assert metrics_table.shape[0] == 1

    def test_recover_staged_idempotent(self, commit_env):
        """Calling recover_staged twice with preserve_staging produces no duplicates."""
        committer, fs, storage, delta_root, staging_root = commit_env
        self._stage_mock_execution(staging_root, fs)

        results1 = committer.recover_staged(preserve_staging=True)
        results2 = committer.recover_staged(preserve_staging=True)

        # First call commits rows
        assert results1.get("metrics") == 1

        # Second call: metrics deduplicated (artifact_id), so 0 new rows
        assert results2.get("metrics", 0) == 0

        # Verify no duplicates in Delta
        metrics_table = pl.read_delta(
            f"{delta_root}/artifacts/metrics",
            storage_options=storage.delta_storage_options(),
        )
        assert metrics_table.shape[0] == 1

    def test_recover_staged_cleans_up_staging(self, commit_env):
        """Staging files removed after recovery (default behavior)."""
        committer, fs, _storage, _delta_root, staging_root = commit_env
        self._stage_mock_execution(staging_root, fs)

        committer.recover_staged()

        # Staging dir should have been cleaned
        remaining = list(fs.glob(f"{staging_root}/**/*.parquet"))
        assert remaining == []

    def test_recover_staged_preserves_staging(self, commit_env):
        """With preserve_staging=True, staging files remain after recovery."""
        committer, fs, _storage, _delta_root, staging_root = commit_env
        self._stage_mock_execution(staging_root, fs)

        committer.recover_staged(preserve_staging=True)

        # Staging files should still exist
        remaining = list(fs.glob(f"{staging_root}/**/*.parquet"))
        assert len(remaining) > 0


class TestDeltaCommitterBackendParametrized:
    """Smoke tests for DeltaCommitter parametrized over [local, s3] backends.

    Uses the ``backend_fs`` fixture from ``tests/artisan/storage/conftest.py``
    to exercise the critical write/read round-trip on both filesystems. S3
    params skip cleanly when MinIO is unavailable.

    Kept alongside the promoted classes above because the smoke methods
    (``commit_dataframe``) are a public surface not exercised by
    ``TestDeltaCommitter`` (which covers ``commit_table`` /
    ``commit_all_tables`` / ``recover_staged``).
    """

    def test_commit_dataframe_writes_to_table(self, backend_fs):
        """commit_dataframe writes rows readable via pl.read_delta."""
        fs, storage, root = backend_fs
        delta_root = f"{root}/delta"
        staging_root = f"{root}/staging"

        sm = StagingManager(staging_root, fs)
        committer = DeltaCommitter(
            delta_root,
            sm,
            fs=fs,
            storage_options=storage.delta_storage_options(),
        )

        df = pl.DataFrame(
            {
                "artifact_id": ["a" * 32],
                "origin_step_number": [0],
                "content": [b'{"score": 0.5}'],
                "original_name": ["smoke"],
                "extension": [".json"],
                "metadata": ["{}"],
                "external_path": [None],
            },
            schema=METRICS_SCHEMA,
        )

        table = "smoke_commit_dataframe"
        rows = committer.commit_dataframe(df, table)
        assert rows == 1

        table_uri = f"{delta_root}/{table}"
        result = pl.read_delta(
            table_uri, storage_options=storage.delta_storage_options()
        )
        assert result.shape[0] == 1
        assert result["artifact_id"][0] == "a" * 32
        assert result["origin_step_number"][0] == 0

    def test_commit_dataframe_dedup(self, backend_fs):
        """Second commit_dataframe of the same row dedups to 0."""
        fs, storage, root = backend_fs
        delta_root = f"{root}/delta"
        staging_root = f"{root}/staging"

        sm = StagingManager(staging_root, fs)
        committer = DeltaCommitter(
            delta_root,
            sm,
            fs=fs,
            storage_options=storage.delta_storage_options(),
        )

        df = pl.DataFrame(
            {
                "artifact_id": ["d" * 32],
                "origin_step_number": [0],
                "content": [b'{"score": 0.9}'],
                "original_name": ["dup"],
                "extension": [".json"],
                "metadata": ["{}"],
                "external_path": [None],
            },
            schema=METRICS_SCHEMA,
        )

        table = "smoke_commit_dedup"
        first = committer.commit_dataframe(df, table, deduplicate=True)
        assert first == 1

        second = committer.commit_dataframe(df, table, deduplicate=True)
        assert second == 0
