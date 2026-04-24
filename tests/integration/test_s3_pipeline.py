"""End-to-end pipeline run against S3-compatible storage (MinIO).

This is the gate that proves all six s3-readiness PRs hold together:

- PR 1's ``StorageConfig.delta_options`` reaches delta-rs.
- PR 2's logs/failure_logs derivations stay local under cloud delta.
- PR 3's MinIO fixture provides a working backend.
- PR 4's cloud-URI inputs ingest via the two-step ``resolve_fs`` rule.
- PR 5's storage layer round-trips against s3.

Marked ``integration``; skips cleanly when MinIO is unavailable
(via the ``s3_pipeline_env`` fixture chain).
"""

from __future__ import annotations

import pathlib

import polars as pl
import pytest

pytestmark = pytest.mark.integration

from artisan.operations.examples import (
    DataGenerator,
    LargeFileGenerator,
    MetricCalculator,
)
from artisan.orchestration import PipelineManager
from artisan.orchestration.backends import Backend
from artisan.schemas.orchestration.pipeline_config import PipelineConfig


def _make_pipeline(env: dict, name: str) -> PipelineManager:
    """Build a PipelineManager from the cloud env dict."""
    config = PipelineConfig(
        name=name,
        delta_root=env["delta_root"],
        staging_root=env["staging_root"],
        working_root=env["working_root"],
        files_root=env["files_root"],
        storage=env["storage"],
    )
    return PipelineManager(config, configure_logging=False)


class TestS3PipelineEndToEnd:
    """A creator → curator pipeline runs end-to-end on MinIO."""

    def test_full_pipeline_writes_delta_tables_on_s3(self, s3_pipeline_env):
        """DataGenerator → MetricCalculator on MinIO; all Delta tables land in s3."""
        env = s3_pipeline_env
        pipeline = _make_pipeline(env, "s3_e2e")

        gen = pipeline.run(
            DataGenerator,
            params={"count": 2, "rows_per_file": 5, "seed": 7},
            backend=Backend.LOCAL,
        )
        pipeline.run(
            MetricCalculator,
            inputs={"dataset": gen.output("datasets")},
            backend=Backend.LOCAL,
        )
        pipeline.finalize()

        # Verify the Delta tables landed under the s3 bucket prefix.
        storage_options = env["storage"].delta_storage_options()
        index_uri = f"{env['delta_root']}/artifacts/index"
        index_df = pl.read_delta(index_uri, storage_options=storage_options)

        # DataGenerator emits 2 data artifacts; MetricCalculator emits
        # 2 metric artifacts. Index is the union.
        assert index_df.height >= 4, (
            f"Expected ≥4 artifacts in index, got {index_df.height}"
        )
        types = set(index_df["artifact_type"].to_list())
        assert "data" in types, f"Expected 'data' in {types}"
        assert "metric" in types, f"Expected 'metric' in {types}"

        # Verify the executions table was committed.
        exec_uri = f"{env['delta_root']}/orchestration/executions"
        exec_df = pl.read_delta(exec_uri, storage_options=storage_options)
        assert exec_df.height >= 2  # one per step (curator) or per worker (creator)
        assert all(exec_df["success"].to_list())

    def test_recover_staging_does_not_crash_on_cloud_failure_logs(
        self, s3_pipeline_env
    ):
        """PipelineManager(recover_staging=True) constructs cleanly on cloud delta.

        Validates PR 2's failure-logs-root fix in the cloud context: the
        manager initializes without crashing on `os.makedirs(s3://...)`.
        Without PR 2, this would fail before any step runs.
        """
        env = s3_pipeline_env
        config = PipelineConfig(
            name="s3_recover_staging",
            delta_root=env["delta_root"],
            staging_root=env["staging_root"],
            working_root=env["working_root"],
            files_root=env["files_root"],
            recover_staging=True,
            storage=env["storage"],
        )
        # Just constructing must not crash.
        pipeline = PipelineManager(config, configure_logging=False)
        pipeline.finalize()

    def test_cloud_uri_input_ingestion(self, s3_pipeline_env):
        """`input_files=["s3://bucket/...", ...]` round-trips end-to-end.

        Validates PR 4's cloud-URI ingestion: the user passes s3:// URIs,
        `_promote_file_paths_to_store` resolves the per-path fs via the
        two-step rule (matching `config.storage.protocol="s3"` so
        `storage.filesystem()` is used — no env-var leakage), and the
        files become `FileRefArtifact`s in the Delta `file_refs` table.
        """
        env = s3_pipeline_env
        fs = env["fs"]
        uri_prefix = env["uri_prefix"]

        # Seed two CSV files into the bucket as user "raw input".
        for i in range(2):
            stripped = f"{uri_prefix.split('://', 1)[1]}/raw/data_{i}.csv"
            with fs.open(stripped, "wb") as f:
                f.write(f"x,y\n{i},{i + 1}\n{i + 2},{i + 3}\n".encode())

        from artisan.operations.curator import IngestData

        pipeline = _make_pipeline(env, "s3_ingest")
        pipeline.run(
            IngestData,
            inputs=[
                f"{uri_prefix}/raw/data_0.csv",
                f"{uri_prefix}/raw/data_1.csv",
            ],
            backend=Backend.LOCAL,
        )
        pipeline.finalize()

        # Verify two FileRefArtifacts landed with the s3:// URIs preserved.
        storage_options = env["storage"].delta_storage_options()
        file_refs_uri = f"{env['delta_root']}/artifacts/file_refs"
        file_refs_df = pl.read_delta(file_refs_uri, storage_options=storage_options)
        assert file_refs_df.height == 2
        # Stored paths should be s3:// URIs (not abspath'd local paths).
        for path in file_refs_df["path"].to_list():
            assert path.startswith("s3://"), f"Expected s3:// path, got {path!r}"

    def test_large_file_outputs_uploaded_to_files_root(self, s3_pipeline_env):
        """LargeFileGenerator on cloud files_root uploads via fs.put and rewrites external_path.

        Without the upload step, the operation writes bytes into the
        local sandbox files_dir, records that local path as
        external_path, and downstream materialization fails because
        the bytes don't exist at the path the consumer resolves. This
        test is the regression guard for PR 7 (files_root cloud
        uploads).
        """
        env = s3_pipeline_env
        pipeline = _make_pipeline(env, "s3_large_file")

        pipeline.run(
            LargeFileGenerator,
            params={"count": 2, "file_size_bytes": 1024, "seed": 0},
            backend=Backend.LOCAL,
        )
        pipeline.finalize()

        # Read the LargeFileArtifact rows from Delta.
        storage_options = env["storage"].delta_storage_options()
        table_uri = f"{env['delta_root']}/artifacts/large_files"
        df = pl.read_delta(table_uri, storage_options=storage_options)
        assert df.height == 2

        # external_path must be cloud URIs; bytes must live on MinIO.
        fs = env["fs"]
        for ext_path in df["external_path"].to_list():
            assert ext_path.startswith("s3://"), (
                f"Expected cloud external_path, got {ext_path!r}"
            )
            assert fs.exists(ext_path), (
                f"external_path {ext_path!r} does not resolve on the fixture fs"
            )

        # No new literal-colon leak from this run under the repo root.
        # (Pre-existing dirs from before PR 7 may still be present; this
        # check only asserts nothing new was created inside one today.)
        repo_root = pathlib.Path(__file__).resolve().parents[2]
        leaked = repo_root / "s3:"
        if leaked.exists():
            import time

            cutoff = time.time() - 600  # anything newer than 10 minutes ago
            fresh = [p for p in leaked.rglob("*") if p.stat().st_mtime > cutoff]
            assert not fresh, f"Fresh literal-colon leak inside {leaked}: {fresh!r}"
