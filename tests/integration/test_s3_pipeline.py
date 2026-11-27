"""End-to-end pipeline run against S3-compatible storage (MinIO).

Covers explicit storage options, cloud input ingestion, external files and
committed Delta tables with local worker execution.

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
    FileGenerator,
    MetricCalculator,
)
from artisan.orchestration import PipelineManager
from artisan.orchestration.runners import LocalRunner


def _make_pipeline(env: dict, name: str) -> PipelineManager:
    """Build a PipelineManager from the cloud env dict."""
    return PipelineManager.create(
        name=name,
        delta_root=env["delta_root"],
        staging_root=env["staging_root"],
        working_root=env["working_root"],
        files_root=env["files_root"],
        storage=env["storage"],
        default_step_runner=LocalRunner(),
    )


class TestS3PipelineEndToEnd:
    """Creator pipelines persist their results on MinIO."""

    def test_full_pipeline_writes_delta_tables_on_s3(self, s3_pipeline_env):
        """DataGenerator → MetricCalculator on MinIO; all Delta tables land in s3."""
        env = s3_pipeline_env
        pipeline = _make_pipeline(env, "s3_e2e")

        gen = pipeline.run(
            DataGenerator,
            params={"count": 2, "rows_per_file": 5, "seed": 7},
            step_runner=LocalRunner(),
        )
        pipeline.run(
            MetricCalculator,
            inputs={"dataset": gen.output("datasets")},
            step_runner=LocalRunner(),
        )
        pipeline.finalize()

        storage_options = env["storage"].delta_storage_options()
        index_uri = f"{env['delta_root']}/artifacts/index"
        index_df = pl.read_delta(index_uri, storage_options=storage_options)

        assert index_df.height >= 4, (
            f"Expected ≥4 artifacts in index, got {index_df.height}"
        )
        types = set(index_df["artifact_type"].to_list())
        assert "csv" in types, f"Expected 'csv' in {types}"
        assert "metric" in types, f"Expected 'metric' in {types}"

        from artisan.orchestration.engine.retained_execution_query import (
            read_retained_execution_membership,
        )
        from artisan.storage.core.execution_source import ScientificExecutionSource

        members = read_retained_execution_membership(
            pipeline.committer, pipeline_run_id=pipeline.config.pipeline_run_id
        )
        assert len(members) >= 2
        assert all(
            isinstance(member.source, ScientificExecutionSource) for member in members
        )

    def test_manager_initializes_with_cloud_failure_logs(self, s3_pipeline_env):
        """PipelineManager constructs cleanly with cloud persistence roots."""
        env = s3_pipeline_env
        pipeline = _make_pipeline(env, "s3_manager_init")
        pipeline.finalize()

    def test_cloud_uri_input_ingestion(self, s3_pipeline_env):
        """`inputs=["s3://bucket/...", ...]` round-trips end-to-end.

        The user passes s3:// URIs,
        `_promote_file_paths_to_store` resolves the per-path fs via the
        two-step rule (matching `config.storage.protocol="s3"` so
        `storage.filesystem()` is used — no env-var leakage), and the
        files become `FileRefArtifact`s in the Delta `file_refs` table.
        """
        env = s3_pipeline_env
        fs = env["fs"]
        uri_prefix = env["uri_prefix"]

        for i in range(2):
            stripped = f"{uri_prefix.split('://', 1)[1]}/raw/data_{i}.csv"
            with fs.open(stripped, "wb") as f:
                f.write(f"x,y\n{i},{i + 1}\n{i + 2},{i + 3}\n".encode())

        from artisan.operations.curator import IngestCSV

        pipeline = _make_pipeline(env, "s3_ingest")
        pipeline.run(
            IngestCSV,
            inputs=[
                f"{uri_prefix}/raw/data_0.csv",
                f"{uri_prefix}/raw/data_1.csv",
            ],
            step_runner=LocalRunner(),
        )
        result = pipeline.finalize()
        assert result["overall_success"]

        storage_options = env["storage"].delta_storage_options()
        file_refs_uri = f"{env['delta_root']}/artifacts/file_refs"
        file_refs_df = pl.read_delta(file_refs_uri, storage_options=storage_options)
        assert file_refs_df.height == 2
        locations_uri = f"{env['delta_root']}/artifacts/locations"
        locations_df = pl.read_delta(locations_uri, storage_options=storage_options)
        file_locations = locations_df.join(
            file_refs_df.select("artifact_id"), on="artifact_id", how="inner"
        )
        assert file_locations.height == 2
        for uri in file_locations["uri"].to_list():
            assert uri.startswith("s3://"), f"Expected s3:// URI, got {uri!r}"

    def test_file_outputs_uploaded_to_files_root(self, s3_pipeline_env):
        """FileGenerator on cloud files_root uploads via fs.put and rewrites path.

        Without the upload step, the operation writes bytes into the
        local sandbox files_dir, records that local path as
        path, and downstream materialization fails because
        the bytes don't exist at the path the consumer resolves. This
        test verifies managed cloud carrier publication.
        """
        env = s3_pipeline_env
        pipeline = _make_pipeline(env, "s3_file")

        pipeline.run(
            FileGenerator,
            params={"count": 2, "file_size_bytes": 1024, "seed": 0},
            step_runner=LocalRunner(),
        )
        result = pipeline.finalize()
        assert result["overall_success"]

        storage_options = env["storage"].delta_storage_options()
        table_uri = f"{env['delta_root']}/artifacts/file_refs"
        df = pl.read_delta(table_uri, storage_options=storage_options)
        assert df.height == 2

        locations_uri = f"{env['delta_root']}/artifacts/locations"
        locations_df = pl.read_delta(locations_uri, storage_options=storage_options)
        file_locations = locations_df.join(
            df.select("artifact_id"), on="artifact_id", how="inner"
        )
        assert file_locations.height == 2
        fs = env["fs"]
        for uri in file_locations["uri"].to_list():
            assert uri.startswith("s3://"), f"Expected cloud URI, got {uri!r}"
            assert fs.exists(uri), f"Artifact location {uri!r} is not readable"

        repo_root = pathlib.Path(__file__).resolve().parents[2]
        leaked = repo_root / "s3:"
        if leaked.exists():
            import time

            cutoff = time.time() - 600
            fresh = [p for p in leaked.rglob("*") if p.stat().st_mtime > cutoff]
            assert not fresh, f"Fresh literal-colon leak inside {leaked}: {fresh!r}"
