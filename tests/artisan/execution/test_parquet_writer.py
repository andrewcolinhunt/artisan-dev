"""Tests for staging module.

Reference: design_provenance_phase3_execution.md
Reference: design_provenance_phase4_storage.md
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import polars as pl
import pytest

from artisan.execution.staging.parquet_writer import (
    StagingResult,
    _stage_artifact_edges,
    _stage_artifact_index,
    _stage_artifacts_by_type,
    _sync_staging_to_nfs,
    _write_execution_record,
)
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.utils.json import artisan_json_default as _json_default


@pytest.fixture
def staging_root(tmp_path):
    """Create a staging root directory."""
    staging = tmp_path / "staging"
    staging.mkdir()
    return staging


class TestStagingResult:
    """Tests for StagingResult dataclass."""

    def test_staging_result_success(self, tmp_path):
        """StagingResult with success=True."""
        result = StagingResult(
            success=True,
            staging_path=tmp_path,
            execution_run_id="a" * 32,
            artifact_ids=["b" * 32, "c" * 32],
        )

        assert result.success is True
        assert result.error is None
        assert result.staging_path == tmp_path
        assert len(result.artifact_ids) == 2

    def test_staging_result_failure(self):
        """StagingResult with success=False and error."""
        result = StagingResult(
            success=False,
            error="Something went wrong",
        )

        assert result.success is False
        assert result.error == "Something went wrong"
        assert result.staging_path is None
        assert result.artifact_ids == []


class TestSyncStagingToNfs:
    """Tests for _sync_staging_to_nfs()."""

    def test_syncs_files_and_directory(self, tmp_path):
        """Verify fsync is called on files and directory without error."""
        # Create test files
        (tmp_path / "test1.parquet").write_bytes(b"data1")
        (tmp_path / "test2.parquet").write_bytes(b"data2")

        # Should not raise - just verify it runs without error
        _sync_staging_to_nfs(tmp_path)

    def test_empty_directory(self, tmp_path):
        """Verify handles empty directory (just syncs directory)."""
        _sync_staging_to_nfs(tmp_path)  # Should not raise

    def test_skips_subdirectories(self, tmp_path):
        """Verify only syncs files, not subdirectories."""
        # Create a file and a subdirectory
        (tmp_path / "test.parquet").write_bytes(b"data")
        (tmp_path / "subdir").mkdir()

        # Should not raise - subdirectory is skipped
        _sync_staging_to_nfs(tmp_path)


class TestStageArtifactEdges:
    """Tests for _stage_artifact_edges function."""

    def test_creates_parquet_file(self, tmp_path):
        """Test that provenance edges are written to Parquet."""
        edges = [
            ArtifactProvenanceEdge(
                execution_run_id="e" * 32,
                source_artifact_id="s" * 32,
                target_artifact_id="t" * 32,
                source_artifact_type="data",
                target_artifact_type="metric",
                source_role="data",
                target_role="energy",
            )
        ]

        _stage_artifact_edges(edges, tmp_path)

        parquet_path = tmp_path / "artifact_edges.parquet"
        assert parquet_path.exists()

        df = pl.read_parquet(parquet_path)
        assert len(df) == 1
        assert df["source_artifact_id"][0] == "s" * 32
        assert df["target_artifact_id"][0] == "t" * 32

    def test_empty_edges_no_file(self, tmp_path):
        """Test that empty edges list creates no file."""
        _stage_artifact_edges([], tmp_path)

        parquet_path = tmp_path / "artifact_edges.parquet"
        assert not parquet_path.exists()

    def test_multiple_edges(self, tmp_path):
        """Test staging multiple provenance edges."""
        edges = [
            ArtifactProvenanceEdge(
                execution_run_id="e" * 32,
                source_artifact_id="s1" + "x" * 30,
                target_artifact_id="t" * 32,
                source_artifact_type="data",
                target_artifact_type="metric",
                source_role="input_struct",
                target_role="accuracy",
            ),
            ArtifactProvenanceEdge(
                execution_run_id="e" * 32,
                source_artifact_id="s2" + "y" * 30,
                target_artifact_id="t" * 32,
                source_artifact_type="data",
                target_artifact_type="metric",
                source_role="output_struct",
                target_role="accuracy",
            ),
        ]

        _stage_artifact_edges(edges, tmp_path)

        df = pl.read_parquet(tmp_path / "artifact_edges.parquet")
        assert len(df) == 2
        assert set(df["source_role"].to_list()) == {"input_struct", "output_struct"}

    def test_schema_columns(self, tmp_path):
        """Test that all expected columns are present (7 fields in v3 schema)."""
        edges = [
            ArtifactProvenanceEdge(
                execution_run_id="e" * 32,
                source_artifact_id="s" * 32,
                target_artifact_id="t" * 32,
                source_artifact_type="data",
                target_artifact_type="metric",
                source_role="data",
                target_role="energy",
            )
        ]

        _stage_artifact_edges(edges, tmp_path)

        df = pl.read_parquet(tmp_path / "artifact_edges.parquet")
        expected_columns = [
            "execution_run_id",
            "source_artifact_id",
            "target_artifact_id",
            "source_artifact_type",
            "target_artifact_type",
            "source_role",
            "target_role",
        ]
        assert len(expected_columns) == 7  # v3 schema has 7 fields
        for col in expected_columns:
            assert col in df.columns


class TestMetadataSerialization:
    """Tests for metadata serialization in staging functions.

    These tests verify that artifact metadata is correctly serialized
    to JSON when staging artifacts to Parquet files.
    """

    def test_stage_metrics_preserves_extra_metadata(self, tmp_path):
        """Verify metric artifact metadata is serialized correctly."""
        artifact = MetricArtifact.draft(
            content={"value": 1.0},
            original_name="test.json",
            step_number=1,
            metadata={"chain": "A", "resolution": 2.5},
        ).finalize()

        artifacts = {"metric": [artifact]}
        _stage_artifacts_by_type(artifacts, tmp_path)

        df = pl.read_parquet(tmp_path / "metrics.parquet")
        assert len(df) == 1

        # Verify metadata was serialized correctly
        stored_metadata = json.loads(df["metadata"][0])
        assert stored_metadata == {"chain": "A", "resolution": 2.5}

    def test_stage_metrics_empty_metadata(self, tmp_path):
        """Verify empty metadata serializes as '{}'."""
        artifact = MetricArtifact.draft(
            content={"value": 1.0},
            original_name="test.json",
            step_number=1,
        ).finalize()

        artifacts = {"metric": [artifact]}
        _stage_artifacts_by_type(artifacts, tmp_path)

        df = pl.read_parquet(tmp_path / "metrics.parquet")
        stored_metadata = json.loads(df["metadata"][0])
        assert stored_metadata == {}

    def test_stage_metrics_preserves_metadata(self, tmp_path):
        """Verify metric artifact metadata is serialized correctly."""
        artifact = MetricArtifact.draft(
            content={"score": 0.95},
            original_name="metrics.json",
            step_number=1,
            metadata={"source": "model_v2", "confidence": "high"},
        ).finalize()

        artifacts = {"metrics": [artifact]}
        _stage_artifacts_by_type(artifacts, tmp_path)

        df = pl.read_parquet(tmp_path / "metrics.parquet")
        stored_metadata = json.loads(df["metadata"][0])
        assert stored_metadata == {"source": "model_v2", "confidence": "high"}

    def test_stage_artifact_index_preserves_metadata(self, tmp_path):
        """Verify artifact index metadata is serialized correctly."""
        artifact = MetricArtifact.draft(
            content={"value": 1.0},
            original_name="test.json",
            step_number=1,
            metadata={"indexed": True},
        ).finalize()

        artifacts = {"metric": [artifact]}
        _stage_artifact_index(artifacts, step_number=1, staging_path=tmp_path)

        df = pl.read_parquet(tmp_path / "index.parquet")
        stored_metadata = json.loads(df["metadata"][0])
        assert stored_metadata == {"indexed": True}


class TestMetricOriginalNameStaging:
    """Tests for MetricArtifact.original_name staging.

    Verifies that original_name is correctly written to Parquet during staging.
    This was a bug fix - original_name existed in memory but wasn't persisted.
    """

    def test_stage_metrics_preserves_original_name(self, tmp_path):
        """Verify metric original_name is staged to Parquet."""
        artifact = MetricArtifact.draft(
            content={"score": 0.95},
            original_name="sample_001_metrics.json",
            step_number=1,
        ).finalize()

        artifacts = {"metrics": [artifact]}
        _stage_artifacts_by_type(artifacts, tmp_path)

        df = pl.read_parquet(tmp_path / "metrics.parquet")
        assert len(df) == 1
        assert df["original_name"][0] == "sample_001_metrics"  # Stem only


class TestStageExecutionConfigs:
    """Tests for _stage_configs function."""

    def test_stage_configs_writes_parquet(self, tmp_path):
        """_stage_configs writes parquet file with correct data."""
        artifact = ExecutionConfigArtifact.draft(
            content={"contig": "40-150", "length": "175-275"},
            original_name="5w3x_motif_0_config.json",
            step_number=1,
        ).finalize()

        artifacts = {"config": [artifact]}
        _stage_artifacts_by_type(artifacts, tmp_path)

        # Verify file exists
        parquet_path = tmp_path / "configs.parquet"
        assert parquet_path.exists()

        # Verify content
        df = pl.read_parquet(parquet_path)
        assert df.shape[0] == 1
        assert df["artifact_id"][0] == artifact.artifact_id
        assert df["original_name"][0] == "5w3x_motif_0_config"  # Stem only

    def test_stage_artifacts_by_type_routes_metrics(self, tmp_path):
        """_stage_artifacts_by_type writes metrics to metrics.parquet."""
        metric = MetricArtifact.draft(
            content={"value": 1.0},
            original_name="test.json",
            step_number=1,
        ).finalize()

        artifacts = {"metric": [metric]}
        _stage_artifacts_by_type(artifacts, tmp_path)

        # Metric should be written to metrics.parquet, not configs.parquet
        assert (tmp_path / "metrics.parquet").exists()
        assert not (tmp_path / "configs.parquet").exists()

    def test_stage_configs_preserves_metadata(self, tmp_path):
        """Verify execution config metadata is serialized correctly."""
        artifact = ExecutionConfigArtifact.draft(
            content={"contig": "40-150"},
            original_name="config.json",
            step_number=1,
            metadata={"tool": "tool_c", "version": "1.0"},
        ).finalize()

        artifacts = {"config": [artifact]}
        _stage_artifacts_by_type(artifacts, tmp_path)

        df = pl.read_parquet(tmp_path / "configs.parquet")
        stored_metadata = json.loads(df["metadata"][0])
        assert stored_metadata == {"tool": "tool_c", "version": "1.0"}

    def test_stage_configs_multiple_artifacts(self, tmp_path):
        """_stage_configs handles multiple artifacts from different roles."""
        artifact1 = ExecutionConfigArtifact.draft(
            content={"contig": "40-150"},
            original_name="config_0.json",
            step_number=1,
        ).finalize()
        artifact2 = ExecutionConfigArtifact.draft(
            content={"contig": "50-200"},
            original_name="config_1.json",
            step_number=1,
        ).finalize()

        artifacts = {"config1": [artifact1], "config2": [artifact2]}
        _stage_artifacts_by_type(artifacts, tmp_path)

        df = pl.read_parquet(tmp_path / "configs.parquet")
        assert df.shape[0] == 2
        assert set(df["original_name"].to_list()) == {
            "config_0",
            "config_1",
        }  # Stem only


class TestStageExecutionRecord:
    """Tests for _write_execution_record result_metadata parameter."""

    def _make_record(self, tmp_path, result_metadata=None):
        kwargs = {
            "execution_run_id": "e" * 32,
            "execution_spec_id": "s" * 32,
            "operation_name": "filter",
            "step_number": 3,
            "success": True,
            "error": None,
            "timestamp_start": datetime(2025, 1, 1, tzinfo=UTC),
            "timestamp_end": datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
            "worker_id": 0,
            "staging_path": tmp_path,
        }
        if result_metadata is not None:
            kwargs["result_metadata"] = result_metadata
        _write_execution_record(**kwargs)
        return pl.read_parquet(tmp_path / "executions.parquet")

    def test_default_metadata_is_empty_json(self, tmp_path):
        """Call without result_metadata -> metadata column is '{}'."""
        df = self._make_record(tmp_path)
        assert json.loads(df["metadata"][0]) == {}

    def test_result_metadata_persisted(self, tmp_path):
        """Pass diagnostics dict -> JSON-serialized in metadata column."""
        data = {"diagnostics": {"version": 1, "total_input": 24}}
        df = self._make_record(tmp_path, result_metadata=data)
        stored = json.loads(df["metadata"][0])
        assert stored == data

    def test_none_metadata_same_as_default(self, tmp_path):
        """Explicit None -> '{}' (same as omitting)."""
        df = self._make_record(tmp_path, result_metadata=None)
        assert json.loads(df["metadata"][0]) == {}

    def test_user_overrides_with_set_values(self, tmp_path):
        """Sets in user_overrides are serialized as sorted lists."""
        _write_execution_record(
            execution_run_id="e" * 32,
            execution_spec_id="s" * 32,
            operation_name="tool_a",
            step_number=1,
            success=True,
            error=None,
            timestamp_start=datetime(2025, 1, 1, tzinfo=UTC),
            timestamp_end=datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
            worker_id=0,
            staging_path=tmp_path,
            user_overrides={"resource_tags": {"bn1", "atp"}},
        )
        df = pl.read_parquet(tmp_path / "executions.parquet")
        stored = json.loads(df["user_overrides"][0])
        assert stored == {"resource_tags": ["atp", "bn1"]}

    def test_params_with_path_values(self, tmp_path):
        """Path objects in params are serialized as strings."""
        from pathlib import Path

        _write_execution_record(
            execution_run_id="e" * 32,
            execution_spec_id="s" * 32,
            operation_name="ingest",
            step_number=0,
            success=True,
            error=None,
            timestamp_start=datetime(2025, 1, 1, tzinfo=UTC),
            timestamp_end=datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
            worker_id=0,
            staging_path=tmp_path,
            params={"input_path": Path("/data/outputs/test.dat")},
        )
        df = pl.read_parquet(tmp_path / "executions.parquet")
        stored = json.loads(df["params"][0])
        assert stored == {"input_path": "/data/outputs/test.dat"}


class TestToolOutputColumns:
    """Tests for tool_output and worker_log columns in execution records."""

    def test_tool_output_written_to_parquet(self, tmp_path):
        """tool_output value persisted in executions.parquet."""
        _write_execution_record(
            execution_run_id="e" * 32,
            execution_spec_id="s" * 32,
            operation_name="tool_a",
            step_number=1,
            success=True,
            error=None,
            timestamp_start=datetime(2025, 1, 1, tzinfo=UTC),
            timestamp_end=datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
            worker_id=0,
            staging_path=tmp_path,
            tool_output="tool stdout content",
        )
        df = pl.read_parquet(tmp_path / "executions.parquet")
        assert df["tool_output"][0] == "tool stdout content"

    def test_worker_log_written_to_parquet(self, tmp_path):
        """worker_log value persisted in executions.parquet."""
        _write_execution_record(
            execution_run_id="e" * 32,
            execution_spec_id="s" * 32,
            operation_name="tool_a",
            step_number=1,
            success=True,
            error=None,
            timestamp_start=datetime(2025, 1, 1, tzinfo=UTC),
            timestamp_end=datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
            worker_id=0,
            staging_path=tmp_path,
            worker_log="slurm job output",
        )
        df = pl.read_parquet(tmp_path / "executions.parquet")
        assert df["worker_log"][0] == "slurm job output"

    def test_null_when_not_provided(self, tmp_path):
        """Columns are null when not provided."""
        _write_execution_record(
            execution_run_id="e" * 32,
            execution_spec_id="s" * 32,
            operation_name="tool_a",
            step_number=1,
            success=True,
            error=None,
            timestamp_start=datetime(2025, 1, 1, tzinfo=UTC),
            timestamp_end=datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
            worker_id=0,
            staging_path=tmp_path,
        )
        df = pl.read_parquet(tmp_path / "executions.parquet")
        assert df["tool_output"][0] is None
        assert df["worker_log"][0] is None


class TestJsonDefault:
    """Tests for _json_default fallback handler."""

    def test_set_to_sorted_list(self):
        assert _json_default({"c", "a", "b"}) == ["a", "b", "c"]

    def test_empty_set(self):
        assert _json_default(set()) == []

    def test_path_to_string(self):
        from pathlib import Path

        assert _json_default(Path("/tmp/test.dat")) == "/tmp/test.dat"

    def test_unsupported_type_raises(self):
        with pytest.raises(TypeError, match="object"):
            _json_default(object())
