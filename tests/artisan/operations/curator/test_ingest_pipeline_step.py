"""Unit tests for the IngestPipelineStep curator operation.

Tests cover:
1. Importing artifacts from a source step
2. Filtering by artifact type
3. Empty step / missing path error handling
4. Import metadata propagation
5. Multiple artifact types at a step
6. Source path validation
"""

from __future__ import annotations

from unittest.mock import Mock

import polars as pl
import pytest

from artisan.operations.curator.ingest_pipeline_step import IngestPipelineStep
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import get_schema


def _mock_store() -> Mock:
    """Create a mock ArtifactStore."""
    return Mock()


def setup_source_store(
    base_path,
    data_rows: list[dict] | None = None,
    metrics: list[dict] | None = None,
    index_entries: list[dict] | None = None,
):
    """Helper to create a Delta Lake store with test data."""
    if data_rows:
        path = base_path / "artifacts/data"
        pl.DataFrame(data_rows, schema=DataArtifact.POLARS_SCHEMA).write_delta(
            str(path)
        )

    if metrics:
        path = base_path / "artifacts/metrics"
        pl.DataFrame(metrics, schema=MetricArtifact.POLARS_SCHEMA).write_delta(
            str(path)
        )

    if index_entries:
        path = base_path / "artifacts/index"
        pl.DataFrame(
            index_entries, schema=get_schema(TablePath.ARTIFACT_INDEX)
        ).write_delta(str(path))


def make_data_row(artifact_id: str, step_number: int, content: bytes = b"test") -> dict:
    """Create a data artifact data dict."""
    return {
        "artifact_id": artifact_id,
        "origin_step_number": step_number,
        "content": content,
        "original_name": "test",
        "extension": ".csv",
        "size_bytes": len(content),
        "columns": None,
        "row_count": None,
        "metadata": "{}",
        "external_path": None,
    }


def make_metric_data(artifact_id: str, step_number: int, value: float = 1.0) -> dict:
    """Create a metric artifact data dict."""
    import json

    content = json.dumps({"test_metric": value}, sort_keys=True).encode("utf-8")
    return {
        "artifact_id": artifact_id,
        "origin_step_number": step_number,
        "content": content,
        "original_name": "test_metric",
        "extension": None,
        "metadata": "{}",
        "external_path": None,
    }


def make_index_entry(artifact_id: str, artifact_type: str, step_number: int) -> dict:
    """Create an artifact_index entry."""
    return {
        "artifact_id": artifact_id,
        "artifact_type": artifact_type,
        "origin_step_number": step_number,
        "metadata": "{}",
    }


class TestIngestPipelineStepBasic:
    """Tests for basic artifact import."""

    def test_should_import_data_from_source_step(self, tmp_path):
        """Test importing data artifacts from a source step."""
        source_root = tmp_path / "source_delta"
        aid = "a" * 32

        setup_source_store(
            source_root,
            data_rows=[make_data_row(aid, step_number=2, content=b"csv data")],
            index_entries=[make_index_entry(aid, "data", step_number=2)],
        )

        op = IngestPipelineStep(source_delta_root=source_root, source_step=2)
        result = op.execute_curator(
            inputs={}, step_number=5, artifact_store=_mock_store()
        )

        assert result.success
        assert "data" in result.artifacts
        assert len(result.artifacts["data"]) == 1

        artifact = result.artifacts["data"][0]
        assert artifact.content == b"csv data"
        assert artifact.origin_step_number == 5
        assert artifact.is_finalized

    def test_should_import_multiple_artifacts(self, tmp_path):
        """Test importing multiple artifacts from same step."""
        source_root = tmp_path / "source_delta"
        aid_a = "a" * 32
        aid_b = "b" * 32

        setup_source_store(
            source_root,
            data_rows=[
                make_data_row(aid_a, step_number=1, content=b"data1"),
                make_data_row(aid_b, step_number=1, content=b"data2"),
            ],
            index_entries=[
                make_index_entry(aid_a, "data", step_number=1),
                make_index_entry(aid_b, "data", step_number=1),
            ],
        )

        op = IngestPipelineStep(source_delta_root=source_root, source_step=1)
        result = op.execute_curator(
            inputs={}, step_number=5, artifact_store=_mock_store()
        )

        assert result.success
        assert len(result.artifacts["data"]) == 2

    def test_should_preserve_content_and_compute_same_artifact_id(self, tmp_path):
        """Importing same content produces same artifact_id (content-addressed)."""
        source_root = tmp_path / "source_delta"
        content = b"deterministic content"

        # Create a DataArtifact draft to get the expected artifact_id
        original = DataArtifact.draft(
            content=content, original_name="test.csv", step_number=2
        ).finalize()

        setup_source_store(
            source_root,
            data_rows=[
                make_data_row(original.artifact_id, step_number=2, content=content)
            ],
            index_entries=[
                make_index_entry(original.artifact_id, "data", step_number=2)
            ],
        )

        op = IngestPipelineStep(source_delta_root=source_root, source_step=2)
        result = op.execute_curator(
            inputs={}, step_number=10, artifact_store=_mock_store()
        )

        imported = result.artifacts["data"][0]
        # Same content → same artifact_id
        assert imported.artifact_id == original.artifact_id
        assert imported.origin_step_number == 10


class TestIngestPipelineStepTypeFilter:
    """Tests for artifact type filtering."""

    def test_should_filter_by_artifact_type(self, tmp_path):
        """Test filtering to a specific artifact type."""
        source_root = tmp_path / "source_delta"
        data_id = "a" * 32
        metric_id = "b" * 32

        setup_source_store(
            source_root,
            data_rows=[make_data_row(data_id, step_number=1)],
            metrics=[make_metric_data(metric_id, step_number=1)],
            index_entries=[
                make_index_entry(data_id, "data", step_number=1),
                make_index_entry(metric_id, "metric", step_number=1),
            ],
        )

        op = IngestPipelineStep(
            source_delta_root=source_root,
            source_step=1,
            artifact_type="data",
        )
        result = op.execute_curator(
            inputs={}, step_number=5, artifact_store=_mock_store()
        )

        assert result.success
        assert "data" in result.artifacts
        assert "metric" not in result.artifacts
        assert len(result.artifacts["data"]) == 1

    def test_should_import_all_types_when_no_filter(self, tmp_path):
        """Test importing all types when artifact_type is None."""
        source_root = tmp_path / "source_delta"
        data_id = "a" * 32
        metric_id = "b" * 32

        setup_source_store(
            source_root,
            data_rows=[make_data_row(data_id, step_number=1)],
            metrics=[make_metric_data(metric_id, step_number=1)],
            index_entries=[
                make_index_entry(data_id, "data", step_number=1),
                make_index_entry(metric_id, "metric", step_number=1),
            ],
        )

        op = IngestPipelineStep(source_delta_root=source_root, source_step=1)
        result = op.execute_curator(
            inputs={}, step_number=5, artifact_store=_mock_store()
        )

        assert result.success
        assert "data" in result.artifacts
        assert "metric" in result.artifacts

    def test_should_only_import_from_specified_step(self, tmp_path):
        """Test that only artifacts from the specified step are imported."""
        source_root = tmp_path / "source_delta"
        step1_id = "a" * 32
        step2_id = "b" * 32

        setup_source_store(
            source_root,
            data_rows=[
                make_data_row(step1_id, step_number=1, content=b"step1"),
                make_data_row(step2_id, step_number=2, content=b"step2"),
            ],
            index_entries=[
                make_index_entry(step1_id, "data", step_number=1),
                make_index_entry(step2_id, "data", step_number=2),
            ],
        )

        op = IngestPipelineStep(source_delta_root=source_root, source_step=2)
        result = op.execute_curator(
            inputs={}, step_number=5, artifact_store=_mock_store()
        )

        assert result.success
        assert len(result.artifacts["data"]) == 1
        assert result.artifacts["data"][0].content == b"step2"


class TestIngestPipelineStepErrorHandling:
    """Tests for error handling."""

    def test_should_fail_for_nonexistent_source_path(self, tmp_path):
        """Test failure when source_delta_root does not exist."""
        fake_path = tmp_path / "nonexistent"

        op = IngestPipelineStep(source_delta_root=fake_path, source_step=0)
        result = op.execute_curator(
            inputs={}, step_number=5, artifact_store=_mock_store()
        )

        assert not result.success
        assert "does not exist" in result.error

    def test_should_fail_for_empty_step(self, tmp_path):
        """Test failure when source step has no artifacts."""
        source_root = tmp_path / "source_delta"
        aid = "a" * 32

        # Artifacts at step 1 only
        setup_source_store(
            source_root,
            data_rows=[make_data_row(aid, step_number=1)],
            index_entries=[make_index_entry(aid, "data", step_number=1)],
        )

        # Request step 99 which has nothing
        op = IngestPipelineStep(source_delta_root=source_root, source_step=99)
        result = op.execute_curator(
            inputs={}, step_number=5, artifact_store=_mock_store()
        )

        assert not result.success
        assert "No artifacts found" in result.error

    def test_should_fail_for_missing_type_at_step(self, tmp_path):
        """Test failure when requested type doesn't exist at step."""
        source_root = tmp_path / "source_delta"
        aid = "a" * 32

        setup_source_store(
            source_root,
            data_rows=[make_data_row(aid, step_number=1)],
            index_entries=[make_index_entry(aid, "data", step_number=1)],
        )

        op = IngestPipelineStep(
            source_delta_root=source_root,
            source_step=1,
            artifact_type="metric",
        )
        result = op.execute_curator(
            inputs={}, step_number=5, artifact_store=_mock_store()
        )

        assert not result.success
        assert "No artifacts found" in result.error
        assert "metric" in result.error


class TestIngestPipelineStepMetadata:
    """Tests for metadata propagation on imported artifacts."""

    def test_should_set_imported_from_step_metadata(self, tmp_path):
        """Test that imported artifacts have imported_from_step in metadata."""
        source_root = tmp_path / "source_delta"
        aid = "a" * 32

        setup_source_store(
            source_root,
            data_rows=[make_data_row(aid, step_number=3)],
            index_entries=[make_index_entry(aid, "data", step_number=3)],
        )

        op = IngestPipelineStep(source_delta_root=source_root, source_step=3)
        result = op.execute_curator(
            inputs={}, step_number=7, artifact_store=_mock_store()
        )

        artifact = result.artifacts["data"][0]
        assert artifact.metadata["imported_from_step"] == 3
        assert artifact.origin_step_number == 7


class TestIngestPipelineStepClassAttributes:
    """Tests for class attributes."""

    def test_should_have_correct_name(self):
        """Test operation name."""
        assert IngestPipelineStep.name == "ingest_pipeline_step"

    def test_should_have_description(self):
        """Test operation has description."""
        assert IngestPipelineStep.description is not None
        assert "import" in IngestPipelineStep.description.lower()

    def test_should_have_empty_inputs(self):
        """Test generative pattern: no inputs."""
        assert IngestPipelineStep.inputs == {}

    def test_should_have_empty_outputs(self):
        """Test dynamic pattern: no declared outputs."""
        assert IngestPipelineStep.outputs == {}

    def test_should_require_source_delta_root(self):
        """Test source_delta_root is a required field."""
        with pytest.raises(Exception):
            IngestPipelineStep(source_step=0)

    def test_should_require_source_step(self):
        """Test source_step is a required field."""
        with pytest.raises(Exception):
            IngestPipelineStep(source_delta_root="/tmp/test")
