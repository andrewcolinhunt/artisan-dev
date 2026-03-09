"""Unit tests for the IngestData curator operation.

Tests cover:
1. Basic file ingestion via execute_curator
2. Multiple file processing
3. Error handling (empty input)
4. external_path propagation
5. Class attributes (name, description, input/output specs)

Note: IngestData returns ArtifactResult with draft DataArtifacts.
"""

from __future__ import annotations

import csv
import io
import random
from unittest.mock import Mock

import polars as pl
import pytest

from artisan.operations.curator import IngestData
from artisan.schemas.artifact import FileRefArtifact


def _df(ids: list[str]) -> pl.DataFrame:
    return pl.DataFrame({"artifact_id": ids})


def make_file_ref(
    path: str,
    content_hash: str = "a" * 32,
    size_bytes: int = 100,
    step_number: int = 0,
    original_name: str | None = None,
    extension: str | None = None,
) -> FileRefArtifact:
    """Helper to create a finalized FileRefArtifact for testing."""
    from pathlib import Path as P

    if original_name is None:
        original_name = P(path).stem
    if extension is None:
        extension = P(path).suffix
    return FileRefArtifact.draft(
        path=path,
        content_hash=content_hash,
        size_bytes=size_bytes,
        step_number=step_number,
        original_name=original_name,
        extension=extension,
    ).finalize()


def _mock_store_with_refs(file_refs: list[FileRefArtifact]) -> Mock:
    store = Mock()
    store.get_artifacts_by_type.return_value = {fr.artifact_id: fr for fr in file_refs}
    return store


def _make_csv(rows: int = 5, seed: int = 42) -> bytes:
    """Generate test CSV content."""
    rng = random.Random(seed)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id", "x", "y", "z", "score"])
    for i in range(rows):
        writer.writerow(
            [
                i,
                round(rng.uniform(0.0, 10.0), 4),
                round(rng.uniform(0.0, 10.0), 4),
                round(rng.uniform(0.0, 10.0), 4),
                round(rng.uniform(0.0, 1.0), 4),
            ]
        )
    return buf.getvalue().encode("utf-8")


class TestIngestDataBasicExecution:
    """Tests for basic IngestData execute_curator() execution."""

    def test_should_ingest_single_csv_file(self, tmp_path):
        """Test should successfully ingest a single CSV file."""
        content = _make_csv(rows=3, seed=42)
        path = tmp_path / "data.csv"
        path.write_bytes(content)

        file_ref = make_file_ref(str(path))

        op = IngestData()
        store = _mock_store_with_refs([file_ref])
        result = op.execute_curator(
            inputs={"file": _df([file_ref.artifact_id])},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert len(result.artifacts["data"]) == 1

        artifact = result.artifacts["data"][0]
        assert artifact.content == content
        assert artifact.original_name == "data"
        assert artifact.extension == ".csv"
        assert artifact.size_bytes == len(content)
        assert artifact.is_draft
        assert artifact.columns == ["id", "x", "y", "z", "score"]
        assert artifact.row_count == 3

    def test_should_ingest_multiple_csv_files(self, tmp_path):
        """Test should successfully ingest multiple CSV files."""
        files = []
        for i in range(3):
            content = _make_csv(rows=5, seed=100 + i)
            path = tmp_path / f"data_{i}.csv"
            path.write_bytes(content)
            files.append(make_file_ref(str(path), content_hash=f"{chr(97 + i)}" * 32))

        op = IngestData()
        store = _mock_store_with_refs(files)
        result = op.execute_curator(
            inputs={"file": _df([f.artifact_id for f in files])},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert len(result.artifacts["data"]) == 3
        names = [a.original_name for a in result.artifacts["data"]]
        assert "data_0" in names
        assert "data_1" in names
        assert "data_2" in names


class TestIngestDataErrorHandling:
    """Tests for error handling."""

    def test_should_fail_for_empty_input(self):
        """Test should fail when no files provided."""
        op = IngestData()
        result = op.execute_curator(
            inputs={"file": _df([])},
            step_number=0,
            artifact_store=Mock(),
        )

        assert not result.success
        assert "No input files" in result.error

    def test_should_fail_for_missing_file_key(self):
        """Test should fail when 'file' key is missing."""
        op = IngestData()
        result = op.execute_curator(
            inputs={},
            step_number=0,
            artifact_store=Mock(),
        )

        assert not result.success
        assert "No input files" in result.error


class TestIngestDataExternalPath:
    """Tests for external_path propagation."""

    def test_should_set_external_path_from_file_ref(self, tmp_path):
        """Test output artifact should have external_path from FileRefArtifact.path."""
        content = _make_csv(rows=2, seed=42)
        path = tmp_path / "data.csv"
        path.write_bytes(content)

        file_ref = make_file_ref(str(path))

        op = IngestData()
        store = _mock_store_with_refs([file_ref])
        result = op.execute_curator(
            inputs={"file": _df([file_ref.artifact_id])},
            step_number=0,
            artifact_store=store,
        )

        artifact = result.artifacts["data"][0]
        assert artifact.external_path == str(path)


class TestIngestDataClassAttributes:
    """Tests for IngestData class attributes."""

    def test_should_have_correct_name(self):
        """Test IngestData has correct operation name."""
        assert IngestData.name == "ingest_data"

    def test_should_have_description(self):
        """Test IngestData has a description."""
        assert IngestData.description is not None
        assert "import files" in IngestData.description.lower()

    def test_should_have_file_input_spec(self):
        """Test IngestData has 'file' input specification."""
        assert "file" in IngestData.inputs
        assert IngestData.inputs["file"].required is True

    def test_should_have_data_output_spec(self):
        """Test IngestData has 'data' output specification."""
        assert "data" in IngestData.outputs
        assert IngestData.outputs["data"].artifact_type == "data"

    def test_should_have_resource_config(self):
        """Test IngestData has resource configuration."""
        op = IngestData()
        assert op.resources.cpus == 1
        assert op.resources.time_limit == "00:10:00"
        assert op.execution.job_name == "ingest"


class TestIngestDataOutputFormat:
    """Tests for output format correctness."""

    def test_output_should_contain_all_required_fields(self, tmp_path):
        """Test output artifact should contain all required fields."""
        content = _make_csv(rows=2, seed=42)
        path = tmp_path / "data.csv"
        path.write_bytes(content)

        file_ref = make_file_ref(str(path))

        op = IngestData()
        store = _mock_store_with_refs([file_ref])
        result = op.execute_curator(
            inputs={"file": _df([file_ref.artifact_id])},
            step_number=0,
            artifact_store=store,
        )

        artifact = result.artifacts["data"][0]
        assert artifact.content is not None
        assert artifact.original_name is not None
        assert artifact.extension is not None
        assert artifact.size_bytes is not None
        assert artifact.columns is not None
        assert artifact.row_count is not None
        assert artifact.is_draft

    def test_output_artifacts_preserve_filenames(self, tmp_path):
        """Test artifacts preserve original filenames."""
        content = _make_csv(rows=2, seed=42)
        path = tmp_path / "my_dataset.csv"
        path.write_bytes(content)

        file_ref = make_file_ref(str(path))

        op = IngestData()
        store = _mock_store_with_refs([file_ref])
        result = op.execute_curator(
            inputs={"file": _df([file_ref.artifact_id])},
            step_number=0,
            artifact_store=store,
        )

        assert result.artifacts["data"][0].original_name == "my_dataset"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
