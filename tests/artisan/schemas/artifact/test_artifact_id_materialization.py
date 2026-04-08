"""Tests for artifact-ID-based materialization filenames.

Verifies that DataArtifact, MetricArtifact, and ExecutionConfigArtifact
materialize using artifact_id as the filename stem, preventing collisions
when parallel workers produce artifacts with the same original_name.
FileRefArtifact is excluded (uses path-based materialization).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact


def _csv_bytes(text: str) -> bytes:
    return text.encode("utf-8")


class TestDataArtifactMaterialization:
    """DataArtifact materializes with artifact_id filename."""

    def test_materialize_uses_artifact_id(self, tmp_path: Path):
        content = _csv_bytes("a,b\n1,2\n")
        artifact = DataArtifact.draft(
            content=content, original_name="my_data.csv", step_number=0
        )
        artifact.finalize()

        path = artifact.materialize_to(str(tmp_path))

        assert os.path.basename(path) == f"{artifact.artifact_id}.csv"
        with open(path, "rb") as f:
            assert f.read() == content

    def test_materialize_preserves_extension(self, tmp_path: Path):
        content = _csv_bytes("a\tb\n1\t2\n")
        artifact = DataArtifact.draft(
            content=content, original_name="data.tsv", step_number=0
        )
        artifact.finalize()

        path = artifact.materialize_to(str(tmp_path))

        assert os.path.basename(path) == f"{artifact.artifact_id}.tsv"

    def test_materialize_default_csv_extension(self, tmp_path: Path):
        """When extension is None, defaults to .csv."""
        artifact = DataArtifact(
            artifact_type="data",
            content=b"a\n1\n",
            original_name="test",
            origin_step_number=0,
            size_bytes=4,
            extension=None,
        )
        artifact.finalize()

        path = artifact.materialize_to(str(tmp_path))

        assert os.path.basename(path) == f"{artifact.artifact_id}.csv"

    def test_materialize_requires_finalized(self, tmp_path: Path):
        """Draft artifacts cannot be materialized (no artifact_id)."""
        artifact = DataArtifact.draft(
            content=_csv_bytes("a\n1\n"), original_name="test.csv", step_number=0
        )
        with pytest.raises(ValueError, match="not finalized"):
            artifact.materialize_to(str(tmp_path))

    def test_no_collision_with_duplicate_original_name(self, tmp_path: Path):
        """Two artifacts with same original_name get different filenames."""
        a = DataArtifact.draft(
            content=_csv_bytes("a\n1\n"), original_name="output.csv", step_number=0
        )
        b = DataArtifact.draft(
            content=_csv_bytes("a\n2\n"), original_name="output.csv", step_number=0
        )
        a.finalize()
        b.finalize()

        path_a = a.materialize_to(str(tmp_path))
        path_b = b.materialize_to(str(tmp_path))

        assert path_a != path_b
        assert os.path.exists(path_a) and os.path.exists(path_b)


class TestMetricArtifactMaterialization:
    """MetricArtifact materializes with artifact_id filename."""

    def test_materialize_uses_artifact_id(self, tmp_path: Path):
        artifact = MetricArtifact.draft(
            content={"score": 0.95},
            original_name="metrics.json",
            step_number=0,
        )
        artifact.finalize()

        path = artifact.materialize_to(str(tmp_path))

        assert os.path.basename(path) == f"{artifact.artifact_id}.json"
        with open(path, "rb") as f:
            parsed = json.loads(f.read())
        assert parsed["score"] == 0.95

    def test_materialize_preserves_extension(self, tmp_path: Path):
        artifact = MetricArtifact.draft(
            content={"val": 1},
            original_name="stats.jsonl",
            step_number=0,
        )
        artifact.finalize()

        path = artifact.materialize_to(str(tmp_path))

        assert os.path.basename(path) == f"{artifact.artifact_id}.jsonl"

    def test_materialize_requires_finalized(self, tmp_path: Path):
        """Draft MetricArtifact cannot be materialized."""
        artifact = MetricArtifact.draft(
            content={"score": 0.5},
            original_name="test.json",
            step_number=0,
        )
        with pytest.raises(ValueError, match="not finalized"):
            artifact.materialize_to(str(tmp_path))

    def test_no_collision_with_duplicate_original_name(self, tmp_path: Path):
        """Two metrics with same original_name get different filenames."""
        a = MetricArtifact.draft(
            content={"score": 0.1}, original_name="score.json", step_number=0
        )
        b = MetricArtifact.draft(
            content={"score": 0.9}, original_name="score.json", step_number=0
        )
        a.finalize()
        b.finalize()

        path_a = a.materialize_to(str(tmp_path))
        path_b = b.materialize_to(str(tmp_path))

        assert path_a != path_b
        assert os.path.exists(path_a) and os.path.exists(path_b)


class TestExecutionConfigArtifactMaterialization:
    """ExecutionConfigArtifact materializes with artifact_id filename."""

    def test_materialize_uses_artifact_id(self, tmp_path: Path):
        artifact = ExecutionConfigArtifact.draft(
            content={"key": "value"},
            original_name="config.json",
            step_number=0,
        )
        artifact.finalize()

        path = artifact.materialize_to(str(tmp_path))

        assert os.path.basename(path) == f"{artifact.artifact_id}.json"

    def test_materialize_preserves_extension(self, tmp_path: Path):
        artifact = ExecutionConfigArtifact.draft(
            content={"k": "v"},
            original_name="settings.yaml",
            step_number=0,
        )
        artifact.finalize()

        path = artifact.materialize_to(str(tmp_path))

        assert os.path.basename(path) == f"{artifact.artifact_id}.yaml"


class TestFileRefArtifactExcluded:
    """FileRefArtifact still uses path-based materialization, not artifact_id."""

    def test_materialize_uses_original_path_name(self, tmp_path: Path):
        """FileRefArtifact materializes using the original file path name."""
        source_file = tmp_path / "source" / "my_file.dat"
        source_file.parent.mkdir(parents=True)
        source_file.write_bytes(b"content")

        artifact = FileRefArtifact.draft(
            path=str(source_file),
            content_hash="a" * 32,
            size_bytes=7,
            step_number=0,
        )
        artifact.finalize()

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        path = artifact.materialize_to(str(output_dir))

        # FileRefArtifact uses os.path.basename(self.path), not artifact_id
        assert os.path.basename(path) == "my_file.dat"
