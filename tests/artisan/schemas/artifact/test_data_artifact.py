"""Tests for DataArtifact and DataTypeDef."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from artisan.schemas.artifact import ArtifactTypes
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef


def _csv_bytes(text: str) -> bytes:
    return text.encode("utf-8")


class TestDraft:
    def test_draft_parses_headers(self):
        content = _csv_bytes("id,x,y,z,score\n1,2,3,4,0.5\n")
        artifact = DataArtifact.draft(
            content=content, original_name="test.csv", step_number=0
        )
        assert artifact.columns == ["id", "x", "y", "z", "score"]

    def test_draft_counts_rows(self):
        content = _csv_bytes("a,b\n1,2\n3,4\n5,6\n")
        artifact = DataArtifact.draft(
            content=content, original_name="test.csv", step_number=0
        )
        assert artifact.row_count == 3

    def test_draft_handles_empty_csv(self):
        artifact = DataArtifact.draft(
            content=b"", original_name="empty.csv", step_number=0
        )
        assert artifact.columns == []
        assert artifact.row_count == 0

    def test_draft_single_row(self):
        content = _csv_bytes("x,y\n1,2\n")
        artifact = DataArtifact.draft(
            content=content, original_name="one.csv", step_number=0
        )
        assert artifact.row_count == 1
        assert artifact.columns == ["x", "y"]

    def test_draft_sets_size_bytes(self):
        content = _csv_bytes("a\n1\n")
        artifact = DataArtifact.draft(
            content=content, original_name="t.csv", step_number=0
        )
        assert artifact.size_bytes == len(content)

    def test_draft_strips_extension_from_name(self):
        artifact = DataArtifact.draft(
            content=b"a\n", original_name="my_data.csv", step_number=0
        )
        assert artifact.original_name == "my_data"
        assert artifact.extension == ".csv"


class TestFinalize:
    def test_finalize_produces_content_addressed_id(self):
        content = _csv_bytes("a,b\n1,2\n")
        artifact = DataArtifact.draft(
            content=content, original_name="t.csv", step_number=0
        )
        finalized = artifact.finalize()
        assert finalized.artifact_id is not None
        assert len(finalized.artifact_id) == 32
        assert all(c in "0123456789abcdef" for c in finalized.artifact_id)

    def test_finalize_idempotent(self):
        content = _csv_bytes("a\n1\n")
        artifact = DataArtifact.draft(
            content=content, original_name="t.csv", step_number=0
        )
        first = artifact.finalize()
        first_id = first.artifact_id
        second = first.finalize()
        assert second.artifact_id == first_id

    def test_finalize_different_content_different_id(self):
        a = DataArtifact.draft(
            content=_csv_bytes("a\n1\n"), original_name="t.csv", step_number=0
        )
        b = DataArtifact.draft(
            content=_csv_bytes("a\n2\n"), original_name="t.csv", step_number=0
        )
        a.finalize()
        b.finalize()
        assert a.artifact_id != b.artifact_id


class TestSerialization:
    def test_to_row_from_row_roundtrip(self):
        content = _csv_bytes("id,x\n1,2.5\n2,3.5\n")
        original = DataArtifact.draft(
            content=content,
            original_name="data.csv",
            step_number=3,
            metadata={"source": "test"},
        )
        original.finalize()

        row = original.to_row()
        restored = DataArtifact.from_row(row)

        assert restored.artifact_id == original.artifact_id
        assert restored.origin_step_number == 3
        assert restored.content == content
        assert restored.original_name == "data"
        assert restored.extension == ".csv"
        assert restored.size_bytes == len(content)
        assert restored.columns == ["id", "x"]
        assert restored.row_count == 2
        assert restored.metadata == {"source": "test"}


class TestMaterialize:
    def test_materialize_to_writes_file(self, tmp_path: Path):
        content = _csv_bytes("a,b\n1,2\n")
        artifact = DataArtifact.draft(
            content=content, original_name="out.csv", step_number=0
        )
        artifact.finalize()
        path = artifact.materialize_to(str(tmp_path))
        expected = os.path.join(str(tmp_path), f"{artifact.artifact_id}.csv")
        assert path == expected
        with open(path, "rb") as f:
            assert f.read() == content
        assert artifact.materialized_path == path


class TestMaterializeFormat:
    def test_materialize_rejects_format(self, tmp_path: Path):
        """materialize_to() raises ValueError when format is set."""
        content = _csv_bytes("a,b\n1,2\n")
        artifact = DataArtifact.draft(
            content=content, original_name="out.csv", step_number=0
        )
        artifact.finalize()
        with pytest.raises(ValueError, match="does not support format conversion"):
            artifact.materialize_to(str(tmp_path), format=".tsv")


class TestTypeRegistration:
    def test_data_type_registered(self):
        assert ArtifactTypes.is_registered("data")
        assert ArtifactTypes.DATA == "data"

    def test_get_model_returns_data_artifact(self):
        assert ArtifactTypeDef.get_model("data") is DataArtifact
