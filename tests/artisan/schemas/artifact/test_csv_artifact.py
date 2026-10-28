"""Tests for CSVArtifact and CSVTypeDef."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from artisan.errors import ApplicationRegistrationError
from artisan.schemas.artifact import ArtifactTypes
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.utils.hashing import compute_artifact_id


def _csv_bytes(text: str) -> bytes:
    return text.encode("utf-8")


class TestDraft:
    def test_draft_parses_headers(self):
        content = _csv_bytes("id,x,y,z,score\n1,2,3,4,0.5\n")
        artifact = CSVArtifact.draft(
            content=content, original_name="test.csv", step_number=0
        )
        assert artifact.columns == ["id", "x", "y", "z", "score"]

    def test_draft_counts_rows(self):
        content = _csv_bytes("a,b\n1,2\n3,4\n5,6\n")
        artifact = CSVArtifact.draft(
            content=content, original_name="test.csv", step_number=0
        )
        assert artifact.row_count == 3

    def test_draft_handles_empty_csv(self):
        artifact = CSVArtifact.draft(
            content=b"", original_name="empty.csv", step_number=0
        )
        assert artifact.columns == []
        assert artifact.row_count == 0

    def test_draft_single_row(self):
        content = _csv_bytes("x,y\n1,2\n")
        artifact = CSVArtifact.draft(
            content=content, original_name="one.csv", step_number=0
        )
        assert artifact.row_count == 1
        assert artifact.columns == ["x", "y"]

    def test_draft_sets_size_bytes(self):
        content = _csv_bytes("a\n1\n")
        artifact = CSVArtifact.draft(
            content=content, original_name="t.csv", step_number=0
        )
        assert artifact.size_bytes == len(content)

    def test_draft_strips_extension_from_name(self):
        artifact = CSVArtifact.draft(
            content=b"a\n", original_name="my_data.csv", step_number=0
        )
        assert artifact.original_name == "my_data"
        assert artifact.extension == ".csv"

    def test_draft_preserves_permissive_csv_and_original_suffix(self) -> None:
        content = b"first,second\n1\n2,3,4\n"
        artifact = CSVArtifact.draft(content, "table.txt", 0).finalize()

        assert artifact.content == content
        assert artifact.columns == ["first", "second"]
        assert artifact.row_count == 2
        assert artifact.extension == ".txt"


class TestFinalize:
    def test_finalize_produces_content_addressed_id(self):
        content = _csv_bytes("a,b\n1,2\n")
        artifact = CSVArtifact.draft(
            content=content, original_name="t.csv", step_number=0
        )
        finalized = artifact.finalize()
        assert finalized.artifact_id is not None
        assert len(finalized.artifact_id) == 32
        assert all(c in "0123456789abcdef" for c in finalized.artifact_id)

    def test_finalize_idempotent(self):
        content = _csv_bytes("a\n1\n")
        artifact = CSVArtifact.draft(
            content=content, original_name="t.csv", step_number=0
        )
        first = artifact.finalize()
        first_id = first.artifact_id
        second = first.finalize()
        assert second.artifact_id == first_id

    def test_finalize_different_content_different_id(self):
        a = CSVArtifact.draft(
            content=_csv_bytes("a\n1\n"), original_name="t.csv", step_number=0
        )
        b = CSVArtifact.draft(
            content=_csv_bytes("a\n2\n"), original_name="t.csv", step_number=0
        )
        a.finalize()
        b.finalize()
        assert a.artifact_id != b.artifact_id

    def test_finalize_uses_csv_identity_domain(self) -> None:
        content = b"value\n1\n"
        metadata = {"source": "fixture"}
        artifact = CSVArtifact.draft(content, "values.csv", 0, metadata).finalize()
        identity_metadata = {
            "metadata": metadata,
            "original_name": "values",
            "extension": ".csv",
        }

        assert artifact.artifact_type == "csv"
        assert artifact.artifact_id == compute_artifact_id(
            "csv", content, identity_metadata
        )
        assert artifact.artifact_id != compute_artifact_id(
            "data", content, identity_metadata
        )


class TestSerialization:
    def test_to_row_from_row_roundtrip(self):
        content = _csv_bytes("id,x\n1,2.5\n2,3.5\n")
        original = CSVArtifact.draft(
            content=content,
            original_name="data.csv",
            step_number=3,
            metadata={"source": "test"},
        )
        original.finalize()

        row = original.to_row()
        restored = CSVArtifact.from_row(row)

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
        artifact = CSVArtifact.draft(
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
        artifact = CSVArtifact.draft(
            content=content, original_name="out.csv", step_number=0
        )
        artifact.finalize()
        with pytest.raises(ValueError, match="does not support format conversion"):
            artifact.materialize_to(str(tmp_path), format=".tsv")


class TestTypeRegistration:
    def test_csv_type_registered(self):
        assert ArtifactTypes.is_registered("csv")
        assert ArtifactTypes.CSV == "csv"

    def test_get_model_returns_csv_artifact(self):
        assert ArtifactTypeDef.get_model("csv") is CSVArtifact

    def test_csv_storage_names_are_derived_from_registration(self) -> None:
        definition = ArtifactTypeDef.get("csv")
        assert definition.table_path == "artifacts/csvs"
        assert definition.parquet_filename() == "csvs.parquet"

    def test_retired_builtin_type_has_no_alias(self) -> None:
        assert "data" not in ArtifactTypes
        assert not hasattr(ArtifactTypes, "DATA")
        with pytest.raises(ApplicationRegistrationError, match="Unknown artifact type"):
            ArtifactTypeDef.get("data")
