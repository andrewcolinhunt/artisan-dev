"""Tests for RecordBundleArtifact."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from artisan.schemas.artifact.record_bundle import RecordBundleArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.utils.hashing import compute_content_hash


def _make_jsonl(path: Path, records: list[dict]) -> None:
    """Write records to a JSONL file."""
    with path.open("w") as f:
        for rec in records:
            f.write(json.dumps(rec, sort_keys=True) + "\n")


def _draft(external_path: str = "/tmp/test.jsonl") -> RecordBundleArtifact:
    """Create a standard draft for reuse across tests."""
    line = json.dumps({"record_id": "rec_000000", "values": {"x": 1.0}}, sort_keys=True)
    return RecordBundleArtifact.draft(
        record_id="rec_000000",
        content_hash=compute_content_hash(line.encode()),
        size_bytes=len(line.encode()),
        step_number=1,
        external_path=external_path,
        original_name="rec_000000",
    )


class TestDraft:
    """Tests for RecordBundleArtifact.draft() factory."""

    def test_draft_sets_record_id(self) -> None:
        art = _draft()
        assert art.record_id == "rec_000000"

    def test_draft_sets_content_hash(self) -> None:
        art = _draft()
        assert art.content_hash is not None
        assert len(art.content_hash) == 32

    def test_draft_sets_size_bytes(self) -> None:
        art = _draft()
        assert art.size_bytes is not None
        assert art.size_bytes > 0

    def test_draft_sets_external_path(self) -> None:
        art = _draft()
        assert art.external_path == "/tmp/test.jsonl"

    def test_draft_sets_step_number(self) -> None:
        art = _draft()
        assert art.origin_step_number == 1

    def test_draft_artifact_id_is_none(self) -> None:
        art = _draft()
        assert art.artifact_id is None
        assert art.is_draft


class TestFinalize:
    """Tests for content-addressed finalization."""

    def test_finalize_produces_content_addressed_id(self) -> None:
        art = _draft()
        art.finalize()
        assert art.artifact_id is not None
        assert len(art.artifact_id) == 32

    def test_finalize_idempotent(self) -> None:
        art = _draft()
        art.finalize()
        first_id = art.artifact_id
        art.finalize()
        assert art.artifact_id == first_id

    def test_different_record_id_different_artifact_id(self) -> None:
        line_a = json.dumps({"record_id": "a", "values": {}}, sort_keys=True)
        line_b = json.dumps({"record_id": "b", "values": {}}, sort_keys=True)
        art_a = RecordBundleArtifact.draft(
            record_id="a",
            content_hash=compute_content_hash(line_a.encode()),
            size_bytes=len(line_a.encode()),
            step_number=0,
            external_path="/tmp/same.jsonl",
        )
        art_b = RecordBundleArtifact.draft(
            record_id="b",
            content_hash=compute_content_hash(line_b.encode()),
            size_bytes=len(line_b.encode()),
            step_number=0,
            external_path="/tmp/same.jsonl",
        )
        art_a.finalize()
        art_b.finalize()
        assert art_a.artifact_id != art_b.artifact_id

    def test_same_record_different_path_different_id(self) -> None:
        """Same content at different external_paths produces distinct IDs."""
        art_a = _draft(external_path="/tmp/worker_0.jsonl")
        art_b = _draft(external_path="/tmp/combined.jsonl")
        art_a.finalize()
        art_b.finalize()
        assert art_a.artifact_id != art_b.artifact_id


class TestSerialization:
    """Tests for to_row()/from_row() round-trip."""

    def test_to_row_from_row_roundtrip(self) -> None:
        art = _draft()
        art.finalize()
        row = art.to_row()
        restored = RecordBundleArtifact.from_row(row)
        assert restored.artifact_id == art.artifact_id
        assert restored.record_id == art.record_id
        assert restored.content_hash == art.content_hash
        assert restored.size_bytes == art.size_bytes
        assert restored.original_name == art.original_name
        assert restored.extension == art.extension
        assert restored.external_path == art.external_path
        assert restored.origin_step_number == art.origin_step_number

    def test_to_row_includes_all_schema_keys(self) -> None:
        art = _draft()
        art.finalize()
        row = art.to_row()
        assert set(row.keys()) == set(RecordBundleArtifact.POLARS_SCHEMA.keys())

    def test_from_row_handles_none_metadata(self) -> None:
        art = _draft()
        art.finalize()
        row = art.to_row()
        row["metadata"] = None
        restored = RecordBundleArtifact.from_row(row)
        assert restored.metadata == {}


class TestMaterialize:
    """Tests for _materialize_content()."""

    def test_materialize_extracts_record_to_json(self, tmp_path: Path) -> None:
        records = [
            {"record_id": "rec_000000", "values": {"x": 1.0}},
            {"record_id": "rec_000001", "values": {"x": 2.0}},
        ]
        jsonl_path = tmp_path / "bundle.jsonl"
        _make_jsonl(jsonl_path, records)

        art = _draft(external_path=str(jsonl_path))
        art.finalize()
        out_dir = tmp_path / "output"
        out_dir.mkdir(parents=True)
        result_path = art.materialize_to(out_dir)

        assert result_path.exists()
        data = json.loads(result_path.read_text())
        assert data["record_id"] == "rec_000000"
        assert data["values"]["x"] == 1.0

    def test_materialize_raises_without_external_path(self, tmp_path: Path) -> None:
        art = RecordBundleArtifact(
            artifact_id="a" * 32,
            artifact_type="record_bundle",
            origin_step_number=0,
            record_id="rec_000000",
            content_hash="b" * 32,
        )
        with pytest.raises(ValueError, match="external_path not set"):
            art.materialize_to(tmp_path)

    def test_materialize_raises_for_missing_record(self, tmp_path: Path) -> None:
        jsonl_path = tmp_path / "bundle.jsonl"
        _make_jsonl(jsonl_path, [{"record_id": "other", "values": {}}])

        art = RecordBundleArtifact(
            artifact_id="a" * 32,
            artifact_type="record_bundle",
            origin_step_number=0,
            record_id="not_found",
            content_hash="b" * 32,
            external_path=str(jsonl_path),
        )
        with pytest.raises(ValueError, match="not_found"):
            art.materialize_to(tmp_path / "out")


class TestTypeRegistration:
    """Tests for artifact type registry integration."""

    def test_record_bundle_type_registered(self) -> None:
        assert ArtifactTypes.is_registered("record_bundle")

    def test_get_model_returns_record_bundle_artifact(self) -> None:
        assert ArtifactTypeDef.get_model("record_bundle") is RecordBundleArtifact
