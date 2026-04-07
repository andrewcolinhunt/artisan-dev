"""Tests for LargeFileArtifact."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from artisan.schemas.artifact.large_file import LargeFileArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.utils.hashing import compute_content_hash


def _draft(external_path: str = "/tmp/test.bin") -> LargeFileArtifact:
    """Create a standard draft for reuse across tests."""
    data = b"test binary content"
    return LargeFileArtifact.draft(
        content_hash=compute_content_hash(data),
        size_bytes=len(data),
        step_number=1,
        external_path=external_path,
        original_name="output_00000",
        extension=".bin",
    )


class TestDraft:
    """Tests for LargeFileArtifact.draft() factory."""

    def test_draft_sets_content_hash(self) -> None:
        art = _draft()
        assert art.content_hash is not None
        assert len(art.content_hash) == 32

    def test_draft_sets_size_bytes(self) -> None:
        art = _draft()
        assert art.size_bytes == len(b"test binary content")

    def test_draft_sets_external_path(self) -> None:
        art = _draft()
        assert art.external_path == "/tmp/test.bin"

    def test_draft_sets_extension(self) -> None:
        art = _draft()
        assert art.extension == ".bin"

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

    def test_different_content_different_id(self) -> None:
        art_a = LargeFileArtifact.draft(
            content_hash=compute_content_hash(b"content_a"),
            size_bytes=9,
            step_number=0,
            external_path="/tmp/same.bin",
        )
        art_b = LargeFileArtifact.draft(
            content_hash=compute_content_hash(b"content_b"),
            size_bytes=9,
            step_number=0,
            external_path="/tmp/same.bin",
        )
        art_a.finalize()
        art_b.finalize()
        assert art_a.artifact_id != art_b.artifact_id

    def test_same_content_different_path_different_id(self) -> None:
        """Same content at different external_paths produces distinct IDs."""
        art_a = _draft(external_path="/tmp/worker_0/file.bin")
        art_b = _draft(external_path="/tmp/worker_1/file.bin")
        art_a.finalize()
        art_b.finalize()
        assert art_a.artifact_id != art_b.artifact_id


class TestSerialization:
    """Tests for to_row()/from_row() round-trip."""

    def test_to_row_from_row_roundtrip(self) -> None:
        art = _draft()
        art.finalize()
        row = art.to_row()
        restored = LargeFileArtifact.from_row(row)
        assert restored.artifact_id == art.artifact_id
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
        assert set(row.keys()) == set(LargeFileArtifact.POLARS_SCHEMA.keys())


class TestMaterialize:
    """Tests for _materialize_content()."""

    def test_materialize_copies_file(self, tmp_path: Path) -> None:
        source = tmp_path / "source.bin"
        data = b"large file content here"
        source.write_bytes(data)

        art = LargeFileArtifact.draft(
            content_hash=compute_content_hash(data),
            size_bytes=len(data),
            step_number=0,
            external_path=str(source),
            extension=".bin",
        )
        art.finalize()

        out_dir = tmp_path / "output"
        out_dir.mkdir()
        result_path = art.materialize_to(str(out_dir))

        assert os.path.exists(result_path)
        with open(result_path, "rb") as f:
            assert f.read() == data

    def test_materialize_uses_artifact_id_filename(self, tmp_path: Path) -> None:
        source = tmp_path / "source.bin"
        source.write_bytes(b"data")

        art = LargeFileArtifact.draft(
            content_hash=compute_content_hash(b"data"),
            size_bytes=4,
            step_number=0,
            external_path=str(source),
            extension=".bin",
        )
        art.finalize()

        out_dir = tmp_path / "output"
        out_dir.mkdir()
        result_path = art.materialize_to(str(out_dir))

        assert os.path.basename(result_path) == f"{art.artifact_id}.bin"

    def test_materialize_raises_without_external_path(self, tmp_path: Path) -> None:
        art = LargeFileArtifact(
            artifact_id="a" * 32,
            artifact_type="large_file",
            origin_step_number=0,
            content_hash="b" * 32,
        )
        with pytest.raises(ValueError, match="external_path not set"):
            art.materialize_to(str(tmp_path))


class TestTypeRegistration:
    """Tests for artifact type registry integration."""

    def test_large_file_type_registered(self) -> None:
        assert ArtifactTypes.is_registered("large_file")

    def test_get_model_returns_large_file_artifact(self) -> None:
        assert ArtifactTypeDef.get_model("large_file") is LargeFileArtifact
