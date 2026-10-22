"""Tests for complete-file identity, verified I/O, and URI auto-inference."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any, cast

import fsspec
import pytest
from fsspec.implementations.memory import MemoryFileSystem

from artisan.errors import ArtifactIntegrityError
from artisan.schemas.artifact import external
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.utils.hashing import STREAM_CHUNK_BYTES


def _draft_at(path: str, content: bytes = b"hello") -> FileRefArtifact:
    """Create a finalized FileRefArtifact pointing at the given path."""
    from artisan.utils.hashing import compute_content_digest

    return cast(
        FileRefArtifact,
        FileRefArtifact.draft(
            path=path,
            content_hash=compute_content_digest(content),
            size_bytes=len(content),
            step_number=0,
            original_name="x",
            extension="",
        ).finalize(),
    )


class TestReadContentAutoInfer:
    """When fs=None, read_content infers fs from self.path via url_to_fs."""

    def test_auto_infers_memory_fs_from_uri(self):
        """memory:// URI → MemoryFileSystem auto-resolved."""
        mem_fs = cast(MemoryFileSystem, fsspec.filesystem("memory"))
        with mem_fs.open("/test_file_ref/auto/data.bin", "wb") as f:
            f.write(b"auto-inferred")

        artifact = _draft_at("memory:///test_file_ref/auto/data.bin", b"auto-inferred")
        assert artifact.read_content() == b"auto-inferred"

        mem_fs.rm("/test_file_ref/auto/data.bin")

    def test_explicit_fs_overrides_inference(self):
        """When fs= is passed, it's used directly without resolve_fs."""
        mem_fs = cast(MemoryFileSystem, fsspec.filesystem("memory"))
        with mem_fs.open("/test_file_ref/explicit/data.bin", "wb") as f:
            f.write(b"explicit")

        # Path stored as a stripped key (no protocol prefix); explicit
        # fs uses the path as-is.
        artifact = _draft_at("test_file_ref/explicit/data.bin", b"explicit")
        assert artifact.read_content(fs=mem_fs) == b"explicit"

        mem_fs.rm("/test_file_ref/explicit/data.bin")

    def test_cached_content_returned_on_second_call(self):
        """First read populates cache; second call returns cached bytes."""
        mem_fs = cast(MemoryFileSystem, fsspec.filesystem("memory"))
        with mem_fs.open("/test_file_ref/cached/data.bin", "wb") as f:
            f.write(b"first-read")

        artifact = _draft_at("memory:///test_file_ref/cached/data.bin", b"first-read")
        assert artifact.read_content() == b"first-read"

        # Mutate the underlying file — cached bytes should still be returned.
        with mem_fs.open("/test_file_ref/cached/data.bin", "wb") as f:
            f.write(b"changed-on-disk")
        assert artifact.read_content() == b"first-read"

        mem_fs.rm("/test_file_ref/cached/data.bin")


def test_different_file_bytes_produce_different_identity() -> None:
    first = _draft_at("/same/file.bin", b"first")
    second = _draft_at("/same/file.bin", b"other")
    assert first.artifact_id != second.artifact_id


def test_materialize_infers_memory_filesystem(tmp_path: Path) -> None:
    uri = "memory:///test_file_ref/materialize/blob.bin"
    fs = fsspec.filesystem("memory")
    fs.pipe(uri, b"memory-bytes")
    try:
        artifact = _draft_at(uri, b"memory-bytes")
        destination = Path(artifact.materialize_to(str(tmp_path)))
        assert destination.read_bytes() == b"memory-bytes"
        assert destination.name == "blob.bin"
        assert artifact.materialized_path == str(destination)
    finally:
        fs.rm(uri)


def test_materialize_requires_location(tmp_path: Path) -> None:
    artifact = _draft_at("/unused/file.bin")
    artifact.path = None
    with pytest.raises(ValueError, match="not hydrated"):
        artifact.materialize_to(str(tmp_path))
    assert not list(tmp_path.iterdir())


def test_verify_and_materialize_use_bounded_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    content = b"x" * (2 * STREAM_CHUNK_BYTES + 1)
    reads: list[int] = []

    class BoundedReader(BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            assert size is not None
            assert 0 < size <= STREAM_CHUNK_BYTES
            reads.append(size)
            return super().read(size)

    def open_source(uri: str, fs: Any = None) -> BoundedReader:
        return BoundedReader(content)

    monkeypatch.setattr(external, "open_external", open_source)
    artifact = _draft_at("/source/file.bin", content)
    artifact.verify_external_content()
    destination = artifact.materialize_to(str(tmp_path))
    assert Path(destination).read_bytes() == content
    assert len(reads) == 8  # Three chunks plus EOF, for each of two passes.
    assert artifact._cached_content is None


@pytest.mark.parametrize("changed", [b"other", b"shorter-or-longer"])
def test_materialize_rejects_changed_bytes_and_removes_partial_file(
    tmp_path: Path, changed: bytes
) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(changed)
    artifact = _draft_at(str(source), b"first")
    destination = tmp_path / "destination"
    with pytest.raises(ArtifactIntegrityError):
        artifact.materialize_to(str(destination))
    assert not list(destination.rglob("*.bin"))


def test_complete_file_type_registration() -> None:
    assert ArtifactTypeDef.get_model("file_ref") is FileRefArtifact
    assert not ArtifactTypes.is_registered("large_file")
