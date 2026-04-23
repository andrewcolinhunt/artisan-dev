"""Tests for FileRefArtifact.read_content fs auto-inference.

Uses ``MemoryFileSystem`` so these tests don't depend on MinIO or the
local disk — they can run on any worker without Docker.
"""

from __future__ import annotations

from typing import cast

import fsspec
from fsspec.implementations.memory import MemoryFileSystem

from artisan.schemas.artifact.file_ref import FileRefArtifact


def _draft_at(path: str, content: bytes = b"hello") -> FileRefArtifact:
    """Create a finalized FileRefArtifact pointing at the given path."""
    from artisan.utils.hashing import compute_content_hash

    return cast(
        FileRefArtifact,
        FileRefArtifact.draft(
            path=path,
            content_hash=compute_content_hash(content),
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

        artifact = _draft_at("memory:///test_file_ref/auto/data.bin")
        assert artifact.read_content() == b"auto-inferred"

        mem_fs.rm("/test_file_ref/auto/data.bin")

    def test_explicit_fs_overrides_inference(self):
        """When fs= is passed, it's used directly without resolve_fs."""
        mem_fs = cast(MemoryFileSystem, fsspec.filesystem("memory"))
        with mem_fs.open("/test_file_ref/explicit/data.bin", "wb") as f:
            f.write(b"explicit")

        # Path stored as a stripped key (no protocol prefix); explicit
        # fs uses the path as-is.
        artifact = _draft_at("/test_file_ref/explicit/data.bin")
        assert artifact.read_content(fs=mem_fs) == b"explicit"

        mem_fs.rm("/test_file_ref/explicit/data.bin")

    def test_cached_content_returned_on_second_call(self):
        """First read populates cache; second call returns cached bytes."""
        mem_fs = cast(MemoryFileSystem, fsspec.filesystem("memory"))
        with mem_fs.open("/test_file_ref/cached/data.bin", "wb") as f:
            f.write(b"first-read")

        artifact = _draft_at("memory:///test_file_ref/cached/data.bin")
        assert artifact.read_content() == b"first-read"

        # Mutate the underlying file — cached bytes should still be returned.
        with mem_fs.open("/test_file_ref/cached/data.bin", "wb") as f:
            f.write(b"changed-on-disk")
        assert artifact.read_content() == b"first-read"

        mem_fs.rm("/test_file_ref/cached/data.bin")
