"""Tests for `resolve_fs` two-step protocol-match rule."""

from __future__ import annotations

from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem

from artisan.schemas.execution.storage_config import StorageConfig
from artisan.utils.fs_resolve import resolve_fs


class TestResolveFsProtocolMatch:
    """Step 1: protocol-match → use storage.filesystem()."""

    def test_matching_protocol_uses_storage_filesystem(self):
        """When path protocol matches storage.protocol, use storage's fs.

        We use the in-memory filesystem here (no Docker/S3 needed) and
        verify the returned fs is the *same instance* fsspec caches by
        protocol+kwargs — proving it came from `storage.filesystem()`,
        not `url_to_fs`.
        """
        storage = StorageConfig(protocol="memory")
        # Pre-populate the cache so we can identity-compare.
        cached_fs = storage.filesystem()

        fs, stripped = resolve_fs("memory://bucket/key", storage)

        assert fs is cached_fs  # fsspec caches by (protocol, kwargs)
        assert stripped == "bucket/key"

    def test_local_path_with_local_storage_uses_storage(self):
        """Bare local path + protocol='file' storage → storage path."""
        storage = StorageConfig()  # protocol="file"
        fs, stripped = resolve_fs("/tmp/file.txt", storage)
        assert isinstance(fs, LocalFileSystem)
        assert stripped == "/tmp/file.txt"


class TestResolveFsFallthrough:
    """Step 2: protocol-mismatch (or no storage) → fall through to url_to_fs."""

    def test_protocol_mismatch_falls_through(self):
        """s3:// path + file:// storage → url_to_fs path (no storage match).

        Routing through `url_to_fs` for cross-protocol inputs is what
        lets a local pipeline ingest from S3.
        """
        storage = StorageConfig()  # protocol="file"
        fs, stripped = resolve_fs("memory://other-bucket/key", storage)
        # url_to_fs returned a memory fs (bypassed local storage)
        assert isinstance(fs, MemoryFileSystem)
        assert stripped == "/other-bucket/key"

    def test_storage_none_falls_through(self):
        """storage=None always falls through to url_to_fs.

        This is the path artifacts take — they have no StorageConfig
        back-reference, so step 1 is unavailable.
        """
        fs, stripped = resolve_fs("memory://no-storage/key", None)
        assert isinstance(fs, MemoryFileSystem)
        assert stripped == "/no-storage/key"

    def test_bare_local_path_no_storage_uses_local(self):
        """Bare local path with no storage → LocalFileSystem via url_to_fs."""
        fs, stripped = resolve_fs("/tmp/x.txt", None)
        assert isinstance(fs, LocalFileSystem)
        # url_to_fs leaves local paths unchanged
        assert stripped == "/tmp/x.txt"


class TestResolveFsRoundTrip:
    """Functional smoke: write+read via resolved fs proves the path is usable."""

    def test_write_and_read_via_resolved_fs(self):
        """Resolved (fs, stripped) is immediately usable for I/O."""
        storage = StorageConfig(protocol="memory")
        fs, stripped = resolve_fs("memory://round-trip/data.bin", storage)
        with fs.open(stripped, "wb") as f:
            f.write(b"payload")
        with fs.open(stripped, "rb") as f:
            assert f.read() == b"payload"
        # Cleanup so other tests don't see leftovers in the in-memory fs.
        fs.rm(stripped)
