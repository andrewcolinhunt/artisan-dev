"""Completed-file publication bounds transfer buffers and preserves evidence."""

from __future__ import annotations

import errno
import os
from io import BytesIO

import pytest
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem

from artisan.errors import StoreIntegrityError
from artisan.storage.io import publication
from artisan.utils.hashing import STREAM_CHUNK_BYTES


class _Source(MemoryFileSystem):
    """Observe source reads and optionally fail after the verification pass."""

    cachable = False  # codespell:ignore cachable

    def __init__(self, data, *, fail_copy=False):
        super().__init__()
        self.data = data
        self.fail_copy = fail_copy
        self.opens = 0
        self.read_sizes = []

    def open(self, path, mode="rb", **kwargs):
        assert mode == "rb"
        assert kwargs["cache_type"] == "none"
        self.opens += 1
        copying = self.opens > 1
        outer = self

        class Reader(BytesIO):
            def read(self, size=-1):
                assert 0 < size <= STREAM_CHUNK_BYTES
                outer.read_sizes.append(size)
                if (
                    copying
                    and outer.fail_copy
                    and self.tell() >= 6 * STREAM_CHUNK_BYTES
                ):
                    msg = "source read interrupted"
                    raise OSError(msg)
                return super().read(size)

        return Reader(self.data)


def test_file_publication_retries_identical_content_preserving_source(
    backend_fs, tmp_path
):
    fs, _, root = backend_fs
    source = tmp_path / "completed.parquet"
    source.write_bytes(os.urandom(6 * STREAM_CHUNK_BYTES + 37))
    expected = source.read_bytes()
    destination = f"{root}/effects/data.parquet"
    for _ in range(2):
        publication.publish_immutable_file(
            fs, destination, str(source), source_fs=LocalFileSystem()
        )
        assert source.read_bytes() == expected
    with fs.open(destination, "rb") as stream:
        assert stream.read() == expected
    conflict = tmp_path / "different.parquet"
    conflict.write_bytes(b"different")
    with pytest.raises(StoreIntegrityError, match="Conflicting immutable"):
        publication.publish_immutable_file(
            fs, destination, str(conflict), source_fs=LocalFileSystem()
        )
    assert conflict.read_bytes() == b"different"


def test_copy_uses_bounded_reads_and_leaves_source_available(backend_fs):
    fs, _, root = backend_fs
    source = _Source(b"x" * (7 * STREAM_CHUNK_BYTES + 19))
    path = f"{root}/effects/data.parquet"
    publication.publish_immutable_file(fs, path, "source", source_fs=source)
    assert source.opens == 2
    assert len(source.read_sizes) >= 16
    assert fs.size(path) == len(source.data)
    assert source.data[-19:] == b"x" * 19
    if not isinstance(fs, LocalFileSystem):
        assert not fs.transaction.files


def test_source_failure_cannot_publish_partial_canonical_object(backend_fs):
    fs, _, root = backend_fs
    source = _Source(b"x" * (8 * STREAM_CHUNK_BYTES), fail_copy=True)
    path = f"{root}/effects/data.parquet"
    with pytest.raises(
        (OSError, StoreIntegrityError),
        match="source read interrupted|Could not publish immutable",
    ):
        publication.publish_immutable_file(fs, path, "source", source_fs=source)
    assert not fs.exists(path)
    assert len(source.data) == 8 * STREAM_CHUNK_BYTES
    if isinstance(fs, LocalFileSystem):
        assert len(fs.glob(f"{path}.tmp-*")) == 1
    else:
        bucket, _, _ = fs.split_path(path)
        assert not fs.call_s3("list_multipart_uploads", Bucket=bucket).get("Uploads")
        assert not fs.transaction.files


def test_source_change_during_copy_is_rejected_before_publication(backend_fs):
    fs, _, root = backend_fs

    class ChangingSource(_Source):
        def open(self, path, mode="rb", **kwargs):
            if self.opens:
                self.data = b"changed"
            return super().open(path, mode, **kwargs)

    path = f"{root}/effects/data.parquet"
    with pytest.raises(
        StoreIntegrityError, match="source changed|Could not publish immutable"
    ):
        publication.publish_immutable_file(
            fs, path, "source", source_fs=ChangingSource(b"original")
        )
    assert not fs.exists(path)


def test_empty_file_publication_is_retryable(backend_fs, tmp_path):
    fs, _, root = backend_fs
    source = tmp_path / "empty.parquet"
    source.touch()
    path = f"{root}/effects/empty.parquet"
    publication.publish_immutable_file(
        fs, path, str(source), source_fs=LocalFileSystem()
    )
    publication.publish_immutable_file(
        fs, path, str(source), source_fs=LocalFileSystem()
    )
    assert fs.size(path) == 0
    assert source.exists()


def test_local_cross_device_copy_preserves_source(tmp_path, monkeypatch):
    fs = LocalFileSystem()
    source = tmp_path / "source.parquet"
    destination = tmp_path / "destination.parquet"
    source.write_bytes(b"completed")
    link = os.link
    calls = []

    def cross_device_once(source_path, target):
        calls.append(source_path)
        if len(calls) == 1:
            raise OSError(errno.EXDEV, "different filesystem")
        return link(source_path, target)

    monkeypatch.setattr(publication.os, "link", cross_device_once)
    publication.publish_immutable_file(fs, str(destination), str(source))
    assert source.read_bytes() == destination.read_bytes() == b"completed"
    assert len(calls) == 2
    assert not list(tmp_path.glob("*.tmp-*"))


def test_local_prelink_failure_retains_temporary_and_source(tmp_path, monkeypatch):
    fs = LocalFileSystem()
    source = _Source(b"completed")
    path = str(tmp_path / "destination.parquet")

    def failed_sync(_descriptor):
        msg = "disk flush failed"
        raise OSError(msg)

    monkeypatch.setattr(publication.os, "fsync", failed_sync)
    with pytest.raises(OSError, match="disk flush failed"):
        publication.publish_immutable_file(fs, path, "source", source_fs=source)
    assert not fs.exists(path)
    assert len(list(tmp_path.glob("*.tmp-*"))) == 1
    assert source.data == b"completed"


def test_local_ambiguous_publication_retry_retains_source(tmp_path, monkeypatch):
    fs = LocalFileSystem()
    source = tmp_path / "source.parquet"
    source.write_bytes(b"completed")
    path = str(tmp_path / "destination.parquet")
    sync = publication._sync_directory_chain

    def failed_sync(_directory):
        msg = "lost directory flush"
        raise OSError(msg)

    monkeypatch.setattr(publication, "_sync_directory_chain", failed_sync)
    with pytest.raises(OSError, match="lost directory flush"):
        publication.publish_immutable_file(fs, path, str(source))
    assert source.exists()
    assert fs.exists(path)
    monkeypatch.setattr(publication, "_sync_directory_chain", sync)
    publication.publish_immutable_file(fs, path, str(source))
    assert source.read_bytes() == b"completed"


def test_s3_ambiguous_completion_is_verified_and_retryable(
    s3_fs, tmp_path, monkeypatch
):
    from s3fs.core import S3File

    fs, _, root = s3_fs
    source = tmp_path / "source.parquet"
    source.write_bytes(b"x" * (7 * STREAM_CHUNK_BYTES))
    commit = S3File.commit
    committed = []

    def committed_then_lost(self):
        commit(self)
        committed.append(self.path)
        msg = "lost upload acknowledgement"
        raise OSError(msg)

    monkeypatch.setattr(S3File, "commit", committed_then_lost)
    path = f"{root}/effect.parquet"
    publication.publish_immutable_file(
        fs, path, str(source), source_fs=LocalFileSystem()
    )
    publication.publish_immutable_file(
        fs, path, str(source), source_fs=LocalFileSystem()
    )
    assert len(committed) == 1
    assert fs.size(path) == source.stat().st_size
    assert source.exists()


def test_s3_empty_file_never_replaces_peer(s3_fs, tmp_path, monkeypatch):
    fs, _, root = s3_fs
    source = tmp_path / "empty.parquet"
    source.touch()
    path = f"{root}/effect.parquet"
    call_s3 = fs.call_s3
    raced = []

    def peer_wins(method, **kwargs):
        if method == "put_object" and not raced:
            raced.append(True)
            call_s3(
                "put_object", Bucket=kwargs["Bucket"], Key=kwargs["Key"], Body=b"peer"
            )
            assert kwargs["IfNoneMatch"] == "*"
        return call_s3(method, **kwargs)

    monkeypatch.setattr(fs, "call_s3", peer_wins)
    with pytest.raises(StoreIntegrityError, match="Conflicting immutable"):
        publication.publish_immutable_file(
            fs, path, str(source), source_fs=LocalFileSystem()
        )
    assert fs.cat_file(path) == b"peer"


def test_s3_upload_keeps_one_small_buffer(s3_fs, monkeypatch):
    from s3fs.core import S3File

    fs, _, root = s3_fs
    source = _Source(b"x" * (17 * STREAM_CHUNK_BYTES + 1))
    upload = S3File._upload_chunk
    buffered = []

    def observe(self, final=False):
        buffered.append(self.buffer.tell())
        assert self.autocommit is False
        return upload(self, final=final)

    monkeypatch.setattr(S3File, "_upload_chunk", observe)
    publication.publish_immutable_file(
        fs, f"{root}/effect.parquet", "source", source_fs=source
    )
    assert len(buffered) >= 4
    assert max(buffered) <= 5 * STREAM_CHUNK_BYTES
    assert not fs.transaction.files


def test_unsupported_file_publication_fails_before_source_read():
    with pytest.raises(StoreIntegrityError, match="unsupported"):
        publication.publish_immutable_file(MemoryFileSystem(), "destination", "missing")
