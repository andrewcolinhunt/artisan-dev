"""Immutable publication retries and local durability failures."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem

from artisan.errors import StoreIntegrityError
from artisan.storage.io import publication


def test_publication_allows_only_identical_retries(backend_fs):
    fs, _, root = backend_fs
    path = f"{root}/evidence/plan.json"
    publication.publish_immutable_bytes(fs, path, b"original")
    publication.publish_immutable_bytes(fs, path, b"original")
    with pytest.raises(StoreIntegrityError, match="Conflicting immutable"):
        publication.publish_immutable_bytes(fs, path, b"changed")
    with fs.open(path, "rb") as stream:
        assert stream.read() == b"original"


def test_local_post_link_fsync_failure_is_not_success(tmp_path, monkeypatch):
    fs = LocalFileSystem()
    path = str(tmp_path / "executions.parquet")
    fsync = os.fsync
    calls = []

    def fail_directory_fsync(fd):
        calls.append(fd)
        if len(calls) == 2:
            msg = "directory flush failed"
            raise OSError(msg)
        fsync(fd)

    monkeypatch.setattr(publication.os, "fsync", fail_directory_fsync)
    with pytest.raises(OSError, match="directory flush failed"):
        publication.publish_immutable_bytes(fs, path, b"sealed")
    assert (tmp_path / "executions.parquet").read_bytes() == b"sealed"
    monkeypatch.setattr(publication.os, "fsync", fsync)
    publication.publish_immutable_bytes(fs, path, b"sealed")


def test_local_link_never_replaces_peer_bytes(tmp_path, monkeypatch):
    fs = LocalFileSystem()
    path = tmp_path / "executions.parquet"

    def peer_wins(_temporary, _final):
        path.write_bytes(b"peer")
        msg = "peer published first"
        raise FileExistsError(msg)

    monkeypatch.setattr(publication.os, "link", peer_wins)
    with pytest.raises(StoreIntegrityError, match="Conflicting immutable"):
        publication.publish_immutable_bytes(fs, str(path), b"ours")
    assert path.read_bytes() == b"peer"


def test_unknown_filesystem_cannot_weaken_publication():
    with pytest.raises(StoreIntegrityError, match="unsupported"):
        publication.publish_immutable_bytes(MemoryFileSystem(), "/seal", b"data")


def test_local_publication_does_not_import_optional_s3_extra(tmp_path):
    script = """
import sys
from pathlib import Path

sys.modules["s3fs"] = None
from fsspec.implementations.local import LocalFileSystem
from artisan.storage.io.publication import publish_immutable_bytes

path = Path(sys.argv[1]) / "seal.json"
publish_immutable_bytes(LocalFileSystem(), str(path), b"local evidence")
assert path.read_bytes() == b"local evidence"
assert sys.modules["s3fs"] is None
"""
    subprocess.run([sys.executable, "-c", script, str(tmp_path)], check=True)


def test_local_publication_flushes_new_ancestor_directory_entries(
    tmp_path, monkeypatch
):
    fsync = os.fsync
    flushed = []

    def observe(fd):
        status = os.fstat(fd)
        flushed.append((status.st_dev, status.st_ino))
        fsync(fd)

    monkeypatch.setattr(publication.os, "fsync", observe)
    parent = tmp_path / "new" / "nested" / "shard"
    publication.publish_immutable_bytes(
        LocalFileSystem(), str(parent / "seal.json"), b"data"
    )
    directories = [
        (path.stat().st_dev, path.stat().st_ino)
        for path in (parent, parent.parent, parent.parent.parent, tmp_path)
    ]
    assert flushed[1:5] == directories


@pytest.mark.parametrize(
    "name",
    [
        "executions.parquet.tmp-" + "a" * 32,
        "step_result.json.tmp-" + "b" * 32,
    ],
)
def test_recognizes_only_publisher_temporary_pattern(name):
    assert publication.is_publication_temporary(name)
    assert not publication.is_publication_temporary(name[:-1])
    assert not publication.is_publication_temporary("../" + name)
