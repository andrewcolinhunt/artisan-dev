"""Tests for the coordinated format-2 store boundary."""

from __future__ import annotations

import json

import pytest
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem

from artisan.errors import IncompatibleStoreError
from artisan.storage.core.store_format import (
    STORE_MANIFEST,
    STORE_MANIFEST_PATH,
    assert_store_format,
    prepare_store_initialization,
    publish_store_manifest,
)
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.staging import StagingManager


@pytest.mark.parametrize("fs", [LocalFileSystem(), MemoryFileSystem()])
def test_manifest_round_trip_on_supported_filesystems(fs, tmp_path) -> None:
    root = (
        str(tmp_path / "delta") if isinstance(fs, LocalFileSystem) else "memory:/delta"
    )

    publish_store_manifest(root, fs)

    assert_store_format(root, fs)
    with fs.open(f"{root}/{STORE_MANIFEST_PATH}", "r") as stream:
        assert json.load(stream) == STORE_MANIFEST


def test_initialize_empty_root_publishes_manifest_last(tmp_path) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    staging = StagingManager(str(tmp_path / "staging"), fs)
    committer = DeltaCommitter(root, staging, fs=fs)

    committer.initialize_tables()

    assert_store_format(root, fs)


@pytest.mark.parametrize(
    "content",
    [
        None,
        "not-json",
        json.dumps({"store_format": 3, "artifact_identity": 1, "cache_identity": 2}),
        json.dumps({"store_format": 2}),
    ],
)
def test_missing_malformed_and_unsupported_manifests_fail(tmp_path, content) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    if content is not None:
        fs.makedirs(f"{root}/_artisan", exist_ok=True)
        with fs.open(f"{root}/{STORE_MANIFEST_PATH}", "w") as stream:
            stream.write(content)

    with pytest.raises(IncompatibleStoreError, match="Use a new Delta root"):
        assert_store_format(root, fs)


def test_nonempty_legacy_root_cannot_be_initialized(tmp_path) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    fs.makedirs(root, exist_ok=True)
    with fs.open(f"{root}/legacy.bin", "wb") as stream:
        stream.write(b"legacy")

    with pytest.raises(IncompatibleStoreError, match="not empty"):
        prepare_store_initialization(root, fs)
