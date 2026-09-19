"""Tests for the coordinated release store boundary."""

from __future__ import annotations

import json

import polars as pl
import pytest
from fsspec.implementations.local import LocalFileSystem

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


def test_manifest_round_trip_for_initialized_store(tmp_path) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    DeltaCommitter(
        root,
        StagingManager(str(tmp_path / "staging"), fs),
        fs=fs,
    ).initialize_tables()

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


def test_current_manifest_without_required_tables_fails(tmp_path) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    publish_store_manifest(root, fs)

    with pytest.raises(IncompatibleStoreError, match="missing table"):
        assert_store_format(root, fs)


def test_malformed_cache_reuse_table_fails(tmp_path) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    DeltaCommitter(
        root,
        StagingManager(str(tmp_path / "staging"), fs),
        fs=fs,
    ).initialize_tables()
    fs.rm(f"{root}/orchestration/cache_reuse", recursive=True)
    fs.makedirs(f"{root}/orchestration/cache_reuse", exist_ok=True)

    with pytest.raises(IncompatibleStoreError, match="malformed table"):
        assert_store_format(root, fs)


def test_wrong_cache_reuse_schema_fails(tmp_path) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    DeltaCommitter(
        root,
        StagingManager(str(tmp_path / "staging"), fs),
        fs=fs,
    ).initialize_tables()
    fs.rm(f"{root}/orchestration/cache_reuse", recursive=True)
    pl.DataFrame(schema={"cached_execution_run_id": pl.String}).write_delta(
        f"{root}/orchestration/cache_reuse"
    )

    with pytest.raises(IncompatibleStoreError, match="has schema"):
        assert_store_format(root, fs)


@pytest.mark.parametrize(
    "content",
    [
        None,
        "not-json",
        json.dumps({"store_format": 2, "artifact_identity": 1, "cache_identity": 2}),
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


def test_previous_manifest_rejected_even_with_current_tables(tmp_path) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    DeltaCommitter(
        root,
        StagingManager(str(tmp_path / "staging"), fs),
        fs=fs,
    ).initialize_tables()
    previous_manifest = {**STORE_MANIFEST, "store_format": 2}
    with fs.open(f"{root}/{STORE_MANIFEST_PATH}", "w") as stream:
        json.dump(previous_manifest, stream)

    with pytest.raises(IncompatibleStoreError, match="found .*store_format.*2"):
        assert_store_format(root, fs)
