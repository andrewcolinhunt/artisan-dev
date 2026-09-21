"""A worker seal proves a complete payload set, including empty executions."""

from __future__ import annotations

import io
import json

import polars as pl
import pytest
from fixtures.execution_records import executions_df

from artisan.errors import StoreIntegrityError
from artisan.storage.io.publication import publish_immutable_bytes
from artisan.storage.io.worker_seal import (
    STAGING_INVENTORY_KEY,
    build_staging_inventory,
    verify_worker_seal,
)


def _seal(fs, root, *, payload=True):
    directory = f"{root}/worker"
    fs.makedirs(directory, exist_ok=True)
    if payload:
        with fs.open(f"{directory}/execution_edges.parquet", "wb") as stream:
            pl.DataFrame({"artifact_id": ["a"]}).write_parquet(stream)
    inventory = build_staging_inventory(directory, fs)
    frame = executions_df(execution_run_id=["a" * 32], success=[True])
    encoded = io.BytesIO()
    frame.write_parquet(encoded, metadata={STAGING_INVENTORY_KEY: inventory.decode()})
    publish_immutable_bytes(fs, f"{directory}/executions.parquet", encoded.getvalue())
    return directory


@pytest.mark.parametrize("payload", [False, True])
def test_seal_verifies_exact_payload_set_including_empty(backend_fs, payload):
    fs, _, root = backend_fs
    directory = _seal(fs, root, payload=payload)
    assert verify_worker_seal(directory, fs).item(0, "success") is True


@pytest.mark.parametrize("mutation", ["missing", "changed", "extra"])
def test_seal_rejects_lost_altered_or_unlisted_payloads(backend_fs, mutation):
    fs, _, root = backend_fs
    directory = _seal(fs, root)
    path = f"{directory}/execution_edges.parquet"
    if mutation == "missing":
        fs.rm(path)
    else:
        path = f"{directory}/artifact_edges.parquet" if mutation == "extra" else path
        with fs.open(path, "wb") as stream:
            stream.write(b"different bytes")
    with pytest.raises(StoreIntegrityError, match="Missing or changed"):
        verify_worker_seal(directory, fs)


def test_seal_retains_recognized_crash_temporary(backend_fs):
    fs, _, root = backend_fs
    directory = _seal(fs, root)
    temporary = f"{directory}/executions.parquet.tmp-{'a' * 32}"
    with fs.open(temporary, "wb") as stream:
        stream.write(b"interrupted publication")
    verify_worker_seal(directory, fs)
    assert fs.exists(temporary)


@pytest.mark.parametrize(
    "filename", ["../data.parquet", "executions.parquet", "unknown.parquet"]
)
def test_seal_rejects_unsafe_or_unregistered_inventory_filename(tmp_path, filename):
    from fsspec.implementations.local import LocalFileSystem

    fs = LocalFileSystem()
    directory = tmp_path / "worker"
    directory.mkdir()
    frame = executions_df(execution_run_id=["a" * 32], success=[True])
    frame.write_parquet(
        directory / "executions.parquet",
        metadata={
            STAGING_INVENTORY_KEY: json.dumps(
                {
                    "version": 1,
                    "files": [
                        {"filename": filename, "size_bytes": 1, "digest": "a" * 32},
                    ],
                }
            ),
        },
    )
    with pytest.raises(StoreIntegrityError, match="Unreadable worker seal"):
        verify_worker_seal(str(directory), fs)
