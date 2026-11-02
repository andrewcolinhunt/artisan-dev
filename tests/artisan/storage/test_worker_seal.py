"""A worker seal proves a complete payload set, including empty executions."""

from __future__ import annotations

import io
import json
from pathlib import Path

import polars as pl
import pytest
from fixtures.execution_records import executions_df

from artisan.errors import ApplicationRegistrationError, StoreIntegrityError
from artisan.storage.io.publication import publish_immutable_bytes
from artisan.storage.io.worker_seal import (
    STAGING_INVENTORY_KEY,
    build_staging_inventory,
    capture_worker_files,
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


def test_worker_capture_is_private_and_owns_its_scratch(tmp_path):
    from fsspec.implementations.local import LocalFileSystem

    fs = LocalFileSystem()
    directory = _seal(fs, str(tmp_path))
    with capture_worker_files(directory, fs) as captured:
        private_path = Path(captured.files["execution_edges.parquet"].path)
        pl.DataFrame({"artifact_id": ["changed"]}).write_parquet(
            f"{directory}/execution_edges.parquet"
        )

        assert (
            captured.read_frame("execution_edges.parquet", ["artifact_id"]).item()
            == "a"
        )
        assert private_path.exists()

    assert not private_path.exists()
    assert Path(directory).exists()


def test_inventory_hashing_disables_remote_readahead(tmp_path, monkeypatch):
    from fsspec.implementations.local import LocalFileSystem

    fs = LocalFileSystem()
    directory = _seal(fs, str(tmp_path))
    original_open = fs.open

    def uncached_open(path, mode="rb", **kwargs):
        assert kwargs.get("cache_type") == "none"
        return original_open(path, mode, **kwargs)

    monkeypatch.setattr(fs, "open", uncached_open)
    inventory = json.loads(build_staging_inventory(directory, fs))

    assert inventory["files"][0]["filename"] == "execution_edges.parquet"


def test_worker_capture_reads_source_in_bounded_chunks_and_decodes_only_seal(
    tmp_path, monkeypatch
):
    from fsspec.implementations.local import LocalFileSystem

    fs = LocalFileSystem()
    directory = _seal(fs, str(tmp_path))
    original_open = fs.open

    class BoundedReader:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            self.stream.close()

        def read(self, size=-1):
            assert 0 <= size <= 1024 * 1024
            return self.stream.read(size)

    def guarded_open(path, mode="rb", **kwargs):
        stream = original_open(path, mode, **kwargs)
        return BoundedReader(stream) if str(path).startswith(directory) else stream

    def forbid_eager(*_args, **_kwargs):
        msg = "worker verification decoded a whole payload"
        raise AssertionError(msg)

    monkeypatch.setattr(fs, "open", guarded_open)
    monkeypatch.setattr(pl, "read_parquet", forbid_eager)

    assert verify_worker_seal(directory, fs).item(0, "success") is True


@pytest.mark.parametrize(
    "filename",
    ["../csvs.parquet", "data.parquet", "executions.parquet", "unknown.parquet"],
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
    expected = (
        ApplicationRegistrationError
        if filename in {"data.parquet", "unknown.parquet"}
        else StoreIntegrityError
    )
    with pytest.raises(expected):
        verify_worker_seal(str(directory), fs)
