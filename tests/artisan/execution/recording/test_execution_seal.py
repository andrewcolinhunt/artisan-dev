"""Real executor failure paths cannot alter already published worker evidence."""

from __future__ import annotations

from pathlib import Path

import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.execution.executors.creator import run_creator_flow
from artisan.execution.executors.curator import run_curator_flow
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording import parquet_writer
from artisan.operations.curator import Merge
from artisan.operations.examples import DataGenerator
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.storage.io.worker_seal import verify_worker_seal


def _execute(tmp_path, kind, *, shared=False):
    runtime = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "working"),
        shared_filesystem=shared,
    )
    operation = DataGenerator(params={"count": 1}) if kind == "creator" else Merge()
    unit = ExecutionUnit(
        operation=operation,
        execution_spec_id="a" * 32,
        step_run_id="b" * 32,
        inputs={} if kind == "creator" else {"source": ["c" * 32]},
    )
    flow = run_creator_flow if kind == "creator" else run_curator_flow
    return flow(unit, runtime)


@pytest.mark.parametrize("kind", ["creator", "curator"])
def test_error_after_success_seal_preserves_all_original_bytes(
    tmp_path, monkeypatch, kind
):
    original = parquet_writer._write_execution_record
    snapshots = {}

    def publish_then_fail(**kwargs):
        original(**kwargs)
        directory = Path(kwargs["staging_path"])
        snapshots.update({p: p.read_bytes() for p in directory.iterdir()})
        msg = "after seal publication"
        raise RuntimeError(msg)

    monkeypatch.setattr(parquet_writer, "_write_execution_record", publish_then_fail)
    with pytest.raises(RuntimeError, match="after seal publication"):
        _execute(tmp_path, kind)
    assert snapshots
    assert {
        p: p.read_bytes() for p in next(iter(snapshots)).parent.iterdir()
    } == snapshots
    frame = verify_worker_seal(str(next(iter(snapshots)).parent), LocalFileSystem())
    assert frame.item(0, "success") is True


@pytest.mark.parametrize("shared", [False, True])
def test_payload_durability_precedes_seal_for_all_local_workers(
    tmp_path, monkeypatch, shared
):
    original = parquet_writer._sync_local_staging
    flushed = []

    def flush_before_seal(path):
        directory = Path(path)
        assert not (directory / "executions.parquet").exists()
        assert (directory / "execution_edges.parquet").exists()
        original(path)
        flushed.append(path)

    monkeypatch.setattr(parquet_writer, "_sync_local_staging", flush_before_seal)
    result = _execute(tmp_path, "creator", shared=shared)
    assert result.success
    assert flushed == [result.staging_path]
    verify_worker_seal(result.staging_path, LocalFileSystem())


def test_fresh_recording_cannot_reopen_sealed_shard(tmp_path):
    result = _execute(tmp_path, "creator")
    directory = Path(result.staging_path)
    before = {p.name: p.read_bytes() for p in directory.iterdir()}
    with pytest.raises(StoreIntegrityError, match="already sealed"):
        parquet_writer._create_staging_path(
            str(tmp_path / "staging"),
            result.execution_run_id,
            0,
            "data_generator",
            LocalFileSystem(),
        )
    assert {p.name: p.read_bytes() for p in directory.iterdir()} == before
