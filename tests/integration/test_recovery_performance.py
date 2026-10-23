"""Recovery work scales with batches, and repeated recovery does no data work."""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from collections import Counter
from pathlib import Path

import polars as pl
import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.schemas.enums import TablePath
from artisan.storage.core.committed_scan import read_committed, read_logical_commits
from artisan.storage.core.run_scope import load_accepted_outputs
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.repair import repair_store
from integration.test_staging_recovery import _cancel_finished_work

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("count", [1, 10])
def test_recovery_writes_one_batch_and_repeated_pass_does_no_data_work(
    pipeline_env, monkeypatch, count
):
    _cancel_finished_work(
        pipeline_env, monkeypatch, preserve=False, count=count, finished=count
    )
    writes: Counter[str] = Counter()
    reads: Counter[str] = Counter()
    original_append = DeltaCommitter._append
    original_scan = pl.scan_delta
    original_complete = DeltaCommitter._complete

    def append(self, frame, table):
        writes[table] += 1
        return original_append(self, frame, table)

    def scan(source, *args, **kwargs):
        table = str(source).removeprefix(pipeline_env["delta_root"] + "/")
        if table != TablePath.LOGICAL_COMMITS.value:
            reads[table] += 1
        return original_scan(source, *args, **kwargs)

    def complete(self, plan):
        writes["completion"] += 1
        return original_complete(self, plan)

    monkeypatch.setattr(DeltaCommitter, "_append", append)
    monkeypatch.setattr(pl, "scan_delta", scan)
    monkeypatch.setattr(DeltaCommitter, "_complete", complete)

    def recover():
        return repair_store(
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            fs=LocalFileSystem(),
            apply=True,
            recover_staging=True,
        )

    assert not recover().blocking
    assert writes[TablePath.EXECUTIONS.value] == 1
    assert writes[TablePath.LOGICAL_COMMITS.value] == 1
    assert writes["completion"] == 1
    assert all(value == 1 for value in writes.values())
    writes.clear()
    reads.clear()

    assert not recover().blocking
    assert not writes
    assert not reads


@pytest.mark.parametrize(
    "boundary",
    [
        "published",
        "planned",
        "table:artifacts/data",
        "table:artifacts/index",
        "table:orchestration/executions",
        "table:provenance/execution_edges",
        "table:provenance/artifact_edges",
        "complete",
        "cleanup",
    ],
)
def test_recovery_batch_survives_process_death(pipeline_env, monkeypatch, boundary):
    original, source_step, evidence = _cancel_finished_work(
        pipeline_env, monkeypatch, preserve=False, count=3, finished=3
    )
    script = textwrap.dedent("""
        import json
        import os
        import sys
        from fsspec.implementations.local import LocalFileSystem
        from artisan.storage.io import repair
        from artisan.storage.io.commit import DeltaCommitter

        roots, boundary = json.loads(sys.argv[1]), sys.argv[2]
        if boundary == 'published':
            original = repair.publish_commit_plan
            def publish(*args, **kwargs):
                original(*args, **kwargs)
                os._exit(77)
            repair.publish_commit_plan = publish
        elif boundary == 'cleanup':
            original = LocalFileSystem.rm
            def remove(fs, path, *args, **kwargs):
                original(fs, path, *args, **kwargs)
                if str(path).startswith(roots['staging_root']) and str(path).endswith('.parquet'):
                    os._exit(77)
            LocalFileSystem.rm = remove
        else:
            def checkpoint(self, phase, plan, table):
                found = phase if table is None else phase + ':' + table
                if found == boundary:
                    os._exit(77)
            DeltaCommitter._checkpoint = checkpoint
        repair.repair_store(
            delta_root=roots['delta_root'], staging_root=roots['staging_root'],
            fs=LocalFileSystem(), apply=True, recover_staging=True,
        )
        raise AssertionError('Recovery did not reach the requested boundary')
    """)
    child = subprocess.run(
        [sys.executable, "-c", script, json.dumps(pipeline_env), boundary],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert child.returncode == 77, child.stdout + child.stderr
    if boundary != "cleanup":
        assert all(path.read_bytes() == data for path, data in evidence.items())
    fs = LocalFileSystem()
    delta = pipeline_env["delta_root"]
    if boundary not in {"complete", "cleanup"}:
        assert (
            read_committed(delta, TablePath.EXECUTIONS, fs=fs)
            .filter(pl.col("step_run_id") == source_step)
            .is_empty()
        )
    report = repair_store(
        delta_root=delta,
        staging_root=pipeline_env["staging_root"],
        fs=fs,
        apply=True,
        recover_staging=True,
    )
    assert not report.blocking
    executions = read_committed(delta, TablePath.EXECUTIONS, fs=fs).filter(
        pl.col("step_run_id") == source_step
    )
    assert executions.height == executions["execution_run_id"].n_unique() == 3
    controls = read_logical_commits(delta, fs=fs).filter(
        pl.col("commit_kind") == "execution_recovery"
    )
    assert controls.height == 1
    assert controls["state"].to_list() == ["complete"]
    assert load_accepted_outputs(delta, fs=fs, step_run_id=source_step).is_empty()
    assert original._step_tracker.current_state(source_step).status.value == "cancelled"
    assert all(not path.exists() for path in evidence)


def test_later_worker_forms_a_new_disjoint_batch(pipeline_env, monkeypatch, tmp_path):
    _, source_step, evidence = _cancel_finished_work(
        pipeline_env, monkeypatch, preserve=False, count=3, finished=3
    )
    seals = sorted(path for path in evidence if path.name == "executions.parquet")
    directory = seals[-1].parent
    parked = tmp_path / "late-worker"
    directory.rename(parked)
    fs = LocalFileSystem()
    delta = pipeline_env["delta_root"]

    def recover():
        return repair_store(
            delta_root=delta,
            staging_root=pipeline_env["staging_root"],
            fs=fs,
            apply=True,
            recover_staging=True,
        )

    assert not recover().blocking
    first = read_logical_commits(delta, fs=fs).filter(
        pl.col("commit_kind") == "execution_recovery"
    )
    assert first.height == 1
    assert (
        read_committed(delta, TablePath.EXECUTIONS, fs=fs)
        .filter(pl.col("step_run_id") == source_step)
        .height
        == 2
    )

    directory.parent.mkdir(parents=True, exist_ok=True)
    parked.rename(directory)
    assert not recover().blocking
    controls = read_logical_commits(delta, fs=fs).filter(
        pl.col("commit_kind") == "execution_recovery"
    )
    assert controls.height == controls["recovery_batch_id"].n_unique() == 2
    assert set(first["logical_commit_id"]) < set(controls["logical_commit_id"])
    executions = read_committed(delta, TablePath.EXECUTIONS, fs=fs).filter(
        pl.col("step_run_id") == source_step
    )
    assert executions.height == executions["execution_run_id"].n_unique() == 3
    assert load_accepted_outputs(delta, fs=fs, step_run_id=source_step).is_empty()
    assert not list(Path(pipeline_env["staging_root"]).rglob("executions.parquet"))
