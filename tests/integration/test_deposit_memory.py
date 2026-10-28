"""A storage-only RSS ceiling covering large deposits, retry, audit, and recovery."""

from __future__ import annotations

import base64
import json
import os
import random
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable
from fixtures.memory import peak_rss_bytes
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import CommitError
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.committed_scan import read_logical_commits, scan_committed
from artisan.storage.core.table_schemas import (
    ARTIFACT_INDEX_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
    EXECUTIONS_SCHEMA,
)
from artisan.storage.io.bounded_io import iter_parquet_frames, write_parquet_rows
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.commit_plan import prepare_commit_evidence, publish_commit_plan
from artisan.storage.io.repair import repair_store
from artisan.storage.io.staging import StagingManager
from artisan.storage.io.worker_seal import (
    STAGING_INVENTORY_KEY,
    build_staging_inventory,
)
from artisan.utils.path import shard_uri

pytestmark = pytest.mark.integration

_MIB = 1024 * 1024
_RSS_LIMIT = 768 * _MIB
_ROWS_PER_TYPE = 192
_RECOVERY_ROWS = 16
_STEP_IDS = {"bulk": "a" * 32, "small": "b" * 32}


def _committer(root):
    fs = LocalFileSystem()
    return DeltaCommitter(
        str(root / "delta"), StagingManager(str(root / "staging"), fs), fs=fs
    )


def _artifact(kind, seed, size, step, compressible):
    if compressible:
        value = b"A" * size
    else:
        value = base64.b64encode(random.Random(seed).randbytes(size * 3 // 4))
    if kind == "csv":
        content = (
            b"value\n"
            + b"\n".join(
                value[index : index + 65536] for index in range(0, len(value), 65536)
            )
            + b"\n"
        )
        artifact = CSVArtifact.draft(content, f"csv-{seed}.csv", step)
    else:
        artifact = MetricArtifact.draft(
            {"value": value.decode()}, f"metric-{seed}.json", step
        )
    return artifact.finalize()


def _write_fixture_rows(path, rows, schema):
    """Use uncompressed source files so staged bytes themselves exceed the limit."""
    arrow_schema = pl.DataFrame(schema=schema).to_arrow().schema
    with pq.ParquetWriter(
        path,
        arrow_schema,
        compression=None,
        use_dictionary=False,
        data_page_size=65536,
        write_batch_size=1,
    ) as writer:
        for row in rows:
            writer.write_table(
                pl.DataFrame([row], schema=schema).to_arrow(), row_group_size=1
            )


def _index_row(artifact):
    return {
        "artifact_id": artifact.artifact_id,
        "artifact_type": str(artifact.artifact_type),
        "origin_step_number": artifact.origin_step_number,
        "metadata": "{}",
    }


def _registration_fixture(root, phase, step, definitions):
    directory = (
        root
        / "staging"
        / f"{step}_memory"
        / "_orchestrator"
        / _STEP_IDS[phase]
        / "input_registration"
    )
    directory.mkdir(parents=True)
    index = []
    payload_bytes = 0
    for kind, records in definitions.items():
        model = CSVArtifact if kind == "csv" else MetricArtifact

        def rows(records=records, kind=kind):
            nonlocal payload_bytes
            for seed, size, compressible in records:
                artifact = _artifact(kind, seed, size, step, compressible)
                payload_bytes += len(artifact.content)
                index.append(_index_row(artifact))
                yield artifact.to_row()

        name = "csvs.parquet" if kind == "csv" else "metrics.parquet"
        _write_fixture_rows(directory / name, rows(), model.POLARS_SCHEMA)
    write_parquet_rows(str(directory / "index.parquet"), index, ARTIFACT_INDEX_SCHEMA)
    return payload_bytes, sum(path.stat().st_size for path in directory.iterdir())


def _recovery_fixture(root, step, kind, source=None):
    fs = LocalFileSystem()
    tracker = StepTracker(str(root / "delta"), "memory-recovery", fs=fs)
    step_id = f"{step:032x}"
    tracker.create_attempt(
        StepStartRecord(
            step_run_id=step_id,
            step_spec_id=f"{step + 10:032x}",
            step_number=step,
            step_name="memory-worker",
            operation_class="tests.MemoryWorker",
            params_json="{}",
            input_refs_json="{}",
            compute_backend="local",
            compute_options_json="{}",
            output_roles_json='["output"]',
            output_types_json=json.dumps({"output": kind}),
        )
    )
    tracker.transition(step_id, StepStatus.PENDING, StepStatus.RUNNING)
    tracker.transition(
        step_id,
        StepStatus.RUNNING,
        StepStatus.FAILED,
        result=StepResult(
            step_name="memory-worker",
            step_number=step,
            step_run_id=step_id,
            status=StepStatus.FAILED,
            error="fixture worker interrupted",
        ),
    )
    execution_id = f"{step + 20:032x}"
    directory = Path(
        shard_uri(
            str(root / "staging"),
            execution_id,
            step_number=step,
            operation_name="memory_worker",
        )
    )
    directory.mkdir(parents=True)
    index = []
    payload_bytes = 0

    def rows():
        nonlocal payload_bytes
        if source is None:
            for number in range(_RECOVERY_ROWS):
                artifact = _artifact(
                    "csv", 20000 + number, 2 * _MIB, step, number % 2 == 0
                )
                payload_bytes += len(artifact.content)
                index.append(_index_row(artifact))
                yield artifact.to_row()
        else:
            for frame in iter_parquet_frames(str(source)):
                for row in frame.iter_rows(named=True):
                    row["origin_step_number"] = step
                    payload_bytes += len(row["content"])
                    index.append(
                        {
                            "artifact_id": row["artifact_id"],
                            "artifact_type": kind,
                            "origin_step_number": step,
                            "metadata": "{}",
                        }
                    )
                    yield row

    model = CSVArtifact if kind == "csv" else MetricArtifact
    filename = "csvs.parquet" if kind == "csv" else "metrics.parquet"
    write_parquet_rows(str(directory / filename), rows(), model.POLARS_SCHEMA)
    write_parquet_rows(str(directory / "index.parquet"), index, ARTIFACT_INDEX_SCHEMA)
    edges = (
        {
            "execution_run_id": execution_id,
            "direction": "output",
            "role": "output",
            "artifact_id": row["artifact_id"],
        }
        for row in index
    )
    write_parquet_rows(
        str(directory / "execution_edges.parquet"), edges, EXECUTION_EDGES_SCHEMA
    )
    record = dict.fromkeys(EXECUTIONS_SCHEMA)
    record.update(
        execution_run_id=execution_id,
        execution_spec_id=f"{step + 30:032x}",
        step_run_id=step_id,
        origin_step_number=step,
        operation_name="memory_worker",
        success=True,
        timestamp_start=datetime.now(UTC),
        timestamp_end=datetime.now(UTC),
        command_recording=CommandRecording.empty().model_dump_json(),
        replay_snapshot=ReplaySnapshot.unavailable("memory fixture").model_dump_json(),
    )
    write_parquet_rows(
        str(directory / "executions.parquet"),
        [record],
        EXECUTIONS_SCHEMA,
        metadata={
            STAGING_INVENTORY_KEY.encode(): build_staging_inventory(str(directory), fs),
        },
    )
    return payload_bytes, sum(path.stat().st_size for path in directory.iterdir())


def _fixtures(root):
    root.mkdir(parents=True)
    _committer(root).initialize_tables()
    bulk = {
        kind: [
            (offset + number, (2 + 2 * (number % 2)) * _MIB, (number // 2) % 2 == 0)
            for number in range(_ROWS_PER_TYPE)
        ]
        for kind, offset in (("csv", 0), ("metric", 1000))
    }
    payload, staged = _registration_fixture(root, "bulk", 0, bulk)
    _registration_fixture(
        root,
        "small",
        1,
        {
            "csv": [(0, 2 * _MIB, True), (10000, _MIB // 8, False)],
            "metric": [(10001, _MIB // 4, False)],
        },
    )
    bulk_directory = (
        root
        / "staging/0_memory/_orchestrator"
        / _STEP_IDS["bulk"]
        / "input_registration"
    )
    recovery_sources = [
        _recovery_fixture(root, 2, "csv", bulk_directory / "csvs.parquet"),
        _recovery_fixture(root, 3, "metric", bulk_directory / "metrics.parquet"),
        _recovery_fixture(root, 4, "csv"),
    ]
    (root / "fixture.json").write_text(
        json.dumps(
            {
                "payload_bytes": payload,
                "staged_bytes": staged,
                "recovery_payload_bytes": sum(item[0] for item in recovery_sources),
                "recovery_staged_bytes": sum(item[1] for item in recovery_sources),
                "recovery_source_attempts": len(recovery_sources),
            }
        )
    )


def _deposit(root, phase):
    committer = _committer(root)
    try:
        evidence = prepare_commit_evidence(
            staging_root=str(root / "staging"),
            fs=LocalFileSystem(),
            commit_kind="input_registration",
            step_run_id=_STEP_IDS[phase],
            step_number=0 if phase == "bulk" else 1,
            operation_name="memory",
        )
        plan = evidence.plan
        prepared = committer.prepare_logical(plan, staged=evidence)
        publish_commit_plan(str(root / "delta"), LocalFileSystem(), plan)
        if phase == "bulk":

            def interrupt(boundary, _plan, table):
                if boundary == "table" and table == "artifacts/csvs":
                    msg = "injected post-table interruption"
                    raise RuntimeError(msg)

            committer._checkpoint = interrupt
            with pytest.raises(CommitError):
                committer.commit_logical(plan, prepared=prepared)
            del committer._checkpoint
            committer.commit_logical(plan)
            assert DeltaTable(str(root / "delta/artifacts/csvs")).version() == 1
            assert DeltaTable(str(root / "delta/artifacts/metrics")).version() == 1
        else:
            committer.commit_logical(plan, prepared=prepared)
        committer.commit_logical(plan)
    finally:
        committer.close()


def _measure(root, phase):
    started = time.monotonic()
    if phase in {"bulk", "small"}:
        _deposit(root, phase)
    else:
        report = repair_store(
            delta_root=str(root / "delta"),
            staging_root=str(root / "staging"),
            fs=LocalFileSystem(),
            apply=phase == "recovery",
            recover_staging=phase == "recovery",
        )
        assert not report.blocking, report
    expected = 2 * _ROWS_PER_TYPE + (0 if phase == "bulk" else 2)
    if phase == "recovery":
        expected += _RECOVERY_ROWS
    ids = (
        scan_committed(
            str(root / "delta"), TablePath.ARTIFACT_INDEX, fs=LocalFileSystem()
        )
        .select("artifact_id", "origin_step_number")
        .collect()
    )
    assert ids.height == ids["artifact_id"].n_unique() == expected
    controls = read_logical_commits(str(root / "delta"), fs=LocalFileSystem())
    assert set(controls["state"]) == {"complete"}
    assert ids.filter(pl.col("origin_step_number") == 0).height == 2 * _ROWS_PER_TYPE
    if phase == "recovery":
        recovered = controls.filter(pl.col("commit_kind") == "execution_recovery")
        assert recovered.height == 3
        assert ids.filter(pl.col("origin_step_number") == 4).height == _RECOVERY_ROWS
        assert DeltaTable(str(root / "delta/artifacts/csvs")).version() == 3
        assert DeltaTable(str(root / "delta/artifacts/metrics")).version() == 2
    peak_bytes = peak_rss_bytes()
    (root / f"{phase}.json").write_text(
        json.dumps(
            {
                "peak_bytes": peak_bytes,
                "visible_artifacts": expected,
                "elapsed_seconds": time.monotonic() - started,
            }
        )
    )


def _rss(pid):
    if sys.platform.startswith("linux"):
        try:
            for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
        except FileNotFoundError:
            return 0
        return 0
    output = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(pid)],
        capture_output=True,
        text=True,
        check=False,
    )
    return int(output.stdout.strip() or 0) * 1024


def _run_phase(root, phase, *, monitor):
    log = root.parent / f"{phase}.log"
    with log.open("w") as output:
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), phase, str(root)],
            stdout=output,
            stderr=subprocess.STDOUT,
            env={
                **os.environ,
                "POLARS_MAX_THREADS": "2",
                "RAYON_NUM_THREADS": "2",
                "PYTHONPATH": str(Path(__file__).resolve().parents[1])
                + os.pathsep
                + os.environ.get("PYTHONPATH", ""),
            },
        )
        deadline = time.monotonic() + 600
        sampled_peak = 0
        try:
            while process.poll() is None:
                if monitor:
                    sampled_peak = max(sampled_peak, _rss(process.pid))
                    assert sampled_peak < _RSS_LIMIT, (
                        f"{phase} exceeded {_RSS_LIMIT // _MIB}MiB RSS: {sampled_peak / _MIB:.1f}MiB"
                    )
                assert time.monotonic() < deadline, (
                    f"{phase} exceeded ten-minute timeout"
                )
                time.sleep(0.05)
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
    assert process.returncode == 0, log.read_text()[-6000:]
    if monitor:
        result = json.loads((root / f"{phase}.json").read_text())
        result["sampled_peak_bytes"] = sampled_peak
        assert result["peak_bytes"] < _RSS_LIMIT, {"phase": phase, **result}
        return result
    return None


@pytest.mark.skipif(
    not (sys.platform.startswith("linux") or sys.platform == "darwin"),
    reason="RSS gate supports the project's Linux and macOS targets",
)
def test_deposits_larger_than_rss_ceiling_survive_retry_audit_and_recovery(
    tmp_path, request
):
    root = tmp_path / "store"
    _run_phase(root, "fixtures", monitor=False)
    fixture = json.loads((root / "fixture.json").read_text())
    assert fixture["payload_bytes"] > 1024 * _MIB
    assert fixture["staged_bytes"] > _RSS_LIMIT
    assert fixture["recovery_payload_bytes"] > 1024 * _MIB
    request.node.user_properties.append(
        ("deposit_fixture_json", json.dumps(fixture, sort_keys=True))
    )
    measurements = {}
    for phase in ("bulk", "small", "audit", "recovery"):
        measurements[phase] = _run_phase(root, phase, monitor=True)
        request.node.user_properties.append(
            (f"deposit_{phase}_json", json.dumps(measurements[phase], sort_keys=True))
        )
    (tmp_path / "rss-measurements.json").write_text(
        json.dumps({"fixture": fixture, "phases": measurements}, indent=2)
    )


if __name__ == "__main__":
    command, directory = sys.argv[1:]
    if command == "fixtures":
        _fixtures(Path(directory))
    else:
        _measure(Path(directory), command)
