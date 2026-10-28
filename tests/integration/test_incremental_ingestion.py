"""Incremental ingestion through real curator workers, commits and recovery."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

import polars as pl
import pytest
from fsspec.implementations.local import LocalFileSystem
from pydantic import BaseModel, Field

from artisan.operations.curator import IngestCSV
from artisan.orchestration import PipelineManager, StepStatus, replay_execution
from artisan.schemas import ArtifactResult
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.storage.cache.cache_lookup import cache_lookup
from artisan.storage.core.committed_scan import read_committed
from artisan.storage.core.run_scope import load_accepted_outputs
from artisan.storage.io.publication import is_publication_temporary
from artisan.storage.io.repair import repair_store

pytestmark = pytest.mark.integration


class EagerReferenceCSV(IngestCSV):
    """Materialize equivalent emissions to compare the existing eager contract."""

    name = "eager_reference_csv_integration"

    def execute_curator(self, inputs, step_number, artifact_store):
        stream = super().execute_curator(inputs, step_number, artifact_store)
        if isinstance(stream, ArtifactResult):
            return stream
        result = ArtifactResult()
        for item in stream.items:
            result.add_artifact(item.role, item.artifact, sources=item.sources)
        return result


class ObservedIngestCSV(IngestCSV):
    """Record conversion order and inject a worker-local failure boundary."""

    name = "observed_ingest_csv_integration"

    class Params(BaseModel):
        audit_path: str = Field(description="Append conversion names to this file.")
        fail_on: str | None = Field(
            default=None, description="Name that fails conversion."
        )
        publication_failure: Literal["none", "content", "seal"] = Field(
            default="none", description="Publication boundary to fail in this worker."
        )

    params: Params

    def convert_file(self, file_ref, step_number, *, fs=None):
        with Path(self.params.audit_path).open("a") as stream:
            stream.write(f"{file_ref.original_name}\n")
        if file_ref.original_name == self.params.fail_on:
            msg = "late file conversion failed"
            raise RuntimeError(msg)
        return super().convert_file(file_ref, step_number, fs=fs)

    def execute_curator(self, inputs, step_number, artifact_store):
        from artisan.execution.recording import parquet_writer, stream_recorder

        if self.params.publication_failure == "content":
            publish = stream_recorder.publish_immutable_file

            def fail_after_content(fs, path, source_path, **kwargs):
                publish(fs, path, source_path, **kwargs)
                if path.endswith("/csvs.parquet"):
                    msg = "content publication acknowledgement lost"
                    raise OSError(msg)

            stream_recorder.publish_immutable_file = fail_after_content
        elif self.params.publication_failure == "seal":
            publish_seal = parquet_writer.publish_immutable_bytes

            def fail_after_seal(fs, path, data):
                publish_seal(fs, path, data)
                msg = "success seal acknowledgement lost"
                raise OSError(msg)

            parquet_writer.publish_immutable_bytes = fail_after_seal
        return super().execute_curator(inputs, step_number, artifact_store)


def _roots(path):
    return {
        "delta_root": str(path / "delta"),
        "staging_root": str(path / "staging"),
        "working_root": str(path / "work"),
    }


def _files(path, count):
    path.mkdir(parents=True)
    files = []
    for index in range(count):
        file = path / f"source_{index:03}.csv"
        file.write_text(f"id,value\n{index},{index * 3}\n")
        files.append(str(file))
    return files


def _read(roots, table):
    return read_committed(roots["delta_root"], table, fs=LocalFileSystem())


def test_hundred_csvs_match_eager_outputs_and_one_execution(tmp_path):
    files = _files(tmp_path / "inputs", 100)
    results = []
    for name, operation in (("stream", IngestCSV), ("eager", EagerReferenceCSV)):
        roots = _roots(tmp_path / name)
        with PipelineManager.create(name, **roots, preserve_staging=True) as pipeline:
            step = pipeline.run(operation, inputs=files, compact=False)
        assert step.status is StepStatus.SUCCEEDED
        executions = _read(roots, TablePath.EXECUTIONS).filter(
            pl.col("step_run_id") == step.step_run_id
        )
        assert executions.height == 1
        assert executions["success"].to_list() == [True]
        csvs = _read(roots, "artifacts/csvs")
        assert csvs.height == 100
        edges = _read(roots, TablePath.ARTIFACT_EDGES)
        results.append((csvs, edges))
    columns = [
        "artifact_id",
        "content",
        "columns",
        "row_count",
        "original_name",
        "metadata",
    ]
    assert (
        results[0][0]
        .select(columns)
        .sort("artifact_id")
        .equals(results[1][0].select(columns).sort("artifact_id"))
    )
    edge_columns = [
        "source_artifact_id",
        "target_artifact_id",
        "source_role",
        "target_role",
        "group_id",
    ]
    assert set(results[0][1].select(edge_columns).iter_rows()) == set(
        results[1][1].select(edge_columns).iter_rows()
    )


def test_late_conversion_failure_accepts_no_prefix(tmp_path):
    roots = _roots(tmp_path)
    files = _files(tmp_path / "inputs", 6)
    audit = tmp_path / "audit.txt"
    with PipelineManager.create(
        "late_failure", **roots, preserve_staging=True
    ) as pipeline:
        step = pipeline.run(
            ObservedIngestCSV,
            inputs=files,
            params={"audit_path": str(audit), "fail_on": "source_004"},
            compact=False,
        )
    assert step.status is StepStatus.FAILED
    assert audit.read_text().splitlines() == [f"source_{i:03}" for i in range(5)]
    assert _read(roots, "artifacts/csvs").is_empty()
    index = _read(roots, TablePath.ARTIFACT_INDEX)
    assert index.filter(pl.col("artifact_type") == "file_ref").height == 6
    assert load_accepted_outputs(
        roots["delta_root"], fs=LocalFileSystem(), step_run_id=step.step_run_id
    ).is_empty()
    seals = list(Path(roots["staging_root"]).rglob("executions.parquet"))
    assert len(seals) == 1
    assert pl.read_parquet(seals[0])["success"].to_list() == [False]
    assert any(is_publication_temporary(p.name) for p in seals[0].parent.iterdir())
    assert not (seals[0].parent / "csvs.parquet").exists()


@pytest.mark.parametrize("boundary", ["content", "seal"])
def test_publication_failure_synthesizes_a_separate_record_and_preserves_evidence(
    tmp_path, boundary
):
    roots = _roots(tmp_path)
    files = _files(tmp_path / "inputs", 3)
    with PipelineManager.create(
        "publication_failure", **roots, preserve_staging=True
    ) as pipeline:
        step = pipeline.run(
            ObservedIngestCSV,
            inputs=files,
            params={
                "audit_path": str(tmp_path / "audit"),
                "publication_failure": boundary,
            },
            compact=False,
        )
    assert step.status is StepStatus.FAILED
    assert _read(roots, "artifacts/csvs").is_empty()
    failure = _read(roots, TablePath.EXECUTIONS).filter(
        pl.col("step_run_id") == step.step_run_id
    )
    assert failure.height == 1
    assert failure["success"].to_list() == [False]
    original_payloads = list(Path(roots["staging_root"]).rglob("csvs.parquet"))
    assert len(original_payloads) == 1
    original = original_payloads[0].parent
    original_id = original.name
    assert original_id != failure["execution_run_id"][0]
    assert original_id in failure["error"][0]
    assert (original / "executions.parquet").exists() is (boundary == "seal")
    retained = {p: p.read_bytes() for p in original.iterdir() if p.is_file()}
    repair_store(
        delta_root=roots["delta_root"],
        staging_root=roots["staging_root"],
        fs=LocalFileSystem(),
        apply=True,
        recover_staging=True,
        preserve_staging=True,
    )
    recovered = _read(roots, TablePath.EXECUTIONS).filter(
        pl.col("execution_run_id") == original_id
    )
    assert recovered.height == (1 if boundary == "seal" else 0)
    assert _read(roots, "artifacts/csvs").height == (3 if boundary == "seal" else 0)
    assert all(path.read_bytes() == content for path, content in retained.items())
    assert load_accepted_outputs(
        roots["delta_root"], fs=LocalFileSystem(), step_run_id=step.step_run_id
    ).is_empty()


def test_ingest_replay_restarts_ordered_repeated_inputs_and_is_diagnostic(tmp_path):
    roots = _roots(tmp_path)
    files = _files(tmp_path / "inputs", 3)
    selected = [files[2], files[0], files[2]]
    audit = tmp_path / "audit"
    with PipelineManager.create("source", **roots, preserve_staging=True) as pipeline:
        step = pipeline.run(
            ObservedIngestCSV,
            inputs=selected,
            params={"audit_path": str(audit)},
            compact=False,
        )
    assert step.status is StepStatus.SUCCEEDED
    source = (
        _read(roots, TablePath.EXECUTIONS)
        .filter(pl.col("step_run_id") == step.step_run_id)
        .row(0, named=True)
    )
    audit.write_text("")
    runtime = RuntimeEnvironment(
        delta_root=roots["delta_root"],
        staging_root=str(tmp_path / "diagnostic"),
        working_root=str(tmp_path / "diagnostic-work"),
        files_root=str(tmp_path / "diagnostic-files"),
        failure_logs_root=str(tmp_path / "diagnostic-logs"),
    )
    replay = replay_execution(
        source["execution_run_id"], runtime=runtime, operation_class=ObservedIngestCSV
    )
    assert replay.step_result.status is StepStatus.SUCCEEDED
    assert audit.read_text().splitlines() == ["source_002", "source_000", "source_002"]
    row = (
        _read(roots, TablePath.EXECUTIONS)
        .filter(pl.col("execution_run_id") == replay.execution_run_id)
        .row(0, named=True)
    )
    assert row["replay_of_execution_run_id"] == source["execution_run_id"]
    expected = json.loads(source["replay_snapshot"])["inputs"]["file"]
    actual = json.loads(row["replay_snapshot"])["inputs"]["file"]
    assert [item["artifact_id"] for item in actual] == [
        item["artifact_id"] for item in expected
    ]
    assert (
        cache_lookup(
            roots["delta_root"], source["execution_spec_id"], LocalFileSystem()
        ).execution_run_id
        == source["execution_run_id"]
    )
    report = repair_store(
        delta_root=roots["delta_root"],
        staging_root=replay.diagnostic_roots["staging_root"],
        fs=LocalFileSystem(),
        recover_staging=True,
    )
    assert not any(item.classification == "recoverable" for item in report.items)


def test_cancelled_diagnostic_stream_seal_is_excluded_from_recovery(
    tmp_path, monkeypatch
):
    from artisan.orchestration.engine.native_pool import _NativePoolOwnership

    roots = _roots(tmp_path)
    files = _files(tmp_path / "inputs", 3)
    with PipelineManager.create("source", **roots) as source:
        source.run(IngestCSV, inputs=files, compact=False)
    before = _read(roots, TablePath.EXECUTIONS)
    source_id = before["execution_run_id"][0]
    runtime = RuntimeEnvironment(
        delta_root=roots["delta_root"],
        staging_root=str(tmp_path / "debug-staging"),
        working_root=str(tmp_path / "debug-work"),
        files_root=str(tmp_path / "debug-files"),
        failure_logs_root=str(tmp_path / "debug-logs"),
    )
    original_finish = _NativePoolOwnership.finish

    def cancel_after_sealing(ownership, *, failed=False, cancel_event=None):
        if cancel_event is not None and list(
            Path(runtime.staging_root).rglob("executions.parquet")
        ):
            cancel_event.set()
        return original_finish(ownership, failed=failed, cancel_event=cancel_event)

    monkeypatch.setattr(_NativePoolOwnership, "finish", cancel_after_sealing)
    replay = replay_execution(source_id, runtime=runtime)
    assert replay.step_result.status is StepStatus.CANCELLED
    assert replay.execution_run_id is None
    assert _read(roots, TablePath.EXECUTIONS).equals(before)
    seals = list(
        Path(replay.diagnostic_roots["staging_root"]).rglob("executions.parquet")
    )
    assert len(seals) == 1
    row = pl.read_parquet(seals[0]).row(0, named=True)
    assert row["success"] is True
    assert row["replay_of_execution_run_id"] == source_id
    retained = {p: p.read_bytes() for p in seals[0].parent.iterdir() if p.is_file()}
    report = repair_store(
        delta_root=roots["delta_root"],
        staging_root=replay.diagnostic_roots["staging_root"],
        fs=LocalFileSystem(),
        recover_staging=True,
        apply=True,
        preserve_staging=True,
    )
    assert any(
        item.classification == "ineligible" and "diagnostic" in item.detail
        for item in report.items
    )
    assert _read(roots, TablePath.EXECUTIONS).equals(before)
    assert all(path.read_bytes() == content for path, content in retained.items())
