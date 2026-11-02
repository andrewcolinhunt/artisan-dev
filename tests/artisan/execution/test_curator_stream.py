"""Incremental curator validation, ownership and all-or-nothing publication."""

from __future__ import annotations

import json
import time
import weakref
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import ClassVar
from unittest.mock import Mock

import polars as pl
import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.execution.executors import curator_stream
from artisan.execution.executors.curator_stream import handle_artifact_stream
from artisan.execution.recording import parquet_writer, stream_recorder
from artisan.execution.recording.stream_recorder import StreamRecorder
from artisan.operations.base import OperationDefinition
from artisan.schemas import (
    ArtifactEmission,
    ArtifactResult,
    ArtifactStreamResult,
    CSVArtifact,
    InputSpec,
    OutputSpec,
)
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.execution_context import ExecutionContext
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.storage.io.publication import is_publication_temporary
from artisan.utils.path import shard_uri


class StreamTestOp(OperationDefinition):
    name = "incremental_curator_test"

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        csv = "csv"

    inputs: ClassVar[dict[str, InputSpec]] = {
        "source": InputSpec(artifact_type="csv", required=False)
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "csv": OutputSpec(artifact_type="csv", derives_from={"inputs": []})
    }

    def execute_curator(self, inputs, step_number, artifact_store):
        return ArtifactResult()


def _emission(value=1, *, sources=None, metadata=None):
    artifact = CSVArtifact.draft(
        f"value\n{value}\n".encode(), "source.csv", step_number=1
    )
    artifact.metadata = metadata or {}
    return ArtifactEmission(role="csv", artifact=artifact, sources=sources or {})


@pytest.fixture
def execute(tmp_path, monkeypatch):
    store = Mock()
    store.provenance.load_type_map.side_effect = lambda ids: dict.fromkeys(ids, "csv")
    context = ExecutionContext(
        execution_run_id="a" * 32,
        execution_spec_id="b" * 32,
        step_number=1,
        timestamp_start=datetime.now(UTC),
        worker_id=1,
        artifact_store=store,
        staging_root=str(tmp_path / "staging"),
        fs=LocalFileSystem(),
        operation_name=StreamTestOp.name,
        operation=StreamTestOp(),
    )
    monkeypatch.setattr(curator_stream, "command_snapshot", CommandRecording.empty)
    monkeypatch.setattr(
        curator_stream,
        "replay_recording_fields",
        lambda: {
            "replay_snapshot": ReplaySnapshot.unavailable("stream_unit_test"),
            "replay_of_execution_run_id": None,
        },
    )
    # Exercise multiple completed chunks with small test data.
    original = StreamRecorder.__init__
    monkeypatch.setattr(
        StreamRecorder,
        "__init__",
        lambda self, ctx, inputs: original(self, ctx, inputs, max_bytes=256),
    )

    def run(items, *, metadata=None, inputs=None):
        return handle_artifact_stream(
            ArtifactStreamResult(items=items, metadata=metadata or {}),
            context,
            inputs or {},
            {},
            time.perf_counter(),
            time.perf_counter(),
            {},
            None,
            None,
        )

    run.context = context
    run.path = Path(
        shard_uri(
            context.staging_root,
            context.execution_run_id,
            step_number=1,
            operation_name=context.operation_name,
        )
    )
    return run


def test_stream_releases_artifacts_and_snapshots_nested_metadata(execute):
    refs = []
    metadata = {"custom": {"value": "original"}}

    def items():
        for value in range(6):
            assert all(reference() is None for reference in refs)
            item = _emission(value, metadata={"ordinal": value})
            refs.append(weakref.ref(item.artifact))
            yield item
            del item
            metadata["custom"]["value"] = "mutated"

    result = execute(items(), metadata=metadata)
    assert result.success
    assert len(result.artifact_ids) == 6
    assert all(reference() is None for reference in refs)
    assert pl.read_parquet(execute.path / "csvs.parquet").height == 6
    index = pl.read_parquet(execute.path / "index.parquet")
    assert [json.loads(row)["ordinal"] for row in index["metadata"]] == list(range(6))
    execution = pl.read_parquet(execute.path / "executions.parquet").row(0, named=True)
    saved = json.loads(execution["metadata"])
    assert saved["custom"] == {"value": "original"}
    assert saved["timings"]["execute"] >= 0
    assert not any(is_publication_temporary(p.name) for p in execute.path.iterdir())


def test_stream_snapshots_custom_nested_fields_before_requesting_next_item(
    execute, monkeypatch
):
    key = "nested_stream_test"
    monkeypatch.setattr(ArtifactTypeDef, "_registry", dict(ArtifactTypeDef._registry))
    monkeypatch.setattr(
        ArtifactTypes, "_registry", {**ArtifactTypes._registry, key: key}
    )

    class NestedArtifact(Artifact):
        artifact_type: str = key
        content: bytes
        labels: list[str]
        details: dict[str, list[str]]
        POLARS_SCHEMA: ClassVar[dict] = {
            "artifact_id": pl.String,
            "origin_step_number": pl.Int32,
            "content": pl.Binary,
            "labels": pl.List(pl.String),
            "details": pl.Struct({"names": pl.List(pl.String)}),
        }

    class NestedType(ArtifactTypeDef):
        key = "nested_stream_test"
        model = NestedArtifact
        table_path = "artifacts/nested_stream_test"

    monkeypatch.setattr(
        StreamTestOp,
        "outputs",
        {"csv": OutputSpec(artifact_type=key, derives_from={"inputs": []})},
    )
    initialize = StreamRecorder.__init__
    append = StreamRecorder.append

    def keep_buffered(self, *args, **kwargs):
        initialize(self, *args, **kwargs)
        self._max_bytes = 16 * 1024 * 1024

    def check_shared_bytes(self, role, artifact, edges):
        append(self, role, artifact, edges)
        assert (
            self._rows[NestedType.parquet_filename()][-1]["content"] is artifact.content
        )

    monkeypatch.setattr(StreamRecorder, "__init__", keep_buffered)
    monkeypatch.setattr(StreamRecorder, "append", check_shared_bytes)

    def items():
        artifact = NestedArtifact(
            content=b"x" * (1024 * 1024),
            labels=["before"],
            details={"names": ["before"]},
            origin_step_number=1,
        )
        yield ArtifactEmission(role="csv", artifact=artifact, sources={})
        artifact.labels.append("after")
        artifact.details["names"].append("after")

    assert execute(items()).success
    row = pl.read_parquet(execute.path / NestedType.parquet_filename()).row(
        0, named=True
    )
    assert row["labels"] == ["before"]
    assert row["details"] == {"names": ["before"]}


def test_repeated_outputs_keep_occurrences_and_deduplicate_edges(execute, monkeypatch):
    monkeypatch.setattr(
        StreamTestOp,
        "outputs",
        {"csv": OutputSpec(artifact_type="csv", derives_from={"inputs": ["source"]})},
    )
    sources = {"source": ["c" * 32, "d" * 32]}
    result = execute((_emission(sources=sources) for _ in range(3)), inputs=sources)
    assert result.success
    assert len(result.artifact_ids) == 3
    assert len(set(result.artifact_ids)) == 1
    assert pl.read_parquet(execute.path / "artifact_edges.parquet").height == 2
    edges = pl.read_parquet(execute.path / "execution_edges.parquet")
    assert edges.height == 3
    assert edges.filter(pl.col("direction") == "output").height == 1
    groups = pl.read_parquet(execute.path / "artifact_edges.parquet")["group_id"]
    assert groups.n_unique() == 1
    assert groups[0] is not None
    execute.context.artifact_store.provenance.load_type_map.assert_called_once()


@pytest.mark.parametrize(
    "failure", ["iteration", "close", "required", "invalid_item", "draft", "metadata"]
)
def test_prepublication_failures_seal_no_converted_prefix(execute, failure):
    class Items:
        count = 0
        closed = False

        def __iter__(self):
            return self

        def __next__(self):
            self.count += 1
            if failure == "required":
                raise StopIteration
            if self.count == 1:
                item = _emission()
                if failure == "invalid_item":
                    return object()
                if failure == "draft":
                    item.artifact.finalize()
                return item
            if failure == "iteration":
                msg = "late conversion failed"
                raise RuntimeError(msg)
            raise StopIteration

        def close(self):
            self.closed = True
            if failure == "close":
                msg = "close failed"
                raise RuntimeError(msg)

    items = Items()
    result = execute(
        items, metadata={"status": "forbidden"} if failure == "metadata" else {}
    )
    assert not result.success
    assert result.execution_run_id == "a" * 32
    assert items.closed
    assert not (execute.path / "csvs.parquet").exists()
    row = pl.read_parquet(execute.path / "executions.parquet").row(0, named=True)
    assert row["success"] is False
    assert not (execute.path / "index.parquet").exists()
    if failure in ("iteration", "close"):
        assert any(is_publication_temporary(p.name) for p in execute.path.iterdir())


@pytest.mark.parametrize(
    "specs",
    [
        {},
        {"csv": OutputSpec(artifact_type="csv", derives_from={"outputs": ["csv"]})},
        {"csv": OutputSpec(artifact_type="csv")},
    ],
)
def test_unsupported_output_contract_rejected_before_iteration(
    execute, monkeypatch, specs
):
    monkeypatch.setattr(StreamTestOp, "outputs", specs)
    pulled = []

    def items():
        pulled.append(True)
        yield _emission()

    result = execute(items())
    assert not result.success
    assert pulled == []


@pytest.mark.parametrize("sources", [{"source": ["d" * 32]}, {}])
def test_stream_enforces_parent_membership_and_coverage(execute, monkeypatch, sources):
    monkeypatch.setattr(
        StreamTestOp,
        "outputs",
        {"csv": OutputSpec(artifact_type="csv", derives_from={"inputs": ["source"]})},
    )
    result = execute(iter([_emission(sources=sources)]), inputs={"source": ["c" * 32]})
    assert not result.success
    assert not (execute.path / "csvs.parquet").exists()


def test_existing_unsealed_canonical_payload_is_preserved(execute):
    execute.path.mkdir(parents=True)
    existing = execute.path / "csvs.parquet"
    existing.write_bytes(b"prior evidence")
    result = execute(iter([_emission()]))
    assert not result.success
    assert result.execution_run_id is None
    assert existing.read_bytes() == b"prior evidence"
    assert not (execute.path / "executions.parquet").exists()


def test_promotion_failure_never_failure_seals_the_original_shard(execute, monkeypatch):
    publish = stream_recorder.publish_immutable_file
    calls = []

    def fail_after_first(*args, **kwargs):
        calls.append(args[1])
        if len(calls) == 2:
            msg = "second promotion failed"
            raise OSError(msg)
        return publish(*args, **kwargs)

    monkeypatch.setattr(stream_recorder, "publish_immutable_file", fail_after_first)
    result = execute(iter([_emission()]))
    assert not result.success
    assert result.execution_run_id is None
    assert (execute.path / "csvs.parquet").exists()
    assert not (execute.path / "executions.parquet").exists()
    assert any(is_publication_temporary(p.name) for p in execute.path.iterdir())


@pytest.mark.parametrize("failure", [False, True])
def test_uncertain_seal_publication_returns_no_original_execution_id(
    execute, monkeypatch, failure
):
    publish = parquet_writer.publish_immutable_bytes

    def publish_then_fail(*args, **kwargs):
        publish(*args, **kwargs)
        msg = "publication acknowledgement lost"
        raise OSError(msg)

    monkeypatch.setattr(parquet_writer, "publish_immutable_bytes", publish_then_fail)

    def items():
        yield _emission()
        if failure:
            msg = "conversion failed"
            raise RuntimeError(msg)

    result = execute(items())
    assert not result.success
    assert result.execution_run_id is None
    row = pl.read_parquet(execute.path / "executions.parquet").row(0, named=True)
    assert row["success"] is not failure


def test_writer_failure_retains_temps_and_records_failure_only(execute, monkeypatch):
    write = stream_recorder.write_parquet_rows
    calls = []

    def fail_first(path, *args, **kwargs):
        calls.append(path)
        if len(calls) == 1:
            Path(path).write_bytes(b"partial temporary payload")
            msg = "writer failed"
            raise OSError(msg)
        return write(path, *args, **kwargs)

    monkeypatch.setattr(stream_recorder, "write_parquet_rows", fail_first)
    result = execute(iter([_emission()]))
    assert not result.success
    assert result.execution_run_id == "a" * 32
    assert not (execute.path / "csvs.parquet").exists()
    assert Path(calls[0]).read_bytes() == b"partial temporary payload"


def test_unexpected_outer_stream_error_never_calls_eager_failure_fallback(
    tmp_path, monkeypatch
):
    from artisan.execution.executors import curator
    from artisan.execution.models.execution_unit import ExecutionUnit
    from artisan.execution.recording.commands import capture_commands
    from artisan.execution.recording.replay_snapshot import capture_replay
    from artisan.schemas.execution.runtime_environment import RuntimeEnvironment

    monkeypatch.setattr(
        StreamTestOp,
        "execute_curator",
        lambda self, **kwargs: ArtifactStreamResult(items=iter([_emission()])),
    )

    def unexpected(*args, **kwargs):
        msg = "unexpected error after stream took ownership"
        raise RuntimeError(msg)

    monkeypatch.setattr(curator, "handle_artifact_stream", unexpected)
    fallback = Mock(side_effect=AssertionError("eager failure fallback must not run"))
    monkeypatch.setattr(curator, "record_execution_failure", fallback)
    unit = ExecutionUnit(operation=StreamTestOp(), inputs={}, step_number=1)
    runtime = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"), staging_root=str(tmp_path / "staging")
    )
    with capture_commands(unit.operation), capture_replay(unit, runtime):
        result = curator._run_curator_flow(unit, runtime)
    assert not result.success
    assert result.execution_run_id is None
    fallback.assert_not_called()


@pytest.mark.parametrize("fails", [False, True])
def test_stream_commands_execute_before_scope_closes(tmp_path, monkeypatch, fails):
    import sys

    from artisan.execution.executors.curator import run_curator_flow
    from artisan.execution.models.execution_unit import ExecutionUnit
    from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
    from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
    from artisan.utils.external_tools import run_command

    def items():
        run_command(LocalEnvironmentSpec(), [sys.executable, "-c", "pass"])
        yield _emission()
        if fails:
            msg = "conversion after command failed"
            raise RuntimeError(msg)

    monkeypatch.setattr(
        StreamTestOp,
        "execute_curator",
        lambda self, **kwargs: ArtifactStreamResult(items=items()),
    )
    runtime = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"), staging_root=str(tmp_path / "staging")
    )
    result = run_curator_flow(
        ExecutionUnit(operation=StreamTestOp(), inputs={}, step_number=1), runtime
    )
    assert result.success is not fails
    row = pl.read_parquet(Path(result.staging_path) / "executions.parquet").row(
        0, named=True
    )
    recording = CommandRecording.model_validate_json(row["command_recording"])
    assert len(recording.commands) == 1
    assert recording.commands[0].outcome == "succeeded"


def test_stream_external_artifacts_preserve_locations_and_metadata(
    execute, monkeypatch, tmp_path
):
    from artisan.schemas import FileRefArtifact
    from artisan.utils.hashing import compute_content_digest

    monkeypatch.setattr(
        StreamTestOp,
        "outputs",
        {"csv": OutputSpec(artifact_type="file_ref", derives_from={"inputs": []})},
    )
    path = tmp_path / "external.bin"
    path.write_bytes(b"external bytes")

    def items():
        for _ in range(2):
            artifact = FileRefArtifact.draft(
                path=str(path),
                content_hash=compute_content_digest(b"external bytes"),
                size_bytes=14,
                original_name="external",
                extension=".bin",
                step_number=1,
            )
            artifact.metadata = {"nested": {"value": 7}}
            yield ArtifactEmission(role="csv", artifact=artifact, sources={})

    result = execute(items())
    assert result.success
    locations = pl.read_parquet(execute.path / "locations.parquet")
    assert locations.height == 1
    assert locations["uri"][0] == str(path)
    index = pl.read_parquet(execute.path / "index.parquet")
    assert [json.loads(value) for value in index["metadata"]] == [
        {"nested": {"value": 7}},
        {"nested": {"value": 7}},
    ]


def test_invalid_terminal_metadata_is_rejected_before_pulling(execute):
    pulled = []

    def items():
        pulled.append(True)
        yield _emission()

    result = execute(items(), metadata={"custom": object()})
    assert not result.success
    assert pulled == []


def test_iteration_and_close_errors_are_both_preserved(execute):
    class Items:
        def __iter__(self):
            return self

        def __next__(self):
            msg = "original conversion error"
            raise RuntimeError(msg)

        def close(self):
            msg = "secondary close error"
            raise RuntimeError(msg)

    result = execute(Items())
    assert not result.success
    assert "original conversion error" in result.error
    assert "secondary close error" in result.error


@pytest.mark.parametrize("earlier_emission", [False, True])
def test_output_identity_never_conceals_a_missing_input_type(
    execute, monkeypatch, earlier_emission
):
    monkeypatch.setattr(
        StreamTestOp,
        "outputs",
        {"csv": OutputSpec(artifact_type="csv", derives_from={"inputs": ["source"]})},
    )
    missing_id = _emission().artifact.finalize().artifact_id
    valid_id = "c" * 32
    execute.context.artifact_store.provenance.load_type_map.side_effect = lambda ids: {
        valid_id: "csv"
    }

    def items():
        if earlier_emission:
            yield _emission(sources={"source": [valid_id]})
        yield _emission(sources={"source": [missing_id]})

    result = execute(items(), inputs={"source": [valid_id, missing_id]})
    assert not result.success
    assert "Missing concrete artifact type" in result.error
    assert not (execute.path / "csvs.parquet").exists()
