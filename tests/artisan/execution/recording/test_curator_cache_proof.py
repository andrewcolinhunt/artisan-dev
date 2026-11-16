"""Curator read proofs belong to sealed execution evidence, outside user metadata."""

from __future__ import annotations

import json
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar, Literal
from unittest.mock import Mock

import polars as pl
import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.execution.executors.curator_stream import handle_artifact_stream
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording.commands import capture_commands
from artisan.execution.recording.parquet_writer import _write_execution_record
from artisan.execution.recording.recorder import (
    record_execution_success,
    record_passthrough,
)
from artisan.execution.recording.replay_snapshot import capture_replay
from artisan.operations.base import OperationDefinition
from artisan.schemas import (
    ArtifactEmission,
    ArtifactResult,
    ArtifactStreamResult,
    CSVArtifact,
    OutputSpec,
)
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.curator_cache import (
    CURATOR_CACHE_PROOF_KEY,
    CuratorCacheProof,
)
from artisan.schemas.execution.execution_context import ExecutionContext
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_result import curator_result_summary
from artisan.storage.core.table_schemas import get_schema
from artisan.storage.io.worker_seal import capture_worker_files


class RootCurator(OperationDefinition):
    name = "sealed_curator_proof"

    class OutputRole(StrEnum):
        data = "data"

    outputs: ClassVar = {
        "data": OutputSpec(artifact_type="csv", derives_from={"inputs": []})
    }

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        return ArtifactResult()


def _proof() -> CuratorCacheProof:
    return CuratorCacheProof(
        definition_digest="a" * 32,
        software_digest="b" * 64,
        cache_key="c" * 32,
        snapshot_digest="d" * 32,
        views=(),
    )


@pytest.mark.parametrize("kind", ["artifacts", "passthrough", "stream"])
def test_all_curator_results_seal_proof_without_exposing_or_redacting_it(
    tmp_path: Path, kind: Literal["artifacts", "passthrough", "stream"]
) -> None:
    proof = _proof()
    context = ExecutionContext(
        execution_run_id="e" * 32,
        execution_spec_id="f" * 32,
        step_number=3,
        timestamp_start=datetime.now(UTC),
        worker_id=0,
        artifact_store=Mock(spec=ArtifactStore),
        staging_root=str(tmp_path),
        fs=LocalFileSystem(),
        operation_name=RootCurator.name,
        operation=RootCurator(),
        step_run_id="1" * 32,
        curator_cache_proof=proof,
    )
    context.artifact_store._view = None
    metadata = {"note": proof.definition_digest}
    overrides = {"api_token": proof.definition_digest}
    common: dict[str, Any] = {
        "execution_context": context,
        "inputs": {},
        "timestamp_end": datetime.now(UTC),
        "command_recording": CommandRecording.empty(),
        "replay_snapshot": ReplaySnapshot.unavailable("recorder_fixture"),
        "replay_of_execution_run_id": None,
        "result_metadata": metadata,
        "user_overrides": overrides,
    }
    if kind == "artifacts":
        staged = record_execution_success(artifacts={}, lineage_edges=[], **common)
    elif kind == "passthrough":
        staged = record_passthrough(passthrough={}, lineage_edges=[], **common)
    else:
        unit = ExecutionUnit(
            operation=context.operation,
            step_number=context.step_number,
            replay_snapshot=ReplaySnapshot.unavailable("recorder_fixture"),
        )
        with capture_commands(context.operation), capture_replay(unit):
            staged = handle_artifact_stream(
                ArtifactStreamResult(
                    items=iter(
                        [
                            ArtifactEmission(
                                role="data",
                                artifact=CSVArtifact.draft(b"value\n1\n", "source.csv"),
                                sources={},
                            )
                        ]
                    ),
                    metadata=metadata,
                ),
                context,
                {},
                {},
                time.perf_counter(),
                time.perf_counter(),
                {},
                overrides,
                None,
            )
    assert staged.success, staged.error
    assert staged.staging_path is not None
    with capture_worker_files(staged.staging_path, context.fs) as captured:
        frames = {
            table.value: captured.read_frame(f"{table.table_name}.parquet")
            if f"{table.table_name}.parquet" in captured.files
            else pl.DataFrame(schema=get_schema(table))
            for table in (
                TablePath.EXECUTIONS,
                TablePath.EXECUTION_EDGES,
                TablePath.ARTIFACT_INDEX,
            )
        }
    saved = json.loads(frames[TablePath.EXECUTIONS.value]["metadata"][0])
    assert CuratorCacheProof.model_validate(saved[CURATOR_CACHE_PROOF_KEY]) == proof
    assert proof.definition_digest not in saved["note"]
    _, authored = curator_result_summary(frames, {context.execution_run_id})
    assert CURATOR_CACHE_PROOF_KEY not in authored
    assert authored == {"note": saved["note"]}
    assert metadata == {"note": proof.definition_digest}


@pytest.mark.parametrize("forged_metadata", [False, True])
def test_failed_or_authored_proof_cannot_publish_an_execution_seal(
    tmp_path: Path, forged_metadata: bool
) -> None:
    with pytest.raises(ValueError, match="reserved framework|Only successful"):
        _write_execution_record(
            execution_run_id="e" * 32,
            execution_spec_id="f" * 32,
            operation_name=RootCurator.name,
            step_number=3,
            success=forged_metadata,
            error=None,
            timestamp_start=datetime.now(UTC),
            timestamp_end=datetime.now(UTC),
            worker_id=0,
            staging_path=str(tmp_path),
            fs=LocalFileSystem(),
            command_recording=CommandRecording.empty(),
            replay_snapshot=ReplaySnapshot.unavailable("recorder_fixture"),
            replay_of_execution_run_id=None,
            result_metadata={CURATOR_CACHE_PROOF_KEY: _proof().model_dump(mode="json")}
            if forged_metadata
            else None,
            curator_cache_proof=None if forged_metadata else _proof(),
        )
    assert not (tmp_path / "executions.parquet").exists()
