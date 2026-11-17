"""Authored stream reads are sealed without including framework validation reads."""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from enum import StrEnum
from pathlib import Path
from typing import ClassVar

import polars as pl
import pytest
from fixtures.retained_acceptance import admit_case, open_committer
from fixtures.retained_curators import admit_curator
from fsspec.implementations.local import LocalFileSystem
from pydantic import PrivateAttr

from artisan.execution.executors.curator import run_curator_flow
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.base import OperationDefinition
from artisan.schemas import (
    ArtifactEmission,
    ArtifactStreamResult,
    CSVArtifact,
    InputSpec,
    OutputSpec,
)
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_cache import (
    CURATOR_CACHE_PROOF_KEY,
    CuratorCacheProof,
)
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_config import WorkerConfig
from artisan.storage.cache.curator_dependencies import CuratorReadRecorder
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_view import CuratorView
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.worker_seal import capture_worker_files


class ReadingStream(Iterator[ArtifactEmission]):
    def __init__(self, store: ArtifactStore, source: str, fail: bool) -> None:
        self.store, self.source, self.fail = store, source, fail
        self.emitted = False
        self.closed = False

    def __next__(self) -> ArtifactEmission:
        self.store.get_artifacts_by_type(["0" * 32], "csv")
        if self.emitted:
            if self.fail:
                msg = "Failure after one streamed output"
                raise RuntimeError(msg)
            raise StopIteration
        self.emitted = True
        return ArtifactEmission(
            role="data",
            artifact=CSVArtifact.draft(b"value\n2\n", "result.csv"),
            sources={"source": [self.source]},
        )

    def close(self) -> None:
        self.closed = True
        self.store.get_artifacts_by_type(["0" * 32], "file_ref")


class ReadingCurator(OperationDefinition):
    name = "observed_stream"

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        data = "data"

    inputs: ClassVar = {"source": InputSpec(artifact_type="csv")}
    outputs: ClassVar = {
        "data": OutputSpec(artifact_type="csv", derives_from={"inputs": ["source"]})
    }
    _fail: bool = PrivateAttr(default=False)
    _stream: ReadingStream | None = PrivateAttr(default=None)

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactStreamResult:
        artifact_store.load_metrics_df(["0" * 32])
        self._stream = ReadingStream(
            artifact_store, inputs["source"]["artifact_id"][0], self._fail
        )
        return ArtifactStreamResult(items=self._stream)


@pytest.mark.parametrize("fail", [False, True])
def test_actual_curator_scopes_stream_reads_and_closes_failed_iterator(
    tmp_path: Path, fail: bool
) -> None:
    storage = StorageConfig()
    committer = open_committer(
        str(tmp_path / "candidate"), LocalFileSystem(), storage, initialize=True
    )
    _, receipt = admit_case(committer, tmp_path / "source", kind="alias")
    committer.accept(receipt)
    inputs = RetainedReader(committer).read(receipt).output("data")[:1]
    source_id = inputs[0].artifact.artifact_id
    assert source_id is not None
    operation = ReadingCurator()
    operation._fail = fail
    coordinator, admission, _ = admit_curator(
        committer,
        operation,
        {"source": inputs},
        step_run_id="b" * 32,
        software_digest="e" * 64,
    )
    view = CuratorView(committer, admission.view)
    recorder = CuratorReadRecorder(view, admission)
    staged = run_curator_flow(
        ExecutionUnit(
            operation=operation,
            inputs={"source": [source_id]},
            step_number=admission.record.step_number,
            step_run_id=admission.record.step_run_id,
            execution_spec_id=admission.digest,
            replay_snapshot=ReplaySnapshot.unavailable("stream-observation-fixture"),
        ),
        WorkerConfig(
            delta_root=committer.delta_root,
            staging_root=coordinator.staging.staging_dir,
            files_root=committer.files_root,
        ),
        curator_view=view,
        read_recorder=recorder,
    )
    assert staged.success is not fail, staged.error
    assert operation._stream is not None
    assert operation._stream.closed
    assert view._read_recorder is None
    assert staged.staging_path is not None
    with capture_worker_files(staged.staging_path, committer.fs) as captured:
        row = captured.read_frame("executions.parquet").row(0, named=True)
    metadata = json.loads(row["metadata"])
    if fail:
        assert CURATOR_CACHE_PROOF_KEY not in metadata
        with pytest.raises(RuntimeError, match="failed"):
            recorder.finish()
    else:
        proof = CuratorCacheProof.model_validate(metadata[CURATOR_CACHE_PROOF_KEY])
        assert proof == recorder.proof
        assert {item.table_path for item in proof.views} == {
            ArtifactTypeDef.get_table_path(kind)
            for kind in ("csv", "metric", "file_ref")
        }
        assert TablePath.ARTIFACT_INDEX.value not in {
            item.table_path for item in proof.views
        }
