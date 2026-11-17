"""Successful curator seals retain external bytes after iterator-owned files close."""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, ClassVar

import polars as pl
import pytest
from fixtures.retained_acceptance import admit_case, open_committer
from fixtures.retained_curators import admit_curator
from fsspec.implementations.local import LocalFileSystem
from pydantic import PrivateAttr

from artisan.errors import StoreIntegrityError
from artisan.execution.executors.curator import run_curator_flow
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording.parquet_writer import _write_execution_record
from artisan.execution.recording.recorder import record_execution_success
from artisan.operations.base import OperationDefinition
from artisan.schemas import (
    ArtifactEmission,
    ArtifactStreamResult,
    FileRefArtifact,
    InputSpec,
    OutputSpec,
)
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.curator_carriers import (
    CURATOR_CARRIER_INVENTORY_KEY,
    CuratorCarrierInventory,
)
from artisan.schemas.execution.execution_context import ExecutionContext
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_config import WorkerConfig
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.curator_carriers import curator_carrier_directory
from artisan.storage.io.external_carriers import capture_external_carriers
from artisan.storage.io.worker_seal import capture_worker_files
from artisan.utils.hashing import compute_content_digest
from artisan.utils.path import shard_uri


class TemporaryFilesCurator(OperationDefinition):
    name = "temporary_files"

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        files = "files"

    inputs: ClassVar = {"source": InputSpec(artifact_type="csv")}
    outputs: ClassVar = {
        "files": OutputSpec(
            artifact_type="file_ref", derives_from={"inputs": ["source"]}
        )
    }
    _fail: bool = PrivateAttr(default=False)
    _paths: list[Path] = PrivateAttr(default_factory=list)

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactStreamResult:
        primary = inputs["source"]["artifact_id"][0]

        def emissions() -> Iterator[ArtifactEmission]:
            for index in range(2):
                with TemporaryDirectory(prefix="curator-author-") as directory:
                    path = Path(directory) / f"output-{index}.bin"
                    payload = f"result {index}".encode()
                    path.write_bytes(payload)
                    self._paths.append(path)
                    yield ArtifactEmission(
                        role="files",
                        artifact=FileRefArtifact.draft(
                            str(path), compute_content_digest(payload), len(payload)
                        ),
                        sources={"source": [primary]},
                    )
                    if self._fail:
                        msg = "Failure after one temporary file"
                        raise RuntimeError(msg)

        return ArtifactStreamResult(items=emissions())


@pytest.mark.parametrize("fail", [False, True])
def test_stream_captures_temporary_files_before_iterator_advance(
    tmp_path: Path, fail: bool
) -> None:
    fs = LocalFileSystem()
    committer = open_committer(
        str(tmp_path / "candidate"), fs, StorageConfig(), initialize=True
    )
    _, receipt = admit_case(committer, tmp_path / "input", kind="alias")
    committer.accept(receipt)
    source = RetainedReader(committer).read(receipt).output("data")[0]
    source_id = source.artifact.artifact_id
    assert source_id is not None
    operation = TemporaryFilesCurator()
    operation._fail = fail
    coordinator, admission, _ = admit_curator(
        committer, operation, {"source": (source,)}, step_run_id="b" * 32
    )
    staged = run_curator_flow(
        ExecutionUnit(
            operation=operation,
            inputs={"source": [source_id]},
            step_number=admission.record.step_number,
            step_run_id=admission.record.step_run_id,
            execution_spec_id=admission.digest,
            replay_snapshot=ReplaySnapshot.unavailable("carrier-fixture"),
        ),
        WorkerConfig(
            delta_root=committer.delta_root,
            files_root=committer.files_root,
            staging_root=coordinator.staging.staging_dir,
        ),
        curator_view=CuratorView(committer, admission.view),
    )
    assert staged.success is not fail, staged.error
    assert operation._paths
    assert all(not path.exists() for path in operation._paths)
    assert staged.staging_path is not None
    with capture_worker_files(staged.staging_path, fs) as captured:
        row = captured.read_frame("executions.parquet").row(0, named=True)
    metadata = json.loads(row["metadata"])
    directory = curator_carrier_directory(staged.staging_path)
    if fail:
        assert CURATOR_CARRIER_INVENTORY_KEY not in metadata
        assert not fs.exists(directory) or fs.ls(directory) == []
        return
    inventory = CuratorCarrierInventory.model_validate(
        metadata[CURATOR_CARRIER_INVENTORY_KEY]
    )
    assert len(inventory.carriers) == len(inventory.external_bindings) == 2
    scratch = tmp_path / "captured"
    scratch.mkdir()
    paths = capture_external_carriers(directory, inventory.carriers, scratch, fs)
    assert {Path(path).read_bytes() for path in paths.values()} == {
        b"result 0",
        b"result 1",
    }


@pytest.mark.parametrize("forged", [False, True])
def test_failed_or_authored_inventory_cannot_publish_execution_seal(
    tmp_path: Path, forged: bool
) -> None:
    inventory = CuratorCarrierInventory()
    with pytest.raises(ValueError, match="reserved framework|Only successful"):
        _write_execution_record(
            execution_run_id="e" * 32,
            execution_spec_id="f" * 32,
            operation_name=TemporaryFilesCurator.name,
            step_number=3,
            success=forged,
            error=None,
            timestamp_start=datetime.now(UTC),
            timestamp_end=datetime.now(UTC),
            worker_id=0,
            staging_path=str(tmp_path),
            fs=LocalFileSystem(),
            command_recording=CommandRecording.empty(),
            replay_snapshot=ReplaySnapshot.unavailable("carrier-fixture"),
            replay_of_execution_run_id=None,
            result_metadata={
                CURATOR_CARRIER_INVENTORY_KEY: inventory.model_dump(mode="json")
            }
            if forged
            else None,
            curator_carriers=None if forged else inventory,
        )
    assert not (tmp_path / "executions.parquet").exists()


@pytest.fixture
def eager_context(tmp_path: Path) -> ExecutionContext:
    fs = LocalFileSystem()
    committer = open_committer(
        str(tmp_path / "store"), fs, StorageConfig(), initialize=True
    )
    store = ArtifactStore(
        committer.delta_root,
        fs=fs,
        files_root=committer.files_root,
        _view=CuratorView(committer, capture_curator_view(committer)),
    )
    return ExecutionContext(
        execution_run_id="e" * 32,
        execution_spec_id="f" * 32,
        step_number=3,
        timestamp_start=datetime.now(UTC),
        worker_id=0,
        artifact_store=store,
        staging_root=str(tmp_path / "staging"),
        fs=fs,
        operation_name=TemporaryFilesCurator.name,
        operation=TemporaryFilesCurator(),
        step_run_id="1" * 32,
    )


@pytest.mark.parametrize(
    "failure", ["during_capture", "before_seal", "after_seal", "unknown_seal"]
)
def test_eager_recording_cleans_only_conclusively_unsealed_carriers(
    tmp_path: Path,
    eager_context: ExecutionContext,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    from artisan.execution.recording import parquet_writer, recorder

    artifacts = []
    for index in range(2):
        path = tmp_path / f"source-{index}.bin"
        payload = f"file {index}".encode()
        path.write_bytes(payload)
        artifacts.append(
            FileRefArtifact.draft(
                str(path), compute_content_digest(payload), len(payload)
            ).finalize()
        )
    staging = shard_uri(
        eager_context.staging_root,
        eager_context.execution_run_id,
        step_number=eager_context.step_number,
        operation_name=eager_context.operation_name,
    )
    directory = Path(curator_carrier_directory(staging))
    if failure == "during_capture":
        (tmp_path / "source-1.bin").unlink()
    else:
        original = parquet_writer._stage_execution

        def fail_write(**kwargs: Any) -> None:
            assert len(list(directory.iterdir())) == 2
            if failure == "after_seal":
                original(**kwargs)
            msg = "Injected execution recording failure"
            raise RuntimeError(msg)

        monkeypatch.setattr(parquet_writer, "_stage_execution", fail_write)
    if failure == "unknown_seal":

        def unknown_seal(context: ExecutionContext) -> bool:
            msg = "Seal status unavailable"
            raise OSError(msg)

        monkeypatch.setattr(recorder, "execution_is_sealed", unknown_seal)
    expected = StoreIntegrityError if failure == "during_capture" else RuntimeError
    with pytest.raises(expected) as raised:
        record_execution_success(
            execution_context=eager_context,
            artifacts={"files": artifacts},
            lineage_edges=[],
            inputs={},
            timestamp_end=datetime.now(UTC),
            command_recording=CommandRecording.empty(),
            replay_snapshot=ReplaySnapshot.unavailable("carrier-fixture"),
            replay_of_execution_run_id=None,
        )
    retained = failure in {"after_seal", "unknown_seal"}
    assert len(list(directory.iterdir())) == (2 if retained else 0)
    if failure == "unknown_seal":
        assert any("Seal status unavailable" in note for note in raised.value.__notes__)


def test_eager_inventory_digest_survives_user_metadata_redaction(
    tmp_path: Path, eager_context: ExecutionContext
) -> None:
    payload = b"captured bytes"
    digest = compute_content_digest(payload)
    path = tmp_path / "source.bin"
    path.write_bytes(payload)
    artifact = FileRefArtifact.draft(str(path), digest, len(payload)).finalize()
    staged = record_execution_success(
        execution_context=eager_context,
        artifacts={"files": [artifact]},
        lineage_edges=[],
        inputs={},
        timestamp_end=datetime.now(UTC),
        command_recording=CommandRecording.empty(),
        replay_snapshot=ReplaySnapshot.unavailable("carrier-fixture"),
        replay_of_execution_run_id=None,
        result_metadata={"note": digest},
        user_overrides={"api_token": digest},
    )
    assert staged.staging_path is not None
    with capture_worker_files(staged.staging_path, eager_context.fs) as captured:
        metadata = json.loads(captured.read_frame("executions.parquet")["metadata"][0])
    inventory = CuratorCarrierInventory.model_validate(
        metadata[CURATOR_CARRIER_INVENTORY_KEY]
    )
    assert inventory.carriers[0].digest == digest
    assert digest not in metadata["note"]
