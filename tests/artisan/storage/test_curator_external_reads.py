"""Closed ingestion reads qualify cache reuse without changing source paths."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

import polars as pl
import pytest
from fixtures.retained_acceptance import admit_case, open_committer
from fixtures.retained_curators import admit_curator
from fixtures.retained_readers import Backend
from fsspec import AbstractFileSystem

from artisan.errors import ArtifactIntegrityError, StoreIntegrityError
from artisan.execution.executors.curator import run_curator_flow
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording.curator_read_filesystem import CuratorReadFileSystem
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.curator.filter import Filter
from artisan.operations.curator.ingest_csv import IngestCSV
from artisan.operations.curator.merge import Merge
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.execution.curator_admission import (
    CuratorAdmission,
    CuratorInputAssociation,
)
from artisan.schemas.execution.curator_result import ArtifactStreamResult
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.storage.cache.curator_dependencies import (
    CuratorReadRecorder,
    observe_curator_reads,
    validate_curator_cache_proof,
)
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.curator_view import CuratorView
from artisan.storage.core.retained_reader import AcceptedArtifact, RetainedReader
from artisan.storage.io.curator_external_reads import capture_curator_file
from artisan.storage.io.retained_commit import RetainedCommitter


def _setup(
    backend_fs: Backend, tmp_path: Path
) -> tuple[RetainedCommitter, tuple[AcceptedArtifact, ...]]:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_case(committer, tmp_path, kind="file")
    committer.accept(receipt)
    return committer, RetainedReader(committer).read(receipt).support[:1]


def _store(view: CuratorView) -> ArtifactStore:
    committer = view.committer
    return ArtifactStore(
        committer.delta_root,
        files_root=committer.files_root,
        fs=committer.fs,
        storage_options=committer.storage_options,
        _view=view,
    )


def _run(
    coordinator: CuratorResults,
    admission: CuratorAdmission,
    operation: OperationDefinition,
    storage: StorageConfig,
) -> tuple[StagingResult, StepResult, CuratorReadRecorder]:
    committer = coordinator.committer
    view = CuratorView(committer, admission.view)
    recorder = CuratorReadRecorder(view, admission)
    staged = run_curator_flow(
        ExecutionUnit(
            operation=operation,
            inputs={
                group.role: [item.artifact_id for item in group.members]
                for group in admission.inputs
            },
            step_number=admission.record.step_number,
            step_run_id=admission.record.step_run_id,
            execution_spec_id=admission.digest,
            replay_snapshot=ReplaySnapshot.unavailable("synthetic-file-ingestion"),
        ),
        RuntimeEnvironment(
            delta_root=committer.delta_root,
            files_root=committer.files_root,
            staging_root=coordinator.staging.staging_dir,
            storage=storage,
        ),
        curator_view=view,
        read_recorder=recorder,
    )
    result = StepResult(
        step_run_id=admission.record.step_run_id,
        step_number=admission.record.step_number,
        step_name=operation.name,
        status=StepStatus.SUCCEEDED if staged.success else StepStatus.FAILED,
        disposition=StepDisposition.EXECUTED if staged.success else None,
        total_count=1,
        succeeded_count=int(staged.success),
        failed_count=int(not staged.success),
        output_roles=frozenset(operation.outputs) if staged.success else frozenset(),
        output_types={
            role: spec.artifact_type for role, spec in operation.outputs.items()
        }
        if staged.success
        else {},
        error=staged.error,
    )
    return staged, result, recorder


def test_ingest_csv_reuses_sealed_carrier_reads_with_original_paths(
    backend_fs: Backend, tmp_path: Path
) -> None:
    committer, supplied = _setup(backend_fs, tmp_path)
    source = cast(FileRefArtifact, supplied[0].artifact)
    operation = IngestCSV()
    coordinator, admission, reference = admit_curator(
        committer,
        operation,
        {"file": supplied},
        step_run_id="b" * 32,
        software_digest="e" * 64,
    )
    staged, result, recorder = _run(coordinator, admission, operation, backend_fs[1])
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    assert not recorder.proof.ineligible_reasons
    assert len(recorder.proof.carrier_reads) == 1
    assert recorder.proof.carrier_reads[0].uri == source.path
    coordinator.finish(reference, staged.execution_run_id, result)
    _, _, current = admit_curator(
        committer,
        operation,
        {"file": supplied},
        step_run_id="c" * 32,
        step_number=9,
        software_digest="e" * 64,
    )
    reader = CuratorReader(committer)
    hit = lookup_curator(current, reader=reader)
    assert hit is not None
    accepted = hit.source.output("csv")[0].artifact
    assert isinstance(accepted, CSVArtifact)
    assert accepted.content == source.read_content(fs=committer.fs)
    assert coordinator.reuse(hit).disposition == StepDisposition.CACHE_HIT
    assert hit.source.plan.cache_proof == recorder.proof


@pytest.mark.parametrize("stage", ["before", "after_conversion", "after_completion"])
def test_ingest_source_mutation_cannot_qualify_or_reuse(
    backend_fs: Backend, tmp_path: Path, stage: str
) -> None:
    committer, supplied = _setup(backend_fs, tmp_path)
    source = cast(FileRefArtifact, supplied[0].artifact)
    assert source.path is not None
    operation = IngestCSV()
    coordinator, admission, reference = admit_curator(
        committer,
        operation,
        {"file": supplied},
        step_run_id="b" * 32,
        software_digest="e" * 64,
    )
    if stage == "before":
        committer.fs.pipe_file(source.path, b"changed bytes\n")
    staged, terminal, _ = _run(coordinator, admission, operation, backend_fs[1])
    if stage == "before":
        assert not staged.success
        assert "integrity" in (staged.error or "").lower()
        return
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    current = None
    if stage == "after_completion":
        coordinator.finish(reference, staged.execution_run_id, terminal)
        _, _, current = admit_curator(
            committer,
            operation,
            {"file": supplied},
            step_run_id="c" * 32,
            step_number=9,
            software_digest="e" * 64,
        )
    committer.fs.pipe_file(source.path, b"changed bytes\n")
    with pytest.raises(ArtifactIntegrityError, match="integrity"):
        if stage == "after_conversion":
            coordinator.finish(reference, staged.execution_run_id, terminal)
        else:
            assert current is not None
            lookup_curator(current, reader=CuratorReader(committer))


@pytest.mark.parametrize("target", ["capture", "original"])
def test_capture_rechecks_full_bytes_and_removes_scratch(
    backend_fs: Backend, tmp_path: Path, target: str
) -> None:
    committer, supplied = _setup(backend_fs, tmp_path)
    artifact = cast(FileRefArtifact, supplied[0].artifact)
    _, admission, _ = admit_curator(
        committer, IngestCSV(), {"file": supplied}, step_run_id="b" * 32
    )
    view = CuratorView(committer, admission.view)
    with (
        pytest.raises(ArtifactIntegrityError, match="integrity"),
        capture_curator_file(view, artifact) as captured,
    ):
        saved_path = captured.path
        if target == "capture":
            saved_path.write_bytes(b"modified copy")
        else:
            committer.fs.pipe_file(captured.witness.uri, b"modified original")
            adapter = CuratorReadFileSystem(
                committer.fs,
                captured.witness.uri,
                saved_path,
                exclude=lambda: pytest.fail("supported read excluded"),
            )
            with adapter.open(captured.witness.uri) as stream:
                assert stream.read() != b"modified original"
            adapter.close()
    assert not saved_path.exists()


def test_external_association_and_generic_hydration_remain_unqualified(
    backend_fs: Backend, tmp_path: Path
) -> None:
    committer, supplied = _setup(backend_fs, tmp_path)
    source = supplied[0].artifact
    assert source.artifact_id is not None
    _, admission, _ = admit_curator(
        committer,
        IngestCSV(),
        {"file": supplied},
        step_run_id="b" * 32,
        software_digest="e" * 64,
        associations=(
            CuratorInputAssociation(
                primary_id=source.artifact_id,
                artifact_type="file_ref",
                artifact_ids=(source.artifact_id,),
            ),
        ),
    )
    view = CuratorView(committer, admission.view)
    store = _store(view)
    with observe_curator_reads(view, admission) as recorder:
        store.get_artifacts_by_type([source.artifact_id], "file_ref")
        result = IngestCSV().execute_curator(
            {"file": pl.DataFrame({"artifact_id": [source.artifact_id]})}, 8, store
        )
        assert isinstance(result, ArtifactStreamResult)
        assert len(list(result.items)) == 1
    assert recorder.proof.carrier_reads
    assert recorder.proof.ineligible_reasons == ("untracked_external_content",)
    validate_curator_cache_proof(recorder.proof, view, admission=admission)


@pytest.mark.parametrize(
    ("backend_fs", "action"),
    [
        pytest.param("local", "bypass"),
        pytest.param("local", "exists"),
        pytest.param("local", "listing"),
        pytest.param("local", "write"),
        pytest.param("s3", "exists", marks=pytest.mark.s3),
        pytest.param("s3", "listing", marks=pytest.mark.s3),
        pytest.param("s3", "write", marks=pytest.mark.s3),
    ],
    indirect=["backend_fs"],
)
def test_unsupported_ingestion_reads_keep_behavior_and_exclude_cache(
    backend_fs: Backend, tmp_path: Path, action: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    committer, supplied = _setup(backend_fs, tmp_path)
    original = IngestCSV.convert_file
    observed: list[object] = []

    def convert(
        self: IngestCSV,
        file_ref: FileRefArtifact,
        step_number: int,
        *,
        fs: AbstractFileSystem | None = None,
    ) -> CSVArtifact:
        assert file_ref.path is not None
        assert fs is not None
        assert file_ref.path == cast(FileRefArtifact, supplied[0].artifact).path
        if action == "bypass":
            return CSVArtifact.draft(Path(file_ref.path).read_bytes(), "direct.csv")
        absent = f"{committer.files_root}/absent-file"
        if action == "exists":
            observed.append(fs.exists(absent))
        elif action == "listing":
            observed.append(fs.glob(f"{committer.files_root}/absent-*"))
        else:
            with fs.open(absent, "wb") as stream:
                stream.write(b"authored write")
            observed.append(committer.fs.exists(absent))
        return original(self, file_ref, step_number, fs=fs)

    monkeypatch.setattr(IngestCSV, "convert_file", convert)
    operation = IngestCSV()
    coordinator, admission, _ = admit_curator(
        committer,
        operation,
        {"file": supplied},
        step_run_id="b" * 32,
        software_digest="e" * 64,
    )
    staged, _, recorder = _run(coordinator, admission, operation, backend_fs[1])
    assert staged.success, staged.error
    expected = (
        "untracked_external_content"
        if action == "bypass"
        else "untracked_filesystem_access"
    )
    assert expected in recorder.proof.ineligible_reasons
    if action != "bypass":
        assert observed == [{"exists": False, "listing": [], "write": True}[action]]


def test_stream_capture_lifetime_and_duplicate_inputs(
    backend_fs: Backend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from artisan.operations.curator import ingest_files

    committer, supplied = _setup(backend_fs, tmp_path)
    _, admission, _ = admit_curator(
        committer,
        IngestCSV(),
        {"file": supplied},
        step_run_id="b" * 32,
        software_digest="e" * 64,
    )
    paths: list[Path] = []
    capture = ingest_files.capture_curator_file

    @contextmanager
    def tracked_capture(*args: Any, **kwargs: Any) -> Iterator[Any]:
        with capture(*args, **kwargs) as captured:
            paths.append(captured.path)
            yield captured

    monkeypatch.setattr(ingest_files, "capture_curator_file", tracked_capture)
    view = CuratorView(committer, admission.view)
    inputs = {
        "file": pl.DataFrame({"artifact_id": [supplied[0].artifact.artifact_id] * 2})
    }
    recorder = CuratorReadRecorder(view, admission)
    with recorder.authored():
        result = IngestCSV().execute_curator(inputs, 8, _store(view))
        assert isinstance(result, ArtifactStreamResult)
        assert paths == []
        iterator = iter(result.items)
        first = next(iterator)
        assert paths[0].exists()
        second = next(iterator)
        assert not paths[0].exists()
        assert paths[1].exists()
        assert first.artifact.content == second.artifact.content
        assert (
            first.artifact.external_path
            == cast(FileRefArtifact, supplied[0].artifact).path
        )
        with pytest.raises(StopIteration):
            next(iterator)
        assert not paths[1].exists()
    assert len(recorder.finish().carrier_reads) == 1


@pytest.mark.parametrize("failure", [False, True])
def test_interrupted_ingestion_releases_capture_without_eligible_proof(
    backend_fs: Backend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: bool,
) -> None:
    from artisan.operations.curator import ingest_files

    committer, supplied = _setup(backend_fs, tmp_path)
    paths: list[Path] = []
    capture = ingest_files.capture_curator_file

    @contextmanager
    def tracked_capture(*args: Any, **kwargs: Any) -> Iterator[Any]:
        with capture(*args, **kwargs) as captured:
            paths.append(captured.path)
            yield captured

    def fail(*args: Any, **kwargs: Any) -> CSVArtifact:
        msg = "conversion failed"
        raise RuntimeError(msg)

    monkeypatch.setattr(ingest_files, "capture_curator_file", tracked_capture)
    if failure:
        monkeypatch.setattr(IngestCSV, "convert_file", fail)
    operation = IngestCSV()
    coordinator, admission, _ = admit_curator(
        committer,
        operation,
        {"file": supplied},
        step_run_id="b" * 32,
        software_digest="e" * 64,
    )
    if failure:
        staged, _, recorder = _run(coordinator, admission, operation, backend_fs[1])
        assert not staged.success
        with pytest.raises(RuntimeError, match="not completed"):
            _ = recorder.proof
    else:
        view = CuratorView(committer, admission.view)
        with observe_curator_reads(view, admission) as recorder:
            result = operation.execute_curator(
                {
                    "file": pl.DataFrame(
                        {"artifact_id": [supplied[0].artifact.artifact_id]}
                    )
                },
                8,
                _store(view),
            )
            assert isinstance(result, ArtifactStreamResult)
            iterator = iter(result.items)
            next(iterator)
            assert paths[0].exists()
            iterator.close()
        assert recorder.proof.ineligible_reasons == ("untracked_external_content",)
        assert not recorder.proof.carrier_reads
    assert paths
    assert all(not path.exists() for path in paths)


def test_original_changes_during_capture_never_qualifies(
    backend_fs: Backend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from artisan.schemas.artifact import external

    committer, supplied = _setup(backend_fs, tmp_path)
    artifact = cast(FileRefArtifact, supplied[0].artifact)
    _, admission, _ = admit_curator(
        committer, IngestCSV(), {"file": supplied}, step_run_id="b" * 32
    )
    original_open = external.open_external
    changed = False

    class MutatingRead:
        def __init__(self, stream: Any) -> None:
            self.stream = stream

        def read(self, size: int) -> bytes:
            nonlocal changed
            if changed:
                return cast(bytes, self.stream.read(size))
            chunk = cast(bytes, self.stream.read(1))
            assert artifact.path is not None
            committer.fs.pipe_file(artifact.path, b"mutation during capture\n")
            changed = True
            return chunk

    @contextmanager
    def mutating_open(uri: str, fs: Any = None) -> Iterator[Any]:
        with original_open(uri, fs) as stream:
            yield MutatingRead(stream) if uri == artifact.path else stream

    monkeypatch.setattr(external, "open_external", mutating_open)
    with (
        pytest.raises(ArtifactIntegrityError, match="integrity|readable verified"),
        capture_curator_file(CuratorView(committer, admission.view), artifact),
    ):
        pass
    assert changed


@pytest.mark.parametrize("mode", ["rb", "r", "raw", "detach"])
def test_stream_name_preserves_source_identity_and_raw_access_is_unqualified(
    backend_fs: Backend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    committer, supplied = _setup(backend_fs, tmp_path)
    source = cast(FileRefArtifact, supplied[0].artifact)
    names: list[str] = []

    def convert(
        self: IngestCSV,
        file_ref: FileRefArtifact,
        step_number: int,
        *,
        fs: AbstractFileSystem | None = None,
    ) -> CSVArtifact:
        assert file_ref.path is not None
        assert fs is not None
        with fs.open(file_ref.path, "r" if mode == "r" else "rb") as stream:
            names.append(stream.name)
            if mode == "raw":
                content = stream.raw.read()
            elif mode == "detach":
                content = stream.detach().read()
            else:
                content = stream.read()
        return CSVArtifact.draft(
            content.encode() if isinstance(content, str) else content,
            "from-stream.csv",
            step_number,
            metadata={"source_name": names[-1]},
        )

    monkeypatch.setattr(IngestCSV, "convert_file", convert)
    operation = IngestCSV()
    coordinator, admission, reference = admit_curator(
        committer,
        operation,
        {"file": supplied},
        step_run_id="b" * 32,
        software_digest="e" * 64,
    )
    staged, terminal, recorder = _run(coordinator, admission, operation, backend_fs[1])
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    assert names == [source.path]
    coordinator.finish(reference, staged.execution_run_id, terminal)
    if mode in {"raw", "detach"}:
        assert "untracked_filesystem_access" in recorder.proof.ineligible_reasons
        assert not recorder.proof.carrier_reads
    else:
        assert not recorder.proof.ineligible_reasons
        assert recorder.proof.carrier_reads


@pytest.mark.parametrize("kind", ["merge", "filter"])
def test_id_only_curators_reuse_external_outputs_without_byte_read_witnesses(
    backend_fs: Backend, tmp_path: Path, kind: str
) -> None:
    committer, supplied = _setup(backend_fs, tmp_path)
    operation = Merge() if kind == "merge" else Filter()
    inputs = {"files" if kind == "merge" else "passthrough": supplied}
    coordinator, admission, reference = admit_curator(
        committer,
        operation,
        inputs,
        step_run_id="b" * 32,
        software_digest="e" * 64,
    )
    staged, terminal, recorder = _run(coordinator, admission, operation, backend_fs[1])
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    assert not recorder.proof.views
    assert not recorder.proof.carrier_reads
    assert not recorder.proof.ineligible_reasons
    coordinator.finish(reference, staged.execution_run_id, terminal)
    _, _, current = admit_curator(
        committer,
        operation,
        inputs,
        step_run_id="c" * 32,
        step_number=9,
        software_digest="e" * 64,
    )
    hit = lookup_curator(current, reader=CuratorReader(committer))
    assert hit is not None
    output = hit.source.outputs[0].occurrences[0].artifact
    assert output.artifact_id == supplied[0].artifact.artifact_id
    assert isinstance(output, FileRefArtifact)
    assert output.read_content(fs=committer.fs)
    assert coordinator.reuse(hit).disposition == StepDisposition.CACHE_HIT


@pytest.mark.parametrize("stage", ["acceptance", "lookup"])
def test_id_only_selection_still_rejects_changed_output_carriers(
    backend_fs: Backend, tmp_path: Path, stage: str
) -> None:
    committer, supplied = _setup(backend_fs, tmp_path)
    operation = Merge()
    coordinator, admission, reference = admit_curator(
        committer,
        operation,
        {"files": supplied},
        step_run_id="b" * 32,
        software_digest="e" * 64,
    )
    staged, terminal, recorder = _run(coordinator, admission, operation, backend_fs[1])
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    assert not recorder.proof.ineligible_reasons
    current = None
    if stage == "lookup":
        coordinator.finish(reference, staged.execution_run_id, terminal)
        _, _, current = admit_curator(
            committer,
            operation,
            {"files": supplied},
            step_run_id="c" * 32,
            step_number=9,
            software_digest="e" * 64,
        )
    path = cast(FileRefArtifact, supplied[0].artifact).path
    assert path is not None
    committer.fs.pipe_file(path, b"changed selected output\n")
    with pytest.raises(
        (ArtifactIntegrityError, StoreIntegrityError), match="integrity|carrier"
    ):
        if current is None:
            coordinator.finish(reference, staged.execution_run_id, terminal)
        else:
            lookup_curator(current, reader=CuratorReader(committer))


def test_a_verified_duplicate_does_not_erase_prior_ingestion_bypass(
    backend_fs: Backend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    committer, supplied = _setup(backend_fs, tmp_path)
    original = IngestCSV.convert_file
    calls = 0

    def convert(
        self: IngestCSV,
        file_ref: FileRefArtifact,
        step_number: int,
        *,
        fs: AbstractFileSystem | None = None,
    ) -> CSVArtifact:
        nonlocal calls
        calls += 1
        return original(
            self, file_ref, step_number, fs=committer.fs if calls == 1 else fs
        )

    monkeypatch.setattr(IngestCSV, "convert_file", convert)
    operation = IngestCSV()
    coordinator, admission, _ = admit_curator(
        committer,
        operation,
        {"file": supplied + supplied},
        step_run_id="b" * 32,
        software_digest="e" * 64,
    )
    staged, _, recorder = _run(coordinator, admission, operation, backend_fs[1])
    assert staged.success, staged.error
    assert calls == 2
    assert len(recorder.proof.carrier_reads) == 1
    assert recorder.proof.ineligible_reasons == ("untracked_external_content",)
