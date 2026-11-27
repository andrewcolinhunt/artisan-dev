"""Raw registrations survive source loss and interrupted exact table publication."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from concurrent.futures import CancelledError
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from fixtures.retained_acceptance import open_committer
from fixtures.retained_curators import stage_curator
from fixtures.retained_readers import Backend, admit_read_case
from fixtures.retained_results import RetainedCase
from fsspec.implementations.local import LocalFileSystem
from pydantic import ValidationError

from artisan.errors import ArtifactIntegrityError, StoreIntegrityError
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.operations.curator.ingest_csv import IngestCSV
from artisan.operations.curator.merge import Merge
from artisan.orchestration.engine.curator_inputs import prepare_curator_inputs
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.core.table_schemas import get_physical_schema_for_path
from artisan.storage.io.curator_external_reads import capture_curator_file
from artisan.storage.io.input_registration import (
    read_registered_inputs,
    recover_input_registration,
    register_raw_inputs,
)
from artisan.storage.io.input_registration_plan import (
    REGISTRATION_TABLES,
    InputRegistrationPlan,
    input_registration_digest,
    read_input_registration,
    registration_directory,
)
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.utils.hashing import STREAM_CHUNK_BYTES, compute_content_digest
from artisan.utils.path import uri_join

OWNER = "a" * 32


class Interrupted(RuntimeError):
    pass


def _stop_at(event: str) -> Callable[[str], None]:
    def checkpoint(observed: str) -> None:
        if observed == event:
            raise Interrupted(event)

    return checkpoint


def _register(
    committer: RetainedCommitter, backend: Backend, paths: list[str], **kwargs: Any
) -> InputRegistrationPlan:
    return register_raw_inputs(
        committer,
        paths,
        step_run_id=kwargs.pop("step_run_id", OWNER),
        step_number=kwargs.pop("step_number", 3),
        operation_name="IngestCSV",
        storage=backend[1],
        **kwargs,
    )


def _open(backend: Backend, *, initialize: bool = True) -> RetainedCommitter:
    fs, storage, root = backend
    return open_committer(f"{root}/registered", fs, storage, initialize=initialize)


def _source(tmp_path: Path, name: str = "values.csv") -> Path:
    path = tmp_path / name
    path.write_bytes(b"x\n1\n")
    return path


def _view(committer: RetainedCommitter) -> CuratorView:
    return CuratorView(committer, capture_curator_view(committer))


def _load(view: CuratorView, artifact_id: str) -> FileRefArtifact:
    committer = view.committer
    store = ArtifactStore(
        committer.delta_root,
        files_root=committer.files_root,
        fs=committer.fs,
        storage_options=committer.storage_options,
        _view=view,
    )
    result = store.get_artifacts_by_type([artifact_id], "file_ref")[artifact_id]
    assert isinstance(result, FileRefArtifact)
    return result


def test_registered_reader_keeps_one_snapshot_and_next_read_is_fresh(
    backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _open(backend_fs)
    source = _source(tmp_path)
    plan = _register(committer, backend_fs, [str(source), str(source)])
    source.unlink()
    path = uri_join(committer.delta_root, REGISTRATION_TABLES[0])
    with committer._read_snapshot() as snapshot:
        first = read_registered_inputs(snapshot, plan)
        assert len(first) == 1
        artifact = first[plan.inputs[0].artifact_id]
        assert artifact.read_content(fs=committer.fs) == b"x\n1\n"
        pl.read_delta(path, storage_options=committer.storage_options).with_columns(
            pl.lit(99, dtype=pl.Int32).alias("origin_step_number")
        ).write_delta(path, mode="overwrite", storage_options=committer.storage_options)
        assert (
            read_registered_inputs(snapshot, plan)[plan.inputs[0].artifact_id].to_row()
            == artifact.to_row()
        )
    with committer._read_snapshot() as fresh, pytest.raises(StoreIntegrityError):
        read_registered_inputs(fresh, plan)


@pytest.mark.parametrize("event", ["plan", "planned"])
def test_registered_reader_does_not_recover_incomplete_selection(
    backend_fs: Backend, tmp_path: Path, event: str
) -> None:
    committer = _open(backend_fs)
    with pytest.raises(Interrupted):
        _register(
            committer,
            backend_fs,
            [str(_source(tmp_path))],
            checkpoint=_stop_at(event),
        )
    plan = read_input_registration(committer.delta_root, OWNER, committer.fs)
    assert plan is not None
    before = committer._controls()
    with committer._read_snapshot() as snapshot, pytest.raises(StoreIntegrityError):
        read_registered_inputs(snapshot, plan)
    assert committer._controls().equals(before)


def test_raw_registration_preserves_order_and_owns_copies(
    backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _open(backend_fs)
    one, two = _source(tmp_path), _source(tmp_path, "other.csv")
    paths = [str(two), str(one), str(two)]
    plan = _register(committer, backend_fs, paths)
    assert [item.uri for item in plan.inputs] == paths
    assert plan.inputs[0].artifact_id == plan.inputs[2].artifact_id
    assert plan.inputs[0].artifact_id != plan.inputs[1].artifact_id
    assert len(plan.carriers) == 1
    assert all(table.row_count == 2 for table in plan.tables)
    assert committer._controls()["commit_kind"].to_list() == ["input_registration"]
    assert committer.journal.states(()) == {}
    one.write_bytes(b"modified original\n")
    two.unlink()
    for item in plan.inputs:
        artifact = _load(_view(committer), item.artifact_id)
        assert artifact.origin_step_number == 3
        assert artifact.path.startswith(committer.files_root + "/carriers/")
        with capture_curator_file(_view(committer), artifact) as captured:
            assert captured.path.read_bytes() == b"x\n1\n"
    assert _register(committer, backend_fs, paths) == plan
    with pytest.raises(StoreIntegrityError, match="ordered acquisition"):
        _register(committer, backend_fs, list(reversed(paths[:2])))


@pytest.mark.parametrize(
    ("filename", "stem", "extension"),
    [
        ("data.csv", "data", ".csv"),
        ("data.csv.gz", "data", ".csv.gz"),
        ("data.v1.csv.zst", "data", ".v1.csv.zst"),
        (".hidden.csv.gz", ".hidden", ".csv.gz"),
        ("data..csv.gz", "data.", ".csv.gz"),
        ("data.", "data.", ""),
        ("no_extension", "no_extension", ""),
    ],
)
def test_registration_retains_complete_filename_after_source_loss(
    backend_fs: Backend, tmp_path: Path, filename: str, stem: str, extension: str
) -> None:
    committer = _open(backend_fs)
    source = _source(tmp_path, filename)
    plan = _register(committer, backend_fs, [str(source)])
    source.unlink()
    reopened = _open(backend_fs, initialize=False)
    artifact = _load(_view(reopened), plan.inputs[0].artifact_id)
    assert artifact.original_name == stem
    assert artifact.extension == extension
    assert artifact.original_name + artifact.extension == filename
    assert artifact.path.endswith(".bin")
    assert artifact.read_content(fs=reopened.fs) == b"x\n1\n"
    destination = Path(
        artifact.materialize_to(str(tmp_path / "materialized"), fs=reopened.fs)
    )
    assert destination.name == filename
    assert destination.read_bytes() == b"x\n1\n"
    assert artifact.artifact_id == plan.inputs[0].artifact_id


@pytest.mark.parametrize(
    "boundary",
    ["plan", "planned", *(f"table:{path}" for path in REGISTRATION_TABLES), "complete"],
)
def test_registration_recovers_exact_effects_without_sources(
    backend_fs: Backend, tmp_path: Path, boundary: str
) -> None:
    committer = _open(backend_fs)
    source = _source(tmp_path)
    with pytest.raises(Interrupted):
        _register(
            committer, backend_fs, [str(source)] * 2, checkpoint=_stop_at(boundary)
        )
    selected = read_input_registration(committer.delta_root, OWNER, committer.fs)
    assert selected is not None
    visible = _view(committer).scan(TablePath.ARTIFACT_INDEX).collect()
    assert visible.height == int(boundary == "complete")
    source.unlink()
    reopened = _open(backend_fs, initialize=False)
    assert recover_input_registration(reopened, OWNER) == selected
    assert recover_input_registration(reopened, OWNER) == selected
    for path in REGISTRATION_TABLES:
        assert reopened._snapshot(path).delta.to_pyarrow_table().num_rows == 1
    assert (
        _load(_view(reopened), selected.inputs[0].artifact_id).read_content(
            fs=reopened.fs
        )
        == b"x\n1\n"
    )


def test_cancellation_before_plan_leaves_no_visible_registration(
    backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _open(backend_fs)
    source = _source(tmp_path)
    prepared = False

    def checkpoint(event: str) -> None:
        nonlocal prepared
        prepared |= event == "prepared"

    def check() -> None:
        if prepared:
            raise CancelledError

    with pytest.raises(CancelledError):
        _register(
            committer,
            backend_fs,
            [str(source)],
            check_cancelled=check,
            checkpoint=checkpoint,
        )
    assert read_input_registration(committer.delta_root, OWNER, committer.fs) is None
    assert committer._controls().is_empty()
    assert _view(committer).scan(TablePath.ARTIFACT_INDEX).collect().is_empty()
    source.write_bytes(b"new acquisition\n")
    plan = _register(committer, backend_fs, [str(source)])
    assert (
        _load(_view(committer), plan.inputs[0].artifact_id).read_content(
            fs=committer.fs
        )
        == b"new acquisition\n"
    )


def test_existing_content_retains_original_physical_owner_and_origin(
    backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _open(backend_fs)
    source = _source(tmp_path)
    first = _register(committer, backend_fs, [str(source)])
    second = _register(
        committer, backend_fs, [str(source)] * 2, step_run_id="b" * 32, step_number=9
    )
    assert (
        second.expected_origin(REGISTRATION_TABLES[0], (first.inputs[0].artifact_id,))
        == 3
    )
    for path in REGISTRATION_TABLES:
        rows = committer._snapshot(path).delta.to_pyarrow_table().to_pylist()
        assert len(rows) == 1
        assert rows[0]["logical_commit_id"] == first.logical_commit_id
    assert recover_input_registration(committer, second.step_run_id) == second


def test_incomplete_foreign_registration_cannot_be_adopted(
    backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _open(backend_fs)
    source = _source(tmp_path)
    with pytest.raises(Interrupted):
        _register(
            committer,
            backend_fs,
            [str(source)],
            checkpoint=_stop_at("table:artifacts/file_refs"),
        )
    with pytest.raises(StoreIntegrityError, match="incomplete"):
        _register(committer, backend_fs, [str(source)], step_run_id="b" * 32)
    assert read_input_registration(committer.delta_root, "b" * 32, committer.fs) is None
    recover_input_registration(committer, OWNER)
    _register(committer, backend_fs, [str(source)], step_run_id="b" * 32)


@pytest.mark.parametrize("corruption", ["carrier", "table", "protected_origin"])
def test_registration_rejects_changed_persisted_evidence(
    backend_fs: Backend, tmp_path: Path, corruption: str
) -> None:
    committer = _open(backend_fs)
    source = _source(tmp_path)
    plan = _register(committer, backend_fs, [str(source)])
    if corruption == "carrier":
        committer.fs.pipe_file(
            uri_join(committer.files_root, plan.carriers[0].relative_path), b"corrupt"
        )
    elif corruption == "table":
        path = uri_join(
            registration_directory(committer.delta_root, OWNER),
            plan.tables[0].files[0].relative_path,
        )
        committer.fs.pipe_file(path, b"not parquet")
    else:
        path = uri_join(committer.delta_root, REGISTRATION_TABLES[0])
        pl.read_delta(path, storage_options=committer.storage_options).with_columns(
            pl.lit(99, dtype=pl.Int32).alias("origin_step_number")
        ).write_delta(path, mode="overwrite", storage_options=committer.storage_options)
    with pytest.raises(
        (StoreIntegrityError, ArtifactIntegrityError, OSError, ValueError)
    ):
        recover_input_registration(committer, OWNER)
    if corruption == "protected_origin":
        with pytest.raises(StoreIntegrityError, match="origin|conflict"):
            _register(committer, backend_fs, [str(source)], step_run_id="b" * 32)


def test_all_raw_sources_validate_before_any_registration(
    backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _open(backend_fs)
    source = _source(tmp_path)
    with pytest.raises(ArtifactIntegrityError, match="Invalid raw inputs"):
        _register(committer, backend_fs, [str(source), str(tmp_path / "missing")])
    assert not committer.fs.exists(registration_directory(committer.delta_root, OWNER))
    assert committer._controls().is_empty()


def test_mixed_local_and_storage_sources_use_individual_filesystems(
    backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _open(backend_fs)
    fs, _, root = backend_fs
    source = _source(tmp_path)
    remote = uri_join(root, "remote.csv")
    fs.pipe_file(remote, b"remote\n2\n")
    plan = _register(committer, backend_fs, [str(source), remote, str(source)])
    assert len(plan.inputs) == 3
    assert len(plan.carriers) == 2
    assert [
        _load(_view(committer), item.artifact_id).read_content(fs=fs)
        for item in plan.inputs
    ] == [b"x\n1\n", b"remote\n2\n", b"x\n1\n"]


@pytest.mark.parametrize("field", ["inputs", "external_bindings", "carriers", "tables"])
def test_registration_plan_rejects_missing_exact_membership(
    tmp_path: Path, field: str
) -> None:
    from fsspec.implementations.local import LocalFileSystem

    from artisan.schemas.execution.storage_config import StorageConfig

    backend = LocalFileSystem(), StorageConfig(), str(tmp_path)
    committer = _open(backend)
    plan = _register(committer, backend, [str(_source(tmp_path))])
    payload = plan.model_dump(mode="json", exclude={"plan_digest"})
    payload[field] = []
    payload["plan_digest"] = input_registration_digest(payload)
    with pytest.raises(ValidationError):
        InputRegistrationPlan.model_validate(payload)


@pytest.mark.parametrize("ingest", [True, False])
def test_registered_inputs_support_curator_admission_execution_and_reading(
    backend_fs: Backend, tmp_path: Path, ingest: bool
) -> None:
    committer = _open(backend_fs)
    source = _source(tmp_path)
    registered = _register(committer, backend_fs, [str(source)] * 2)
    source.unlink()
    operation = IngestCSV() if ingest else Merge()
    record = StepStartRecord(
        step_run_id="b" * 32,
        step_number=4,
        step_name=operation.name,
        operation_class=f"{type(operation).__module__}.{type(operation).__qualname__}",
        params_json="{}",
        input_refs_json="{}",
        compute_backend="local",
        compute_options_json=json.dumps(
            {"dynamic_output_roles": not operation.outputs}
        ),
        output_roles_json=json.dumps(list(operation.outputs)),
        output_types_json=json.dumps(
            {role: spec.artifact_type for role, spec in operation.outputs.items()}
        ),
    )
    coordinator = CuratorResults(
        RetainedStepTracker(committer, "raw-input-test"),
        f"{committer.delta_root}/curator-staging",
    )
    coordinator.initialize()
    admission = prepare_curator_inputs(
        operation,
        record,
        {"file": [item.artifact_id for item in registered.inputs]},
        software_digest="c" * 64,
        software_cacheable=False,
        committer=committer,
    )
    assert len(admission.inputs[0].members) == 2
    assert all(not member.producers for member in admission.inputs[0].members)
    assert registered.logical_commit_id in {
        control.logical_commit_id for control in admission.view.controls
    }
    reference = coordinator.admit(admission)
    staged, terminal = stage_curator(coordinator, admission, operation)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    plan = coordinator._plan(record.step_run_id)
    assert plan is not None
    result = CuratorReader(committer).read_result(record.step_run_id, plan.plan_digest)
    outputs = result.output("csv" if ingest else "merged")
    assert len(outputs) == 1
    assert [
        item.artifact.read_content(fs=committer.fs)
        if isinstance(item.artifact, FileRefArtifact)
        else item.artifact.content
        for item in outputs
    ] == [b"x\n1\n"]


def test_registration_preserves_preexisting_standalone_origin(
    backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _open(backend_fs)
    source = _source(tmp_path)
    artifact = FileRefArtifact.draft(
        str(source),
        compute_content_digest(b"x\n1\n"),
        4,
        original_name="values",
        extension=".csv",
    ).finalize()
    graph = OccurrenceGraph()
    address = graph.add_input("file", artifact)
    case = RetainedCase(
        graph.project({"file": [address]}, {"file": OutputSpec()}),
        {address: artifact},
        {},
    )
    _, receipt = admit_read_case(committer, case, step_run_id="c" * 32)
    committer.accept(receipt)
    plan = _register(committer, backend_fs, [str(source)])
    assert (
        plan.expected_origin(REGISTRATION_TABLES[0], (plan.inputs[0].artifact_id,))
        is None
    )
    assert (
        _load(_view(committer), plan.inputs[0].artifact_id).origin_step_number is None
    )


def test_empty_raw_file_has_complete_managed_publication(
    backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _open(backend_fs)
    source = tmp_path / "empty.bin"
    source.write_bytes(b"")
    plan = _register(committer, backend_fs, [str(source)])
    assert plan.carriers[0].size_bytes == 0
    source.unlink()
    assert recover_input_registration(committer, OWNER) == plan
    assert (
        _load(_view(committer), plan.inputs[0].artifact_id).read_content(
            fs=committer.fs
        )
        == b""
    )


def test_failed_source_copy_does_not_publish_partial_registration(
    backend_fs: Backend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import artisan.storage.io.input_registration_sources as module

    committer = _open(backend_fs)
    one, two = _source(tmp_path), _source(tmp_path, "second.csv")
    digest = module.compute_stream_digest
    calls = 0

    def fail_second(stream: Any, *, destination: Any) -> tuple[str, int]:
        nonlocal calls
        calls += 1
        if calls == 2:
            destination.write(b"partial")
            msg = "synthetic copy failure"
            raise OSError(msg)
        return digest(stream, destination=destination)

    monkeypatch.setattr(module, "compute_stream_digest", fail_second)
    with pytest.raises(ArtifactIntegrityError, match="Cannot capture raw input"):
        _register(committer, backend_fs, [str(one), str(two)])
    assert committer._controls().is_empty()
    assert not committer.fs.exists(registration_directory(committer.delta_root, OWNER))
    monkeypatch.setattr(module, "compute_stream_digest", digest)
    assert len(_register(committer, backend_fs, [str(one), str(two)]).inputs) == 2


def test_registration_cannot_own_scientific_edges_in_current_or_frozen_views(
    backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _open(backend_fs)
    plan = _register(committer, backend_fs, [str(_source(tmp_path))])
    CuratorResults(
        RetainedStepTracker(committer, "raw-test"), f"{committer.delta_root}/staging"
    ).initialize()
    path = uri_join(committer.delta_root, TablePath.ARTIFACT_EDGES.value)
    schema = get_physical_schema_for_path(TablePath.ARTIFACT_EDGES)
    row = {
        "execution_run_id": "f" * 32,
        "source_artifact_id": plan.inputs[0].artifact_id,
        "target_artifact_id": "e" * 32,
        "source_artifact_type": "file_ref",
        "target_artifact_type": "file_ref",
        "source_role": "file",
        "target_role": "derived",
        "group_id": None,
        "step_boundary": True,
        "logical_commit_id": plan.logical_commit_id,
    }
    pl.DataFrame([row], schema=schema).write_delta(
        path, mode="append", storage_options=committer.storage_options
    )
    frozen = _view(committer)
    with pytest.raises(StoreIntegrityError, match="undeclared"):
        frozen.artifact_edges(direct=True).collect()
    with pytest.raises(StoreIntegrityError, match="unplanned"):
        recover_input_registration(committer, OWNER)
    pl.DataFrame(schema=schema).write_delta(
        path, mode="overwrite", storage_options=committer.storage_options
    )
    assert recover_input_registration(committer, OWNER) == plan
    assert _view(committer).artifact_edges(direct=True).collect().is_empty()
    with pytest.raises(StoreIntegrityError, match="undeclared"):
        frozen.artifact_edges(direct=True).collect()


def test_cancellation_interrupts_source_capture_between_bounded_reads(
    backend_fs: Backend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    committer = _open(backend_fs)
    source = tmp_path / "large.bin"
    source.write_bytes(b"x" * (STREAM_CHUNK_BYTES + 1))
    original = LocalFileSystem.open
    cancelled = False
    reads: list[int] = []

    class ObservedSource:
        def __init__(self, stream: Any) -> None:
            self.stream = stream

        def read(self, size: int = -1) -> bytes:
            nonlocal cancelled
            chunk = self.stream.read(size)
            reads.append(len(chunk))
            cancelled = True
            return chunk

    @contextmanager
    def opened(
        fs: LocalFileSystem, path: str, *args: Any, **kwargs: Any
    ) -> Iterator[Any]:
        with original(fs, path, *args, **kwargs) as stream:
            yield ObservedSource(stream) if path == str(source) else stream

    def check() -> None:
        if cancelled:
            raise CancelledError

    monkeypatch.setattr(LocalFileSystem, "open", opened)
    with pytest.raises(CancelledError):
        _register(committer, backend_fs, [str(source)], check_cancelled=check)
    assert reads == [STREAM_CHUNK_BYTES]
    assert committer._controls().is_empty()
    assert read_input_registration(committer.delta_root, OWNER, committer.fs) is None
