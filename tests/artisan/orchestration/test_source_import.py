"""Candidate source imports preserve content while owning independent full bytes."""

from __future__ import annotations

import json
import shutil
from collections.abc import Mapping
from functools import partial
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from deltalake import DeltaTable
from fixtures.candidate_outputs import external_case, prepare_scientific
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_acceptance import open_committer
from fixtures.retained_curators import admit_curator, stage_curator
from fixtures.retained_results import retained_case
from fsspec import AbstractFileSystem
from fsspec.implementations.memory import MemoryFileSystem

from artisan.errors import (
    ArtifactIntegrityError,
    IncompatibleStoreError,
    StoreIntegrityError,
)
from artisan.execution.curator_resources import curator_resources
from artisan.execution.executors.curator import run_curator_flow
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.curator.ingest_pipeline_step import IngestPipelineStep
from artisan.orchestration.engine import source_import
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_config import WorkerConfig
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.cache.retained_reuse import verify_cache_source
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.curator_external_reads import curator_carrier_read
from artisan.storage.io.curator_plan import read_curator_plan
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_store import create_retained_store
from artisan.utils.path import uri_join


def _operation(
    source: RetainedCommitter, *, through: bool = False
) -> IngestPipelineStep:
    return IngestPipelineStep(
        params=IngestPipelineStep.Params(
            source_delta_root=source.delta_root,
            source_files_root=source.files_root,
            source_run_id="synthetic-pipeline",
            source_step=8 if through else 7,
            include_prior_steps=through,
        )
    )


def _source(
    root: str,
    storage: StorageConfig,
    tmp_path: Path,
    *,
    appendable: bool = False,
    files_root: str | None = None,
) -> RetainedCommitter:
    source = create_retained_store(
        root,
        files_root or f"{root}/files",
        fs=storage.filesystem(),
        storage_options=storage.delta_storage_options(),
    )
    results, admission, receipts = prepare_scientific(
        source, external_case(tmp_path, appendable=appendable)
    )
    source.accept(receipts[0])
    results.finish(admission.step_run_id)
    return source


@pytest.mark.parametrize(
    "direction",
    [
        "local",
        pytest.param("local-s3", marks=pytest.mark.s3),
        pytest.param("s3-local", marks=pytest.mark.s3),
        pytest.param("s3-s3", marks=pytest.mark.s3),
    ],
)
@pytest.mark.parametrize("appendable", [False, True])
def test_import_seals_independent_destination_bytes_before_source_deletion(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    direction: str,
    appendable: bool,
) -> None:
    local = StorageConfig()
    remote, remote_root = local, str(tmp_path / "unused")
    if direction != "local":
        _, remote, remote_root = request.getfixturevalue("s3_fs")
    source_storage = remote if direction.startswith("s3") else local
    source_storage = source_storage.model_copy(
        update={"options": {**source_storage.options, "skip_instance_cache": True}}
    )
    destination_storage = remote if direction.endswith("s3") else local
    source_root = (
        f"{remote_root}/source"
        if direction.startswith("s3")
        else str(tmp_path / "source")
    )
    destination_root = (
        f"{remote_root}/destination"
        if direction.endswith("s3")
        else str(tmp_path / "destination")
    )
    source = _source(
        source_root, source_storage, tmp_path / "original", appendable=appendable
    )
    original = read_current_step_outputs(source, "a" * 32)[0].occurrences[0].artifact
    locator = next(iter(original.LOCATOR_FIELDS))
    with source.fs.open(getattr(original, locator), "rb") as stream:
        payload = stream.read()
    destination = open_committer(
        destination_root,
        destination_storage.filesystem(),
        destination_storage,
        initialize=True,
    )
    assert source.fs is not destination.fs
    destination_open = destination.fs.open

    def destination_only(path: str, *args: Any, **kwargs: Any) -> Any:
        assert not str(path).startswith(source.files_root)
        return destination_open(path, *args, **kwargs)

    # Teardown must remove the instance override from fsspec's shared cached object.
    monkeypatch.setitem(destination.fs.__dict__, "open", destination_only)
    operation = _operation(source)
    operation.params.source_storage = source_storage
    imported: list[ArtifactResult] = []
    original_execute = IngestPipelineStep.execute_curator

    def execute(
        self: IngestPipelineStep,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        result = original_execute(self, inputs, step_number, artifact_store)
        imported.append(result)
        assert Path(
            getattr(result.artifacts[original.artifact_type][0], locator)
        ).exists()
        return result

    monkeypatch.setattr(IngestPipelineStep, "execute_curator", execute)
    coordinator, admission, reference = admit_curator(
        destination, operation, {}, step_run_id="b" * 32
    )
    staged = run_curator_flow(
        ExecutionUnit(
            operation=operation,
            inputs={},
            step_number=8,
            step_run_id=reference.step_run_id,
            execution_spec_id=admission.digest,
            replay_snapshot=ReplaySnapshot.unavailable("synthetic-import"),
        ),
        WorkerConfig(
            delta_root=destination.delta_root,
            staging_root=coordinator.staging.staging_dir,
            files_root=destination.files_root,
            working_root=str(tmp_path / "worker"),
            storage=destination_storage,
        ),
        curator_view=CuratorView(destination, admission.view),
    )
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    result = imported[0]
    assert len(result.artifacts[original.artifact_type]) == 1
    draft = result.artifacts[original.artifact_type][0]
    assert not Path(getattr(draft, locator)).exists()
    assert not tuple((tmp_path / "worker").glob("artisan-curator-source-*"))
    assert result.lineage == {original.artifact_type: []}
    assert draft.artifact_id != original.artifact_id
    assert draft.metadata["imported_from"] == {
        "pipeline_run_id": "synthetic-pipeline",
        "artifact_id": original.artifact_id,
        "origin_step_number": original.origin_step_number,
    }
    terminal = StepResult(
        step_run_id=reference.step_run_id,
        step_number=8,
        step_name=operation.name,
        status=StepStatus.SUCCEEDED,
        disposition=StepDisposition.EXECUTED,
        total_count=1,
        succeeded_count=1,
        failed_count=0,
        output_roles=frozenset(result.artifacts),
        output_types={role: role for role in result.artifacts},
        metadata=result.metadata,
    )
    coordinator.finish(reference, staged.execution_run_id, terminal)
    source.fs.rm(source.files_root, recursive=True)
    accepted = read_current_step_outputs(destination, reference.step_run_id)
    hydrated = accepted[0].occurrences[0].artifact
    assert getattr(hydrated, locator).startswith(destination.files_root)
    with destination.fs.open(getattr(hydrated, locator), "rb") as stream:
        assert stream.read() == payload
    hydrated.verify_external_content(fs=destination.fs)


def test_import_deduplicates_sorted_typed_content_and_records_current_contributors(
    tmp_path: Path,
) -> None:
    storage = StorageConfig()
    source = open_committer(
        str(tmp_path / "source"), storage.filesystem(), storage, initialize=True
    )
    results, admission, receipts = prepare_scientific(
        source, retained_case(tmp_path, kind="alias"), count=2
    )
    for receipt in receipts:
        source.accept(receipt)
    results.finish(admission.step_run_id)
    reused, current, _ = prepare_scientific(
        source, retained_case(tmp_path, kind="alias"), step_run_id="c" * 32
    )
    source.journal.reuse(
        current.assignments[0],
        receipts[0],
        verify_source=partial(verify_cache_source, reader=RetainedReader(source)),
    )
    reused.finish(current.step_run_id)
    operation = DynamicCurator()
    coordinator, admitted, reference = admit_curator(
        source, operation, {}, step_run_id="b" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(coordinator, admitted, operation)
    coordinator.finish(reference, staged.execution_run_id, terminal)
    _, _, current_curator = admit_curator(
        source, operation, {}, step_run_id="d" * 32, step_number=9, **STAMPS
    )
    hit = lookup_curator(current_curator, reader=CuratorReader(source))
    assert hit is not None
    coordinator.reuse(hit)
    importer = _operation(source, through=True)
    importer.params.source_step = 9
    result = source_import.import_candidate_outputs(importer, source, step_number=10)
    assert result.success
    sources = [
        item.metadata["imported_from"]["artifact_id"]
        for item in result.artifacts["csv"]
    ]
    assert sources == sorted(set(sources))
    assert len(sources) == 2
    assert result.metadata["ingest_source"]["step_run_ids"] == [
        "b" * 32,
        "c" * 32,
        "d" * 32,
    ]
    importer.params.artifact_type = "metric"
    empty = source_import.import_candidate_outputs(importer, source, step_number=10)
    assert not empty.success
    assert empty.error is not None
    assert "No accepted artifacts" in empty.error


def test_complete_appendable_mutation_after_selection_rejects_import(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _source(
        str(tmp_path / "source"),
        StorageConfig(),
        tmp_path / "original",
        appendable=True,
    )
    original_select = source_import.read_source_run_outputs

    def mutate(*args: Any, **kwargs: Any) -> Any:
        selected = original_select(*args, **kwargs)
        artifact = selected[0].outputs[0].occurrences[0].artifact
        with source.fs.open(artifact.external_path, "ab") as stream:
            stream.write(b'{"record_id": "added", "value": 3}\n')
        artifact.verify_external_content(fs=source.fs)
        return selected

    monkeypatch.setattr(source_import, "read_source_run_outputs", mutate)
    with (
        curator_resources(str(tmp_path / "worker")),
        pytest.raises(ArtifactIntegrityError),
    ):
        source_import.import_candidate_outputs(
            _operation(source), source, step_number=8
        )


def test_source_root_mismatch_rejects_before_reading(tmp_path: Path) -> None:
    source = _source(str(tmp_path / "source"), StorageConfig(), tmp_path / "original")
    operation = _operation(source)
    operation.params.source_delta_root = str(tmp_path / "another")
    with pytest.raises(ValueError, match="differs"):
        source_import.import_candidate_outputs(operation, source, step_number=8)


def test_carrier_effect_is_verified_at_the_captured_location_version(
    tmp_path: Path,
) -> None:
    source = _source(str(tmp_path / "source"), StorageConfig(), tmp_path / "original")
    artifact = read_current_step_outputs(source, "a" * 32)[0].occurrences[0].artifact
    uri = artifact.path
    valid = CuratorView(source, capture_curator_view(source))
    witness = curator_carrier_read(valid, artifact.artifact_id, uri)
    path = uri_join(source.delta_root, TablePath.ARTIFACT_LOCATIONS.value)
    table = DeltaTable(path)
    table.update({"uri": "uri || '/changed'"})
    corrupt = CuratorView(source, capture_curator_view(source))
    table.update({"uri": "replace(uri, '/changed', '')"})
    assert curator_carrier_read(valid, artifact.artifact_id, uri) == witness
    with pytest.raises(StoreIntegrityError):
        curator_carrier_read(corrupt, artifact.artifact_id, uri + "/changed")


def test_configured_import_resolves_local_sibling_files_at_invocation(
    tmp_path: Path,
) -> None:
    source = _source(
        str(tmp_path / "workspace" / "delta"),
        StorageConfig(),
        tmp_path / "original",
        files_root=str(tmp_path / "workspace" / "files"),
    )
    operation = IngestPipelineStep(
        params={
            "source_delta_root": str(tmp_path / "before-submission" / "delta"),
            "source_run_id": "synthetic-pipeline",
            "source_step": 7,
        }
    )
    operation.params.source_delta_root = source.delta_root
    with curator_resources(str(tmp_path / "work")):
        result = source_import.import_configured_outputs(operation, step_number=8)
        assert result.success
        artifact = result.artifacts["file_ref"][0]
        assert (
            Path(artifact.path).read_bytes() == b"independent complete source bytes\n"
        )
    assert operation.params.source_files_root is None


def test_configured_import_does_not_infer_a_custom_root_from_locations(
    tmp_path: Path,
) -> None:
    source = _source(
        str(tmp_path / "source" / "delta"), StorageConfig(), tmp_path / "original"
    )
    operation = _operation(source)
    operation.params.source_files_root = None
    with curator_resources(str(tmp_path / "work")), pytest.raises(StoreIntegrityError):
        source_import.import_configured_outputs(operation, step_number=8)


@pytest.mark.parametrize(
    ("delta_root", "files_root", "protocol", "message"),
    [
        ("s3://source/delta", None, "s3", "explicit source_files_root"),
        ("s3://source/delta", "/local/files", "file", "protocol differs"),
        ("/local/delta", "s3://source/files", "file", "protocol differs"),
        (" ", "/local/files", "file", "nonempty"),
        ("/local/delta", " ", "file", "nonempty"),
        ("/local/delta", "file:///local/files?token=secret", "file", "credentials"),
    ],
)
def test_configured_import_rejects_invalid_roots_before_filesystem_creation(
    monkeypatch: pytest.MonkeyPatch,
    delta_root: str,
    files_root: str | None,
    protocol: str,
    message: str,
) -> None:
    operation = IngestPipelineStep(
        params={
            "source_delta_root": delta_root,
            "source_files_root": files_root,
            "source_run_id": "source-run",
            "source_step": 0,
            "source_storage": StorageConfig(protocol=protocol),
        }
    )

    def forbidden(_storage: StorageConfig) -> AbstractFileSystem:
        pytest.fail("Invalid source binding reached filesystem construction")

    monkeypatch.setattr(StorageConfig, "filesystem", forbidden)
    with pytest.raises(ValueError, match=message):
        source_import.import_configured_outputs(operation, step_number=1)


def test_configured_import_keeps_source_filesystem_and_delta_options_separate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = f"memory://{tmp_path.name}/delta"
    files = f"memory://{tmp_path.name}/custom-files"
    filesystem = MemoryFileSystem()
    filesystem.makedirs(root)
    storage = StorageConfig(
        protocol="memory",
        options={"source_option": "synthetic"},
        delta_options={"SOURCE_DELTA_OPTION": "synthetic-delta"},
    )
    operation = IngestPipelineStep(
        params={
            "source_delta_root": root,
            "source_files_root": files,
            "source_run_id": "source-run",
            "source_step": 0,
            "source_storage": storage,
        }
    )
    calls: list[tuple[str, dict[str, Any]]] = []

    def construct(protocol: str, **options: Any) -> AbstractFileSystem:
        calls.append((protocol, options))
        return filesystem

    def opened(
        delta_root: str,
        files_root: str,
        *,
        fs: AbstractFileSystem,
        storage_options: dict[str, str] | None = None,
    ) -> RetainedCommitter:
        assert (delta_root, files_root) == (root, files)
        assert fs is filesystem
        assert storage_options == storage.delta_options
        assert storage_options is not storage.delta_options
        msg = "source opener observed"
        raise RuntimeError(msg)

    monkeypatch.setattr("fsspec.filesystem", construct)
    monkeypatch.setattr(source_import, "open_retained_store", opened)
    with pytest.raises(RuntimeError, match="source opener observed"):
        source_import.import_configured_outputs(operation, step_number=1)
    assert calls == [("memory", {"source_option": "synthetic"})]


def test_configured_import_missing_root_returns_failure_without_initializing(
    tmp_path: Path,
) -> None:
    root = tmp_path / "absent" / "delta"
    operation = IngestPipelineStep(
        params={
            "source_delta_root": str(root),
            "source_run_id": "source-run",
            "source_step": 0,
        }
    )
    result = source_import.import_configured_outputs(operation, step_number=1)
    assert not result.success
    assert result.error == f"Source delta root does not exist: {root}"
    assert not root.parent.exists()


@pytest.mark.parametrize("kind", ["legacy", "unlabeled", "publication", "missing"])
def test_configured_import_rejects_incompatible_or_incomplete_store_without_writes(
    tmp_path: Path, kind: str
) -> None:
    root = tmp_path / "source"
    if kind == "missing":
        create_retained_store(
            str(root), str(tmp_path / "files"), fs=StorageConfig().filesystem()
        )
        shutil.rmtree(root / TablePath.EXECUTIONS.value)
    else:
        root.mkdir()
        if kind == "legacy":
            (root / "_artisan").mkdir()
            (root / "_artisan" / "store.json").write_text(
                json.dumps(
                    {"store_format": 9, "artifact_identity": 1, "cache_identity": 3}
                )
            )
        else:
            (
                root / ("inspection.json" if kind == "publication" else "data")
            ).write_text("{}")
    before = {
        str(path.relative_to(root)): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }
    operation = IngestPipelineStep(
        params={
            "source_delta_root": str(root),
            "source_run_id": "source-run",
            "source_step": 0,
        }
    )
    with pytest.raises((IncompatibleStoreError, StoreIntegrityError)):
        source_import.import_configured_outputs(operation, step_number=1)
    assert {
        str(path.relative_to(root)): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    } == before


def test_configured_import_missing_semantic_referent_rejects_destination_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = StorageConfig()
    source = create_retained_store(
        str(tmp_path / "source" / "delta"),
        str(tmp_path / "source" / "files"),
        fs=storage.filesystem(),
    )
    case = retained_case(tmp_path / "original", kind="file")
    results, admitted, receipts = prepare_scientific(source, case)
    source.accept(receipts[0])
    results.finish(admitted.step_run_id)
    original = (
        read_current_step_outputs(source, admitted.step_run_id)[0]
        .occurrences[0]
        .artifact
    )
    destination = create_retained_store(
        str(tmp_path / "destination" / "delta"),
        str(tmp_path / "destination" / "files"),
        fs=storage.filesystem(),
    )
    operation = _operation(source)
    imported: list[ArtifactResult] = []
    original_execute = IngestPipelineStep.execute_curator

    def execute(
        self: IngestPipelineStep,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        result = original_execute(self, inputs, step_number, artifact_store)
        imported.append(result)
        return result

    monkeypatch.setattr(IngestPipelineStep, "execute_curator", execute)
    coordinator, admission, reference = admit_curator(
        destination, operation, {}, step_run_id="b" * 32
    )
    staged, terminal = stage_curator(coordinator, admission, operation)
    assert staged.success
    assert staged.execution_run_id is not None
    assert set(imported[0].artifacts) == {"config"}
    assert imported[0].lineage == {"config": []}
    assert imported[0].artifacts["config"][0].content == original.content
    terminal = StepResult.model_validate(
        {
            **terminal.model_dump(),
            "output_roles": frozenset({"config"}),
            "output_types": {"config": "config"},
        }
    )
    with pytest.raises(
        ValueError, match="Configuration referent is missing or ambiguous"
    ):
        coordinator.finish(reference, staged.execution_run_id, terminal)
    assert (
        read_curator_plan(destination.delta_root, reference.step_run_id, destination.fs)
        is None
    )
    assert all(
        frame.is_empty()
        for frame in destination._snapshot(TablePath.ARTIFACT_INDEX.value).iter_frames()
    )
    assert (
        read_current_step_outputs(source, admitted.step_run_id)[0]
        .occurrences[0]
        .artifact.to_row()
        == original.to_row()
    )
