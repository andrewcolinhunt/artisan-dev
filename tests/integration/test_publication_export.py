"""Publication copies preserve scientific identities and verified store evidence."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import ClassVar

import polars as pl
import pytest

from artisan.errors import IncompatibleStoreError, StoreIntegrityError
from artisan.operations.curator import IngestCSV
from artisan.operations.examples import CsvHead
from artisan.orchestration import PipelineManager, StepStatus
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas import FileRefArtifact
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.operation_config.environment_spec import DockerEnvironmentSpec
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage import ArtifactStore, PublicationExport, export_publication
from artisan.storage.core.committed_scan import read_committed
from artisan.storage.io.repair import repair_store

pytestmark = pytest.mark.integration
PRIVATE_TOKEN = "private-workspace"


class PublicationHead(CsvHead):
    """Exercise replay configuration redaction without executing a container."""

    name = "publication_head"
    execute_as_tool: ClassVar[bool] = False
    environments: Environments = Environments(
        active="local",
        docker=DockerEnvironmentSpec(
            image="example:unused", binds=[("/private-workspace/code", "/workspace")]
        ),
    )


@dataclass(frozen=True)
class PublicationSource:
    root: Path
    store: ArtifactStore
    transform_step_run_id: str


@pytest.fixture(scope="module")
def publication_source(tmp_path_factory: pytest.TempPathFactory) -> PublicationSource:
    root = tmp_path_factory.mktemp("publication") / "private-workspace"
    root.mkdir()
    incoming = root / "input.csv"
    incoming.write_text("x,y\n1,2\n3,4\n")
    second = root / "second.csv"
    second.write_text("x,y\n5,6\n7,8\n")
    with PipelineManager.create(
        "publication-example",
        delta_root=str(root / "delta"),
        staging_root=str(root / "staging"),
        working_root=str(root / "work"),
        files_root=str(root / "files"),
    ) as manager:
        ingested = manager.run(
            IngestCSV, inputs=[str(incoming), str(second)], compact=False
        )
        assert ingested.status == StepStatus.SUCCEEDED
        transformed = manager.run(
            PublicationHead,
            inputs={"dataset": ingested.output("csv")},
            params={"rows": 1},
            compact=False,
        )
        assert transformed.status == StepStatus.SUCCEEDED
        repeated = manager.run(
            IngestCSV,
            inputs=[str(incoming), str(second)],
            skip_cache=True,
            compact=False,
        )
        assert repeated.status == StepStatus.SUCCEEDED
        cached = manager.run(
            IngestCSV, inputs=[str(incoming), str(second)], compact=False
        )
        assert cached.status == StepStatus.SUCCEEDED
    return PublicationSource(
        root,
        ArtifactStore(str(root / "delta"), files_root=str(root / "files")),
        transformed.step_run_id,
    )


def _inventory(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file()
    }


def _rows(store: ArtifactStore, table: TablePath) -> pl.DataFrame:
    return read_committed(store.base_path, table, fs=store.filesystem)


def test_export_publication_preserves_artifacts_lineage_and_source(
    publication_source: PublicationSource, tmp_path: Path
) -> None:
    source = publication_source.store
    before = _inventory(publication_source.root)
    receipt = export_publication(
        source, tmp_path / "published", redact=(PRIVATE_TOKEN,)
    )
    published = receipt.open_store()
    types = source.provenance.load_type_map()
    assert types
    assert published.provenance.load_type_map() == types
    for artifact_id in types:
        original = source.get_artifact(artifact_id)
        copied = published.get_artifact(artifact_id)
        assert original is not None
        assert copied is not None
        assert copied.to_row() == original.to_row()
        if isinstance(original, FileRefArtifact):
            assert isinstance(copied, FileRefArtifact)
            assert copied.path == (
                f"/publication/files/{artifact_id}/"
                f"{original.original_name}{original.extension}"
            )
            assert original.read_content(fs=source.filesystem) == copied.read_content(
                fs=published.filesystem
            )
    for table in (TablePath.ARTIFACT_EDGES, TablePath.EXECUTION_EDGES):
        original_rows = _rows(source, table)
        assert original_rows.height > 0
        assert original_rows.sort(original_rows.columns).equals(
            _rows(published, table).sort(original_rows.columns)
        )
    for table, columns in (
        (
            TablePath.EXECUTIONS,
            ["execution_run_id", "execution_spec_id", "step_run_id"],
        ),
        (TablePath.STEPS, ["pipeline_run_id", "step_run_id", "state_sequence"]),
    ):
        assert (
            _rows(source, table)
            .select(columns)
            .sort(columns)
            .equals(_rows(published, table).select(columns).sort(columns))
        )
    assert _inventory(publication_source.root) == before


def test_export_publication_reopens_after_bundle_moves(
    publication_source: PublicationSource, tmp_path: Path
) -> None:
    receipt = export_publication(
        publication_source.store, tmp_path / "published", redact=(PRIVATE_TOKEN,)
    )
    moved = tmp_path / "relocated"
    receipt.directory.rename(moved)
    relocated = PublicationExport(moved)
    published = relocated.open_store()
    report = repair_store(
        delta_root=str(relocated.delta_root),
        staging_root=str(relocated.staging_root),
        files_root=str(relocated.files_root),
        fs=published.filesystem,
    )
    assert not report.blocking
    assert not report.unresolved
    for artifact_id, artifact_type in published.provenance.load_type_map().items():
        if artifact_type == "file_ref":
            artifact = published.get_artifact(artifact_id)
            assert isinstance(artifact, FileRefArtifact)
            content = artifact.read_content(fs=published.filesystem)
            materialized = Path(
                artifact.materialize_to(
                    str(tmp_path / "materialized"), fs=published.filesystem
                )
            )
            assert materialized.name == f"{artifact.original_name}{artifact.extension}"
            assert materialized.suffix == ".csv"
            assert materialized.read_bytes() == content


def test_export_publication_rejects_pipeline_resume(
    publication_source: PublicationSource, tmp_path: Path
) -> None:
    receipt = export_publication(
        publication_source.store, tmp_path / "published", redact=(PRIVATE_TOKEN,)
    )
    before = _inventory(receipt.directory)
    with pytest.raises(IncompatibleStoreError):
        PipelineManager.create(
            "resume-publication",
            delta_root=str(receipt.delta_root),
            staging_root=str(receipt.staging_root),
            working_root=str(tmp_path / "new-work"),
            files_root=str(receipt.files_root),
        )
    assert _inventory(receipt.directory) == before


def test_export_publication_rejects_repair_mutation(
    publication_source: PublicationSource, tmp_path: Path
) -> None:
    receipt = export_publication(
        publication_source.store, tmp_path / "published", redact=(PRIVATE_TOKEN,)
    )
    published = receipt.open_store()
    before = _inventory(receipt.directory)
    with pytest.raises(IncompatibleStoreError):
        repair_store(
            delta_root=published.base_path,
            staging_root=str(receipt.staging_root),
            files_root=published.files_root,
            fs=published.filesystem,
            apply=True,
        )
    assert _inventory(receipt.directory) == before


def test_export_publication_rejects_missing_source_plan(
    publication_source: PublicationSource, tmp_path: Path
) -> None:
    damaged = tmp_path / "damaged-source"
    shutil.copytree(publication_source.root, damaged)
    next((damaged / "delta" / "_artisan" / "commit_plans").rglob("*.json")).unlink()
    before = _inventory(damaged)
    destination = tmp_path / "rejected"
    with pytest.raises(StoreIntegrityError):
        export_publication(
            ArtifactStore(str(damaged / "delta"), files_root=str(damaged / "files")),
            destination,
            redact=(),
        )
    assert _inventory(damaged) == before
    assert not destination.exists()


def test_export_publication_rejects_pending_source_step(
    publication_source: PublicationSource, tmp_path: Path
) -> None:
    pending = tmp_path / "pending-source"
    shutil.copytree(publication_source.root, pending)
    source = ArtifactStore(str(pending / "delta"), files_root=str(pending / "files"))
    tracker = StepTracker(source.base_path, "pending-example", fs=source.filesystem)
    tracker.create_attempt(
        StepStartRecord(
            step_run_id="a" * 32,
            step_number=4,
            step_name="pending",
            operation_class="artisan.operations.curator.IngestCSV",
            params_json="{}",
            input_refs_json="{}",
            compute_backend="local",
            compute_options_json="{}",
            output_roles_json='["csv"]',
            output_types_json='{"csv":"csv"}',
        )
    )
    before = _inventory(pending)
    destination = tmp_path / "rejected"
    with pytest.raises(StoreIntegrityError, match="quiescent"):
        export_publication(source, destination, redact=())
    assert _inventory(pending) == before
    assert not destination.exists()


def test_export_publication_rebuilds_verifiable_plans(
    publication_source: PublicationSource, tmp_path: Path
) -> None:
    receipt = export_publication(
        publication_source.store,
        tmp_path / "published",
        redact=(PRIVATE_TOKEN,),
    )
    published = receipt.open_store()
    original = _rows(publication_source.store, TablePath.LOGICAL_COMMITS)
    copied = _rows(published, TablePath.LOGICAL_COMMITS)
    columns = ["logical_commit_id", "state"]
    assert (
        original.select(columns)
        .sort(columns)
        .equals(copied.select(columns).sort(columns))
    )
    assert (
        original.sort(columns)["plan_digest"].to_list()
        != copied.sort(columns)["plan_digest"].to_list()
    )
    for store, staging in (
        (publication_source.store, publication_source.root / "staging"),
        (published, Path(receipt.directory) / "staging"),
    ):
        report = repair_store(
            delta_root=store.base_path,
            staging_root=str(staging),
            fs=store.filesystem,
            files_root=store.files_root,
        )
        assert not report.blocking
        assert not report.unresolved
    for table in TablePath:
        assert PRIVATE_TOKEN not in str(_rows(published, table).to_dicts())


def test_export_publication_retains_replay_with_explicit_replacements(
    publication_source: PublicationSource, tmp_path: Path
) -> None:
    receipt = export_publication(
        publication_source.store,
        tmp_path / "published",
        redact=(PRIVATE_TOKEN,),
    )
    snapshots = []
    for store in (publication_source.store, receipt.open_store()):
        rows = _rows(store, TablePath.EXECUTIONS).filter(
            pl.col("step_run_id") == publication_source.transform_step_run_id
        )
        assert rows.height > 0
        rows = rows.sort("execution_run_id")
        snapshots.append(ReplaySnapshot.model_validate_json(rows["replay_snapshot"][0]))
    original, copied = snapshots
    assert copied.status == original.status
    assert copied.inputs == original.inputs
    assert copied.associated == original.associated
    assert copied.group_ids == original.group_ids
    assert copied.operation is not None
    assert original.operation is not None
    assert copied.operation.identity == original.operation.identity
    assert (
        copied.operation.configuration["environments"]["docker"]["binds"][0][0] is None
    )
    assert "/environments/docker/binds/0/0" in {
        replacement.pointer for replacement in copied.required_replacements
    }
    assert copied.source is not None
    assert original.source is not None
    assert copied.source.step_run_id == original.source.step_run_id
    assert all(value is None for value in copied.source.roots.values())


def test_export_publication_rejects_nonempty_destination_without_mutation(
    publication_source: PublicationSource, tmp_path: Path
) -> None:
    destination = tmp_path / "existing"
    destination.mkdir()
    marker = destination / "keep.txt"
    marker.write_text("preserve existing bytes")
    destination_before = _inventory(destination)
    before = _inventory(publication_source.root)
    with pytest.raises((ValueError, StoreIntegrityError)):
        export_publication(publication_source.store, destination, redact=())
    assert _inventory(destination) == destination_before
    assert _inventory(publication_source.root) == before


def test_export_publication_rejects_identity_bearing_redaction(
    publication_source: PublicationSource, tmp_path: Path
) -> None:
    before = _inventory(publication_source.root)
    with pytest.raises((ValueError, StoreIntegrityError)):
        export_publication(
            publication_source.store, tmp_path / "rejected", redact=("input",)
        )
    assert _inventory(publication_source.root) == before


def test_export_publication_rejects_destination_inside_source(
    publication_source: PublicationSource,
) -> None:
    before = _inventory(publication_source.root)
    destination = publication_source.root / "delta" / "nested-export"
    with pytest.raises((ValueError, StoreIntegrityError)):
        export_publication(publication_source.store, destination, redact=())
    assert not destination.exists()
    assert _inventory(publication_source.root) == before


def test_publication_filesystem_rejects_mutations(
    publication_source: PublicationSource, tmp_path: Path
) -> None:
    receipt = export_publication(
        publication_source.store, tmp_path / "published", redact=(PRIVATE_TOKEN,)
    )
    filesystem = receipt.open_store().filesystem
    before = _inventory(receipt.directory)
    existing = next(path for path in receipt.files_root.rglob("*") if path.is_file())
    with pytest.raises((PermissionError, NotImplementedError)):
        filesystem.rm(str(existing))
    with pytest.raises((PermissionError, NotImplementedError)):
        filesystem.mkdir(str(receipt.directory / "unexpected"))
    with pytest.raises((PermissionError, NotImplementedError)):
        filesystem.pipe_file(str(existing), b"changed")
    with pytest.raises((PermissionError, NotImplementedError)):
        filesystem.open(str(existing), "wb")
    assert _inventory(receipt.directory) == before
    assert not (receipt.directory / "unexpected").exists()


def test_publication_filesystem_rejects_external_and_symlink_reads(
    publication_source: PublicationSource, tmp_path: Path
) -> None:
    receipt = export_publication(
        publication_source.store, tmp_path / "published", redact=(PRIVATE_TOKEN,)
    )
    filesystem = receipt.open_store().filesystem
    outside = tmp_path / "outside.txt"
    outside.write_text("unpublished bytes")
    with pytest.raises(PermissionError):
        filesystem.open(str(outside), "rb")
    existing = next(path for path in receipt.files_root.rglob("*") if path.is_file())
    existing.unlink()
    existing.symlink_to(outside)
    with pytest.raises((PermissionError, ValueError)):
        filesystem.open(str(existing), "rb")


@pytest.mark.parametrize("field", ["artifact_id", "path", "uri"])
def test_publication_manifest_rejects_conflicting_file_entries(
    publication_source: PublicationSource, tmp_path: Path, field: str
) -> None:
    receipt = export_publication(
        publication_source.store, tmp_path / "published", redact=(PRIVATE_TOKEN,)
    )
    manifest_path = receipt.delta_root / "_artisan" / "publication.json"
    manifest = json.loads(manifest_path.read_text())
    assert len(manifest["files"]) >= 2
    manifest["files"][1][field] = manifest["files"][0][field]
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        receipt.open_store()


def test_publication_manifest_rejects_external_locator(
    publication_source: PublicationSource, tmp_path: Path
) -> None:
    receipt = export_publication(
        publication_source.store, tmp_path / "published", redact=(PRIVATE_TOKEN,)
    )
    manifest_path = receipt.delta_root / "_artisan" / "publication.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["files"][0]["uri"] = "/outside/files/example.csv"
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        receipt.open_store()
