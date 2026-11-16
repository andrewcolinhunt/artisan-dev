"""Complete private store initialization stays separate from reads and recovery."""

from __future__ import annotations

import hashlib
from pathlib import Path

import polars as pl
import pytest
from fixtures.retained_acceptance import admit_case
from fixtures.retained_curators import admit_curator, stage_curator
from fixtures.retained_readers import Backend
from fsspec import AbstractFileSystem

from artisan.errors import IncompatibleStoreError, StoreIntegrityError
from artisan.operations.curator.merge import Merge
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.assignment_schema import ASSIGNMENT_DECISIONS_PATH
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.projection_schemas import PROJECTION_TABLES
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.core.store_format import initialize_retained_store
from artisan.storage.core.table_schemas import FRAMEWORK_SCHEMAS
from artisan.storage.io.assignment_journal import AssignmentJournal
from artisan.storage.io.curator_plan import read_curator_plan
from artisan.storage.io.retained_store import (
    create_retained_store,
    open_configured_store,
    open_retained_store,
)
from artisan.utils.path import uri_join


def _objects(fs: AbstractFileSystem, root: str) -> dict[str, str]:
    if not fs.exists(root):
        return {}
    found = {}
    for path in fs.find(root):
        with fs.open(path, "rb", cache_type="none") as stream:
            found[path] = hashlib.file_digest(stream, "sha256").hexdigest()
    return found


@pytest.mark.parametrize("custom_files", [False, True])
def test_configured_open_uses_local_default_or_explicit_files_without_writes(
    tmp_path: Path, custom_files: bool
) -> None:
    root = str(tmp_path / "delta")
    files = str(tmp_path / ("custom-files" if custom_files else "files"))
    fs = StorageConfig().filesystem()
    create_retained_store(root, files, fs=fs)
    before = _objects(fs, root)
    opened = open_configured_store(root, files_root=files if custom_files else None)
    assert (opened.delta_root, opened.files_root) == (root, files)
    assert _objects(fs, root) == before
    assert not fs.exists(files)


@pytest.mark.parametrize(
    ("root", "files", "protocol", "message"),
    [
        ("memory://test/delta", None, "memory", "explicit files_root"),
        ("memory://test/delta", "/local/files", "memory", "protocol differs"),
        ("/local/delta", "memory://test/files", "file", "protocol differs"),
        (" ", None, "file", "nonempty"),
        ("/local/delta", " ", "file", "nonempty"),
        ("file:///local/delta?token=synthetic", None, "file", "credentials"),
        ("s3://user:secret@bucket", "s3://bucket/files", "s3", "credentials"),
        ("s3://bucket?token=synthetic", "s3://bucket/files", "s3", "credentials"),
    ],
)
def test_configured_open_rejects_invalid_binding_before_filesystem_creation(
    monkeypatch: pytest.MonkeyPatch,
    root: str,
    files: str | None,
    protocol: str,
    message: str,
) -> None:
    def forbidden(_storage: StorageConfig) -> AbstractFileSystem:
        pytest.fail("Invalid read binding reached filesystem construction")

    monkeypatch.setattr(StorageConfig, "filesystem", forbidden)
    with pytest.raises(ValueError, match=message):
        open_configured_store(
            root, files_root=files, storage=StorageConfig(protocol=protocol)
        )


def test_configured_open_does_not_initialize_missing_store(tmp_path: Path) -> None:
    root = tmp_path / "missing"
    with pytest.raises(IncompatibleStoreError, match="manifest"):
        open_configured_store(str(root))
    assert not root.exists()


@pytest.fixture
def application_type(monkeypatch: pytest.MonkeyPatch) -> type[ArtifactTypeDef]:
    monkeypatch.setattr(ArtifactTypeDef, "_registry", dict(ArtifactTypeDef._registry))
    monkeypatch.setattr(ArtifactTypes, "_registry", dict(ArtifactTypes._registry))
    monkeypatch.setattr(ArtifactTypes, "OPEN_STORE_CSV", None, raising=False)

    class ApplicationCSV(CSVArtifact):
        artifact_type: str = "open_store_csv"

    class ApplicationCSVType(ArtifactTypeDef):
        key = "open_store_csv"
        table_path = "artifacts/open_store_csv"
        model = ApplicationCSV

    return ApplicationCSVType


def test_create_and_open_validate_all_registered_tables_without_writes(
    backend_fs: Backend, application_type: type[ArtifactTypeDef]
) -> None:
    fs, storage, base = backend_fs
    root, files = f"{base}/candidate", f"{base}/custom-managed-files"
    options = storage.delta_storage_options() or {}
    created = create_retained_store(root, files, fs=fs, storage_options=options)
    options["unused_caller_mutation"] = "detached"
    assert "unused_caller_mutation" not in created.storage_options
    paths = {
        *(item.table_path for item in ArtifactTypeDef.get_all().values()),
        *(item.table_path for item in PROJECTION_TABLES),
        *(table.value for table in FRAMEWORK_SCHEMAS),
        ASSIGNMENT_DECISIONS_PATH,
    }
    assert application_type.table_path in paths
    assert all(fs.exists(uri_join(root, path, "_delta_log")) for path in paths)
    before = _objects(fs, root)
    opened = open_retained_store(
        root, files, fs=fs, storage_options=storage.delta_storage_options()
    )
    assert (opened.delta_root, opened.files_root) == (root, files)
    assert opened.fs is fs
    assert _objects(fs, root) == before
    assert not fs.exists(files)


def test_explicit_create_finishes_same_domain_interrupted_initialization(
    backend_fs: Backend,
) -> None:
    fs, storage, base = backend_fs
    root, files = f"{base}/candidate", f"{base}/managed"
    initialize_retained_store(root, fs)
    AssignmentJournal(
        root, fs=fs, storage_options=storage.delta_storage_options()
    ).initialize()
    before = _objects(fs, root)
    with pytest.raises(StoreIntegrityError, match="missing required table"):
        open_retained_store(
            root, files, fs=fs, storage_options=storage.delta_storage_options()
        )
    assert _objects(fs, root) == before
    create_retained_store(
        root, files, fs=fs, storage_options=storage.delta_storage_options()
    )
    completed = _objects(fs, root)
    assert before.items() <= completed.items()
    create_retained_store(
        root, files, fs=fs, storage_options=storage.delta_storage_options()
    )
    assert _objects(fs, root) == completed


def test_create_does_not_repair_a_missing_table_after_lifecycle_data(
    backend_fs: Backend,
) -> None:
    fs, storage, base = backend_fs
    root, files = f"{base}/candidate", f"{base}/managed"
    committer = create_retained_store(
        root, files, fs=fs, storage_options=storage.delta_storage_options()
    )
    RetainedStepTracker(committer, "store-test").create_attempt(
        StepStartRecord(
            step_run_id="a" * 32,
            step_number=0,
            step_name="pending",
            operation_class="synthetic.Pending",
            params_json="{}",
            input_refs_json="{}",
            compute_backend="local",
            compute_options_json="{}",
            output_roles_json="[]",
            output_types_json="{}",
        )
    )
    fs.rm(uri_join(root, PROJECTION_TABLES[0].table_path), recursive=True)
    before = _objects(fs, root)
    with pytest.raises(StoreIntegrityError, match="after retained store data"):
        create_retained_store(
            root, files, fs=fs, storage_options=storage.delta_storage_options()
        )
    assert _objects(fs, root) == before


@pytest.mark.parametrize(
    "table",
    [
        ArtifactTypeDef.get("csv").table_path,
        TablePath.STEPS.value,
        PROJECTION_TABLES[0].table_path,
        ASSIGNMENT_DECISIONS_PATH,
    ],
)
@pytest.mark.parametrize("changed", ["missing", "schema"])
def test_open_rejects_missing_or_changed_required_table_without_repair(
    backend_fs: Backend, table: str, changed: str
) -> None:
    fs, storage, base = backend_fs
    root, files = f"{base}/candidate", f"{base}/managed"
    create_retained_store(
        root, files, fs=fs, storage_options=storage.delta_storage_options()
    )
    path = uri_join(root, table)
    fs.rm(path, recursive=True)
    if changed == "schema":
        pl.DataFrame(schema={"wrong": pl.String}).write_delta(
            path, storage_options=storage.delta_storage_options()
        )
    before = _objects(fs, root)
    with pytest.raises(StoreIntegrityError, match="required table|unexpected schema"):
        open_retained_store(
            root, files, fs=fs, storage_options=storage.delta_storage_options()
        )
    assert _objects(fs, root) == before
    if changed == "schema":
        fs.rm(uri_join(root, TablePath.ARTIFACT_LOCATIONS), recursive=True)
        before = _objects(fs, root)
        with pytest.raises(StoreIntegrityError, match="unexpected schema"):
            create_retained_store(
                root, files, fs=fs, storage_options=storage.delta_storage_options()
            )
        assert _objects(fs, root) == before


@pytest.mark.parametrize("kind", ["legacy", "unlabeled", "publication", "inspection"])
def test_foreign_and_inspection_roots_reject_without_mutation(
    backend_fs: Backend, kind: str
) -> None:
    fs, storage, base = backend_fs
    root = f"{base}/candidate"
    if kind == "legacy":
        fs.makedirs(uri_join(root, "_artisan"), exist_ok=True)
        with fs.open(uri_join(root, "_artisan/store.json"), "wb") as stream:
            stream.write(b'{"store_format":9,"artifact_identity":1,"cache_identity":3}')
    elif kind == "publication":
        initialize_retained_store(root, fs)
        with fs.open(uri_join(root, "_artisan/publication.json"), "wb") as stream:
            stream.write(b'{"purpose":"sanitized_inspection"}')
    else:
        fs.makedirs(root, exist_ok=True)
        filename = "inspection.json" if kind == "inspection" else "existing.bin"
        with fs.open(uri_join(root, filename), "wb") as stream:
            stream.write(b"existing bytes")
    before = _objects(fs, root)
    for action in (open_retained_store, create_retained_store):
        with pytest.raises(IncompatibleStoreError):
            action(
                root,
                f"{base}/managed",
                fs=fs,
                storage_options=storage.delta_storage_options(),
            )
        assert _objects(fs, root) == before


def test_reopened_custom_files_root_preserves_scientific_and_curator_outputs(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, base = backend_fs
    root, files = f"{base}/candidate", f"{base}/managed-elsewhere"
    committer = create_retained_store(
        root, files, fs=fs, storage_options=storage.delta_storage_options()
    )
    _, external = admit_case(committer, tmp_path / "file", kind="file")
    committer.accept(external)
    _, receipt = admit_case(committer, tmp_path / "alias", kind="alias", unit_index=1)
    committer.accept(receipt)
    original = RetainedReader(committer).read(receipt).output("data")[0]
    operation = Merge()
    coordinator, admission, reference = admit_curator(
        committer,
        operation,
        {"first": (original,), "second": (original,)},
        step_run_id="b" * 32,
    )
    staged, terminal = stage_curator(coordinator, admission, operation)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    plan = read_curator_plan(root, reference.step_run_id, fs)
    assert plan is not None
    before = _objects(fs, root), _objects(fs, files)
    reopened = open_retained_store(
        root, files, fs=fs, storage_options=storage.delta_storage_options()
    )
    assert (_objects(fs, root), _objects(fs, files)) == before
    assert RetainedReader(reopened).read(external).support
    assert RetainedReader(reopened).read(receipt).output("data")[0] == original
    outputs = CuratorReader(reopened).read_result(plan.step_run_id, plan.plan_digest)
    assert outputs.projection.occurrences[0].sources == (original.reference,)
    assert fs.exists(files)
    assert not fs.exists(f"{root}/files")


def test_open_leaves_selected_incomplete_and_orphan_evidence_unchanged(
    backend_fs: Backend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fs, storage, base = backend_fs
    root, files = f"{base}/candidate", f"{base}/managed"
    committer = create_retained_store(
        root, files, fs=fs, storage_options=storage.delta_storage_options()
    )
    case, selected = admit_case(committer, tmp_path / "selected", kind="alias")
    _, orphan = admit_case(committer, tmp_path / "orphan", kind="alias", unit_index=1)

    def stop(boundary: str, receipt: SealedDelivery, table_path: str | None) -> None:
        if boundary == "planned":
            message = "interrupted selected publication"
            raise RuntimeError(message)

    monkeypatch.setattr(committer, "_checkpoint", stop)
    with pytest.raises(RuntimeError, match="interrupted selected publication"):
        committer.accept(selected)
    before = _objects(fs, root), _objects(fs, files)
    reopened = open_retained_store(
        root, files, fs=fs, storage_options=storage.delta_storage_options()
    )
    assert (_objects(fs, root), _objects(fs, files)) == before
    selected_state = reopened.journal.state(selected.delivery.attempt.assignment)
    orphan_state = reopened.journal.state(orphan.delivery.attempt.assignment)
    assert selected_state is not None
    assert selected_state.winner is not None
    assert orphan_state is not None
    assert orphan_state.winner is None
    with pytest.raises(StoreIntegrityError, match="complete"):
        reopened.accepted_projection(selected)
    reopened.recover(selected.delivery.attempt.assignment)
    assert reopened.accepted_projection(selected) == case.projection
    assert reopened.journal.state(orphan.delivery.attempt.assignment) == orphan_state
    assert fs.exists(orphan.seal_uri)
