"""External access records retain exact descriptors without acquiring payloads."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

import pytest
from fixtures.retained_results import RetainedCase
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.external_descriptor import (
    ExternalFile,
    ExternalFilesDescriptor,
    PreparedExternalFiles,
    expected_receipt_digest,
)
from artisan.schemas.composites.plan import ExecutionRecipe
from artisan.schemas.execution.admission import (
    AdmissionRecord,
    AdmissionRecordWithAccess,
    AdmissionReference,
    parse_admission_record,
)
from artisan.schemas.execution.assignment import AssignmentAttempt, AssignmentIdentity
from artisan.schemas.execution.external_access import (
    ExternalAccessBinding,
    ExternalAccessMount,
    ExternalAccessRecord,
)
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.schemas.provenance.projection import BoundaryProjection
from artisan.storage.core.store_format import initialize_retained_store
from artisan.storage.io.admission import (
    admission_directory,
    capture_admission,
    publish_admission,
    read_admission_record,
)
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest

pytestmark = pytest.mark.usefixtures("external_artifact_types")


@dataclass(frozen=True)
class ExternalAdmissionCase:
    artifact: Artifact
    recipe: ExecutionRecipe
    boundary: BoundaryProjection
    artifacts: dict[OccurrenceAddress, Artifact]
    attempt: AssignmentAttempt
    access: ExternalAccessRecord
    root: str
    files_root: str


def _case(tmp_path: Path) -> ExternalAdmissionCase:
    from artisan.schemas.external_artifacts import ExternalFilesArtifact

    descriptor = ExternalFilesDescriptor(
        files=(
            ExternalFile(
                path="index.bin",
                content_hash=compute_content_digest(b"index"),
                size_bytes=5,
            ),
        )
    )
    artifact = ExternalFilesArtifact.draft(descriptor=descriptor).finalize()
    graph = OccurrenceGraph()
    address = graph.add_input("database", artifact)
    boundary = graph.project({}, {})
    artifacts = {address: artifact}
    recipe = RetainedCase(boundary, artifacts, {}).recipe
    attempt = AssignmentAttempt(
        assignment=AssignmentIdentity(step_run_id="a" * 32, unit_index=0),
        attempt=0,
        generation=0,
        recipe_digest=recipe.digest,
        software_digest="c" * 64,
        input_context_digest=boundary.input_context_digest,
    )
    prepared = PreparedExternalFiles(
        root=tmp_path / "unavailable-installation",
        descriptor=descriptor,
        receipt_digest=expected_receipt_digest(descriptor),
    )
    assert artifact.artifact_id is not None
    access = ExternalAccessRecord(
        attempt=attempt,
        provider="local",
        bindings=(
            ExternalAccessBinding(artifact_id=artifact.artifact_id, prepared=prepared),
        ),
    )
    root, files_root = str(tmp_path / "store"), str(tmp_path / "managed")
    initialize_retained_store(root, LocalFileSystem())
    return ExternalAdmissionCase(
        artifact, recipe, boundary, artifacts, attempt, access, root, files_root
    )


def _publish(
    case: ExternalAdmissionCase, access: ExternalAccessRecord | None
) -> AdmissionReference:
    return publish_admission(
        case.attempt,
        case.recipe,
        case.boundary,
        case.artifacts,
        step_number=1,
        delta_root=case.root,
        files_root=case.files_root,
        fs=LocalFileSystem(),
        external_access=access,
    )


def test_external_admission_retains_descriptor_without_payload_access(
    tmp_path: Path,
) -> None:
    case = _case(tmp_path)
    with patch(
        "artisan.storage.io.admission.open_external",
        side_effect=AssertionError("External payload was opened"),
    ):
        reference = _publish(case, case.access)
        with capture_admission(
            reference,
            delta_root=case.root,
            files_root=case.files_root,
            fs=LocalFileSystem(),
        ) as captured:
            assert isinstance(captured.record, AdmissionRecordWithAccess)
            assert captured.external_access == case.access
            assert captured.record.carriers == captured.record.external_bindings == ()
            assert (
                next(iter(captured.artifacts.values())).to_row()
                == case.artifact.to_row()
            )
    assert not Path(case.files_root).exists()
    assert not case.access.bindings[0].prepared.root.exists()


@pytest.mark.parametrize(
    "change", ["missing", "artifact", "descriptor", "attempt", "extra"]
)
def test_external_admission_rejects_incomplete_or_substituted_access(
    tmp_path: Path, change: str
) -> None:
    case = _case(tmp_path)
    data = case.access.model_dump(mode="json")
    if change == "artifact":
        data["bindings"][0]["artifact_id"] = "e" * 32
    elif change == "descriptor":
        descriptor = ExternalFilesDescriptor(
            files=(ExternalFile(path="other.bin", content_hash="d" * 32, size_bytes=5),)
        )
        data["bindings"][0]["prepared"].update(
            descriptor=descriptor.model_dump(mode="json"),
            receipt_digest=expected_receipt_digest(descriptor),
        )
    elif change == "attempt":
        data["attempt"]["generation"] = 1
    elif change == "extra":
        extra = dict(data["bindings"][0], artifact_id="f" * 32)
        data["bindings"] = sorted(
            [*data["bindings"], extra], key=lambda item: item["artifact_id"]
        )
    access = None if change == "missing" else ExternalAccessRecord.model_validate(data)
    with pytest.raises(
        StoreIntegrityError, match="explicit access|differ from admitted"
    ):
        _publish(case, access)
    assert not Path(case.files_root).exists()


@pytest.mark.parametrize("change", ["digest", "missing", "descriptor"])
def test_external_admission_capture_rejects_changed_access_evidence(
    tmp_path: Path, change: str
) -> None:
    case = _case(tmp_path)
    reference = _publish(case, case.access)
    directory = Path(admission_directory(case.root, case.attempt))
    path = directory / "external-access.json"
    if change == "missing":
        path.unlink()
        expected = FileNotFoundError
    elif change == "digest":
        path.write_bytes(b"{}")
        expected = StoreIntegrityError
    else:
        data = json.loads(path.read_bytes())
        descriptor = ExternalFilesDescriptor(
            files=(ExternalFile(path="wrong.bin", content_hash="d" * 32, size_bytes=5),)
        )
        data["bindings"][0]["prepared"].update(
            descriptor=descriptor.model_dump(mode="json"),
            receipt_digest=expected_receipt_digest(descriptor),
        )
        encoded = canonical_json_bytes(data)
        path.write_bytes(encoded)
        record = json.loads((directory / "admission.json").read_bytes())
        record["access_digest"] = compute_content_digest(encoded)
        admission_bytes = canonical_json_bytes(record)
        (directory / "admission.json").write_bytes(admission_bytes)
        reference = reference.model_copy(
            update={"record_digest": compute_content_digest(admission_bytes)}
        )
        expected = StoreIntegrityError
    with (
        pytest.raises(expected),
        capture_admission(
            reference,
            delta_root=case.root,
            files_root=case.files_root,
            fs=LocalFileSystem(),
        ),
    ):
        pass


def test_external_admission_rejects_reference_inputs_in_version_one(
    tmp_path: Path,
) -> None:
    case = _case(tmp_path)
    reference = _publish(case, case.access)
    path = Path(admission_directory(case.root, case.attempt)) / "admission.json"
    data = json.loads(path.read_bytes())
    data.pop("access_digest")
    data.pop("access_artifact_ids")
    data["schema_version"] = 1
    encoded = canonical_json_bytes(data)
    path.write_bytes(encoded)
    reference = reference.model_copy(
        update={"record_digest": compute_content_digest(encoded)}
    )
    with pytest.raises(StoreIntegrityError, match="versioned access"):
        read_admission_record(reference, delta_root=case.root, fs=LocalFileSystem())


def test_admission_version_one_canonical_fields_are_unchanged(tmp_path: Path) -> None:
    case = _case(tmp_path)
    fields = {
        "domain": "artisan.assignment-admission",
        "schema_version": 1,
        "attempt": case.attempt.model_dump(mode="json"),
        "step_number": 1,
        "recipe": case.recipe.model_dump(mode="json"),
        "boundary": case.boundary.model_dump(mode="json"),
        "tables": [],
        "carriers": [],
        "external_bindings": [],
    }
    encoded = canonical_json_bytes(fields)
    parsed = parse_admission_record(encoded)
    assert type(parsed) is AdmissionRecord
    assert canonical_json_bytes(parsed.model_dump(mode="json")) == encoded


@pytest.mark.parametrize("version", [True, 1.0, "1", 0, 3, None])
def test_admission_record_rejects_noninteger_or_unknown_versions(
    version: object,
) -> None:
    with pytest.raises(ValueError, match="version"):
        parse_admission_record(json.dumps({"schema_version": version}).encode())


def test_external_access_mounts_preserve_exact_provider_scope(tmp_path: Path) -> None:
    case = _case(tmp_path)
    mount = ExternalAccessMount(source_id="vo-example", target=tmp_path)
    modal = ExternalAccessRecord(
        attempt=case.attempt,
        provider="modal",
        bindings=case.access.bindings,
        mounts=(mount,),
    )
    assert modal.digest != case.access.digest
    assert modal.bindings == case.access.bindings
    with pytest.raises(ValueError, match="exactly one"):
        ExternalAccessRecord(
            attempt=case.attempt,
            provider="modal",
            bindings=case.access.bindings,
            mounts=(),
        )
    with pytest.raises(ValueError, match="remote volume"):
        ExternalAccessRecord(
            attempt=case.attempt,
            provider="local",
            bindings=case.access.bindings,
            mounts=(mount,),
        )


@pytest.mark.parametrize(
    "target", ["relative", "/", "/model/../other", "/model//other", "/model,path"]
)
def test_external_access_mount_rejects_ambiguous_paths(target: str) -> None:
    with pytest.raises(ValueError, match="canonical absolute"):
        ExternalAccessMount(source_id="vo-example", target=target)


@pytest.mark.parametrize("readonly", [False, 1, "true"])
def test_external_access_mount_requires_literal_readonly(readonly: object) -> None:
    with pytest.raises(ValueError, match="read-only"):
        ExternalAccessMount(source_id="vo-example", target="/models", readonly=readonly)


def test_external_admission_roundtrips_shared_s3_without_payload_access(
    tmp_path: Path,
    s3_fs: tuple[AbstractFileSystem, StorageConfig, str],
) -> None:
    fs, _, prefix = s3_fs
    case = _case(tmp_path)
    delta_root, files_root = f"{prefix}/store", f"{prefix}/managed"
    initialize_retained_store(delta_root, fs)
    with patch(
        "artisan.storage.io.admission.open_external",
        side_effect=AssertionError("External payload was opened"),
    ):
        reference = publish_admission(
            case.attempt,
            case.recipe,
            case.boundary,
            case.artifacts,
            step_number=1,
            delta_root=delta_root,
            files_root=files_root,
            fs=fs,
            external_access=case.access,
        )
        with capture_admission(
            reference, delta_root=delta_root, files_root=files_root, fs=fs
        ) as captured:
            assert isinstance(captured.record, AdmissionRecordWithAccess)
            assert captured.external_access == case.access
            assert captured.record.carriers == captured.record.external_bindings == ()
            assert (
                next(iter(captured.artifacts.values())).to_row()
                == case.artifact.to_row()
            )
    assert not fs.exists(files_root)
    assert not case.access.bindings[0].prepared.root.exists()
