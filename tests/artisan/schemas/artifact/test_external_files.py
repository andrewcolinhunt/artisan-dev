"""Ordinary artifact identity for optionally registered external file bundles."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import ClassVar

import pytest
from pydantic import Field, ValidationError

from artisan.errors import ArtifactIntegrityError
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.external_descriptor import (
    EXTERNAL_FILES_RECEIPT,
    ExternalFile,
    ExternalFilesDescriptor,
    PreparedExternalFiles,
    expected_receipt_digest,
)
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.external_artifacts import (
    ExternalFilesArtifact,
    ModelWeightsArtifact,
)
from artisan.utils.hashing import compute_content_digest

pytestmark = pytest.mark.usefixtures("external_artifact_types")


def _descriptor(
    path: str = "weights.bin", content: bytes = b"abc"
) -> ExternalFilesDescriptor:
    return ExternalFilesDescriptor(
        files=(
            ExternalFile(
                path=path,
                content_hash=compute_content_digest(content),
                size_bytes=len(content),
            ),
        )
    )


def test_external_types_are_optional_in_fresh_process() -> None:
    script = """
from artisan.schemas import ArtifactTypeDef
from artisan.schemas.artifact.external_descriptor import PreparedExternalFiles
from artisan.storage.external_access import prepare_external_files
assert 'external_files' not in ArtifactTypeDef.get_all()
assert 'model_weights' not in ArtifactTypeDef.get_all()
import artisan.schemas.external_artifacts
assert 'external_files' in ArtifactTypeDef.get_all()
assert 'model_weights' in ArtifactTypeDef.get_all()
"""
    subprocess.run([sys.executable, "-c", script], check=True, capture_output=True)


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/absolute",
        "../escape",
        "a/../b",
        "a//b",
        "a/./b",
        "a/",
        "a\\b",
        "C:/file",
        "a\x00b",
        f"sub/{EXTERNAL_FILES_RECEIPT}",
        EXTERNAL_FILES_RECEIPT,
    ],
)
def test_external_file_rejects_unsafe_paths(path: str) -> None:
    with pytest.raises(ValidationError, match="portable relative"):
        _descriptor(path)


@pytest.mark.parametrize("paths", [("a", "a"), ("a", "a/b")])
def test_external_descriptor_rejects_conflicting_members(
    paths: tuple[str, ...],
) -> None:
    with pytest.raises(ValidationError, match="duplicate|overlap"):
        ExternalFilesDescriptor(
            files=tuple(_descriptor(path).files[0] for path in paths)
        )


def test_external_descriptor_sorting_is_canonical() -> None:
    files = (_descriptor("b").files[0], _descriptor("a").files[0])
    first = ExternalFilesDescriptor(files=files)
    second = ExternalFilesDescriptor(files=tuple(reversed(files)))
    assert first == second
    assert first.digest == second.digest
    assert tuple(item.path for item in first.files) == ("a", "b")


@pytest.mark.parametrize("version", [True, "1", 2])
def test_external_descriptor_rejects_unknown_or_coerced_version(
    version: object,
) -> None:
    with pytest.raises(ValidationError):
        ExternalFilesDescriptor.model_validate(
            {"schema_version": version, "files": _descriptor().model_dump()["files"]}
        )


def test_prepared_metadata_requires_no_filesystem_access(tmp_path: Path) -> None:
    descriptor = _descriptor()
    prepared = PreparedExternalFiles(
        root=tmp_path / "not-present",
        descriptor=descriptor,
        receipt_digest=expected_receipt_digest(descriptor),
    )
    assert (
        PreparedExternalFiles.model_validate_json(prepared.model_dump_json())
        == prepared
    )
    assert not prepared.root.exists()
    with pytest.raises(ValidationError, match="receipt differs"):
        PreparedExternalFiles(
            root=prepared.root, descriptor=descriptor, receipt_digest="0" * 32
        )


@pytest.mark.parametrize("weights", [False, True])
def test_external_artifact_rows_and_runtime_root_preserve_identity(
    tmp_path: Path,
    weights: bool,
) -> None:
    descriptor = _descriptor()
    artifact = (
        ModelWeightsArtifact.draft_weights(descriptor, model_family="example")
        if weights
        else ExternalFilesArtifact.draft(descriptor)
    ).finalize()
    original_row = artifact.to_row()
    original_dump = artifact.model_dump()
    restored = type(artifact).from_row(original_row)
    assert restored.artifact_id == artifact.artifact_id
    with pytest.raises(ValueError, match="no bound"):
        _ = restored.external_root
    artifact.bind_external_root(tmp_path / "first")
    artifact.bind_external_root(tmp_path / "second")
    assert artifact.external_root == tmp_path / "second"
    assert artifact.to_row() == original_row
    assert artifact.model_dump() == original_dump
    assert artifact.materialized_path is None


def test_model_family_and_descriptor_changes_affect_identity() -> None:
    first = ModelWeightsArtifact.draft_weights(
        _descriptor(), model_family="a"
    ).finalize()
    others = (
        ModelWeightsArtifact.draft_weights(_descriptor(), model_family="b"),
        ModelWeightsArtifact.draft_weights(
            _descriptor(content=b"xyz"), model_family="a"
        ),
        ModelWeightsArtifact.draft_weights(
            _descriptor("renamed.bin"), model_family="a"
        ),
        ExternalFilesArtifact.draft(_descriptor()),
    )
    assert all(item.finalize().artifact_id != first.artifact_id for item in others)


def test_external_artifact_rejects_direct_payload_access(tmp_path: Path) -> None:
    artifact = ExternalFilesArtifact.draft(_descriptor()).finalize()
    with pytest.raises(NotImplementedError, match="payload materialization"):
        artifact.materialize_to(str(tmp_path))
    with pytest.raises(ValueError, match="format conversion"):
        artifact.materialize_to(str(tmp_path), format=".bin")
    with pytest.raises(NotImplementedError, match="payload reads"):
        artifact.read_content()
    assert list(tmp_path.iterdir()) == []


def test_runtime_root_binding_requires_finalized_reference(tmp_path: Path) -> None:
    draft = ExternalFilesArtifact.draft(_descriptor())
    with pytest.raises(ValueError, match="finalized external reference"):
        draft.bind_external_root(tmp_path)
    with pytest.raises(ValueError, match="finalized external reference"):
        Artifact(artifact_type="metric").bind_external_root(tmp_path)
    artifact = draft.finalize()
    with pytest.raises(ValueError, match="absolute"):
        artifact.bind_external_root(Path("relative"))


def test_external_descriptor_mutation_invalidates_finalized_artifact() -> None:
    artifact = ExternalFilesArtifact.draft(_descriptor()).finalize()
    object.__setattr__(artifact.descriptor.files[0], "size_bytes", 17)
    with pytest.raises(ArtifactIntegrityError, match="mutated"):
        artifact.to_row()


def test_registry_rejects_combined_managed_and_external_storage() -> None:
    class ConflictingArtifact(ExternalFilesArtifact):
        EXTERNALLY_BACKED: ClassVar[bool] = True
        artifact_type: str = Field(default="_conflicting_external", frozen=True)

    with pytest.raises(TypeError, match="cannot declare managed"):

        class ConflictingType(ArtifactTypeDef):
            key = "_conflicting_external"
            table_path = "artifacts/_conflicting_external"
            model = ConflictingArtifact
            required_references = None

    assert "_conflicting_external" not in ArtifactTypeDef.get_all()
