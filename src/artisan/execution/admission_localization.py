"""Localize complete admitted carriers while preserving exact input occurrences."""

from __future__ import annotations

from pathlib import Path

from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.execution.assignment import Assignment
from artisan.execution.assignment_transport import AdmittedAssignment
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.external import copy_verified_file
from artisan.storage.io.admission import CapturedAdmission
from artisan.utils.path import uri_join


def localize_admission(
    admitted: AdmittedAssignment,
    captured: CapturedAdmission,
    *,
    files_root: str,
    directory: Path,
    fs: AbstractFileSystem,
) -> Assignment:
    """Copy each full carrier once into worker-owned scratch before any science."""
    if (
        captured.reference != admitted.admission
        or captured.record.recipe != admitted.plan.recipe
    ):
        msg = "Captured inputs differ from the exact admitted call"
        raise StoreIntegrityError(msg)
    directory.mkdir(parents=True, exist_ok=False)
    paths = {}
    for carrier in captured.record.carriers:
        path = directory / f"{carrier.digest}.bin"
        copy_verified_file(
            artifact_id=None,
            artifact_type="managed carrier",
            uri=uri_join(files_root, carrier.relative_path),
            destination=str(path),
            expected_digest=carrier.digest,
            expected_size=carrier.size_bytes,
            fs=fs,
        )
        paths[carrier.digest] = path
    artifacts = {}
    for address, artifact in captured.artifacts.items():
        binding = captured.carriers.get(artifact.artifact_id or "")
        artifacts[address] = (
            _localized_artifact(artifact, paths[binding.digest])
            if binding is not None
            else artifact.model_copy(deep=True)
        )
    boundary = captured.record.boundary
    inputs = {
        role: tuple(artifacts[address] for address in addresses)
        for role, addresses in boundary.input_roles().items()
    }
    roots = {
        address
        for addresses in boundary.input_roles().values()
        for address in addresses
    }
    return Assignment(
        admitted.attempt,
        admitted.plan,
        inputs,
        frozen_context=boundary.frozen_context,
        context_artifacts={
            address: artifact
            for address, artifact in artifacts.items()
            if address not in roots
        },
        input_attribution=boundary.input_attribution,
        step_number=captured.record.step_number,
        external_access=captured.external_access,
    )


def _localized_artifact(artifact: Artifact, path: Path) -> Artifact:
    clone = artifact.model_copy(
        deep=True, update={next(iter(artifact.LOCATOR_FIELDS)): str(path)}
    )
    if clone.to_row() != artifact.to_row():
        msg = "Localizing an admitted carrier changed its protected artifact fields"
        raise StoreIntegrityError(msg)
    clone.verify_external_content(fs=LocalFileSystem())
    return clone
