"""Publish immutable curator inputs and fixed store visibility before execution."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fsspec import AbstractFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.execution.curator_admission import (
    CuratorAdmission,
    CuratorAdmissionReference,
)
from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.schemas.provenance.projection import ProjectedOccurrence
from artisan.storage.io.publication import publish_immutable_bytes
from artisan.utils.hashing import canonical_json_bytes
from artisan.utils.path import uri_join

if TYPE_CHECKING:
    from artisan.storage.io.retained_commit import RetainedCommitter


def curator_admission_path(delta_root: str, step_run_id: str) -> str:
    """Return the immutable invocation path, independent of mutable staging."""
    return uri_join(
        delta_root, "_artisan", "curator-admissions", step_run_id, "admission.json"
    )


def publish_curator_admission(
    admission: CuratorAdmission, committer: RetainedCommitter
) -> CuratorAdmissionReference:
    """Verify selected content and exact source owners before freezing the view."""
    from artisan.storage.core.artifact_store import ArtifactStore
    from artisan.storage.core.curator_view import CuratorView
    from artisan.storage.io.curator_control import verify_curator_inputs

    admission = CuratorAdmission.model_validate(admission)
    verify_curator_inputs(admission, committer)
    view = CuratorView(committer, admission.view)
    store = ArtifactStore(
        committer.delta_root,
        fs=committer.fs,
        storage_options=committer.storage_options,
        files_root=committer.files_root,
        _view=view,
    )
    expected = {
        member.artifact_id: member.artifact_type
        for role in admission.inputs
        for member in role.members
    }
    expected.update(
        (artifact_id, association.artifact_type)
        for association in admission.associations
        for artifact_id in association.artifact_ids
    )
    if store.load_type_map(list(expected)) != expected:
        msg = "Curator input content is absent from its frozen visible store"
        raise StoreIntegrityError(msg)
    for role in admission.inputs:
        for index, member in enumerate(role.members):
            occurrence = ProjectedOccurrence(
                address=OccurrenceAddress(
                    invocation_id=admission.record.step_run_id,
                    role=role.role,
                    index=index,
                ),
                artifact_id=member.artifact_id,
                artifact_type=member.artifact_type,
            )
            for producer in member.producers:
                committer._verify_producer(producer, occurrence)
    reference = CuratorAdmissionReference(
        step_run_id=admission.record.step_run_id, admission_digest=admission.digest
    )
    publish_immutable_bytes(
        committer.fs,
        curator_admission_path(committer.delta_root, reference.step_run_id),
        canonical_json_bytes(admission.model_dump(mode="json")),
    )
    return reference


def read_curator_admission(
    reference: CuratorAdmissionReference, *, delta_root: str, fs: AbstractFileSystem
) -> CuratorAdmission:
    """Read exact canonical metadata; this alone grants no content authority."""
    with fs.open(
        curator_admission_path(delta_root, reference.step_run_id),
        "rb",
        cache_type="none",
    ) as stream:
        encoded = stream.read()
    admission = CuratorAdmission.model_validate_json(encoded)
    if (
        admission.record.step_run_id != reference.step_run_id
        or admission.digest != reference.admission_digest
        or canonical_json_bytes(admission.model_dump(mode="json")) != encoded
    ):
        msg = "Curator admission differs from its exact immutable reference"
        raise StoreIntegrityError(msg)
    return admission
