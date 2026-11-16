"""Retained external bytes transfer before worker scratch can disappear."""

from __future__ import annotations

import base64
from pathlib import Path

import pytest
from qualification.lineage.projection import BoundaryProjection, Occurrence
from qualification.persistence.prototype import CandidateStore
from qualification.persistence.test_acceptance import (
    Backend,
    child_request,
    envelope,
    run_child,
)

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.utils.hashing import compute_content_digest


@pytest.mark.parametrize("content", [b"", b"retained structure bytes\n"])
def test_external_payload_hydrates_after_worker_exit_and_scratch_deletion(
    persistence_backend: Backend, tmp_path: Path, content: bytes
) -> None:
    fs, storage, root = persistence_backend
    store = CandidateStore(
        f"{root}/candidate", fs=fs, storage_options=storage.delta_storage_options()
    )
    scratch = tmp_path / "file-worker-scratch"
    artifact = FileRefArtifact.draft(
        path=str(scratch / "structure.bin"),
        content_hash=compute_content_digest(content),
        size_bytes=len(content),
        original_name="structure",
        extension=".bin",
        step_number=1,
    ).finalize()
    projection = BoundaryProjection(
        boundary_inputs={},
        occurrences=(
            Occurrence(
                key="predict:structure:0",
                artifact_id=artifact.artifact_id,
                artifact_type="file_ref",
            ),
        ),
        exports={"structure": ("predict:structure:0",)},
    )
    identity = envelope()
    store.activate(identity, projection)
    directory = f"{root}/external-delivery"
    child = run_child(
        child_request(
            persistence_backend,
            directory,
            identity,
            action="worker",
            scratch=str(scratch),
            projection=projection.model_dump(mode="json"),
            artifacts={
                artifact.artifact_id: base64.b64encode(
                    artifact.model_dump_json().encode()
                ).decode()
            },
            external_files={"structure.bin": base64.b64encode(content).decode()},
        )
    )
    assert child.returncode == 0, child.stderr
    assert not scratch.exists()
    digest = store.accept(directory)
    hydrated = store.hydrate(identity.unit_id, artifact.artifact_id)
    assert isinstance(hydrated, FileRefArtifact)
    assert hydrated.read_content(fs=fs) == content
    assert str(scratch) not in hydrated.path
    assert store.cache(identity, projection)
    with fs.open(
        f"{store.root}/evidence/{digest}/file-{artifact.artifact_id}.bin", "wb"
    ) as stream:
        stream.write(b"changed external content")
    with pytest.raises(StoreIntegrityError, match="Changed sealed payload"):
        store.cache(identity, projection)
    with pytest.raises(StoreIntegrityError, match="Changed sealed payload"):
        store.recover()
