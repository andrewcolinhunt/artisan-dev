"""External curator snapshots preserve whole files across stream lifetimes."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fixtures.retained_readers import Backend

from artisan.execution.recording.parquet_writer import _stage_artifacts
from artisan.schemas.artifact.appendable import AppendableArtifact
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.storage.io.curator_carriers import (
    CuratorCarrierStager,
    curator_carrier_directory,
)
from artisan.utils.hashing import compute_content_digest
from artisan.utils.path import uri_join


def _record(record_id: str) -> bytes:
    return json.dumps({"record_id": record_id, "value": record_id}).encode()


def _appendable(record_id: str, path: Path) -> AppendableArtifact:
    content = _record(record_id)
    artifact = AppendableArtifact.draft(
        record_id, compute_content_digest(content), len(content), 8, str(path)
    )
    artifact.finalize()
    return artifact


def _stage(
    backend: Backend, artifacts: list[Artifact], *, files_root: str | None = None
) -> CuratorCarrierStager:
    fs, _, root = backend
    staging_path = f"{root}/worker/{'a' * 32}"
    fs.makedirs(staging_path, exist_ok=True)
    _stage_artifacts({"result": artifacts}, [], 8, staging_path, fs)
    return CuratorCarrierStager(staging_path, fs, files_root=files_root)


@pytest.mark.parametrize("reverse", [False, True])
def test_same_appendable_record_selects_managed_carrier_independent_of_emission_order(
    backend_fs: Backend, tmp_path: Path, reverse: bool
) -> None:
    outside = tmp_path / "a-source.jsonl"
    managed = tmp_path / "managed"
    managed.mkdir()
    preferred = managed / "z-source.jsonl"
    outside.write_bytes(_record("selected") + b"\n" + _record("outside") + b"\n")
    payload = _record("selected") + b"\n" + _record("managed") + b"\n"
    preferred.write_bytes(payload)
    artifacts: list[Artifact] = [
        _appendable("selected", outside),
        _appendable("selected", preferred),
    ]
    if reverse:
        artifacts.reverse()
    stager = _stage(backend_fs, artifacts, files_root=str(managed))
    for artifact in artifacts:
        stager.capture(artifact)
    outside.unlink()
    preferred.unlink()
    inventory = stager.finish()
    assert len(inventory.carriers) == len(inventory.external_bindings) == 1
    assert inventory.carriers[0].digest == compute_content_digest(payload)
    assert inventory.carriers[0].size_bytes == len(payload)
    fs = backend_fs[0]
    assert len(fs.ls(stager.directory)) == 1


def test_growing_shared_appendable_carrier_uses_final_snapshot_after_source_deletion(
    backend_fs: Backend, tmp_path: Path
) -> None:
    source = tmp_path / "growing.jsonl"
    first = _appendable("first", source)
    second = _appendable("second", source)
    stager = _stage(backend_fs, [first, second])
    source.write_bytes(_record("first") + b"\n")
    stager.capture(first)
    payload = _record("first") + b"\n" + _record("second") + b"\n"
    source.write_bytes(payload)
    stager.capture(second)
    source.unlink()
    inventory = stager.finish()
    assert len(inventory.carriers) == 1
    assert len(inventory.external_bindings) == 2
    carrier = inventory.carriers[0]
    assert (carrier.digest, carrier.size_bytes) == (
        compute_content_digest(payload),
        len(payload),
    )
    fs = backend_fs[0]
    assert len(fs.ls(stager.directory)) == 1
    with fs.open(uri_join(stager.directory, carrier.filename), "rb") as stream:
        assert stream.read() == payload


def test_file_carrier_survives_source_deletion_without_global_publication(
    backend_fs: Backend, tmp_path: Path
) -> None:
    source = tmp_path / "transient.bin"
    payload = b"complete external payload\n"
    source.write_bytes(payload)
    artifact = FileRefArtifact.draft(
        str(source), compute_content_digest(payload), len(payload), 8
    )
    artifact.finalize()
    stager = _stage(backend_fs, [artifact])
    stager.capture(artifact)
    source.unlink()
    inventory = stager.finish()
    assert inventory.external_bindings[0].artifact_id == artifact.artifact_id
    assert stager.directory == curator_carrier_directory(stager.staging_path)
    assert not backend_fs[0].exists(f"{backend_fs[2]}/carriers")
