"""Verify transported candidate bytes after the producing worker has exited."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import bootstrap
import fsspec

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from qualification.persistence.encoding import decode_projection, read_delivery
from qualification.persistence.prototype import CandidateStore

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.file_ref import FileRefArtifact


def accept_candidates(
    output: Path,
    candidates: list[Path],
    *,
    descriptor_path: Path,
    partial_path: Path | None = None,
) -> dict[str, object]:
    """Claim one delivery, reject cancelled/late delivery, and read exact exports."""
    fs = fsspec.filesystem("file")
    seal, expected_digest, tables, _ = read_delivery(str(candidates[0]), fs=fs)
    projection = decode_projection(tables)
    descriptor = json.loads(descriptor_path.read_text())
    assert seal.envelope.software_digest == bootstrap.software_digest(descriptor)
    cancelled = CandidateStore(str(output / "cancelled-store"), fs=fs)
    cancelled.activate(seal.envelope, projection)
    cancelled.cancel(seal.envelope.unit_id)
    assert cancelled.cache(seal.envelope, projection) == []
    try:
        cancelled.accept(str(candidates[0]))
    except StoreIntegrityError:
        pass
    else:
        msg = "cancelled unit accepted a late provider delivery"
        raise AssertionError(msg)
    store = CandidateStore(str(output / "accepted-store"), fs=fs)
    store.activate(seal.envelope, projection)
    observed = store.accept(str(candidates[0]))
    assert observed == expected_digest
    assert store.result(seal.envelope.unit_id) == projection
    assert store.cache(seal.envelope, projection) == [projection]
    other_target = {
        **descriptor,
        "target": {**descriptor["target"], "kind": "unqualified-other-target"},
    }
    other_envelope = seal.envelope.model_copy(
        update={"software_digest": bootstrap.software_digest(other_target)}
    )
    assert store.cache(other_envelope, projection) == []
    if partial_path is not None:
        incomplete = CandidateStore(str(output / "partial-store"), fs=fs)
        incomplete.activate(seal.envelope, projection)
        try:
            incomplete.accept(str(partial_path))
        except FileNotFoundError:
            pass
        else:
            msg = "partial upload without a seal was accepted"
            raise AssertionError(msg)
        assert incomplete.cache(seal.envelope, projection) == []
    external_verified = []
    for occurrence in projection.occurrences:
        if occurrence.artifact_type != "file_ref":
            continue
        artifact = store.hydrate(seal.envelope.unit_id, occurrence.artifact_id)
        assert isinstance(artifact, FileRefArtifact)
        assert json.loads(artifact.read_content()) == {"value": 16}
        external_verified.append(occurrence.artifact_id)
    rejected = []
    for directory in candidates[1:]:
        try:
            store.accept(str(directory))
        except StoreIntegrityError:
            rejected.append(directory.name)
        else:
            msg = "another provider delivery replaced the winner"
            raise AssertionError(msg)
    report = {
        "status": "passed",
        "accepted_digest": observed,
        "exports": projection.exports,
        "cancelled_before_claim_rejected": True,
        "late_deliveries_rejected": rejected,
        "cache_uses_accepted_evidence": True,
        "external_files_hydrated": external_verified,
        "different_target_same_core_and_lock_cache_miss": True,
        "partial_upload_rejected": partial_path is not None,
    }
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("candidates", nargs="+", type=Path)
    parser.add_argument("--descriptor", type=Path, required=True)
    parser.add_argument("--partial", type=Path)
    arguments = parser.parse_args()
    print(
        json.dumps(
            accept_candidates(
                arguments.output,
                arguments.candidates,
                descriptor_path=arguments.descriptor,
                partial_path=arguments.partial,
            ),
            indent=2,
        )
    )
