"""Real Parquet/Delta proofs for winner ownership and crash-safe retained results."""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
from pathlib import Path

import polars as pl
import pytest
from fsspec import AbstractFileSystem
from pydantic import ValidationError
from qualification.lineage.projection import (
    BoundaryProjection,
    FrozenAssociation,
    FrozenContext,
    Occurrence,
    Relation,
)
from qualification.persistence.encoding import SCHEMAS, decode_projection, read_delivery
from qualification.persistence.prototype import CandidateStore, Envelope, stage_delivery

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.io.publication import publish_immutable_bytes

Backend = tuple[AbstractFileSystem, StorageConfig, str]
SOURCE_A = MetricArtifact.draft(
    content={"input": "A"}, original_name="a", step_number=0
).finalize()
SOURCE_B = MetricArtifact.draft(
    content={"input": "B"}, original_name="b", step_number=0
).finalize()
RESULT = MetricArtifact.draft(
    content={"value": 7}, original_name="result", step_number=1
).finalize()
INPUT_A, INPUT_B, OUTPUT = (
    SOURCE_A.artifact_id,
    SOURCE_B.artifact_id,
    RESULT.artifact_id,
)


def output_payload(*, alternate_encoding: bool = False) -> bytes:
    """Use identical artifact identity with optionally different transport bytes."""
    data = RESULT.model_dump(mode="json")
    return json.dumps(data, indent=2 if alternate_encoding else None).encode()


def projection() -> BoundaryProjection:
    """Two equal-content outputs keep distinct ancestors and B/A/B export order."""
    return BoundaryProjection(
        boundary_inputs={"input": ("input:A", "input:B")},
        occurrences=(
            Occurrence(key="input:A", artifact_id=INPUT_A, artifact_type="metric"),
            Occurrence(key="input:B", artifact_id=INPUT_B, artifact_type="metric"),
            Occurrence(
                key="call:A:metric:0", artifact_id=OUTPUT, artifact_type="metric"
            ),
            Occurrence(
                key="call:B:metric:0", artifact_id=OUTPUT, artifact_type="metric"
            ),
        ),
        exports={
            "metric": ("call:B:metric:0", "call:A:metric:0", "call:B:metric:0"),
            "optional": (),
        },
        parents=(
            Relation(source="input:A", target="call:A:metric:0"),
            Relation(source="input:B", target="call:B:metric:0"),
        ),
        associations=(
            Relation(source="input:A", target="call:A:metric:0"),
            Relation(source="input:B", target="call:B:metric:0"),
        ),
        frozen_context=FrozenContext(
            associations=(
                FrozenAssociation(
                    primary="input:A", artifact_type="metric", members=()
                ),
            )
        ),
    )


def envelope(**changes: object) -> Envelope:
    return Envelope.model_validate(
        {
            "unit_id": "unit-1",
            "attempt": 0,
            "generation": 0,
            "delivery_id": "delivery-1",
            "recipe_digest": "recipe-v1",
            "software_digest": "software-v1",
            **changes,
        }
    )


def prepare(
    backend: Backend, *, identity: Envelope | None = None
) -> tuple[CandidateStore, str, Envelope]:
    fs, storage, root = backend
    store = CandidateStore(
        f"{root}/candidate", fs=fs, storage_options=storage.delta_storage_options()
    )
    store.seed_inputs(
        {
            INPUT_A: ("metric", SOURCE_A.model_dump_json().encode()),
            INPUT_B: ("metric", SOURCE_B.model_dump_json().encode()),
        }
    )
    identity = identity or envelope()
    store.activate(identity, projection())
    directory = f"{root}/deliveries/{identity.delivery_id}"
    stage_delivery(directory, identity, projection(), {OUTPUT: output_payload()}, fs=fs)
    return store, directory, identity


def child_request(
    backend: Backend, directory: str, identity: Envelope, **options: object
) -> dict[str, object]:
    _, storage, root = backend
    return {
        "protocol": storage.protocol,
        "options": storage.options,
        "delta_options": storage.delta_storage_options(),
        "root": f"{root}/candidate",
        "directory": directory,
        "envelope": identity.model_dump(mode="json"),
        "projection": projection().model_dump(mode="json"),
        **options,
    }


def run_child(request: dict[str, object]) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    tests_root = str(Path(__file__).resolve().parents[2])
    environment["PYTHONPATH"] = os.pathsep.join(
        filter(None, [tests_root, environment.get("PYTHONPATH")])
    )
    environment["POLARS_MAX_THREADS"] = "2"
    return subprocess.run(
        [sys.executable, "-m", "qualification.persistence.process_worker"],
        input=json.dumps(request),
        text=True,
        capture_output=True,
        env=environment,
        timeout=90,
        check=False,
    )


def assert_exact_result(store: CandidateStore, identity: Envelope) -> None:
    result = store.result(identity.unit_id)
    assert result is not None
    expected = projection()
    assert result.exports == expected.exports
    assert result.boundary_inputs == expected.boundary_inputs
    assert result.parents == expected.parents
    assert result.associations == expected.associations
    assert result.frozen_context == expected.frozen_context
    assert {item.key: item for item in result.occurrences} == {
        item.key: item for item in expected.occurrences
    }
    assert store.cache(identity, expected) == [result]
    journal = store.journal()
    assert journal.filter(pl.col("kind") == "claim").height == 1
    assert journal.filter(pl.col("kind") == "complete").height == 1
    artifacts = store._read("artifacts", store._physical_schema("artifacts"))
    assert artifacts.height == 1


def test_roundtrip_occurrences_cache_and_downstream(
    persistence_backend: Backend,
) -> None:
    store, directory, identity = prepare(persistence_backend)
    store.accept(directory)
    assert_exact_result(store, identity)
    cached = store.cache(identity, projection())[0]
    downstream_keys = tuple(cached.exports["metric"])
    ancestors = {item.target: item.source for item in cached.parents}
    assert [ancestors[key] for key in downstream_keys] == [
        "input:B",
        "input:A",
        "input:B",
    ]
    assert cached.exports["optional"] == ()
    assert cached.frozen_context.associated("input:A", "metric") == ()
    with pytest.raises(ValueError, match="not captured"):
        cached.frozen_context.associated("input:B", "metric")


def test_worker_process_exits_and_scratch_disappears_before_acceptance(
    persistence_backend: Backend, tmp_path: Path
) -> None:
    store, directory, identity = prepare(persistence_backend)
    store.fs.rm(directory, recursive=True)
    scratch = tmp_path / "worker-scratch"
    child = run_child(
        child_request(
            persistence_backend,
            directory,
            identity,
            action="worker",
            scratch=str(scratch),
            artifacts={OUTPUT: base64.b64encode(output_payload()).decode()},
        )
    )
    assert child.returncode == 0, child.stderr
    assert not scratch.exists()
    store.accept(directory)
    assert_exact_result(store, identity)


def test_worker_dies_mid_transfer_without_an_acceptable_seal(
    persistence_backend: Backend, tmp_path: Path
) -> None:
    store, directory, identity = prepare(persistence_backend)
    store.fs.rm(directory, recursive=True)
    child = run_child(
        child_request(
            persistence_backend,
            directory,
            identity,
            action="worker",
            scratch=str(tmp_path / "interrupted-scratch"),
            boundary="staged:occurrences",
            artifacts={OUTPUT: base64.b64encode(output_payload()).decode()},
        )
    )
    assert child.returncode == 73, child.stderr
    assert store.fs.exists(f"{directory}/artifacts.parquet")
    assert not store.fs.exists(f"{directory}/seal.json")
    with pytest.raises(FileNotFoundError):
        store.accept(directory)
    store.recover()
    assert store.result(identity.unit_id) is None
    assert store.cache(identity, projection()) == []


@pytest.mark.parametrize(
    "boundary",
    ["claim", "planned", *(f"table:{name}" for name in SCHEMAS), "complete", "cleanup"],
)
def test_process_death_at_every_durable_boundary_converges(
    persistence_backend: Backend, boundary: str
) -> None:
    store, directory, identity = prepare(persistence_backend)
    child = run_child(
        child_request(
            persistence_backend, directory, identity, action="accept", boundary=boundary
        )
    )
    assert child.returncode == 73, child.stderr
    if boundary not in {"complete", "cleanup"}:
        assert store.result(identity.unit_id) is None
        assert store.cache(identity, projection()) == []
    store.recover()
    store.recover()
    assert_exact_result(store, identity)
    assert not store.fs.exists(directory)


@pytest.mark.parametrize("claim_first", [False, True])
def test_cancellation_order_is_shared_by_acceptance_repair_and_cache(
    persistence_backend: Backend, claim_first: bool
) -> None:
    store, directory, identity = prepare(persistence_backend)
    if claim_first:
        child = run_child(
            child_request(
                persistence_backend,
                directory,
                identity,
                action="accept",
                boundary="claim",
            )
        )
        assert child.returncode == 73, child.stderr
    store.cancel(identity.unit_id)
    if not claim_first:
        with pytest.raises(StoreIntegrityError, match="cancelled"):
            store.accept(directory)
        store.recover()
        assert store.result(identity.unit_id) is None
        assert store.cache(identity, projection()) == []
    else:
        store.recover()
        assert_exact_result(store, identity)


def test_redelivery_obsolete_generation_and_independent_unit(
    persistence_backend: Backend,
) -> None:
    store, directory, identity = prepare(persistence_backend)
    next_attempt = envelope(attempt=1, generation=1, delivery_id="retry-1")
    store.activate(next_attempt, projection())
    with pytest.raises(StoreIntegrityError, match="Obsolete"):
        store.accept(directory)
    next_directory = directory + "-retry"
    stage_delivery(
        next_directory,
        next_attempt,
        projection(),
        {OUTPUT: output_payload()},
        fs=store.fs,
    )
    child = run_child(
        child_request(
            persistence_backend,
            next_directory,
            next_attempt,
            action="accept",
            boundary="claim",
        )
    )
    assert child.returncode == 73, child.stderr
    duplicate = envelope(attempt=1, generation=1, delivery_id="provider-redelivery")
    duplicate_directory = directory + "-duplicate"
    stage_delivery(
        duplicate_directory,
        duplicate,
        projection(),
        {OUTPUT: output_payload(alternate_encoding=True)},
        fs=store.fs,
    )
    with pytest.raises(StoreIntegrityError, match="Another delivery"):
        store.accept(duplicate_directory)
    store.cancel(identity.unit_id)
    independent = envelope(unit_id="unit-2", delivery_id="unit-2-delivery")
    store.activate(independent, projection())
    independent_directory = directory + "-independent"
    stage_delivery(
        independent_directory,
        independent,
        projection(),
        {OUTPUT: output_payload()},
        fs=store.fs,
    )
    store.accept(independent_directory)
    store.recover()
    assert store.result(identity.unit_id) is not None
    assert store.result(independent.unit_id) is not None
    assert store.journal().filter(pl.col("kind") == "claim")[
        "delivery_id"
    ].to_list() == ["retry-1", "unit-2-delivery"]


@pytest.mark.parametrize(
    "damage",
    ["missing-seal", "missing-table", "tampered-table", "extra-file", "foreign-domain"],
)
def test_unfinished_or_tampered_staging_cannot_claim(
    persistence_backend: Backend, damage: str
) -> None:
    store, directory, identity = prepare(persistence_backend)
    if damage == "missing-seal":
        store.fs.rm(f"{directory}/seal.json")
    elif damage == "missing-table":
        store.fs.rm(f"{directory}/occurrences.parquet")
    elif damage == "tampered-table":
        with store.fs.open(f"{directory}/artifacts.parquet", "wb") as stream:
            stream.write(b"interrupted transfer")
    elif damage == "extra-file":
        publish_immutable_bytes(
            store.fs, f"{directory}/foreign.parquet", b"hidden ancestor"
        )
    else:
        with store.fs.open(f"{directory}/seal.json", "rb") as stream:
            seal = json.load(stream)
        seal["envelope"]["domain"] = "historical-format"
        with store.fs.open(f"{directory}/seal.json", "w") as stream:
            json.dump(seal, stream)
    with pytest.raises((StoreIntegrityError, FileNotFoundError, ValidationError)):
        store.accept(directory)
    store.recover()
    assert store.cache(identity, projection()) == []
    assert store.journal().filter(pl.col("kind") == "claim").is_empty()


def test_frozen_context_and_software_cannot_refresh_after_admission(
    persistence_backend: Backend,
) -> None:
    store, directory, identity = prepare(persistence_backend)
    changed = projection().model_copy(update={"frozen_context": FrozenContext()})
    with pytest.raises(StoreIntegrityError, match="Retry changed"):
        store.activate(envelope(attempt=1, generation=1), changed)
    changed_directory = directory + "-changed"
    stage_delivery(
        changed_directory, identity, changed, {OUTPUT: output_payload()}, fs=store.fs
    )
    with pytest.raises(StoreIntegrityError, match="Frozen boundary"):
        store.accept(changed_directory)
    store.accept(directory)
    later = MetricArtifact.draft(
        content={"later": True}, original_name="later", step_number=2
    ).finalize()
    store.seed_inputs({later.artifact_id: ("metric", later.model_dump_json().encode())})
    assert store.cache(identity, changed) == []
    assert store.cache(envelope(software_digest="software-v2"), projection()) == []
    assert_exact_result(store, identity)


def test_claim_binds_the_full_seal_before_recovery(
    persistence_backend: Backend,
) -> None:
    store, directory, identity = prepare(persistence_backend)
    child = run_child(
        child_request(
            persistence_backend, directory, identity, action="accept", boundary="claim"
        )
    )
    assert child.returncode == 73, child.stderr
    store.fs.rm(directory, recursive=True)
    stage_delivery(
        directory,
        identity,
        projection(),
        {OUTPUT: output_payload(alternate_encoding=True)},
        fs=store.fs,
    )
    with pytest.raises(StoreIntegrityError, match="Claimed evidence changed"):
        store.recover()
    assert store.result(identity.unit_id) is None
    assert store.cache(identity, projection()) == []


def test_claim_uses_controller_decision_and_identical_accept_is_idempotent(
    persistence_backend: Backend,
) -> None:
    store, first_directory, _identity = prepare(persistence_backend)
    selected = envelope(delivery_id="z-selected")
    selected_directory = first_directory + "-selected"
    stage_delivery(
        selected_directory,
        selected,
        projection(),
        {OUTPUT: output_payload()},
        fs=store.fs,
    )
    digest = store.accept(selected_directory)
    assert store.accept(selected_directory) == digest
    with pytest.raises(StoreIntegrityError, match="Another delivery"):
        store.accept(first_directory)
    assert store.journal().filter(pl.col("kind") == "claim")[
        "delivery_id"
    ].to_list() == ["z-selected"]
    assert_exact_result(store, selected)


def test_diagnostics_require_their_own_recovery_and_never_enter_cache(
    persistence_backend: Backend,
) -> None:
    store, directory, identity = prepare(
        persistence_backend, identity=envelope(diagnostic=True)
    )
    child = run_child(
        child_request(
            persistence_backend, directory, identity, action="accept", boundary="claim"
        )
    )
    assert child.returncode == 73, child.stderr
    store.recover()
    assert store.result(identity.unit_id) is None
    store.recover_diagnostic(identity.unit_id)
    assert store.result(identity.unit_id) is not None
    assert store.cache(identity, projection()) == []


def test_physical_and_claimed_evidence_corruption_remains_fatal(
    persistence_backend: Backend,
) -> None:
    store, directory, identity = prepare(persistence_backend)
    digest = store.accept(directory)
    table = store._read("memberships", store._physical_schema("memberships"))
    table.head(1).write_delta(
        f"{store.root}/memberships",
        mode="overwrite",
        storage_options=store.storage_options,
    )
    with pytest.raises(StoreIntegrityError, match="Accepted table changed"):
        store.cache(identity, projection())
    with pytest.raises(StoreIntegrityError, match="Accepted table changed"):
        store.recover()
    assert (
        store.journal()
        .filter((pl.col("kind") == "complete") & (pl.col("seal_digest") == digest))
        .height
        == 1
    )


def test_exact_inventory_rejects_missing_support_and_unselected_content(
    persistence_backend: Backend,
) -> None:
    store, directory, identity = prepare(persistence_backend)
    for suffix, artifacts in (
        ("missing", {}),
        ("extra", {OUTPUT: b"selected", "d" * 32: b"unselected"}),
    ):
        candidate = directory + suffix
        stage_delivery(candidate, identity, projection(), artifacts, fs=store.fs)
        with pytest.raises(StoreIntegrityError, match="membership"):
            store.accept(candidate)
    assert store.journal().filter(pl.col("kind") == "claim").is_empty()


def test_old_store_manifest_is_untouched_and_rejected(
    persistence_backend: Backend,
) -> None:
    fs, storage, root = persistence_backend
    old_root = f"{root}/historical"
    original = b'{"store_format":9,"cache_identity":3,"artifact_identity":1}'
    publish_immutable_bytes(fs, f"{old_root}/store-format.json", original)
    with pytest.raises(StoreIntegrityError, match="outside the candidate"):
        CandidateStore(old_root, fs=fs, storage_options=storage.delta_storage_options())
    with fs.open(f"{old_root}/store-format.json", "rb") as stream:
        assert stream.read() == original


def test_normalized_tables_are_authoritative_not_physical_row_order(
    persistence_backend: Backend,
) -> None:
    store, directory, _ = prepare(persistence_backend)
    _, _, tables, _ = read_delivery(directory, fs=store.fs)
    shuffled = {name: frame.reverse() for name, frame in tables.items()}
    restored = decode_projection(shuffled)
    assert restored.exports == projection().exports
    assert restored.parents == projection().parents
    malformed = dict(tables)
    malformed["memberships"] = pl.concat(
        [tables["memberships"], tables["memberships"].head(1)]
    )
    with pytest.raises(StoreIntegrityError, match="Noncanonical"):
        decode_projection(malformed)


@pytest.mark.parametrize("activated", [False, True])
def test_fully_empty_optional_success_requires_an_admitted_assignment(
    persistence_backend: Backend, activated: bool
) -> None:
    fs, storage, root = persistence_backend
    store = CandidateStore(
        f"{root}/candidate", fs=fs, storage_options=storage.delta_storage_options()
    )
    empty = BoundaryProjection(
        boundary_inputs={}, occurrences=(), exports={"optional": ()}
    )
    identity = envelope()
    if activated:
        store.activate(identity, empty)
    directory = f"{root}/empty-delivery"
    stage_delivery(directory, identity, empty, {}, fs=fs)
    if not activated:
        with pytest.raises(StoreIntegrityError, match="no active attempt"):
            store.accept(directory)
        assert store.result(identity.unit_id) is None
        assert store.cache(identity, empty) == []
        return
    store.accept(directory)
    assert store.result(identity.unit_id) == empty
    assert store.cache(identity, empty) == [empty]
    assert store._read("artifacts", store._physical_schema("artifacts")).is_empty()
