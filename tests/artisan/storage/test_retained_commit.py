"""Controller winner eligibility and exact retained effects survive process death."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import polars as pl
import pytest
from fixtures.retained_acceptance import admit_case, admit_prepared_case, open_committer
from fixtures.retained_results import RetainedCase, _derive, boundary_artifact

from artisan.errors import StoreIntegrityError
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.assignment import DeliveryIdentity, SuccessfulDelivery
from artisan.schemas.provenance.projection import (
    AcceptedOccurrence,
    BoundaryInputAttribution,
)
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.projection_schemas import PROJECTION_TABLES
from artisan.storage.io.retained_effects import (
    read_retained_plan,
    retained_plan_directory,
)
from artisan.storage.io.retained_plan import retained_plan_digest
from artisan.storage.io.retained_seal import stage_retained_result
from artisan.utils.hashing import canonical_json_bytes


@pytest.mark.parametrize("kind", ["file", "appendable", "alias", "empty"])
def test_retained_commit_survives_worker_and_staging_removal(
    backend_fs, tmp_path: Path, kind: str
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    scratch = tmp_path / "worker"
    case, receipt = admit_case(committer, scratch, kind=kind)
    if scratch.exists():
        shutil.rmtree(scratch)
    plan = committer.accept(receipt)
    assert committer.accepted_projection(receipt) == case.projection
    assert not fs.exists(receipt.seal_uri)
    reopened = open_committer(committer.delta_root, fs, storage)
    assert reopened.recover(receipt.delivery.attempt.assignment) == plan
    assert reopened.accepted_projection(receipt) == case.projection
    if kind in {"file", "appendable"}:
        assert len(plan.carriers) == 1
        locations = pl.read_delta(
            f"{committer.delta_root}/{TablePath.ARTIFACT_LOCATIONS.value}",
            storage_options=storage.delta_storage_options(),
        )
        assert locations.height == 2
        for path in locations["uri"]:
            assert fs.exists(path)


@pytest.mark.parametrize("claim_first", [False, True])
def test_retained_commit_uses_same_winner_eligibility_for_cancelled_recovery(
    backend_fs, tmp_path: Path, claim_first: bool, monkeypatch
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    case, receipt = admit_case(committer, tmp_path / "worker")
    if claim_first:

        def stop(boundary, *_):
            if boundary == "claimed":
                msg = "stopped after claim"
                raise RuntimeError(msg)

        monkeypatch.setattr(committer, "_checkpoint", stop)
        with pytest.raises(RuntimeError, match="stopped after claim"):
            committer.accept(receipt)
    committer.journal.cancel(receipt.delivery.attempt)
    reopened = open_committer(committer.delta_root, fs, storage)
    if claim_first:
        reopened.recover(receipt.delivery.attempt.assignment)
        assert reopened.accepted_projection(receipt) == case.projection
    else:
        with pytest.raises(StoreIntegrityError, match="ineligible|cancel|winner"):
            reopened.accept(receipt)
        with pytest.raises(StoreIntegrityError, match="orphan seals"):
            reopened.recover(receipt.delivery.attempt.assignment)


def test_retained_commit_rejects_redelivery_and_preserves_independent_units(
    backend_fs, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    case, receipt = admit_case(committer, tmp_path / "first")
    other_case, independent = admit_case(committer, tmp_path / "second", unit_index=1)
    redelivery = stage_retained_result(
        f"{committer.delta_root}/staging",
        SuccessfulDelivery(
            delivery=DeliveryIdentity.new(receipt.delivery.attempt),
            computation_complete=True,
            cleanup_complete=True,
        ),
        case.projection,
        case.artifacts,
        fs=fs,
    )
    with pytest.raises(StoreIntegrityError, match="orphan seals"):
        committer.recover(receipt.delivery.attempt.assignment)
    assert committer.journal.state(receipt.delivery.attempt.assignment).winner is None
    committer.accept(receipt)
    with pytest.raises(StoreIntegrityError, match="Another delivery"):
        committer.accept(redelivery)
    committer.accept(independent)
    assert committer.accepted_projection(independent) == other_case.projection
    assert committer.accepted_projection(receipt) == case.projection


def test_retained_commit_rejects_obsolete_delivery_before_replacement_acceptance(
    backend_fs, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    case, obsolete = admit_case(committer, tmp_path / "worker")
    replacement = admit_prepared_case(committer, case, generation=1)
    with pytest.raises(StoreIntegrityError, match="current admitted"):
        committer.accept(obsolete)
    with pytest.raises(StoreIntegrityError, match="orphan seals"):
        committer.recover(obsolete.delivery.attempt.assignment)
    committer.accept(replacement)
    assert committer.accepted_projection(replacement) == case.projection


def test_recovery_rejects_self_consistent_replacement_of_unapplied_plan(
    backend_fs, tmp_path: Path, monkeypatch
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_case(committer, tmp_path / "worker")

    def stop(boundary, *_):
        if boundary == "plan":
            msg = "stopped after plan"
            raise RuntimeError(msg)

    monkeypatch.setattr(committer, "_checkpoint", stop)
    with pytest.raises(RuntimeError, match="stopped after plan"):
        committer.accept(receipt)
    path = f"{retained_plan_directory(committer.delta_root, receipt.delivery.attempt.assignment)}/plan.json"
    with fs.open(path, "rb") as stream:
        payload = json.load(stream)
    payload["step_number"] = 99
    payload["plan_digest"] = retained_plan_digest(
        {key: value for key, value in payload.items() if key != "plan_digest"}
    )
    with fs.open(path, "wb") as stream:
        stream.write(canonical_json_bytes(payload))
    reopened = open_committer(committer.delta_root, fs, storage)
    with pytest.raises(StoreIntegrityError, match="Conflicting immutable"):
        reopened.recover(receipt.delivery.attempt.assignment)
    controls = pl.read_delta(
        f"{committer.delta_root}/{TablePath.LOGICAL_COMMITS.value}",
        storage_options=storage.delta_storage_options(),
    )
    assert controls.is_empty()


@pytest.mark.parametrize("mutation", ["carrier", "origin", "files-root"])
def test_accepted_retained_projection_rejects_changed_managed_evidence(
    backend_fs, tmp_path: Path, mutation: str
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_case(committer, tmp_path / "worker")
    plan = committer.accept(receipt)
    if mutation == "carrier":
        with fs.open(
            f"{committer.files_root}/{plan.carriers[0].relative_path}", "wb"
        ) as stream:
            stream.write(b"changed")
    elif mutation == "origin":
        path = f"{committer.delta_root}/{TablePath.ARTIFACT_INDEX.value}"
        frame = pl.read_delta(path, storage_options=storage.delta_storage_options())
        frame.with_columns(
            pl.lit(99, dtype=pl.Int32).alias("origin_step_number")
        ).write_delta(
            path, mode="overwrite", storage_options=storage.delta_storage_options()
        )
    else:
        committer.files_root = f"{root}/wrong-files"
    with pytest.raises(StoreIntegrityError, match="origin|carrier|files root"):
        committer.accepted_projection(receipt)


def test_retained_acceptance_preserves_order_and_exact_previous_equal_content_producers(
    backend_fs, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    artifact = boundary_artifact()
    graph = OccurrenceGraph()
    source = graph.add_input("data", artifact)
    first = _derive(graph, "first", artifact, [source])
    second = _derive(graph, "second", artifact, [first])
    case = RetainedCase(
        graph.project({"data": [second, first, second]}, {"data": OutputSpec()}),
        {source: artifact},
        {first: artifact, second: artifact},
    )
    receipt = admit_prepared_case(committer, case)
    committer.accept(receipt)
    assert committer.accepted_projection(receipt) == case.projection
    assert case.projection.exports[0].occurrences == (second, first, second)

    consuming = OccurrenceGraph(boundary_invocation="next-boundary")
    slots = [consuming.add_input("data", artifact) for _ in range(2)]
    attribution = tuple(
        BoundaryInputAttribution(
            boundary=slot,
            producer=AcceptedOccurrence(
                delivery_id=receipt.delivery.delivery_id,
                seal_digest=receipt.seal_digest,
                address=producer,
            ),
        )
        for slot, producer in zip(slots, (first, second), strict=True)
    )
    next_case = RetainedCase(
        consuming.project(
            {"data": [slots[1], slots[0], slots[1]]},
            {"data": OutputSpec()},
            input_attribution=attribution,
        ),
        dict.fromkeys(slots, artifact),
        {},
    )
    next_receipt = admit_prepared_case(committer, next_case, unit_index=1)
    committer.accept(next_receipt)
    projected = committer.accepted_projection(next_receipt)
    assert projected == next_case.projection
    assert projected.input_attribution == attribution
    wrong = attribution[0].producer.model_copy(update={"seal_digest": "0" * 32})
    with pytest.raises(StoreIntegrityError, match="another complete seal"):
        committer._verify_producer(wrong, projected.occurrences[0])


def test_selected_producer_uses_fresh_controls_with_stale_directory_listing(
    backend_fs, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fs, storage, root = backend_fs
    writer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, first = admit_case(writer, tmp_path / "first", kind="alias")
    writer.accept(first)
    worker = open_committer(writer.delta_root, fs, storage)
    pattern = f"{worker.delta_root}/_artisan/retained-commits/*/plan.json"
    cached_paths = fs.glob(pattern)
    original_glob = fs.glob

    def stale_glob(path: str, **kwargs: object) -> list[str]:
        return cached_paths if path == pattern else original_glob(path, **kwargs)

    with worker._read_snapshot() as old_read:
        case, second = admit_case(
            writer, tmp_path / "second", kind="alias", unit_index=1
        )
        writer.accept(second)
        occurrence = case.projection.occurrences[0]
        producer = AcceptedOccurrence(
            delivery_id=second.delivery.delivery_id,
            seal_digest=second.seal_digest,
            address=occurrence.address,
        )
        monkeypatch.setattr(fs, "glob", stale_glob)
        with pytest.raises(StoreIntegrityError, match="no unique completed"):
            worker._verify_producer(producer, occurrence, _read=old_read)
        worker._verify_producer(producer, occurrence)


@pytest.mark.parametrize("mutation", ["missing", "tampered"])
def test_selected_producer_rejects_changed_controller_plan(
    backend_fs, tmp_path: Path, mutation: str
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    case, receipt = admit_case(committer, tmp_path / "worker", kind="alias")
    committer.accept(receipt)
    path = (
        retained_plan_directory(
            committer.delta_root, receipt.delivery.attempt.assignment
        )
        + "/plan.json"
    )
    if mutation == "missing":
        fs.rm(path)
    else:
        with fs.open(path, "rb") as stream:
            payload = json.load(stream)
        payload["step_number"] = 99
        payload["plan_digest"] = retained_plan_digest(
            {key: value for key, value in payload.items() if key != "plan_digest"}
        )
        with fs.open(path, "wb") as stream:
            stream.write(canonical_json_bytes(payload))
    occurrence = case.projection.occurrences[0]
    producer = AcceptedOccurrence(
        delivery_id=receipt.delivery.delivery_id,
        seal_digest=receipt.seal_digest,
        address=occurrence.address,
    )
    with pytest.raises((FileNotFoundError, StoreIntegrityError)):
        committer._verify_producer(producer, occurrence)


_CRASHES = [
    ("claimed", None),
    ("plan", None),
    ("carrier", None),
    ("planned", None),
    ("table", "artifacts/csvs"),
    ("table", TablePath.ARTIFACT_INDEX.value),
    ("table", TablePath.ARTIFACT_LOCATIONS.value),
    *(("table", table.table_path) for table in PROJECTION_TABLES),
    ("complete", None),
    ("cleanup", None),
]


@pytest.mark.parametrize(("boundary", "table_path"), _CRASHES)
def test_controller_process_death_recovers_only_claimed_exact_effects(
    backend_fs, tmp_path: Path, boundary: str, table_path: str | None
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    scratch = tmp_path / "worker"
    case, receipt = admit_case(committer, scratch)
    shutil.rmtree(scratch)
    request = {
        "root": committer.delta_root,
        "storage": storage.model_dump(),
        "receipt": receipt.model_dump(mode="json"),
        "boundary": boundary,
        "table_path": table_path,
    }
    process = subprocess.run(
        [sys.executable, "-m", "fixtures.retained_acceptance"],
        input=json.dumps(request),
        text=True,
        capture_output=True,
        check=False,
        timeout=45,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[2])},
    )
    assert process.returncode == 41, process.stderr
    if boundary == "claimed":
        assert (
            read_retained_plan(
                committer.delta_root, receipt.delivery.attempt.assignment, fs
            )
            is None
        )
    reopened = open_committer(committer.delta_root, fs, storage)
    plan = reopened.recover(receipt.delivery.attempt.assignment)
    assert reopened.accepted_projection(receipt) == case.projection
    assert reopened.recover(receipt.delivery.attempt.assignment) == plan
