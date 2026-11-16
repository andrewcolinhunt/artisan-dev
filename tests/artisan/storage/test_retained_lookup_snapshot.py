"""Cache selection shares one finite proof without retaining it across lookups."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

import polars as pl
import pytest
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_acceptance import open_committer
from fixtures.retained_curators import admit_curator
from fixtures.retained_readers import Backend, admit_read_case
from fixtures.retained_results import RetainedCase, retained_case

from artisan.errors import StoreIntegrityError
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.schemas.provenance.projection import (
    BoundaryInputAttribution,
    ProjectedOccurrence,
)
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.cache.retained_lookup import lookup_assignment, lookup_step
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.retained_reader import AcceptedArtifact, RetainedReader
from artisan.storage.io.assignment_journal import WinningDelivery
from artisan.storage.io.curator_plan import read_curator_plan
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_read import RetainedControlProof
from artisan.utils.path import uri_join


def _producer(
    committer: RetainedCommitter, backend: Backend, scratch: Path, kind: str
) -> AcceptedArtifact:
    if kind == "scientific":
        _, receipt = admit_read_case(
            committer, retained_case(scratch, kind="alias"), step_run_id="a" * 32
        )
        committer.accept(receipt)
        return RetainedReader(committer).read(receipt).output("data")[0]
    operation = DynamicCurator()
    coordinator, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(
        coordinator, admission, operation, storage=backend[1]
    )
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    plan = read_curator_plan(committer.delta_root, reference.step_run_id, committer.fs)
    assert plan is not None
    return (
        CuratorReader(committer)
        .read_result(plan.step_run_id, plan.plan_digest)
        .output("found")[0]
    )


def _consuming(producer: AcceptedArtifact) -> RetainedCase:
    graph = OccurrenceGraph()
    source = graph.add_input("data", producer.artifact)
    projection = graph.project({"data": [source, source]}, {"data": OutputSpec()})
    projection = projection.model_copy(
        update={
            "input_attribution": (
                BoundaryInputAttribution(boundary=source, producer=producer.reference),
            )
        }
    )
    return RetainedCase(projection, {source: producer.artifact}, {})


@pytest.mark.parametrize("producer_kind", ["scientific", "curator"])
def test_cache_lookup_shares_one_proof_for_current_and_selected_input_producers(
    raw_backend_fs: Backend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    producer_kind: str,
) -> None:
    fs, storage, root = raw_backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    reader = RetainedReader(committer)
    case = _consuming(_producer(committer, raw_backend_fs, tmp_path, producer_kind))
    _, source = admit_read_case(committer, case, step_run_id="b" * 32)
    committer.accept(source)
    current = tuple(
        admit_read_case(committer, case, step_run_id="c" * 32, unit_index=index)[0]
        for index in range(2)
    )
    control_proof = committer._control_proof
    require_winners = committer.journal.require_winners
    proofs = 0
    winner_batches = 0

    def count_proof() -> RetainedControlProof:
        nonlocal proofs
        proofs += 1
        return control_proof()

    def count_winners(
        receipts: Sequence[SealedDelivery],
    ) -> Mapping[SealedDelivery, WinningDelivery]:
        nonlocal winner_batches
        winner_batches += 1
        return require_winners(receipts)

    monkeypatch.setattr(committer, "_control_proof", count_proof)
    monkeypatch.setattr(committer.journal, "require_winners", count_winners)
    hits = lookup_step(current, reader=reader)
    assert hits is not None
    assert tuple(hit.receipt for hit in hits) == (source, source)
    assert proofs == 1
    assert winner_batches == 1
    assert lookup_assignment(current[0], reader=reader) == hits[0]
    assert proofs == 2
    assert winner_batches == 2


def _plain(value: int) -> RetainedCase:
    artifact = CSVArtifact.draft(
        f"value\n{value}\n".encode(), "input.csv", 1
    ).finalize()
    graph = OccurrenceGraph()
    address = graph.add_input("data", artifact)
    return RetainedCase(
        graph.project({"data": [address]}, {"data": OutputSpec()}),
        {address: artifact},
        {},
    )


@pytest.mark.parametrize("change", ["content", "origin", "duplicate"])
def test_candidate_discovery_never_marks_unread_payloads_verified(
    raw_backend_fs: Backend, change: str
) -> None:
    fs, storage, root = raw_backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    reader = RetainedReader(committer)
    first_case, other_case = _plain(1), _plain(2)
    _, first = admit_read_case(committer, first_case, step_run_id="a" * 32)
    _, other = admit_read_case(committer, other_case, step_run_id="b" * 32)
    committer.accept(first)
    committer.accept(other)
    current, _ = admit_read_case(committer, first_case, step_run_id="c" * 32)
    other_current, _ = admit_read_case(committer, other_case, step_run_id="d" * 32)
    assert lookup_assignment(other_current, reader=reader) is not None
    path = uri_join(committer.delta_root, ArtifactTypeDef.get_table_path("csv"))
    rows = pl.read_delta(path, storage_options=committer.storage_options)
    artifact_id = next(iter(other_case.boundary.values())).artifact_id
    selected = pl.col("artifact_id") == artifact_id
    if change == "duplicate":
        changed = pl.concat((rows, rows.filter(selected)))
    else:
        column, value = (
            ("content", pl.lit(b"value\nchanged\n"))
            if change == "content"
            else ("origin_step_number", pl.col("origin_step_number") + 1)
        )
        changed = rows.with_columns(
            pl.when(selected).then(value).otherwise(pl.col(column)).alias(column)
        )
    changed.write_delta(
        path, mode="overwrite", storage_options=committer.storage_options
    )
    assert tuple(plan.receipt for plan in reader.completed_plans()) == tuple(
        sorted(
            (first, other),
            key=lambda item: item.delivery.attempt.assignment.assignment_id,
        )
    )
    assert lookup_assignment(current, reader=reader).receipt == first
    with pytest.raises(StoreIntegrityError):
        lookup_assignment(other_current, reader=reader)
    with pytest.raises(StoreIntegrityError):
        lookup_assignment(other_current, reader=reader)


@pytest.mark.parametrize("producer_kind", ["scientific", "curator"])
def test_shared_producer_proof_keeps_exact_reference_and_store_binding(
    raw_backend_fs: Backend, tmp_path: Path, producer_kind: str
) -> None:
    fs, storage, root = raw_backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    producer = _producer(committer, raw_backend_fs, tmp_path, producer_kind)
    assert producer.artifact.artifact_id is not None
    boundary = ProjectedOccurrence(
        address=producer.reference.address,
        artifact_id=producer.artifact.artifact_id,
        artifact_type=producer.artifact.artifact_type,
    )
    other = open_committer(committer.delta_root, fs, storage)
    with committer._read_snapshot() as snapshot:
        committer._verify_producer(producer.reference, boundary, _read=snapshot)
        with pytest.raises(StoreIntegrityError, match="store binding"):
            other._verify_producer(producer.reference, boundary, _read=snapshot)
        wrong_address = producer.reference.model_copy(
            update={
                "address": OccurrenceAddress(
                    invocation_id="absent", role="data", index=0
                )
            }
        )
        with pytest.raises(StoreIntegrityError, match="occurrence|content"):
            committer._verify_producer(wrong_address, boundary, _read=snapshot)
        wrong_content = boundary.model_copy(update={"artifact_id": "0" * 32})
        with pytest.raises(StoreIntegrityError, match="content"):
            committer._verify_producer(
                producer.reference, wrong_content, _read=snapshot
            )


def test_lookup_rechecks_current_cancellation_after_a_successful_hit(
    raw_backend_fs: Backend,
) -> None:
    fs, storage, root = raw_backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    reader = RetainedReader(committer)
    case = _plain(1)
    _, source = admit_read_case(committer, case, step_run_id="a" * 32)
    committer.accept(source)
    current, _ = admit_read_case(committer, case, step_run_id="b" * 32)
    assert lookup_assignment(current, reader=reader) is not None
    committer.journal.cancel(current.attempt)
    assert lookup_assignment(current, reader=reader) is None
    assert lookup_step((current,), reader=reader) is None


def test_lookup_discovers_new_completion_after_an_earlier_miss(
    raw_backend_fs: Backend,
) -> None:
    fs, storage, root = raw_backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    reader = RetainedReader(committer)
    case = _plain(1)
    _, source = admit_read_case(committer, case, step_run_id="a" * 32)
    current, _ = admit_read_case(committer, case, step_run_id="b" * 32)
    assert lookup_assignment(current, reader=reader) is None
    committer.accept(source)
    hit = lookup_assignment(current, reader=reader)
    assert hit is not None
    assert hit.receipt == source
