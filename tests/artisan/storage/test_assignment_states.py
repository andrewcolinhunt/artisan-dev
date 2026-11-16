"""Fresh batched journal reads preserve the same exact per-owner validation."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import polars as pl
import pytest
from fixtures.retained_readers import Backend
from fixtures.retained_results import retained_case

from artisan.errors import StoreIntegrityError
from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    AssignmentIdentity,
    DeliveryIdentity,
    SealedDelivery,
    SuccessfulDelivery,
)
from artisan.storage.core.assignment_schema import ASSIGNMENT_DECISIONS_SCHEMA
from artisan.storage.core.store_format import initialize_retained_store
from artisan.storage.io.assignment_journal import AssignmentJournal
from artisan.storage.io.delta_files import DeltaSnapshot, append_table_files
from artisan.storage.io.retained_seal import (
    capture_retained_delivery,
    stage_retained_result,
)


def _journal(backend: Backend) -> AssignmentJournal:
    fs, storage, root = backend
    initialize_retained_store(f"{root}/candidate", fs)
    journal = AssignmentJournal(
        f"{root}/candidate", fs=fs, storage_options=storage.delta_storage_options()
    )
    journal.initialize()
    return journal


def _attempt(index: int) -> AssignmentAttempt:
    return AssignmentAttempt(
        assignment=AssignmentIdentity(step_run_id="a" * 32, unit_index=index),
        attempt=0,
        generation=0,
        recipe_digest="b" * 32,
        software_digest="c" * 64,
        input_context_digest="d" * 32,
    )


def test_states_scans_once_and_never_reuses_an_earlier_snapshot(
    raw_backend_fs: Backend, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = _journal(raw_backend_fs)
    first, second, missing = (_attempt(index) for index in range(3))
    journal.admit(first, admission_digest="e" * 32)
    journal.admit(second, admission_digest="f" * 32)
    journal.cancel(first)
    owners = [item.assignment for item in (first, second, missing, first)]
    expected = {owner: journal.state(owner) for owner in owners}
    scans = []
    original = DeltaSnapshot.iter_located

    def counted(
        snapshot: DeltaSnapshot,
        columns: list[str] | None = None,
        *,
        rows_by_file: dict[str, set[int]] | None = None,
    ) -> Iterator[tuple[str, list[int], pl.DataFrame]]:
        scans.append(snapshot)
        yield from original(snapshot, columns, rows_by_file=rows_by_file)

    with monkeypatch.context() as patch:
        patch.setattr(DeltaSnapshot, "iter_located", counted)
        observed = journal.states(owners)
        assert journal.states(()) == {}
    assert observed == expected
    assert len(scans) == 1
    with pytest.raises(TypeError):
        observed[first.assignment] = None
    journal.cancel(second)
    updated = journal.states(owners)
    assert updated[second.assignment].cancelled
    assert not observed[second.assignment].cancelled
    assert updated[missing.assignment] is None


@pytest.mark.parametrize(
    "corruption", ["duplicate", "gap", "null", "owner", "invalid-claim"]
)
def test_states_and_state_reject_the_same_malformed_queried_history(
    raw_backend_fs: Backend, corruption: str
) -> None:
    journal = _journal(raw_backend_fs)
    first, other = _attempt(0), _attempt(1)
    journal.admit(first, admission_digest="e" * 32)
    journal.admit(other, admission_digest="e" * 32)
    snapshot = journal._snapshot()
    frame = pl.concat([frame for _, _, frame in snapshot.iter_located()])
    row = frame.filter(pl.col("assignment_id") == first.assignment.assignment_id).row(
        0, named=True
    )
    row.update(
        {
            "duplicate": {},
            "gap": {"sequence": 2, "action": "cancel"},
            "null": {"sequence": 1, "action": None},
            "owner": {"sequence": 1, "unit_index": 99},
            "invalid-claim": {"sequence": 1, "action": "claim"},
        }[corruption]
    )
    append_table_files(
        snapshot,
        [pl.DataFrame([row], schema=ASSIGNMENT_DECISIONS_SCHEMA)],
        step_number=0,
        statistics_columns=["assignment_id", "sequence"],
    )
    with pytest.raises(StoreIntegrityError) as single:
        journal.state(first.assignment)
    with pytest.raises(StoreIntegrityError) as batch:
        journal.states((first.assignment, other.assignment))
    assert str(batch.value) == str(single.value)
    assert journal.states((other.assignment,))[other.assignment] == journal.state(
        other.assignment
    )


def _claim(
    journal: AssignmentJournal, backend: Backend, scratch: Path, index: int
) -> SealedDelivery:
    fs, _, root = backend
    case = retained_case(scratch, kind="empty")
    attempt = case.completion().delivery.attempt.model_copy(
        update={
            "assignment": AssignmentIdentity(step_run_id="a" * 32, unit_index=index),
        }
    )
    journal.admit(attempt, admission_digest="e" * 32)
    receipt = stage_retained_result(
        f"{root}/staging",
        SuccessfulDelivery(
            delivery=DeliveryIdentity.new(attempt),
            computation_complete=True,
            cleanup_complete=True,
        ),
        case.projection,
        case.artifacts,
        fs=fs,
    )
    with capture_retained_delivery(
        receipt, case.boundary, expected_recipe=case.recipe, fs=fs
    ) as captured:
        journal.claim(receipt, captured)
    return receipt


def test_require_winners_batches_histories_and_global_uuid_proof(
    raw_backend_fs: Backend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = _journal(raw_backend_fs)
    first, second = (
        _claim(journal, raw_backend_fs, tmp_path, index) for index in range(2)
    )
    expected = {receipt: journal.require_winner(receipt) for receipt in (first, second)}
    scans = []
    original = DeltaSnapshot.iter_located

    def counted(
        snapshot: DeltaSnapshot,
        columns: list[str] | None = None,
        *,
        rows_by_file: dict[str, set[int]] | None = None,
    ) -> Iterator[tuple[str, list[int], pl.DataFrame]]:
        scans.append(snapshot)
        yield from original(snapshot, columns, rows_by_file=rows_by_file)

    with monkeypatch.context() as patch:
        patch.setattr(DeltaSnapshot, "iter_located", counted)
        observed = journal.require_winners((first, second, first))
        assert journal.require_winners(()) == {}
    assert observed == expected
    assert len(scans) == 2
    assert scans[0] is scans[1]
    with pytest.raises(TypeError):
        observed[first] = observed[second]


def test_batched_winner_proof_rejects_foreign_global_delivery_collision(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    journal = _journal(raw_backend_fs)
    receipt = _claim(journal, raw_backend_fs, tmp_path, 0)
    snapshot = journal._snapshot()
    rows = pl.concat([frame for _, _, frame in snapshot.iter_located()])
    row = rows.filter(pl.col("action") == "claim").row(0, named=True)
    foreign = AssignmentIdentity(step_run_id="b" * 32, unit_index=99)
    row.update(
        assignment_id=foreign.assignment_id,
        step_run_id=foreign.step_run_id,
        unit_index=foreign.unit_index,
        sequence=0,
    )
    append_table_files(
        snapshot,
        [pl.DataFrame([row], schema=ASSIGNMENT_DECISIONS_SCHEMA)],
        step_number=0,
        statistics_columns=["assignment_id", "sequence"],
    )
    assert journal.state(receipt.delivery.attempt.assignment).winner.receipt == receipt
    with pytest.raises(StoreIntegrityError, match="another assignment"):
        journal.require_winner(receipt)
    with pytest.raises(StoreIntegrityError, match="another assignment"):
        journal.require_winners((receipt,))
