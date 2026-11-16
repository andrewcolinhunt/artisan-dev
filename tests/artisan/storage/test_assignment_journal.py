"""Cancellation and recovery share one durable, per-unit winner decision."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import polars as pl
import pytest
from fixtures.retained_results import RetainedCase, retained_case
from pydantic import ValidationError

from artisan.errors import StoreIntegrityError
from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    AssignmentIdentity,
    DeliveryIdentity,
    SealedDelivery,
    SuccessfulDelivery,
)
from artisan.schemas.execution.assignment_decision import AssignmentDecision
from artisan.storage.core.assignment_schema import ASSIGNMENT_DECISIONS_SCHEMA
from artisan.storage.core.store_format import initialize_retained_store
from artisan.storage.io.assignment_journal import AssignmentJournal
from artisan.storage.io.delta_files import append_table_files
from artisan.storage.io.retained_seal import (
    capture_retained_delivery,
    stage_retained_result,
)


def _stage(
    case: RetainedCase, attempt: AssignmentAttempt, root: str, fs
) -> SealedDelivery:
    return stage_retained_result(
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


def _journal(backend_fs) -> AssignmentJournal:
    fs, storage, root = backend_fs
    initialize_retained_store(f"{root}/candidate", fs)
    journal = AssignmentJournal(
        f"{root}/candidate",
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )
    journal.initialize()
    return journal


@pytest.mark.parametrize("cancel_first", [False, True])
def test_cancellation_and_claim_keep_their_durable_order(
    backend_fs, tmp_path: Path, cancel_first: bool
) -> None:
    fs, storage, root = backend_fs
    case = retained_case(tmp_path, kind="empty")
    attempt = case.completion().delivery.attempt
    journal = _journal(backend_fs)
    journal.admit(attempt, admission_digest="d" * 32)
    receipt = _stage(case, attempt, root, fs)
    with capture_retained_delivery(
        receipt, case.boundary, expected_recipe=case.recipe, fs=fs
    ) as captured:
        if cancel_first:
            journal.cancel(attempt)
            with pytest.raises(StoreIntegrityError, match="ineligible"):
                journal.claim(receipt, captured)
        else:
            winner = journal.claim(receipt, captured)
            journal.cancel(attempt)
            assert journal.claim(receipt, captured) == winner
    resumed = AssignmentJournal(
        f"{root}/candidate", fs=fs, storage_options=storage.delta_storage_options()
    )
    assert resumed.state(attempt.assignment) == journal.state(attempt.assignment)
    if cancel_first:
        with pytest.raises(StoreIntegrityError, match="no durable"):
            resumed.require_winner(receipt)
    else:
        assert resumed.require_winner(receipt) == winner


def test_redeliveries_and_units_have_independent_owners(
    backend_fs, tmp_path: Path
) -> None:
    fs, _, root = backend_fs
    case = retained_case(tmp_path, kind="empty")
    attempt = case.completion().delivery.attempt
    other = attempt.model_copy(
        update={"assignment": AssignmentIdentity(step_run_id="a" * 32, unit_index=1)}
    )
    journal = _journal(backend_fs)
    journal.admit(attempt, admission_digest="d" * 32)
    journal.admit(other, admission_digest="d" * 32)
    receipts = [_stage(case, owner, root, fs) for owner in (attempt, attempt, other)]
    for index, receipt in enumerate(receipts):
        with capture_retained_delivery(
            receipt, case.boundary, expected_recipe=case.recipe, fs=fs
        ) as captured:
            if index == 1:
                with pytest.raises(StoreIntegrityError, match="Another delivery"):
                    journal.claim(receipt, captured)
            else:
                journal.claim(receipt, captured)
    journal.cancel(attempt)
    assert journal.require_winner(receipts[2]).receipt == receipts[2]


def test_obsolete_attempts_and_changed_retries_are_rejected(
    backend_fs, tmp_path: Path
) -> None:
    fs, _, root = backend_fs
    case = retained_case(tmp_path, kind="empty")
    attempt = case.completion().delivery.attempt
    next_attempt = attempt.model_copy(update={"attempt": 1, "generation": 1})
    journal = _journal(backend_fs)
    journal.admit(attempt, admission_digest="d" * 32)
    journal.admit(next_attempt, admission_digest="d" * 32)
    assert journal.admit(next_attempt, admission_digest="d" * 32).sequence == 1
    with pytest.raises(StoreIntegrityError, match="obsolete"):
        journal.cancel(attempt)
    receipt = _stage(case, attempt, root, fs)
    with (
        capture_retained_delivery(
            receipt, case.boundary, expected_recipe=case.recipe, fs=fs
        ) as captured,
        pytest.raises(StoreIntegrityError, match="obsolete"),
    ):
        journal.claim(receipt, captured)
    for field, value in (
        ("recipe_digest", "f" * 32),
        ("software_digest", "f" * 64),
        ("input_context_digest", "f" * 32),
        ("replay_of_execution_run_id", "f" * 32),
    ):
        changed = next_attempt.model_copy(
            update={"attempt": 2, "generation": 2, field: value}
        )
        with pytest.raises(StoreIntegrityError, match="frozen assignment"):
            journal.admit(changed, admission_digest="d" * 32)


def test_delivery_id_cannot_be_claimed_by_two_units(backend_fs, tmp_path: Path) -> None:
    fs, _, root = backend_fs
    case = retained_case(tmp_path, kind="empty")
    attempt = case.completion().delivery.attempt
    other = attempt.model_copy(
        update={"assignment": AssignmentIdentity(step_run_id="a" * 32, unit_index=1)}
    )
    journal = _journal(backend_fs)
    journal.admit(attempt, admission_digest="d" * 32)
    journal.admit(other, admission_digest="d" * 32)
    receipt = _stage(case, attempt, root, fs)
    with capture_retained_delivery(
        receipt, case.boundary, expected_recipe=case.recipe, fs=fs
    ) as captured:
        journal.claim(receipt, captured)
    reused = stage_retained_result(
        f"{root}/different-provider/staging",
        SuccessfulDelivery(
            delivery=DeliveryIdentity(
                attempt=other, delivery_id=receipt.delivery.delivery_id
            ),
            computation_complete=True,
            cleanup_complete=True,
        ),
        case.projection,
        case.artifacts,
        fs=fs,
    )
    with (
        capture_retained_delivery(
            reused, case.boundary, expected_recipe=case.recipe, fs=fs
        ) as captured,
        pytest.raises(StoreIntegrityError, match="another assignment"),
    ):
        journal.claim(reused, captured)
    assert journal.require_winner(receipt).receipt == receipt
    assert journal.state(other.assignment).winner is None


def test_orphan_and_substituted_capture_cannot_claim(
    backend_fs, tmp_path: Path
) -> None:
    fs, _, root = backend_fs
    case = retained_case(tmp_path, kind="empty")
    attempt = case.completion().delivery.attempt
    journal = _journal(backend_fs)
    receipt = _stage(case, attempt, root, fs)
    other = _stage(case, attempt, root, fs)
    with capture_retained_delivery(
        receipt, case.boundary, expected_recipe=case.recipe, fs=fs
    ) as captured:
        with pytest.raises(StoreIntegrityError, match="unadmitted"):
            journal.claim(receipt, captured)
        journal.admit(attempt, admission_digest="d" * 32)
        for changed in (
            other,
            receipt.model_copy(update={"seal_digest": "f" * 32}),
            receipt.model_copy(update={"seal_uri": receipt.seal_uri + ".other"}),
        ):
            with pytest.raises(StoreIntegrityError, match="Captured evidence"):
                journal.claim(changed, captured)
    assert journal.state(attempt.assignment).winner is None


def test_claim_and_cancel_threads_share_one_decision_order(
    backend_fs, tmp_path: Path
) -> None:
    fs, _, root = backend_fs
    case = retained_case(tmp_path, kind="empty")
    attempt = case.completion().delivery.attempt
    journal = _journal(backend_fs)
    journal.admit(attempt, admission_digest="d" * 32)
    receipt = _stage(case, attempt, root, fs)
    barrier = Barrier(2)
    with capture_retained_delivery(
        receipt, case.boundary, expected_recipe=case.recipe, fs=fs
    ) as captured:

        def claim() -> bool:
            barrier.wait()
            try:
                journal.claim(receipt, captured)
            except StoreIntegrityError:
                return False
            return True

        def cancel() -> None:
            barrier.wait()
            journal.cancel(attempt)

        with ThreadPoolExecutor(max_workers=2) as pool:
            claimed = pool.submit(claim)
            cancelled = pool.submit(cancel)
            accepted = claimed.result()
            cancelled.result()
    state = journal.state(attempt.assignment)
    assert state.cancelled
    assert (state.winner is not None) == accepted
    assert state.sequence == (2 if accepted else 1)


@pytest.mark.parametrize(
    "corruption", ["duplicate", "gap", "nullable", "invalid_winner"]
)
def test_malformed_durable_decision_history_fails_closed(
    backend_fs, tmp_path: Path, corruption: str
) -> None:
    case = retained_case(tmp_path, kind="empty")
    attempt = case.completion().delivery.attempt
    journal = _journal(backend_fs)
    journal.admit(attempt, admission_digest="d" * 32)
    snapshot = journal._snapshot()
    row = next(snapshot.iter_located())[2].row(0, named=True)
    changes = {
        "duplicate": {},
        "gap": {"sequence": 2, "action": "cancel"},
        "nullable": {"sequence": 1, "action": None},
        "invalid_winner": {"sequence": 1, "action": "claim"},
    }
    row.update(changes[corruption])
    append_table_files(
        snapshot,
        [pl.DataFrame([row], schema=ASSIGNMENT_DECISIONS_SCHEMA)],
        step_number=0,
        statistics_columns=["assignment_id", "sequence"],
    )
    with pytest.raises(StoreIntegrityError):
        journal.state(attempt.assignment)


def test_assignment_decision_rejects_coerced_sequence(tmp_path: Path) -> None:
    attempt = retained_case(tmp_path, kind="empty").completion().delivery.attempt
    with pytest.raises(ValidationError):
        AssignmentDecision(
            sequence=True, action="admit", attempt=attempt, admission_digest="d" * 32
        )


def test_admission_record_digest_cannot_change_for_an_admitted_attempt(
    backend_fs, tmp_path: Path
) -> None:
    attempt = retained_case(tmp_path, kind="empty").completion().delivery.attempt
    journal = _journal(backend_fs)
    journal.admit(attempt, admission_digest="d" * 32)
    with pytest.raises(StoreIntegrityError, match="input record changed"):
        journal.admit(attempt, admission_digest="e" * 32)
    snapshot = journal._snapshot()
    row = next(snapshot.iter_located())[2].row(0, named=True)
    row.update(sequence=1, action="cancel", admission_digest="e" * 32)
    append_table_files(
        snapshot,
        [pl.DataFrame([row], schema=ASSIGNMENT_DECISIONS_SCHEMA)],
        step_number=0,
        statistics_columns=["assignment_id", "sequence"],
    )
    with pytest.raises(StoreIntegrityError, match="admitted input record"):
        journal.state(attempt.assignment)
