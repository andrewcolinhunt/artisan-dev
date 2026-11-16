"""Cache reuse and cancellation share one durable current-unit decision order."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from functools import partial
from pathlib import Path

import polars as pl
import pytest
from fixtures.retained_acceptance import open_committer
from fixtures.retained_readers import Backend, admit_read_case
from fixtures.retained_results import retained_case

from artisan.errors import StoreIntegrityError
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.storage.cache.retained_reuse import verify_cache_source
from artisan.storage.core.assignment_schema import ASSIGNMENT_DECISIONS_SCHEMA
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.assignment_journal import AssignmentJournal
from artisan.storage.io.delta_files import append_table_files
from artisan.storage.io.retained_seal import capture_retained_delivery


@pytest.mark.parametrize("cancel_first", [False, True])
def test_reuse_and_cancellation_preserve_their_durable_order(
    backend_fs: Backend, tmp_path: Path, cancel_first: bool
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    case = retained_case(tmp_path, kind="empty")
    _, source = admit_read_case(committer, case, step_run_id="a" * 32)
    committer.accept(source)
    current, current_delivery = admit_read_case(committer, case, step_run_id="b" * 32)
    verify = partial(verify_cache_source, reader=RetainedReader(committer))
    if cancel_first:
        committer.journal.cancel(current.attempt)
        with pytest.raises(StoreIntegrityError, match="ineligible"):
            committer.journal.reuse(current, source, verify_source=verify)
    else:
        reused = committer.journal.reuse(current, source, verify_source=verify)
        committer.journal.cancel(current.attempt)
        assert committer.journal.reuse(current, source, verify_source=verify) == reused
    reopened = AssignmentJournal(
        committer.delta_root, fs=fs, storage_options=storage.delta_storage_options()
    )
    state = reopened.state(current.attempt.assignment)
    assert state is not None
    assert state.cancelled
    assert state.winner is None
    if cancel_first:
        assert state.reuse is None
        with pytest.raises(StoreIntegrityError, match="no durable cache reuse"):
            reopened.require_reuse(current, source)
    else:
        assert state.reuse == reused
        assert reopened.require_reuse(current, source) == reused
    with pytest.raises(StoreIntegrityError, match="no durable controller winner"):
        reopened.require_winner(current_delivery)


def test_reused_source_can_serve_independent_units_but_each_unit_selects_once(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    case = retained_case(tmp_path, kind="empty")
    _, source = admit_read_case(committer, case, step_run_id="a" * 32)
    _, other_source = admit_read_case(committer, case, step_run_id="c" * 32)
    committer.accept(source)
    committer.accept(other_source)
    verify = partial(verify_cache_source, reader=RetainedReader(committer))
    for index in range(2):
        current, receipt = admit_read_case(
            committer, case, step_run_id="b" * 32, unit_index=index
        )
        reused = committer.journal.reuse(current, source, verify_source=verify)
        assert committer.journal.require_reuse(current, source) == reused
        with pytest.raises(StoreIntegrityError, match="Another source"):
            committer.journal.reuse(current, other_source, verify_source=verify)
        with pytest.raises(StoreIntegrityError, match="reused"):
            committer.journal.admit(
                current.attempt.model_copy(update={"attempt": 1, "generation": 1}),
                admission_digest=current.record_digest,
            )
        with (
            capture_retained_delivery(
                receipt, case.boundary, expected_recipe=case.recipe, fs=fs
            ) as captured,
            pytest.raises(StoreIntegrityError, match="ineligible"),
        ):
            committer.journal.claim(receipt, captured)
    assert committer.journal.require_winner(source).receipt == source


def test_claimed_current_unit_cannot_select_a_cache_source(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    case = retained_case(tmp_path, kind="empty")
    _, source = admit_read_case(committer, case, step_run_id="a" * 32)
    committer.accept(source)
    current, receipt = admit_read_case(committer, case, step_run_id="b" * 32)
    committer.accept(receipt)
    with pytest.raises(StoreIntegrityError, match="ineligible"):
        committer.journal.reuse(
            current,
            source,
            verify_source=partial(
                verify_cache_source, reader=RetainedReader(committer)
            ),
        )
    assert committer.journal.state(current.attempt.assignment).reuse is None
    assert committer.journal.require_winner(receipt).receipt == receipt


@pytest.mark.parametrize("change", ["cancel", "supersede", "admission"])
def test_reuse_rechecks_current_admission_after_source_verification(
    backend_fs: Backend, tmp_path: Path, change: str
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    case = retained_case(tmp_path, kind="empty")
    _, source = admit_read_case(committer, case, step_run_id="a" * 32)
    committer.accept(source)
    current, _ = admit_read_case(committer, case, step_run_id="b" * 32)

    def verify(admission: AdmissionReference, receipt: SealedDelivery) -> None:
        verify_cache_source(admission, receipt, reader=RetainedReader(committer))
        if change == "cancel":
            committer.journal.cancel(current.attempt)
        elif change == "supersede":
            committer.journal.admit(
                current.attempt.model_copy(update={"attempt": 1, "generation": 1}),
                admission_digest=current.record_digest,
            )

    selected = (
        current
        if change != "admission"
        else current.model_copy(update={"record_digest": "0" * 32})
    )
    with pytest.raises(StoreIntegrityError, match="ineligible|obsolete|journal-bound"):
        committer.journal.reuse(selected, source, verify_source=verify)
    assert committer.journal.state(current.attempt.assignment).reuse is None


@pytest.mark.parametrize(
    "change",
    [
        "source-software",
        "source-diagnostic",
        "source-noncacheable",
        "source-unclaimed",
        "current-diagnostic",
        "current-noncacheable",
    ],
)
def test_exact_source_verifier_rejects_ineligible_science_without_substitution(
    backend_fs: Backend, tmp_path: Path, change: str
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    case = retained_case(tmp_path, kind="empty")
    _, good = admit_read_case(committer, case, step_run_id="a" * 32)
    committer.accept(good)
    _, proposed = admit_read_case(
        committer,
        case,
        step_run_id="c" * 32,
        software_digest=("d" if change == "source-software" else "c") * 64,
        replay_of="e" * 32 if change == "source-diagnostic" else None,
        recipe=case.recipe.model_copy(update={"cacheable": False})
        if change == "source-noncacheable"
        else None,
    )
    if change != "source-unclaimed":
        committer.accept(proposed)
    current, _ = admit_read_case(
        committer,
        case,
        step_run_id="b" * 32,
        replay_of="e" * 32 if change == "current-diagnostic" else None,
        recipe=case.recipe.model_copy(update={"cacheable": False})
        if change == "current-noncacheable"
        else None,
    )
    with pytest.raises(StoreIntegrityError):
        committer.journal.reuse(
            current,
            proposed,
            verify_source=partial(
                verify_cache_source, reader=RetainedReader(committer)
            ),
        )
    assert committer.journal.state(current.attempt.assignment).reuse is None


@pytest.mark.parametrize("corruption", ["source-owner", "source-seal", "wrong-action"])
def test_reuse_journal_rejects_partial_or_misplaced_source_evidence(
    backend_fs: Backend, tmp_path: Path, corruption: str
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    case = retained_case(tmp_path, kind="empty")
    _, source = admit_read_case(committer, case, step_run_id="a" * 32)
    committer.accept(source)
    current, _ = admit_read_case(committer, case, step_run_id="b" * 32)
    committer.journal.reuse(
        current,
        source,
        verify_source=partial(verify_cache_source, reader=RetainedReader(committer)),
    )
    snapshot = committer.journal._snapshot()
    rows = pl.concat([frame for _, _, frame in snapshot.iter_located()])
    row = rows.filter(pl.col("action") == "reuse").row(0, named=True)
    row["sequence"] += 1
    row.update(
        {
            "source-owner": {"source_step_run_id": None},
            "source-seal": {"source_seal_digest": None},
            "wrong-action": {"action": "cancel"},
        }[corruption]
    )
    append_table_files(
        snapshot,
        [pl.DataFrame([row], schema=ASSIGNMENT_DECISIONS_SCHEMA)],
        step_number=0,
        statistics_columns=["assignment_id", "sequence"],
    )
    with pytest.raises(StoreIntegrityError):
        committer.journal.state(current.attempt.assignment)


def test_selected_cache_source_survives_controller_death_before_step_publication(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    case = retained_case(tmp_path, kind="empty")
    _, source = admit_read_case(committer, case, step_run_id="a" * 32)
    committer.accept(source)
    current, _ = admit_read_case(committer, case, step_run_id="b" * 32)
    process = subprocess.run(
        [sys.executable, "-m", "fixtures.retained_reuse"],
        input=json.dumps(
            {
                "root": committer.delta_root,
                "storage": storage.model_dump(),
                "admission": current.model_dump(mode="json"),
                "receipt": source.model_dump(mode="json"),
            }
        ),
        text=True,
        capture_output=True,
        check=False,
        timeout=90,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[2])},
    )
    assert process.returncode == 41, process.stderr
    reopened = open_committer(committer.delta_root, fs, storage)
    reopened.journal.cancel(current.attempt)
    selected = reopened.journal.require_reuse(current, source)
    assert selected.receipt == source
    assert reopened.journal.state(current.attempt.assignment).winner is None
    verify_cache_source(current, source, reader=RetainedReader(reopened))
