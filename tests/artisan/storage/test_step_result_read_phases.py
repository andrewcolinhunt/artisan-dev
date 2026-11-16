"""Terminal publication shares exact evidence only between durable writes."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from fixtures.retained_acceptance import open_committer
from fixtures.retained_readers import Backend

from artisan.errors import StoreIntegrityError
from artisan.orchestration.engine import retained_step_results
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.assignment import AssignmentIdentity
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.storage.cache.retained_step_lookup import verify_step_inputs
from artisan.storage.core.assignment_schema import ASSIGNMENT_DECISIONS_SCHEMA
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.commit_contract import LogicalEffect
from artisan.storage.io.delta_files import DeltaSnapshot, append_table_files
from artisan.storage.io.retained_read import RetainedControlProof
from artisan.utils.path import uri_join

from .test_step_results import _committer, _prepare


def test_terminal_helpers_reject_a_proof_from_another_binding(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, _ = _prepare(committer, tmp_path, count=1)
    fs, storage, _ = raw_backend_fs
    other = open_committer(committer.delta_root, fs, storage)
    with other._read_snapshot() as read:
        with pytest.raises(StoreIntegrityError, match="another store binding"):
            results.tracker._read_state(admission.step_run_id, read)
        with pytest.raises(StoreIntegrityError, match="another store binding"):
            verify_step_inputs(
                admission.assignments,
                admission.assignments,
                RetainedReader(committer),
                _read=read,
            )


@pytest.mark.parametrize("complete", [False, True])
def test_terminal_reads_share_proof_until_the_next_write(
    raw_backend_fs: Backend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    complete: bool,
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, receipts = _prepare(committer, tmp_path, count=2)
    for receipt in receipts:
        committer.accept(receipt)
    if complete:
        results.finish(admission.step_run_id)
    original = committer._control_proof
    proofs = []

    def counted() -> RetainedControlProof:
        proof = original()
        proofs.append(proof)
        return proof

    with monkeypatch.context() as scope:
        scope.setattr(committer, "_control_proof", counted)
        result = (
            results.recover(admission.step_run_id)
            if complete
            else results.finish(admission.step_run_id)
        )
    assert result.status == StepStatus.SUCCEEDED
    assert len(proofs) == (1 if complete else 6)
    assert len(results.outputs(admission.step_run_id)[0].occurrences) == 4


@pytest.mark.parametrize("boundary", ["plan", "planned", "terminal"])
def test_publication_rechecks_whole_carriers_after_each_write(
    raw_backend_fs: Backend, tmp_path: Path, boundary: str
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, receipts = _prepare(
        committer, tmp_path, count=1, kind="appendable"
    )
    accepted = committer.accept(receipts[0])
    path = uri_join(committer.files_root, accepted.carriers[0].relative_path)
    original = committer.fs.cat_file(path)

    def change_carrier(event: str) -> None:
        if event == boundary:
            committer.fs.pipe_file(path, original + b"changed unselected remainder")

    results.checkpoint = change_carrier
    with pytest.raises(StoreIntegrityError, match="carrier"):
        results.finish(admission.step_run_id)
    controls = pl.read_delta(
        uri_join(committer.delta_root, TablePath.LOGICAL_COMMITS.value),
        storage_options=committer.storage_options,
    )
    assert controls.filter(
        (pl.col("logical_commit_id") == f"step_result:{admission.step_run_id}")
        & (pl.col("state") == "complete")
    ).is_empty()
    committer.fs.pipe_file(path, original)
    results.checkpoint = None
    assert results.recover(admission.step_run_id).status == StepStatus.SUCCEEDED


def test_complete_control_requires_a_fresh_terminal_read(
    raw_backend_fs: Backend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, receipts = _prepare(committer, tmp_path, count=1)
    committer.accept(receipts[0])
    complete = retained_step_results.complete_control
    steps_path = uri_join(committer.delta_root, TablePath.STEPS.value)
    saved = []

    def change_after_complete(
        plan: LogicalEffect, path: str, *, storage_options: dict[str, str]
    ) -> None:
        complete(plan, path, storage_options=storage_options)
        rows = pl.read_delta(steps_path, storage_options=storage_options)
        saved.append(rows)
        rows.with_columns(
            pl.when(pl.col("logical_commit_id") == plan.logical_commit_id)
            .then(pl.lit(99.0))
            .otherwise(pl.col("duration_seconds"))
            .alias("duration_seconds")
        ).write_delta(steps_path, mode="overwrite", storage_options=storage_options)

    with monkeypatch.context() as scope:
        scope.setattr(retained_step_results, "complete_control", change_after_complete)
        with pytest.raises(StoreIntegrityError):
            results.finish(admission.step_run_id)
    assert len(saved) == 1
    saved[0].write_delta(
        steps_path, mode="overwrite", storage_options=committer.storage_options
    )
    assert results.recover(admission.step_run_id).status == StepStatus.SUCCEEDED


@pytest.mark.parametrize("change", ["origin", "winner"])
def test_terminal_write_cannot_hide_changed_source_authority(
    raw_backend_fs: Backend, tmp_path: Path, change: str
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, receipts = _prepare(committer, tmp_path, count=1)
    accepted = committer.accept(receipts[0])

    def corrupt(event: str) -> None:
        if event != "terminal":
            return
        if change == "origin":
            table = next(
                item
                for item in accepted.tables
                if item.table_path.startswith("artifacts/")
            )
            path = uri_join(committer.delta_root, table.table_path)
            rows = pl.read_delta(path, storage_options=committer.storage_options)
            rows.with_columns(pl.col("origin_step_number") + 1).write_delta(
                path, mode="overwrite", storage_options=committer.storage_options
            )
            return
        other = receipts[0].delivery.attempt.model_copy(
            update={
                "assignment": AssignmentIdentity(step_run_id="f" * 32, unit_index=0)
            }
        )
        committer.journal.admit(
            other, admission_digest=accepted.admission.record_digest
        )
        snapshot = committer.journal._snapshot()
        rows = pl.concat(tuple(snapshot.iter_frames()))
        row = rows.filter(pl.col("action") == "claim").row(0, named=True)
        row.update(
            assignment_id=other.assignment.assignment_id,
            step_run_id=other.assignment.step_run_id,
        )
        append_table_files(
            snapshot,
            [pl.DataFrame([row], schema=ASSIGNMENT_DECISIONS_SCHEMA)],
            step_number=0,
            statistics_columns=["assignment_id", "sequence"],
        )

    results.checkpoint = corrupt
    with pytest.raises(StoreIntegrityError):
        results.finish(admission.step_run_id)
    controls = pl.read_delta(
        uri_join(committer.delta_root, TablePath.LOGICAL_COMMITS.value),
        storage_options=committer.storage_options,
    )
    assert controls.filter(
        (pl.col("logical_commit_id") == f"step_result:{admission.step_run_id}")
        & (pl.col("state") == "complete")
    ).is_empty()


def test_complete_recovery_does_not_retain_positive_carrier_proof(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, receipts = _prepare(committer, tmp_path, count=1, kind="file")
    accepted = committer.accept(receipts[0])
    expected = results.finish(admission.step_run_id)
    assert results.recover(admission.step_run_id) == expected
    path = uri_join(committer.files_root, accepted.carriers[0].relative_path)
    original = committer.fs.cat_file(path)
    committer.fs.pipe_file(path, original + b"changed bytes")
    with pytest.raises(StoreIntegrityError, match="carrier"):
        results.recover(admission.step_run_id)
    committer.fs.pipe_file(path, original)
    assert results.recover(admission.step_run_id) == expected


def test_lifecycle_uses_one_fixed_steps_version_and_next_read_is_fresh(
    raw_backend_fs: Backend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, _ = _prepare(committer, tmp_path, count=1)
    expected = results.tracker.current_state(admission.step_run_id)
    snapshot = committer._snapshot
    changed = False

    def change_after_capture(table_path: str) -> DeltaSnapshot:
        nonlocal changed
        captured = snapshot(table_path)
        if table_path == TablePath.STEPS.value and not changed:
            changed = True
            rows = pl.read_delta(
                captured.path, storage_options=committer.storage_options
            )
            rows.with_columns(pl.lit("changed owner").alias("step_name")).write_delta(
                captured.path,
                mode="overwrite",
                storage_options=committer.storage_options,
            )
        return captured

    monkeypatch.setattr(committer, "_snapshot", change_after_capture)
    with committer._read_snapshot() as read:
        assert results.tracker._read_state(admission.step_run_id, read)[1] == expected
        assert results.tracker._read_state(admission.step_run_id, read)[1] == expected
    with committer._read_snapshot() as read:
        current = results.tracker._read_state(admission.step_run_id, read)[1]
        assert current.step_name == "changed owner"
    assert changed
