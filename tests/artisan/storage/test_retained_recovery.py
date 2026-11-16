"""Explicit repair finishes only durable selections and preserves unresolved work."""

from __future__ import annotations

import hashlib
from pathlib import Path

import polars as pl
import pytest
from fixtures.candidate_outputs import prepare_scientific
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_acceptance import admit_case
from fixtures.retained_curators import admit_curator
from fixtures.retained_readers import admit_read_case
from fixtures.retained_results import retained_case

from artisan.schemas.enums import TablePath
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.storage.io.input_registration import register_raw_inputs
from artisan.storage.io.input_registration_plan import read_input_registration
from artisan.storage.io.repair import repair_store
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_recovery import apply_retained_repair
from artisan.storage.io.retained_repair import inspect_retained_store
from artisan.storage.io.retained_store import create_retained_store


@pytest.fixture
def committer(raw_backend_fs) -> RetainedCommitter:
    fs, storage, root = raw_backend_fs
    return create_retained_store(
        f"{root}/store",
        f"{root}/store/files",
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )


def _inventory(committer: RetainedCommitter) -> dict[str, str]:
    return {
        path: hashlib.sha256(committer.fs.cat_file(path)).hexdigest()
        for path in committer.fs.find(committer.delta_root)
    }


def _interrupt(
    committer: RetainedCommitter,
    receipt: SealedDelivery,
    monkeypatch: pytest.MonkeyPatch,
    *,
    event: str = "claimed",
) -> None:
    def checkpoint(boundary: str, _receipt: SealedDelivery, _table: str | None) -> None:
        if boundary == event:
            raise InterruptedError(event)

    with monkeypatch.context() as patch:
        patch.setattr(committer, "_checkpoint", checkpoint)
        with pytest.raises(InterruptedError, match=event):
            committer.accept(receipt, preserve_staging=True)


@pytest.mark.parametrize("preserve", [False, True])
def test_repair_finishes_claimed_unit_without_accepting_sibling_or_finishing_step(
    committer: RetainedCommitter,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    preserve: bool,
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias"), count=2
    )
    _interrupt(committer, receipts[0], monkeypatch)
    report = repair_store(
        delta_root=committer.delta_root,
        files_root=committer.files_root,
        fs=committer.fs,
        storage_options=committer.storage_options,
        apply=True,
        staging_root=f"{committer.delta_root}/staging",
        preserve_staging=preserve,
    )
    assert not report.blocking
    assert report.unresolved
    assert committer.accepted_projection(receipts[0]).exports
    assert (
        results.tracker.current_state(admission.step_run_id).status
        == StepStatus.RUNNING
    )
    sibling = committer.journal.state(receipts[1].delivery.attempt.assignment)
    assert sibling is not None
    assert sibling.winner is None
    assert committer.fs.exists(receipts[1].seal_uri)
    assert committer.fs.exists(receipts[0].seal_uri) is preserve
    before = _inventory(committer)
    assert (
        repair_store(
            delta_root=committer.delta_root,
            files_root=committer.files_root,
            fs=committer.fs,
            storage_options=committer.storage_options,
            apply=True,
            staging_root=f"{committer.delta_root}/staging",
            preserve_staging=preserve,
        )
        == report
    )
    assert _inventory(committer) == before


def test_repair_finishes_selection_without_resolving_unknown_worker(
    committer: RetainedCommitter, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias")
    )
    _interrupt(committer, receipts[0], monkeypatch)
    for status in (CancellationStatus.REQUESTED, CancellationStatus.UNKNOWN):
        results.tracker.record_cancellation(
            admission.step_run_id,
            StepStatus.RUNNING,
            CancellationAcknowledgement(status),
        )
    report = apply_retained_repair(
        committer, staging_root=f"{committer.delta_root}/staging"
    )
    assert not report.blocking
    assert any(item.classification == "unknown_owner" for item in report.items)
    state = results.tracker.current_state(admission.step_run_id)
    assert state.status == StepStatus.RUNNING
    assert state.cancellation_status == CancellationStatus.UNKNOWN
    assert committer.accepted_projection(receipts[0]).exports


def test_invalid_outer_membership_prevents_every_selected_recovery(
    committer: RetainedCommitter, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = retained_case(tmp_path, kind="alias")
    _, admission, receipts = prepare_scientific(committer, case)
    _interrupt(committer, receipts[0], monkeypatch)
    _, outside = admit_read_case(
        committer, case, step_run_id=admission.step_run_id, unit_index=1
    )
    _interrupt(committer, outside, monkeypatch)
    before = _inventory(committer)
    report = apply_retained_repair(
        committer, staging_root=f"{committer.delta_root}/staging"
    )
    assert report.blocking
    assert any("outer step membership" in item.detail for item in report.items)
    assert _inventory(committer) == before


@pytest.mark.parametrize("claim_first", [False, True])
def test_repair_preserves_cancellation_order(
    committer: RetainedCommitter,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    claim_first: bool,
) -> None:
    _, receipt = admit_case(committer, tmp_path, kind="alias")
    if claim_first:
        _interrupt(committer, receipt, monkeypatch)
    committer.journal.cancel(receipt.delivery.attempt)
    report = apply_retained_repair(
        committer, staging_root=f"{committer.delta_root}/staging"
    )
    assert not report.blocking
    state = committer.journal.state(receipt.delivery.attempt.assignment)
    assert state is not None
    assert state.cancelled
    assert (state.winner is not None) is claim_first
    assert (
        any(item.classification == "complete" for item in report.items) is claim_first
    )
    assert committer.fs.exists(receipt.seal_uri) is not claim_first


@pytest.mark.parametrize("boundary", ["plan", "planned", "terminal"])
def test_repair_finishes_exact_selected_step_terminal(
    committer: RetainedCommitter, tmp_path: Path, boundary: str
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias")
    )
    committer.accept(receipts[0])

    def checkpoint(event: str) -> None:
        if event == boundary:
            raise InterruptedError(boundary)

    results.checkpoint = checkpoint
    with pytest.raises(InterruptedError, match=boundary):
        results.finish(admission.step_run_id)
    report = apply_retained_repair(
        committer, staging_root=f"{committer.delta_root}/staging"
    )
    assert not report.unresolved
    assert (
        results.tracker.current_state(admission.step_run_id).status
        == StepStatus.SUCCEEDED
    )
    assert len(results.outputs(admission.step_run_id)[0].occurrences) == 2
    before = _inventory(committer)
    assert (
        apply_retained_repair(committer, staging_root=f"{committer.delta_root}/staging")
        == report
    )
    assert _inventory(committer) == before


@pytest.mark.parametrize("preserve", [False, True])
def test_repair_finishes_curator_selection_using_original_staged_evidence(
    committer: RetainedCommitter, raw_backend_fs, preserve: bool
) -> None:
    operation = DynamicCurator()
    results, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(
        results, admission, operation, storage=raw_backend_fs[1]
    )

    def checkpoint(event: str) -> None:
        if event == "plan":
            raise InterruptedError(event)

    results.checkpoint = checkpoint
    with pytest.raises(InterruptedError, match="plan"):
        results.finish(reference, staged.execution_run_id, terminal)
    report = apply_retained_repair(
        committer, staging_root=results.staging.staging_dir, preserve_staging=preserve
    )
    assert not report.unresolved
    assert (
        results.tracker.current_state(reference.step_run_id).status
        == StepStatus.SUCCEEDED
    )
    assert staged.staging_path is not None
    assert committer.fs.exists(f"{staged.staging_path}/executions.parquet") is preserve
    before = _inventory(committer)
    assert (
        apply_retained_repair(
            committer,
            staging_root=results.staging.staging_dir,
            preserve_staging=preserve,
        )
        == report
    )
    assert _inventory(committer) == before


def test_repair_recovers_raw_registration_without_source_file(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    source = tmp_path / "input.csv"
    source.write_text("value\n1\n")

    def checkpoint(event: str) -> None:
        if event == "plan":
            raise InterruptedError(event)

    with pytest.raises(InterruptedError, match="plan"):
        register_raw_inputs(
            committer,
            [str(source)],
            step_run_id="b" * 32,
            step_number=2,
            operation_name="ingest",
            storage=StorageConfig(),
            checkpoint=checkpoint,
        )
    source.unlink()
    report = apply_retained_repair(
        committer, staging_root=f"{committer.delta_root}/staging"
    )
    assert not report.unresolved
    assert (
        read_input_registration(committer.delta_root, "b" * 32, committer.fs)
        is not None
    )


def test_blocking_audit_prevents_recovery_and_cleanup_of_other_selected_results(
    committer: RetainedCommitter, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, first = admit_case(committer, tmp_path / "first", unit_index=0)
    plan = committer.accept(first, preserve_staging=True)
    _, second = admit_case(committer, tmp_path / "second", kind="alias", unit_index=1)
    _interrupt(committer, second, monkeypatch)
    committer.fs.pipe_file(
        f"{committer.files_root}/{plan.carriers[0].relative_path}", b"changed"
    )
    before = _inventory(committer)
    report = apply_retained_repair(
        committer, staging_root=f"{committer.delta_root}/staging"
    )
    assert report.blocking
    assert _inventory(committer) == before
    assert committer.fs.exists(first.seal_uri)
    assert committer.fs.exists(second.seal_uri)


def test_abandoned_selection_is_not_recovered_or_cleaned(
    committer: RetainedCommitter, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, receipt = admit_case(committer, tmp_path, kind="alias")
    _interrupt(committer, receipt, monkeypatch, event="planned")
    controls_path = f"{committer.delta_root}/{TablePath.LOGICAL_COMMITS.value}"
    controls = pl.read_delta(controls_path, storage_options=committer.storage_options)
    controls.with_columns(
        pl.lit("abandoned").alias("state"),
        pl.lit("retained for inspection").alias("abandon_reason"),
    ).write_delta(
        controls_path, mode="overwrite", storage_options=committer.storage_options
    )
    before = _inventory(committer)
    report = apply_retained_repair(
        committer, staging_root=f"{committer.delta_root}/staging"
    )
    assert [item.classification for item in report.items] == ["abandoned"]
    assert _inventory(committer) == before


def test_recovery_failure_reports_owner_and_stops_following_mutations(
    committer: RetainedCommitter, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    receipts = []
    for index in range(2):
        _, receipt = admit_case(
            committer, tmp_path / str(index), kind="alias", unit_index=index
        )
        _interrupt(committer, receipt, monkeypatch)
        receipts.append(receipt)
    selected = committer.journal.all_states()
    first = selected[0]
    assert first.winner is not None
    committer.fs.rm(first.winner.receipt.seal_uri)
    assert not inspect_retained_store(
        committer, staging_root=f"{committer.delta_root}/staging"
    ).blocking
    before = _inventory(committer)
    report = apply_retained_repair(
        committer, staging_root=f"{committer.delta_root}/staging"
    )
    assert report.blocking
    assert any(
        item.evidence_id == f"retained_result:{first.attempt.assignment.assignment_id}"
        and item.classification == "corrupt"
        for item in report.items
    )
    assert _inventory(committer) == before
