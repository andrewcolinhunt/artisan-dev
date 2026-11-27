"""Offline inspection cannot claim results, retry work or clean staged evidence."""

from __future__ import annotations

import hashlib
from pathlib import Path

import polars as pl
import pytest
from fixtures.candidate_outputs import prepare_scientific
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_acceptance import admit_case
from fixtures.retained_curators import admit_curator
from fixtures.retained_results import retained_case

from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.enums import CachePolicy, TablePath
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.core.assignment_schema import ASSIGNMENT_DECISIONS_PATH
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.io.curator_plan import (
    CuratorCommitPlan,
    curator_plan_digest,
    curator_plan_path,
    read_curator_plan,
)
from artisan.storage.io.input_registration import register_raw_inputs
from artisan.storage.io.repair_report import RepairReport
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_repair import inspect_retained_store
from artisan.storage.io.retained_store import create_retained_store
from artisan.storage.io.step_result_plan import (
    StepUnitFailure,
    read_step_result_plan,
    step_result_digest,
    step_result_directory,
)
from artisan.utils.hashing import canonical_json_bytes


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


def _inspect(committer: RetainedCommitter) -> RepairReport:
    return inspect_retained_store(
        committer, staging_root=f"{committer.delta_root}/staging"
    )


def _interrupt(
    committer: RetainedCommitter,
    receipt: SealedDelivery,
    boundary: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def checkpoint(event: str, _receipt: SealedDelivery, _table: str | None) -> None:
        if event == boundary:
            raise InterruptedError(boundary)

    with monkeypatch.context() as patch:
        patch.setattr(committer, "_checkpoint", checkpoint)
        with pytest.raises(InterruptedError, match=boundary):
            committer.accept(receipt, preserve_staging=True)


def test_empty_store_inventory_is_read_only(committer: RetainedCommitter) -> None:
    before = _inventory(committer)
    assert not _inspect(committer).items
    assert _inventory(committer) == before


@pytest.mark.parametrize("preserve", [False, True])
def test_completed_effects_and_carriers_are_verified_without_cleanup(
    committer: RetainedCommitter, tmp_path: Path, preserve: bool
) -> None:
    _, receipt = admit_case(committer, tmp_path / "worker")
    plan = committer.accept(receipt, preserve_staging=preserve)
    before = _inventory(committer)
    report = _inspect(committer)
    assert [(item.evidence_id, item.classification) for item in report.items] == [
        (plan.logical_commit_id, "complete")
    ]
    assert not report.blocking
    assert not report.unresolved
    assert _inventory(committer) == before


@pytest.mark.parametrize(
    "boundary", ["claimed", "plan", "planned", "table", "complete"]
)
def test_interrupted_selection_is_not_finished_by_inspection(
    committer: RetainedCommitter,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    _, receipt = admit_case(committer, tmp_path / "worker", kind="alias")
    _interrupt(committer, receipt, boundary, monkeypatch)
    before = _inventory(committer)
    report = _inspect(committer)
    assert not report.blocking
    assert [item.classification for item in report.items] == [
        "complete" if boundary == "complete" else "incomplete"
    ]
    assert _inventory(committer) == before
    assert committer.fs.exists(receipt.seal_uri)
    committer.recover(receipt.delivery.attempt.assignment)
    assert [item.classification for item in _inspect(committer).items] == ["complete"]


@pytest.mark.parametrize("cancelled", [False, True])
def test_unclaimed_seal_stays_diagnostic_and_cannot_authorize_retry(
    committer: RetainedCommitter, tmp_path: Path, cancelled: bool
) -> None:
    _, receipt = admit_case(committer, tmp_path / "worker", kind="alias")
    if cancelled:
        committer.journal.cancel(receipt.delivery.attempt)
    before = _inventory(committer)
    report = _inspect(committer)
    assert sorted(item.classification for item in report.items) == sorted(
        ["ineligible" if cancelled else "incomplete", "unplanned"]
    )
    assert not report.blocking
    assert _inventory(committer) == before
    state = committer.journal.state(receipt.delivery.attempt.assignment)
    assert state is not None
    assert state.winner is None


def test_cancellation_does_not_revoke_an_earlier_claim(
    committer: RetainedCommitter, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, receipt = admit_case(committer, tmp_path / "worker", kind="alias")
    _interrupt(committer, receipt, "claimed", monkeypatch)
    committer.journal.cancel(receipt.delivery.attempt)
    assert [item.classification for item in _inspect(committer).items] == ["incomplete"]
    committer.recover(receipt.delivery.attempt.assignment)
    assert [item.classification for item in _inspect(committer).items] == ["complete"]


@pytest.mark.parametrize("damage", ["carrier", "plan", "table-owner", "journal-key"])
def test_corrupt_completed_evidence_blocks_repair_without_mutation(
    committer: RetainedCommitter, tmp_path: Path, damage: str
) -> None:
    _, receipt = admit_case(committer, tmp_path / "worker")
    plan = committer.accept(receipt, preserve_staging=True)
    if damage == "carrier":
        committer.fs.pipe_file(
            f"{committer.files_root}/{plan.carriers[0].relative_path}", b"changed"
        )
    elif damage == "plan":
        committer.fs.rm(
            f"{committer.delta_root}/_artisan/retained-commits/{receipt.delivery.attempt.assignment.assignment_id}/plan.json"
        )
    else:
        table_path = (
            ASSIGNMENT_DECISIONS_PATH
            if damage == "journal-key"
            else TablePath.ARTIFACT_INDEX.value
        )
        path = f"{committer.delta_root}/{table_path}"
        frame = pl.read_delta(path, storage_options=committer.storage_options)
        frame = frame.with_columns(
            pl.lit("f" * 32).alias(
                "assignment_id" if damage == "journal-key" else "logical_commit_id"
            )
        )
        frame.write_delta(
            path, mode="overwrite", storage_options=committer.storage_options
        )
    before = _inventory(committer)
    report = _inspect(committer)
    assert report.blocking
    assert any(item.classification == "corrupt" for item in report.items)
    assert _inventory(committer) == before


def test_partial_and_whole_step_fences_are_resolved_by_exact_terminal_plans(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    case = retained_case(tmp_path, kind="alias")
    results, admission, receipts = prepare_scientific(committer, case, count=2)
    committer.accept(receipts[0])
    results.finish(
        admission.step_run_id, failures={1: StepUnitFailure(error="tool failed")}
    )
    current, selected, _ = prepare_scientific(
        committer, case, step_run_id="b" * 32, count=2
    )
    hit = current.lookup(selected.step_run_id, CachePolicy.STEP_COMPLETED)
    assert hit is not None
    current.reuse(hit)
    report = _inspect(committer)
    assert not report.blocking
    assert all(
        item.classification in {"complete", "unplanned"} for item in report.items
    )
    assert (
        len([item for item in report.items if item.classification == "complete"]) == 3
    )


def test_curator_execution_and_reuse_are_included_in_complete_inventory(
    committer: RetainedCommitter,
    raw_backend_fs,
) -> None:
    operation = DynamicCurator()
    results, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(
        results, admission, operation, storage=raw_backend_fs[1]
    )
    results.finish(reference, staged.execution_run_id, terminal)
    _, _, current = admit_curator(
        committer, operation, {}, step_run_id="b" * 32, step_number=9, **STAMPS
    )
    hit = lookup_curator(current, reader=CuratorReader(committer))
    assert hit is not None
    results.reuse(hit)
    before = _inventory(committer)
    report = _inspect(committer)
    assert [item.classification for item in report.items] == ["complete", "complete"]
    assert _inventory(committer) == before


def test_raw_registration_is_audited_without_reopening_original_input(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    source = tmp_path / "original.csv"
    source.write_text("value\n1\n")
    plan = register_raw_inputs(
        committer,
        [str(source)],
        step_run_id="c" * 32,
        step_number=2,
        operation_name="ingest",
        storage=StorageConfig(),
    )
    source.unlink()
    report = _inspect(committer)
    assert [(item.evidence_id, item.classification) for item in report.items] == [
        (plan.logical_commit_id, "complete")
    ]


@pytest.mark.parametrize("status", [StepStatus.PENDING, StepStatus.RUNNING])
def test_unadmitted_unfinished_steps_are_not_reported_as_clean(
    committer: RetainedCommitter, status: StepStatus
) -> None:
    tracker = RetainedStepTracker(committer, "synthetic-run")
    record = StepStartRecord(
        step_run_id="a" * 32,
        step_number=1,
        step_name="produce",
        operation_class="synthetic.Operation",
        params_json="{}",
        input_refs_json="{}",
        compute_backend="local",
        compute_options_json="{}",
        output_roles_json="[]",
        output_types_json="{}",
    )
    tracker.create_attempt(record)
    if status == StepStatus.RUNNING:
        tracker.transition(record.step_run_id, StepStatus.PENDING, StepStatus.RUNNING)
        tracker.record_cancellation(
            record.step_run_id,
            StepStatus.RUNNING,
            CancellationAcknowledgement(CancellationStatus.REQUESTED),
        )
        tracker.record_cancellation(
            record.step_run_id,
            StepStatus.RUNNING,
            CancellationAcknowledgement(CancellationStatus.UNKNOWN),
        )
    tracker.create_attempt(record.model_copy(update={"step_run_id": "b" * 32}))
    before = _inventory(committer)
    report = _inspect(committer)
    assert report.unresolved
    assert len(report.items) == 2
    assert {item.classification for item in report.items} == (
        {"incomplete", "unknown_owner"}
        if status == StepStatus.RUNNING
        else {"incomplete"}
    )
    assert _inventory(committer) == before


@pytest.mark.parametrize("selected", [False, True])
def test_curator_seal_inventory_distinguishes_selected_from_unclaimed(
    committer: RetainedCommitter, raw_backend_fs, selected: bool
) -> None:
    operation = DynamicCurator()
    results, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(
        results, admission, operation, storage=raw_backend_fs[1]
    )
    results.preserve_staging = True
    if selected:
        results.finish(reference, staged.execution_run_id, terminal)
    before = _inventory(committer)
    report = inspect_retained_store(committer, staging_root=results.staging.staging_dir)
    assert sorted(item.classification for item in report.items) == (
        ["complete"] if selected else ["incomplete", "unplanned"]
    )
    assert _inventory(committer) == before


def test_malformed_lifecycle_history_is_reported_as_corrupt(
    committer: RetainedCommitter,
) -> None:
    tracker = RetainedStepTracker(committer, "synthetic-run")
    tracker.create_attempt(
        StepStartRecord(
            step_run_id="a" * 32,
            step_number=1,
            step_name="produce",
            operation_class="synthetic.Operation",
            params_json="{}",
            input_refs_json="{}",
            compute_backend="local",
            compute_options_json="{}",
            output_roles_json="[]",
            output_types_json="{}",
        )
    )
    path = f"{committer.delta_root}/{TablePath.STEPS.value}"
    rows = pl.read_delta(path, storage_options=committer.storage_options)
    rows.with_columns(pl.lit(7).cast(pl.UInt32).alias("state_sequence")).write_delta(
        path, mode="overwrite", storage_options=committer.storage_options
    )
    before = _inventory(committer)
    report = _inspect(committer)
    assert report.blocking
    assert any(item.classification == "corrupt" for item in report.items)
    assert _inventory(committer) == before


@pytest.mark.parametrize("kind", ["curator", "scientific"])
def test_complete_plan_metadata_must_match_its_exact_terminal_and_sealed_rows(
    committer: RetainedCommitter, raw_backend_fs, tmp_path: Path, kind: str
) -> None:
    step_run_id = "a" * 32
    if kind == "curator":
        operation = DynamicCurator()
        results, admission, reference = admit_curator(
            committer, operation, {}, step_run_id=step_run_id, **STAMPS
        )
        staged, terminal, _ = stage_observed(
            results, admission, operation, storage=raw_backend_fs[1]
        )
        results.finish(reference, staged.execution_run_id, terminal)
        original = read_curator_plan(committer.delta_root, step_run_id, committer.fs)
        assert isinstance(original, CuratorCommitPlan)
        payload = original.model_dump(mode="json", exclude={"plan_digest"})
        payload["cache_proof"] = None
        digest = curator_plan_digest(payload)
        plan_path = curator_plan_path(committer.delta_root, step_run_id)
    else:
        results, admission, receipts = prepare_scientific(
            committer, retained_case(tmp_path, kind="alias")
        )
        committer.accept(receipts[0])
        results.finish(step_run_id)
        original = read_step_result_plan(
            committer.delta_root, step_run_id, committer.fs
        )
        assert original is not None
        payload = original.model_dump(mode="json", exclude={"plan_digest"})
        payload["result"]["metadata"] = {"changed": True}
        digest = step_result_digest(payload)
        plan_path = (
            f"{step_result_directory(committer.delta_root, step_run_id)}/plan.json"
        )
    committer.fs.pipe_file(
        plan_path, canonical_json_bytes({**payload, "plan_digest": digest})
    )
    controls_path = f"{committer.delta_root}/{TablePath.LOGICAL_COMMITS.value}"
    controls = pl.read_delta(controls_path, storage_options=committer.storage_options)
    controls.with_columns(
        pl.when(pl.col("logical_commit_id") == f"step_result:{step_run_id}")
        .then(pl.lit(digest))
        .otherwise(pl.col("plan_digest"))
        .alias("plan_digest")
    ).write_delta(
        controls_path, mode="overwrite", storage_options=committer.storage_options
    )
    before = _inventory(committer)
    report = _inspect(committer)
    assert report.blocking
    assert any(
        item.classification == "corrupt"
        and item.evidence_id == f"step_result:{step_run_id}"
        for item in report.items
    )
    assert _inventory(committer) == before


def test_public_inspection_does_not_modify_supported_store(
    committer: RetainedCommitter,
) -> None:
    from artisan.storage.io.repair import repair_store

    before = _inventory(committer)
    report = repair_store(
        delta_root=committer.delta_root,
        files_root=committer.files_root,
        staging_root=f"{committer.delta_root}/staging",
        fs=committer.fs,
        storage_options=committer.storage_options,
    )
    assert not report.items
    assert _inventory(committer) == before


@pytest.mark.parametrize("unsupported", ["abandon", "reason", "recover_staging"])
def test_removed_repair_actions_reject_before_storage_access(
    unsupported: str,
    tmp_path: Path,
) -> None:
    from unittest.mock import Mock

    from artisan.storage.io.repair import repair_store

    fs = Mock()
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        repair_store(
            delta_root=str(tmp_path / "store"),
            staging_root=str(tmp_path / "staging"),
            fs=fs,
            **{unsupported: "unsupported"},
        )
    assert not fs.mock_calls
