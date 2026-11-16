"""Run recovery honors existing selections and leaves unresolved work untouched."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fixtures.curator_cache import DynamicCurator, stage_observed
from fixtures.retained_acceptance import boundary_projection, open_committer
from fixtures.retained_curators import admit_curator
from fixtures.retained_readers import admit_read_case
from fixtures.retained_results import retained_case

from artisan.errors import StoreIntegrityError
from artisan.orchestration.engine.pipeline_recovery import recover_pipeline_results
from artisan.orchestration.engine.retained_step_results import RetainedStepResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.execution.assignment import (
    DeliveryIdentity,
    SealedDelivery,
    SuccessfulDelivery,
)
from artisan.schemas.execution.step_admission import StepAdmission
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.orchestration.step_state import StepState
from artisan.storage.io.admission import publish_admission
from artisan.storage.io.curator_plan import CuratorCommitPlan, read_curator_plan
from artisan.storage.io.input_registration import register_raw_inputs
from artisan.storage.io.input_registration_plan import read_input_registration
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_seal import stage_retained_result

RUN = "synthetic-pipeline"


@pytest.fixture
def committer(tmp_path: Path) -> RetainedCommitter:
    storage = StorageConfig()
    return open_committer(
        str(tmp_path / "store"), storage.filesystem(), storage, initialize=True
    )


def _record(step_run_id: str, *, name: str = "produce") -> StepStartRecord:
    return StepStartRecord(
        step_run_id=step_run_id,
        step_number=7,
        step_name=name,
        operation_class="synthetic.Operation",
        params_json="{}",
        input_refs_json="{}",
        compute_backend="local",
        compute_options_json="{}",
        output_roles_json='["data"]',
        output_types_json='{"data":"csv"}',
    )


def _scientific_step(
    committer: RetainedCommitter, tmp_path: Path, *, count: int = 1
) -> tuple[RetainedStepResults, tuple[SealedDelivery, ...]]:
    case = retained_case(tmp_path / "science", kind="alias")
    tracker = RetainedStepTracker(committer, RUN)
    record = _record("a" * 32).model_copy(
        update={
            "operation_class": (
                f"{case.recipe.definition.module}.{case.recipe.definition.qualname}"
            ),
            "params_json": case.recipe.params_json,
            "output_roles_json": json.dumps(
                sorted(spec.role for spec in case.recipe.outputs)
            ),
            "output_types_json": json.dumps(
                {spec.role: spec.artifact_type for spec in case.recipe.outputs}
            ),
        }
    )
    tracker.create_attempt(record)
    admitted = tuple(
        admit_read_case(
            committer, case, step_run_id=record.step_run_id, unit_index=index
        )
        for index in range(count)
    )
    tracker.admit(
        StepAdmission(
            step_run_id=record.step_run_id,
            step_number=7,
            recipe=case.recipe,
            software_digest="c" * 64,
            assignments=tuple(item[0] for item in admitted),
        )
    )
    return RetainedStepResults(tracker), tuple(item[1] for item in admitted)


def _recover(
    committer: RetainedCommitter, *, preserve_staging: bool = False
) -> tuple[StepState, ...]:
    return recover_pipeline_results(
        committer,
        pipeline_run_id=RUN,
        staging_root=f"{committer.delta_root}/staging",
        preserve_staging=preserve_staging,
    )


@pytest.mark.parametrize("cancelled", [False, True])
@pytest.mark.parametrize("preserve", [False, True])
def test_recover_claimed_delivery_leaves_unselected_step_and_orphan_untouched(
    committer: RetainedCommitter,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cancelled: bool,
    preserve: bool,
) -> None:
    results, receipts = _scientific_step(committer, tmp_path, count=2)
    if cancelled:
        committer.journal.cancel(receipts[1].delivery.attempt)

    def interrupt(event: str, _receipt: SealedDelivery, _table: str | None) -> None:
        if event == "claimed":
            raise InterruptedError(event)

    monkeypatch.setattr(committer, "_checkpoint", interrupt)
    with pytest.raises(InterruptedError, match="claim"):
        committer.accept(receipts[0])
    monkeypatch.undo()
    states = _recover(committer, preserve_staging=preserve)
    assert len(states) == 1
    assert states[0].status == StepStatus.RUNNING
    assert committer.accepted_projection(receipts[0]).exports
    second = committer.journal.state(receipts[1].delivery.attempt.assignment)
    assert second is not None
    assert second.winner is None
    assert second.reuse is None
    assert second.cancelled == cancelled
    assert results.tracker.current_state("a" * 32).status == StepStatus.RUNNING
    assert committer.fs.exists(receipts[1].seal_uri)
    assert committer.fs.exists(receipts[0].seal_uri) == preserve
    assert _recover(committer, preserve_staging=preserve) == states
    assert committer.fs.exists(receipts[0].seal_uri) == preserve


@pytest.mark.parametrize("boundary", ["plan", "terminal"])
def test_recover_selected_terminal_plan_after_controller_interruption(
    committer: RetainedCommitter, tmp_path: Path, boundary: str
) -> None:
    results, receipts = _scientific_step(committer, tmp_path)
    committer.accept(receipts[0])

    def interrupt(event: str) -> None:
        if event == boundary:
            raise InterruptedError(event)

    results.checkpoint = interrupt
    with pytest.raises(InterruptedError, match=boundary):
        results.finish("a" * 32)
    states = _recover(committer)
    assert states[0].status == StepStatus.SUCCEEDED
    assert len(results.outputs("a" * 32)[0].occurrences) == 2
    assert _recover(committer) == states


@pytest.mark.parametrize("step_number", [7, 8])
def test_recovery_checks_retry_origin_before_publishing_a_claimed_winner(
    committer: RetainedCommitter,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    step_number: int,
) -> None:
    _, receipts = _scientific_step(committer, tmp_path)
    case = retained_case(tmp_path / "science", kind="alias")
    attempt = receipts[0].delivery.attempt.model_copy(
        update={"attempt": 1, "generation": 1}
    )
    reference = publish_admission(
        attempt,
        case.recipe,
        boundary_projection(case),
        case.boundary,
        step_number=step_number,
        delta_root=committer.delta_root,
        files_root=committer.files_root,
        fs=committer.fs,
    )
    committer.journal.admit(attempt, admission_digest=reference.record_digest)
    receipt = stage_retained_result(
        f"{committer.delta_root}/staging",
        SuccessfulDelivery(
            delivery=DeliveryIdentity.new(attempt),
            computation_complete=True,
            cleanup_complete=True,
        ),
        case.projection,
        case.artifacts,
        fs=committer.fs,
    )

    def interrupt(event: str, _receipt: SealedDelivery, _table: str | None) -> None:
        if event == "claimed":
            raise InterruptedError(event)

    monkeypatch.setattr(committer, "_checkpoint", interrupt)
    with pytest.raises(InterruptedError, match="claimed"):
        committer.accept(receipt)
    monkeypatch.undo()
    if step_number == 8:
        with pytest.raises(StoreIntegrityError, match="outer step number"):
            _recover(committer)
        assert committer._controls().is_empty()
        assert committer.fs.exists(receipt.seal_uri)
    else:
        assert _recover(committer)[0].status == StepStatus.RUNNING
        assert committer.accepted_projection(receipt).exports


def test_recover_raw_registration_after_original_source_is_deleted(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    tracker = RetainedStepTracker(committer, RUN)
    tracker.create_attempt(_record("a" * 32, name="ingest"))
    source = tmp_path / "input.csv"
    source.write_text("value\n1\n")

    def interrupt(event: str) -> None:
        if event == "plan":
            raise InterruptedError(event)

    with pytest.raises(InterruptedError, match="plan"):
        register_raw_inputs(
            committer,
            [str(source)],
            step_run_id="a" * 32,
            step_number=7,
            operation_name="ingest",
            storage=StorageConfig(),
            checkpoint=interrupt,
        )
    source.unlink()
    states = _recover(committer)
    assert states[0].status == StepStatus.PENDING
    plan = read_input_registration(committer.delta_root, "a" * 32, committer.fs)
    assert plan is not None
    assert committer._controls()["state"].to_list() == ["complete"]
    assert _recover(committer) == states


@pytest.mark.parametrize("preserve", [False, True])
def test_recover_selected_curator_without_running_it_again(
    committer: RetainedCommitter, preserve: bool
) -> None:
    operation = DynamicCurator()
    results, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="b" * 32, software_digest="c" * 64
    )
    staged, result, _ = stage_observed(results, admission, operation)

    def interrupt(event: str) -> None:
        if event == "plan":
            raise InterruptedError(event)

    results.checkpoint = interrupt
    with pytest.raises(InterruptedError, match="plan"):
        results.finish(reference, staged.execution_run_id, result)
    plan = read_curator_plan(committer.delta_root, reference.step_run_id, committer.fs)
    assert isinstance(plan, CuratorCommitPlan)
    files = [
        f"{results.staging.staging_dir}/{file.relative_path}"
        for table in plan.effects.tables
        for file in table.files
    ]
    assert files
    assert all(committer.fs.exists(path) for path in files)
    states = recover_pipeline_results(
        committer,
        pipeline_run_id=RUN,
        staging_root=results.staging.staging_dir,
        preserve_staging=preserve,
    )
    assert states[0].status == StepStatus.SUCCEEDED
    assert all(committer.fs.exists(path) == preserve for path in files)
    assert (
        recover_pipeline_results(
            committer,
            pipeline_run_id=RUN,
            staging_root=results.staging.staging_dir,
            preserve_staging=preserve,
        )
        == states
    )
    assert all(committer.fs.exists(path) == preserve for path in files)


def test_recovery_rejects_registration_with_a_different_step_owner(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    RetainedStepTracker(committer, RUN).create_attempt(_record("a" * 32))
    source = tmp_path / "input.csv"
    source.write_text("value\n1\n")

    def interrupt(event: str) -> None:
        if event == "plan":
            raise InterruptedError(event)

    with pytest.raises(InterruptedError, match="plan"):
        register_raw_inputs(
            committer,
            [str(source)],
            step_run_id="a" * 32,
            step_number=7,
            operation_name="different_operation",
            storage=StorageConfig(),
            checkpoint=interrupt,
        )
    with pytest.raises(StoreIntegrityError, match="pipeline step owner"):
        _recover(committer)
    assert committer._controls().is_empty()


def test_recovery_preserves_older_unresolved_attempts_and_ignores_other_runs(
    committer: RetainedCommitter,
) -> None:
    tracker = RetainedStepTracker(committer, RUN)
    tracker.create_attempt(_record("a" * 32))
    tracker.record_cancellation(
        "a" * 32,
        StepStatus.PENDING,
        CancellationAcknowledgement(CancellationStatus.REQUESTED),
    )
    tracker.record_cancellation(
        "a" * 32,
        StepStatus.PENDING,
        CancellationAcknowledgement(CancellationStatus.UNKNOWN),
    )
    tracker.create_attempt(_record("b" * 32))
    RetainedStepTracker(committer, "other-pipeline").create_attempt(_record("c" * 32))
    states = _recover(committer)
    assert {state.step_run_id for state in states} == {"a" * 32, "b" * 32}
    assert all(state.status == StepStatus.PENDING for state in states)
    assert (
        tracker.current_state("a" * 32).cancellation_status
        == CancellationStatus.UNKNOWN
    )


@pytest.mark.parametrize("run", ["", "missing"])
def test_recovery_requires_an_existing_explicit_run(
    committer: RetainedCommitter, run: str
) -> None:
    with pytest.raises(ValueError):
        recover_pipeline_results(
            committer,
            pipeline_run_id=run,
            staging_root=f"{committer.delta_root}/staging",
        )
