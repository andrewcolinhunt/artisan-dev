"""Tests for report-first offline logical-commit repair."""

from __future__ import annotations

import json

import polars as pl
import pytest
from deltalake import DeltaTable
from fixtures.store_format import publish_test_store

from artisan.errors import StoreIntegrityError
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.committed_scan import read_committed, read_logical_commits
from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA, STEPS_SCHEMA
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.commit_plan import (
    CommitPlan,
    build_commit_plan,
    commit_plan_path,
)
from artisan.storage.io.repair import repair_store
from artisan.storage.io.staging import StagingManager


@pytest.fixture
def repair_env(backend_fs):
    fs, storage, root = backend_fs
    delta_root = f"{root}/repair_delta"
    staging_root = f"{root}/repair_staging"
    options = storage.delta_storage_options()
    publish_test_store(delta_root, fs, options)
    committer = DeltaCommitter(
        delta_root,
        StagingManager(staging_root, fs),
        fs=fs,
        storage_options=options,
    )
    return committer, fs, options, delta_root, staging_root


def _plan(committer: DeltaCommitter, step_run_id: str = "a" * 32) -> CommitPlan:
    frame = pl.DataFrame(
        {
            "artifact_id": [step_run_id],
            "artifact_type": ["metric"],
            "origin_step_number": [0],
            "metadata": ["{}"],
        },
        schema=ARTIFACT_INDEX_SCHEMA,
    )
    committer.staging_manager.stage_orchestrator_dataframe(
        frame,
        TablePath.ARTIFACT_INDEX.value,
        commit_kind="input_registration",
        step_run_id=step_run_id,
        step_number=0,
        operation_name="repair",
    )
    return build_commit_plan(
        delta_root=committer.delta_base_path,
        staging_root=committer.staging_manager.staging_dir,
        fs=committer._fs,
        commit_kind="input_registration",
        step_run_id=step_run_id,
        step_number=0,
        operation_name="repair",
    )


def _leave_planned(committer: DeltaCommitter, plan: CommitPlan, monkeypatch) -> None:
    class SimulatedCrash(BaseException):
        pass

    def crash(boundary, _plan, _table):
        if boundary == "planned":
            raise SimulatedCrash

    monkeypatch.setattr(committer, "_checkpoint", crash)
    with pytest.raises(SimulatedCrash):
        committer.commit_logical(plan)
    monkeypatch.setattr(committer, "_checkpoint", lambda *_: None)


def _planned_terminal(repair_env, monkeypatch):
    committer, fs, options, delta_root, _ = repair_env
    tracker = StepTracker(delta_root, "run", storage_options=options, fs=fs)
    tracker.create_attempt(
        StepStartRecord(
            step_run_id="c" * 32,
            step_spec_id="d" * 32,
            step_number=1,
            step_name="repair_step",
            operation_class="tests.RepairStep",
            params_json="{}",
            input_refs_json="{}",
            compute_backend="local",
            compute_options_json="{}",
            output_roles_json='["metric"]',
            output_types_json='{"metric":"metric"}',
        )
    )
    tracker.transition("c" * 32, StepStatus.PENDING, StepStatus.RUNNING)
    result = StepResult(
        step_name="repair_step",
        step_number=1,
        step_run_id="c" * 32,
        status=StepStatus.SUCCEEDED,
        disposition=StepDisposition.EXECUTED,
        total_count=1,
        succeeded_count=1,
        output_roles=frozenset({"metric"}),
        output_types={"metric": "metric"},
    )
    candidate = tracker.prepare_terminal_candidate(
        "c" * 32,
        StepStatus.RUNNING,
        StepStatus.SUCCEEDED,
        step_spec_id="d" * 32,
        result=result,
    )
    committer.staging_manager.stage_orchestrator_dataframe(
        candidate,
        TablePath.STEPS.value,
        commit_kind="step_result",
        step_run_id="c" * 32,
        step_number=1,
        operation_name="repair_step",
    )
    plan = build_commit_plan(
        delta_root=delta_root,
        staging_root=committer.staging_manager.staging_dir,
        fs=fs,
        commit_kind="step_result",
        step_run_id="c" * 32,
        step_number=1,
        operation_name="repair_step",
    )

    class SimulatedCrash(BaseException):
        pass

    def crash(boundary, _plan, table):
        if boundary == "table" and table == TablePath.STEPS.value:
            raise SimulatedCrash

    monkeypatch.setattr(committer, "_checkpoint", crash)
    with pytest.raises(SimulatedCrash):
        committer.commit_logical(plan)
    monkeypatch.setattr(committer, "_checkpoint", lambda *_: None)
    return tracker, plan


def _repair(repair_env, **kwargs):
    _, fs, options, delta_root, staging_root = repair_env
    return repair_store(
        delta_root=delta_root,
        staging_root=staging_root,
        fs=fs,
        storage_options=options,
        **kwargs,
    )


def test_report_only_does_not_mutate_either_root(repair_env):
    committer, fs, options, delta_root, staging_root = repair_env
    plan = _plan(committer)
    versions = {
        table.value: DeltaTable(
            f"{delta_root}/{table.value}", storage_options=options
        ).version()
        for table in TablePath
    }
    staged_before = sorted(fs.find(staging_root))

    report = _repair(repair_env)

    assert [(item.evidence_id, item.classification) for item in report.items] == [
        (plan.logical_commit_id, "unplanned")
    ]
    assert staged_before == sorted(fs.find(staging_root))
    assert versions == {
        table.value: DeltaTable(
            f"{delta_root}/{table.value}", storage_options=options
        ).version()
        for table in TablePath
    }


def test_apply_replays_only_validated_planned_evidence(
    repair_env,
    monkeypatch,
):
    committer, fs, options, delta_root, _ = repair_env
    replayable = _plan(committer, "a" * 32)
    _leave_planned(committer, replayable, monkeypatch)
    unplanned = _plan(committer, "b" * 32)

    before = _repair(repair_env)
    assert {item.evidence_id: item.classification for item in before.items} == {
        replayable.logical_commit_id: "replayable",
        unplanned.logical_commit_id: "unplanned",
    }

    after = _repair(repair_env, apply=True)

    assert {item.evidence_id: item.classification for item in after.items} == {
        replayable.logical_commit_id: "complete",
        unplanned.logical_commit_id: "unplanned",
    }
    assert read_committed(
        delta_root,
        TablePath.ARTIFACT_INDEX,
        fs=fs,
        storage_options=options,
    )["artifact_id"].to_list() == ["a" * 32]


def test_failed_attempt_cannot_be_revived_by_repair(
    repair_env,
    monkeypatch,
):
    tracker, plan = _planned_terminal(repair_env, monkeypatch)
    failure = StepResult(
        step_name="repair_step",
        step_number=1,
        step_run_id=plan.step_run_id,
        status=StepStatus.FAILED,
        error="logical commit failed",
    )
    tracker.transition(
        plan.step_run_id,
        StepStatus.RUNNING,
        StepStatus.FAILED,
        result=failure,
        error=failure.error,
    )

    before = _repair(repair_env)
    after = _repair(repair_env, apply=True)

    assert before == after
    assert before.items[0].classification == "conflict"
    assert "already failed" in before.items[0].detail
    assert tracker.current_state(plan.step_run_id).status is StepStatus.FAILED
    assert read_logical_commits(
        repair_env[3],
        fs=repair_env[1],
        storage_options=repair_env[2],
    )["state"].to_list() == ["planned"]


def test_apply_cleans_completed_plan_staging_exactly(repair_env):
    committer, fs, _, _, staging_root = repair_env
    plan = _plan(committer)
    committer.commit_logical(plan, preserve_staging=True)
    directory = (
        f"{staging_root}/0_repair/_orchestrator/{plan.step_run_id}/input_registration"
    )
    assert fs.exists(directory)

    report = _repair(repair_env, apply=True)

    assert report.items[0].classification == "complete"
    assert not fs.exists(directory)


def test_missing_plan_is_corrupt_and_never_reconstructed(
    repair_env,
    monkeypatch,
):
    committer, fs, _, _, _ = repair_env
    plan = _plan(committer)
    _leave_planned(committer, plan, monkeypatch)
    fs.rm(
        commit_plan_path(
            committer.delta_base_path,
            plan.step_run_id,
            plan.commit_kind,
        )
    )

    report = _repair(repair_env)

    assert {item.classification for item in report.items} == {"corrupt", "unplanned"}
    assert report.items[0].evidence_id == plan.logical_commit_id
    assert _repair(repair_env, apply=True) == report


def test_tampered_plan_is_corrupt(repair_env, monkeypatch):
    committer, fs, _, _, _ = repair_env
    plan = _plan(committer)
    _leave_planned(committer, plan, monkeypatch)
    path = commit_plan_path(
        committer.delta_base_path,
        plan.step_run_id,
        plan.commit_kind,
    )
    with fs.open(path, "r") as stream:
        payload = json.load(stream)
    payload["plan_digest"] = "0" * 32
    with fs.open(path, "w") as stream:
        json.dump(payload, stream)

    report = _repair(repair_env)

    assert {item.classification for item in report.items} == {"corrupt", "unplanned"}
    assert report.items[0].evidence_id == plan.logical_commit_id


def test_control_plan_disagreement_is_conflict(repair_env, monkeypatch):
    committer, _, options, delta_root, _ = repair_env
    plan = _plan(committer)
    _leave_planned(committer, plan, monkeypatch)
    DeltaTable(
        f"{delta_root}/{TablePath.LOGICAL_COMMITS.value}",
        storage_options=options,
    ).update(
        predicate=f"logical_commit_id = '{plan.logical_commit_id}'",
        updates={"plan_digest": f"'{'0' * 32}'"},
    )

    report = _repair(repair_env)

    assert report.items[0].classification == "conflict"


def test_unplanned_staging_without_plan_is_reported(repair_env):
    _, fs, _, _, staging_root = repair_env
    path = f"{staging_root}/unknown/data.parquet"
    fs.makedirs(f"{staging_root}/unknown", exist_ok=True)
    with fs.open(path, "wb") as stream:
        pl.DataFrame({"value": [1]}).write_parquet(stream)

    report = _repair(repair_env)

    assert [(item.evidence_id, item.classification) for item in report.items] == [
        ("staging:unknown/data.parquet", "unplanned")
    ]


def test_legacy_store_is_reported_but_cannot_be_mutated(tmp_path):
    from fsspec.implementations.local import LocalFileSystem

    fs = LocalFileSystem()
    delta_root = str(tmp_path / "legacy")
    staging_root = str(tmp_path / "staging")

    report = repair_store(
        delta_root=delta_root,
        staging_root=staging_root,
        fs=fs,
    )

    assert report.items[0].classification == "legacy_unverifiable"
    with pytest.raises(Exception, match="missing manifest"):
        repair_store(
            delta_root=delta_root,
            staging_root=staging_root,
            fs=fs,
            apply=True,
        )


def test_abandonment_is_one_way_idempotent_and_reason_exact(
    repair_env,
    monkeypatch,
):
    committer, fs, options, delta_root, staging_root = repair_env
    plan = _plan(committer)
    _leave_planned(committer, plan, monkeypatch)

    first = _repair(
        repair_env,
        abandon=plan.logical_commit_id,
        reason="staging can't be restored",
    )
    second = _repair(
        repair_env,
        abandon=plan.logical_commit_id,
        reason="staging can't be restored",
    )

    assert first.items[0].classification == "abandoned"
    assert second == first
    with pytest.raises(StoreIntegrityError, match="different reason"):
        _repair(
            repair_env,
            abandon=plan.logical_commit_id,
            reason="another reason",
        )
    with pytest.raises(StoreIntegrityError, match="abandoned"):
        committer.commit_logical(plan)
    assert read_committed(
        delta_root,
        TablePath.ARTIFACT_INDEX,
        fs=fs,
        storage_options=options,
    ).is_empty()
    assert fs.exists(
        f"{staging_root}/0_repair/_orchestrator/{plan.step_run_id}/input_registration"
    )
    assert (
        read_logical_commits(
            delta_root,
            fs=fs,
            storage_options=options,
        ).row(0, named=True)["abandon_reason"]
        == "staging can't be restored"
    )


def test_corrupt_staging_can_be_abandoned_without_erasing_evidence(
    repair_env,
    monkeypatch,
):
    committer, fs, _, _, staging_root = repair_env
    plan = _plan(committer)
    _leave_planned(committer, plan, monkeypatch)
    staged_path = f"{staging_root}/{plan.tables[0].files[0].relative_path}"
    with fs.open(staged_path, "wb") as stream:
        stream.write(b"corrupt parquet evidence")

    before = _repair(repair_env)
    after = _repair(
        repair_env,
        abandon=plan.logical_commit_id,
        reason="staged bytes cannot be restored",
    )

    assert before.items[0].classification == "corrupt"
    assert after.items[0].classification == "abandoned"
    with fs.open(staged_path, "rb") as stream:
        assert stream.read() == b"corrupt parquet evidence"


def test_conflicting_rows_can_be_abandoned_when_plan_identity_matches(
    repair_env,
    monkeypatch,
):
    committer, _, options, delta_root, _ = repair_env
    plan = _plan(committer)
    _leave_planned(committer, plan, monkeypatch)
    pl.DataFrame(
        {
            "artifact_id": [plan.step_run_id],
            "artifact_type": ["data"],
            "origin_step_number": [0],
            "metadata": ["{}"],
            "logical_commit_id": ["input_registration:" + "f" * 32],
        },
        schema={
            **ARTIFACT_INDEX_SCHEMA,
            "logical_commit_id": pl.String,
        },
    ).write_delta(
        f"{delta_root}/{TablePath.ARTIFACT_INDEX.value}",
        mode="append",
        storage_options=options,
    )

    before = _repair(repair_env)
    after = _repair(
        repair_env,
        abandon=plan.logical_commit_id,
        reason="conflicting physical row requires investigation",
    )

    assert before.items[0].classification == "conflict"
    assert after.items[0].classification == "abandoned"


def test_complete_commit_cannot_be_abandoned(repair_env):
    committer, *_ = repair_env
    plan = _plan(committer)
    committer.commit_logical(plan)

    with pytest.raises(StoreIntegrityError, match="Cannot abandon complete"):
        _repair(
            repair_env,
            abandon=plan.logical_commit_id,
            reason="too late",
        )


def test_abandonment_records_guarded_failed_reconciliation(
    repair_env,
    monkeypatch,
):
    tracker, plan = _planned_terminal(repair_env, monkeypatch)
    assert tracker.current_state(plan.step_run_id).status is StepStatus.RUNNING

    _repair(
        repair_env,
        abandon=plan.logical_commit_id,
        reason="operator chose not to replay",
    )
    first = tracker.current_state(plan.step_run_id)
    _repair(
        repair_env,
        abandon=plan.logical_commit_id,
        reason="operator chose not to replay",
    )
    second = tracker.current_state(plan.step_run_id)

    assert first.status is StepStatus.FAILED
    assert first.state_sequence == 3
    assert first.error == (
        f"Logical commit {plan.logical_commit_id} abandoned: "
        "operator chose not to replay"
    )
    assert second == first


def test_abandonment_rejects_nonrunning_attempt_before_control_mutation(
    repair_env,
):
    committer, fs, options, delta_root, _ = repair_env
    tracker = StepTracker(delta_root, "run", storage_options=options, fs=fs)
    step_run_id = "e" * 32
    tracker.create_attempt(
        StepStartRecord(
            step_run_id=step_run_id,
            step_spec_id="f" * 32,
            step_number=2,
            step_name="pending_step",
            operation_class="tests.PendingStep",
            params_json="{}",
            input_refs_json="{}",
            compute_backend="local",
            compute_options_json="{}",
            output_roles_json="[]",
            output_types_json="{}",
        )
    )
    candidate = tracker._state_to_row(tracker.current_state(step_run_id))
    candidate.update(status="failed", state_sequence=1, error="candidate")
    committer.staging_manager.stage_orchestrator_dataframe(
        pl.DataFrame([candidate], schema=STEPS_SCHEMA),
        TablePath.STEPS.value,
        commit_kind="step_result",
        step_run_id=step_run_id,
        step_number=2,
        operation_name="pending_step",
    )
    plan = build_commit_plan(
        delta_root=delta_root,
        staging_root=committer.staging_manager.staging_dir,
        fs=fs,
        commit_kind="step_result",
        step_run_id=step_run_id,
        step_number=2,
        operation_name="pending_step",
    )
    committer._insert_planned(plan)

    with pytest.raises(StoreIntegrityError, match="not reached running"):
        _repair(
            repair_env,
            abandon=plan.logical_commit_id,
            reason="must not skip the running transition",
        )

    assert tracker.current_state(step_run_id).status is StepStatus.PENDING
    assert read_logical_commits(
        delta_root,
        fs=fs,
        storage_options=options,
    )["state"].to_list() == ["planned"]


@pytest.mark.parametrize(
    ("apply", "abandon", "reason", "match"),
    [
        (True, "input_registration:" + "a" * 32, "reason", "separate"),
        (False, "input_registration:" + "a" * 32, None, "requires"),
        (False, None, "reason", "only"),
    ],
)
def test_repair_action_validation(
    repair_env,
    apply,
    abandon,
    reason,
    match,
):
    with pytest.raises(ValueError, match=match):
        _repair(
            repair_env,
            apply=apply,
            abandon=abandon,
            reason=reason,
        )
