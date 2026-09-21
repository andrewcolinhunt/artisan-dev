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
        (plan.logical_commit_id, "replayable")
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
        unplanned.logical_commit_id: "replayable",
    }

    after = _repair(repair_env, apply=True)

    assert {item.evidence_id: item.classification for item in after.items} == {
        replayable.logical_commit_id: "complete",
        unplanned.logical_commit_id: "complete",
    }
    assert read_committed(
        delta_root,
        TablePath.ARTIFACT_INDEX,
        fs=fs,
        storage_options=options,
    )["artifact_id"].sort().to_list() == ["a" * 32, "b" * 32]


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


def _worker_candidate(
    repair_env,
    *,
    execution_id="e" * 32,
    status="running",
    empty=False,
    success=True,
    diagnostic=False,
    owner=True,
    artifact=None,
):
    """Stage one real typed payload with a closed worker seal and original owner."""
    from datetime import UTC, datetime

    from fixtures.execution_records import executions_df

    from artisan.execution.recording.parquet_writer import _stage_artifacts
    from artisan.execution.recording.recorder import build_execution_edges
    from artisan.schemas.artifact.data import DataArtifact
    from artisan.schemas.execution.command_record import CommandRecording
    from artisan.schemas.execution.replay import ReplaySnapshot
    from artisan.schemas.orchestration.step_lifecycle import (
        CancellationAcknowledgement,
        CancellationStatus,
    )
    from artisan.storage.io.worker_seal import (
        STAGING_INVENTORY_KEY,
        build_staging_inventory,
    )
    from artisan.utils.path import shard_uri

    _, fs, options, delta_root, staging_root = repair_env
    step_id = "c" * 32
    tracker = StepTracker(delta_root, "recovery-source", fs=fs, storage_options=options)
    if owner and not tracker.load_all_current_states():
        tracker.create_attempt(
            StepStartRecord(
                step_run_id=step_id,
                step_spec_id="d" * 32,
                step_number=0,
                step_name="user-label",
                operation_class="tests.Worker",
                params_json="{}",
                input_refs_json="{}",
                compute_backend="local",
                compute_options_json="{}",
                output_roles_json='["output"]',
                output_types_json=json.dumps(
                    {
                        "output": artifact.artifact_type
                        if artifact is not None
                        else "data"
                    }
                ),
            )
        )
        if status != "pending":
            tracker.transition(step_id, StepStatus.PENDING, StepStatus.RUNNING)
        if status == "failed":
            tracker.transition(
                step_id,
                StepStatus.RUNNING,
                StepStatus.FAILED,
                result=StepResult(
                    step_name="user-label",
                    step_number=0,
                    step_run_id=step_id,
                    status=StepStatus.FAILED,
                    error="source interrupted",
                ),
            )
        elif status == "cancelled":
            for ack in (CancellationStatus.REQUESTED, CancellationStatus.CONFIRMED):
                tracker.record_cancellation(
                    step_id, StepStatus.RUNNING, CancellationAcknowledgement(ack)
                )
            tracker.transition(
                step_id,
                StepStatus.RUNNING,
                StepStatus.CANCELLED,
                result=StepResult(
                    step_name="user-label",
                    step_number=0,
                    step_run_id=step_id,
                    status=StepStatus.CANCELLED,
                    cancellation_status=CancellationStatus.CONFIRMED,
                ),
            )
    directory = shard_uri(
        staging_root, execution_id, step_number=0, operation_name="worker"
    )
    fs.makedirs(directory, exist_ok=True)
    artifact = (
        artifact
        or DataArtifact.draft(
            content=b"value\n1\n", original_name="result.csv", step_number=0
        ).finalize()
    )
    if not empty:
        _stage_artifacts({"output": [artifact]}, [], 0, directory, fs)
        with fs.open(f"{directory}/execution_edges.parquet", "wb") as stream:
            build_execution_edges(
                execution_id, {}, {"output": [artifact.artifact_id]}
            ).write_parquet(stream)
    frame = executions_df(
        execution_run_id=[execution_id],
        execution_spec_id=["f" * 32],
        step_run_id=[step_id],
        origin_step_number=[0],
        operation_name=["worker"],
        success=[success],
        timestamp_start=[datetime.now(UTC)],
        timestamp_end=[datetime.now(UTC)],
        command_recording=[CommandRecording.empty().model_dump_json()],
        replay_snapshot=[ReplaySnapshot.unavailable("test worker").model_dump_json()],
        replay_of_execution_run_id=["a" * 32 if diagnostic else None],
    )
    inventory = build_staging_inventory(directory, fs).decode()
    with fs.open(f"{directory}/executions.parquet", "wb") as stream:
        frame.write_parquet(stream, metadata={STAGING_INVENTORY_KEY: inventory})
    return tracker, directory, artifact.artifact_id


def _root_bytes(fs, root):
    result = {}
    if fs.exists(root):
        for path in fs.find(root):
            with fs.open(path, "rb") as stream:
                result[path] = stream.read()
    return result


@pytest.mark.parametrize("status", ["running", "failed", "cancelled"])
@pytest.mark.parametrize("preserve", [False, True])
def test_recover_worker_keeps_source_history_and_is_idempotent(
    repair_env, status, preserve
):
    from artisan.schemas.execution.cache_result import CacheHit
    from artisan.storage.cache.cache_lookup import cache_lookup
    from artisan.storage.core.run_scope import load_accepted_outputs

    _, fs, options, delta_root, _staging_root = repair_env
    tracker, directory, artifact_id = _worker_candidate(repair_env, status=status)
    original = tracker.current_state("c" * 32)
    if status in {"failed", "cancelled"}:
        assert original.output_types == {}
        source = read_committed(
            delta_root, TablePath.STEPS, fs=fs, storage_options=options
        ).sort("state_sequence")
        assert json.loads(source["output_types_json"][0]) == {"output": "data"}
    before = _root_bytes(fs, directory)
    report = _repair(repair_env, recover_staging=True)
    assert [item.classification for item in report.items] == ["recoverable"]
    assert not report.blocking
    assert report.unresolved
    assert read_logical_commits(delta_root, fs=fs, storage_options=options).is_empty()
    assert _root_bytes(fs, directory) == before
    complete = _repair(
        repair_env, recover_staging=True, apply=True, preserve_staging=preserve
    )
    assert [item.classification for item in complete.items] == ["complete"]
    assert tracker.current_state("c" * 32) == original
    assert load_accepted_outputs(
        delta_root, fs=fs, storage_options=options, step_run_id="c" * 32
    ).is_empty()
    assert isinstance(cache_lookup(delta_root, "f" * 32, fs, options), CacheHit)
    assert read_committed(
        delta_root, TablePath.ARTIFACT_INDEX, fs=fs, storage_options=options
    )["artifact_id"].to_list() == [artifact_id]
    assert _root_bytes(fs, directory) == (before if preserve else {})
    snapshot = _root_bytes(fs, delta_root)
    _repair(repair_env, recover_staging=True, apply=True, preserve_staging=preserve)
    assert _root_bytes(fs, delta_root) == snapshot


@pytest.mark.parametrize("empty", [False, True])
def test_recovery_report_is_read_only_and_disabled_repair_retains_workers(
    repair_env, empty
):
    _, fs, _, delta_root, staging_root = repair_env
    _worker_candidate(repair_env, empty=empty)
    before = (_root_bytes(fs, delta_root), _root_bytes(fs, staging_root))
    report = _repair(repair_env, recover_staging=True)
    assert [item.classification for item in report.items] == ["recoverable"]
    _repair(repair_env, apply=True)
    assert (_root_bytes(fs, delta_root), _root_bytes(fs, staging_root)) == before


@pytest.mark.parametrize(
    ("kwargs", "classification"),
    [
        ({"success": False}, "ineligible"),
        ({"diagnostic": True}, "ineligible"),
        ({"owner": False}, "unknown_owner"),
        ({"status": "pending"}, "ineligible"),
    ],
)
def test_ineligible_workers_are_retained_without_blocking(
    repair_env, kwargs, classification
):
    _, fs, _, _, staging_root = repair_env
    _worker_candidate(repair_env, **kwargs)
    before = _root_bytes(fs, staging_root)
    report = _repair(repair_env, recover_staging=True, apply=True)
    assert [item.classification for item in report.items] == [classification]
    assert not report.blocking
    assert report.unresolved
    assert _root_bytes(fs, staging_root) == before


@pytest.mark.parametrize("remove", ["execution_edges.parquet", "all"])
def test_missing_sealed_payloads_block_recovery_without_mutation(repair_env, remove):
    _, fs, _, delta_root, staging_root = repair_env
    _, directory, _ = _worker_candidate(repair_env)
    paths = fs.find(directory) if remove == "all" else [f"{directory}/{remove}"]
    for path in paths:
        if not path.endswith("/executions.parquet"):
            fs.rm(path)
    before = (_root_bytes(fs, delta_root), _root_bytes(fs, staging_root))
    report = _repair(repair_env, recover_staging=True, apply=True)
    assert report.blocking
    assert {item.classification for item in report.items} == {"corrupt"}
    assert (_root_bytes(fs, delta_root), _root_bytes(fs, staging_root)) == before


def test_incomplete_shard_is_retained_and_late_seal_waits_for_next_pass(
    repair_env, monkeypatch
):
    _, fs, _, _delta_root, _staging_root = repair_env
    _, directory, _ = _worker_candidate(repair_env)
    seal = f"{directory}/executions.parquet"
    with fs.open(seal, "rb") as stream:
        payload = stream.read()
    fs.rm(seal)
    from artisan.storage.io import repair

    original = repair._apply_report

    def publish_after_snapshot(*args, **kwargs):
        if not fs.exists(seal):
            with fs.open(seal, "wb") as stream:
                stream.write(payload)
        return original(*args, **kwargs)

    monkeypatch.setattr(repair, "_apply_report", publish_after_snapshot)
    first = _repair(repair_env, recover_staging=True, apply=True)
    assert [item.classification for item in first.items] == ["incomplete"]
    second = _repair(repair_env, recover_staging=True, apply=True)
    assert [item.classification for item in second.items] == ["complete"]


def test_recovery_plan_before_control_and_abandonment_keep_ownership(
    repair_env, monkeypatch
):
    from artisan.storage.io.commit_plan import prepare_commit_plan, publish_commit_plan

    committer, fs, _, delta_root, staging_root = repair_env
    tracker, directory, _ = _worker_candidate(repair_env, status="cancelled")
    plan = prepare_commit_plan(
        staging_root=staging_root,
        fs=fs,
        commit_kind="execution_recovery",
        step_run_id="c" * 32,
        step_number=0,
        operation_name="worker",
        execution_run_id="e" * 32,
    )
    publish_commit_plan(delta_root, fs, plan)
    assert (
        _repair(repair_env, recover_staging=True).items[0].classification
        == "replayable"
    )
    _leave_planned(committer, plan, monkeypatch)
    source = tracker.current_state("c" * 32)
    _repair(repair_env, abandon=plan.logical_commit_id, reason="retain for diagnosis")
    before = _root_bytes(fs, directory)
    report = _repair(repair_env, recover_staging=True, apply=True)
    assert [item.classification for item in report.items] == ["abandoned"]
    assert tracker.current_state("c" * 32) == source
    assert _root_bytes(fs, directory) == before


def test_completed_cleanup_retains_changed_and_unlisted_files(repair_env):
    _, fs, _, _, _ = repair_env
    _, directory, _ = _worker_candidate(repair_env)
    _repair(repair_env, recover_staging=True, apply=True, preserve_staging=True)
    changed = f"{directory}/execution_edges.parquet"
    unknown = f"{directory}/notes.txt"
    for path in (changed, unknown):
        with fs.open(path, "wb") as stream:
            stream.write(b"retained evidence")
    _repair(repair_env, recover_staging=True, apply=True)
    assert set(fs.find(directory)) == {
        fs._strip_protocol(changed),
        fs._strip_protocol(unknown),
    }
    for path in (changed, unknown):
        with fs.open(path, "rb") as stream:
            assert stream.read() == b"retained evidence"


def _reseal_inventory(fs, directory):
    """Seal deliberately invalid semantic evidence for validation-boundary tests."""
    from artisan.storage.io.worker_seal import (
        STAGING_INVENTORY_KEY,
        build_staging_inventory,
    )

    seal = f"{directory}/executions.parquet"
    with fs.open(seal, "rb") as stream:
        frame = pl.read_parquet(stream)
    metadata = {STAGING_INVENTORY_KEY: build_staging_inventory(directory, fs).decode()}
    with fs.open(seal, "wb") as stream:
        frame.write_parquet(stream, metadata=metadata)


@pytest.mark.parametrize("status", ["failed", "cancelled"])
def test_recovery_enforces_original_output_roles_after_terminal_result(
    repair_env, status
):
    _, fs, _, delta_root, staging_root = repair_env
    _, directory, _ = _worker_candidate(repair_env, status=status)
    path = f"{directory}/execution_edges.parquet"
    with fs.open(path, "rb") as stream:
        frame = pl.read_parquet(stream).with_columns(pl.lit("undeclared").alias("role"))
    with fs.open(path, "wb") as stream:
        frame.write_parquet(stream)
    _reseal_inventory(fs, directory)
    before = (_root_bytes(fs, delta_root), _root_bytes(fs, staging_root))
    report = _repair(repair_env, recover_staging=True, apply=True)
    assert report.blocking
    assert [item.detail for item in report.blocking_items] == [
        "Recovery output edges disagree with source role declarations"
    ]
    assert (_root_bytes(fs, delta_root), _root_bytes(fs, staging_root)) == before


@pytest.mark.parametrize(
    "mutation", ["content_identity", "missing_reference", "diagnostics"]
)
def test_recovery_rejects_invalid_semantics_even_with_valid_inventory(
    repair_env, mutation
):
    _, fs, _, delta_root, staging_root = repair_env
    _, directory, _ = _worker_candidate(repair_env)
    filename = {
        "content_identity": "data.parquet",
        "missing_reference": "execution_edges.parquet",
        "diagnostics": "executions.parquet",
    }[mutation]
    path = f"{directory}/{filename}"
    with fs.open(path, "rb") as stream:
        frame = pl.read_parquet(stream)
    if mutation == "content_identity":
        frame = frame.with_columns(pl.lit(b"value\n2\n").alias("content"))
    elif mutation == "missing_reference":
        frame = frame.with_columns(pl.lit("a" * 32).alias("artifact_id"))
    else:
        frame = frame.with_columns(
            pl.lit('{"secret":"must-not-appear"}').alias("command_recording")
        )
    with fs.open(path, "wb") as stream:
        frame.write_parquet(stream)
    _reseal_inventory(fs, directory)
    before = (_root_bytes(fs, delta_root), _root_bytes(fs, staging_root))
    report = _repair(repair_env, recover_staging=True, apply=True)
    assert report.blocking
    assert "must-not-appear" not in report.model_dump_json()
    assert (_root_bytes(fs, delta_root), _root_bytes(fs, staging_root)) == before


@pytest.mark.parametrize("damage", [None, "missing", "changed"])
def test_recovery_verifies_external_content_using_configured_filesystem(
    repair_env, damage
):
    from artisan.schemas.artifact.file_ref import FileRefArtifact
    from artisan.utils.hashing import compute_content_digest

    _, fs, options, delta_root, _staging_root = repair_env
    external = f"{delta_root}/external.txt"
    payload = b"original external payload"
    with fs.open(external, "wb") as stream:
        stream.write(payload)
    artifact = FileRefArtifact.draft(
        path=external,
        content_hash=compute_content_digest(payload),
        size_bytes=len(payload),
        step_number=0,
    ).finalize()
    _worker_candidate(repair_env, artifact=artifact)
    if damage == "missing":
        fs.rm(external)
    elif damage == "changed":
        with fs.open(external, "wb") as stream:
            stream.write(b"changed external payload")
    report = _repair(
        repair_env, recover_staging=True, apply=True, files_root=delta_root
    )
    assert report.blocking is (damage is not None)
    executions = read_committed(
        delta_root, TablePath.EXECUTIONS, fs=fs, storage_options=options
    )
    assert executions.height == (1 if damage is None else 0)
