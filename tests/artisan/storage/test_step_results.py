"""Current step membership survives controller interruption without merging units."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from concurrent.futures import CancelledError
from dataclasses import replace
from functools import partial
from pathlib import Path

import polars as pl
import pytest
from fixtures.retained_acceptance import boundary_projection, open_committer
from fixtures.retained_readers import Backend, admit_read_case
from fixtures.retained_results import retained_case

from artisan.errors import PersistenceIntegrityError, StoreIntegrityError
from artisan.orchestration.engine.retained_step_results import RetainedStepResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.enums import CachePolicy, FailurePolicy
from artisan.schemas.execution.assignment import (
    DeliveryIdentity,
    SealedDelivery,
    SuccessfulDelivery,
)
from artisan.schemas.execution.step_admission import StepAdmission
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.provenance.projection import RoleOccurrences
from artisan.storage.cache.retained_reuse import verify_cache_source
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.admission import (
    admission_directory,
    publish_admission,
    read_admission_record,
)
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_seal import stage_retained_result
from artisan.storage.io.step_result_plan import (
    CurrentStepUnits,
    StepUnitFailure,
    WholeStepReuse,
    read_step_result_plan,
)


def _prepare(
    committer: RetainedCommitter,
    scratch: Path,
    *,
    step_run_id: str = "a" * 32,
    count: int = 2,
    optional: tuple[str, ...] | None = None,
    roles: tuple[str, ...] | None = None,
    kind: str = "alias",
    cacheable: bool = True,
    replay_of: str | None = None,
    software_digest: str = "c" * 64,
) -> tuple[RetainedStepResults, StepAdmission, tuple[SealedDelivery, ...]]:
    case = retained_case(scratch, kind=kind)
    if roles is not None:
        case = replace(
            case,
            projection=case.projection.model_copy(
                update={
                    "exports": tuple(
                        case.projection.exports[0].model_copy(update={"role": role})
                        for role in roles
                    )
                }
            ),
        )
    recipe = case.recipe.model_copy(update={"cacheable": cacheable})
    if optional is not None:
        count = len(optional)
        recipe = recipe.model_copy(
            update={
                "outputs": (recipe.outputs[0].model_copy(update={"required": False}),),
            }
        )
    tracker = RetainedStepTracker(committer, "synthetic-pipeline")
    tracker.create_attempt(
        StepStartRecord(
            step_run_id=step_run_id,
            step_number=7,
            step_name="produce",
            replay_of_execution_run_id=replay_of,
            operation_class=f"{recipe.definition.module}.{recipe.definition.qualname}",
            params_json=recipe.params_json,
            input_refs_json="{}",
            compute_backend="local",
            compute_options_json="{}",
            output_roles_json=json.dumps(sorted(item.role for item in recipe.outputs)),
            output_types_json=json.dumps(
                {item.role: item.artifact_type for item in recipe.outputs}
            ),
        )
    )
    admitted = []
    for index in range(count):
        selected = case
        if optional is not None and optional[index] != "present":
            selected = replace(
                case,
                projection=case.projection.model_copy(
                    update={
                        "exports": (RoleOccurrences(role="data", occurrences=()),)
                        if optional[index] == "empty"
                        else (),
                    }
                ),
            )
        admitted.append(
            admit_read_case(
                committer,
                selected,
                recipe=recipe,
                step_run_id=step_run_id,
                unit_index=index,
                replay_of=replay_of,
                software_digest=software_digest,
            )
        )
    admission = StepAdmission(
        step_run_id=step_run_id,
        step_number=7,
        recipe=recipe,
        software_digest=software_digest,
        assignments=tuple(item[0] for item in admitted),
    )
    tracker.admit(admission)
    return RetainedStepResults(tracker), admission, tuple(item[1] for item in admitted)


def _committer(backend: Backend) -> RetainedCommitter:
    fs, storage, root = backend
    return open_committer(f"{root}/candidate", fs, storage, initialize=True)


def test_whole_partial_step_reuse_preserves_failed_slots_and_original_outputs(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _committer(raw_backend_fs)
    source, admission, receipts = _prepare(committer, tmp_path / "source", count=3)
    committer.accept(receipts[0])
    committer.accept(receipts[2])
    original = source.finish(
        admission.step_run_id, failures={1: StepUnitFailure(error="tool failed")}
    )
    current, selected, _ = _prepare(
        committer, tmp_path / "current", step_run_id="b" * 32, count=3
    )
    assert current.lookup(selected.step_run_id, CachePolicy.ALL_SUCCEEDED) is None
    hit = current.lookup(selected.step_run_id, CachePolicy.STEP_COMPLETED)
    assert hit is not None
    reused = current.reuse(hit, duration_seconds=0.5)
    assert reused.status == StepStatus.PARTIAL
    assert reused.disposition == StepDisposition.CACHE_HIT
    assert (reused.total_count, reused.succeeded_count, reused.failed_count) == (
        3,
        2,
        1,
    )
    assert reused.error == original.error
    assert current.outputs(selected.step_run_id) == source.outputs(
        admission.step_run_id
    )
    assert current.recover(selected.step_run_id) == reused
    for reference in selected.assignments:
        state = committer.journal.state(reference.attempt.assignment)
        assert state is not None
        assert state.cancelled
        assert state.winner is None
        assert state.reuse is None


@pytest.mark.parametrize("changed", ["failed_input", "successful_carrier"])
def test_whole_step_rechecks_failed_input_and_successful_carrier_before_selection(
    raw_backend_fs: Backend, tmp_path: Path, changed: str
) -> None:
    committer = _committer(raw_backend_fs)
    source, original, receipts = _prepare(committer, tmp_path / "source", kind="file")
    accepted = committer.accept(receipts[0])
    source.finish(
        original.step_run_id, failures={1: StepUnitFailure(error="original failure")}
    )
    current, admission, _ = _prepare(
        committer, tmp_path / "current", step_run_id="b" * 32, kind="file"
    )
    hit = current.lookup(admission.step_run_id, CachePolicy.STEP_COMPLETED)
    assert hit is not None
    if changed == "failed_input":
        failed = original.assignments[1]
        record = read_admission_record(
            failed, delta_root=committer.delta_root, fs=committer.fs
        )
        path = f"{admission_directory(committer.delta_root, failed.attempt)}/{record.tables[0].filename}"
        with committer.fs.open(path, "rb") as stream:
            payload = pl.read_parquet(stream).with_columns(
                pl.col("origin_step_number") + 1
            )
        with committer.fs.open(path, "wb") as stream:
            payload.write_parquet(stream)
    else:
        path = f"{committer.files_root}/{accepted.carriers[0].relative_path}"
        with committer.fs.open(path, "wb") as stream:
            stream.write(b"tampered")
    with pytest.raises(StoreIntegrityError, match="payload|carrier"):
        current.reuse(hit)
    assert (
        read_step_result_plan(committer.delta_root, admission.step_run_id, committer.fs)
        is None
    )
    for reference in admission.assignments:
        state = committer.journal.state(reference.attempt.assignment)
        assert state is not None
        assert not state.cancelled


@pytest.mark.parametrize(
    "ineligible",
    ["noncacheable", "diagnostic_source", "diagnostic_current", "software"],
)
def test_whole_step_identity_excludes_unqualified_sources_and_current_work(
    raw_backend_fs: Backend, tmp_path: Path, ineligible: str
) -> None:
    committer = _committer(raw_backend_fs)
    source, original, receipts = _prepare(
        committer,
        tmp_path / "source",
        count=1,
        cacheable=ineligible != "noncacheable",
        replay_of="e" * 32 if ineligible == "diagnostic_source" else None,
    )
    committer.accept(receipts[0])
    source.finish(original.step_run_id)
    current, admission, _ = _prepare(
        committer,
        tmp_path / "current",
        count=1,
        step_run_id="b" * 32,
        cacheable=ineligible != "noncacheable",
        replay_of="e" * 32 if ineligible == "diagnostic_current" else None,
        software_digest="d" * 64 if ineligible == "software" else "c" * 64,
    )
    assert current.lookup(admission.step_run_id, CachePolicy.STEP_COMPLETED) is None
    assert (
        read_step_result_plan(committer.delta_root, admission.step_run_id, committer.fs)
        is None
    )


def test_completed_multi_output_plan_survives_process_hash_randomization(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, receipts = _prepare(
        committer, tmp_path, count=1, roles=("structure", "metrics", "scores", "trace")
    )
    committer.accept(receipts[0])
    final = results.finish(admission.step_run_id)
    plan = read_step_result_plan(
        committer.delta_root, admission.step_run_id, committer.fs
    )
    assert plan is not None
    program = """
import json, sys
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.io.step_result_plan import CurrentStepUnits, read_step_result_plan
request = json.load(sys.stdin)
fs = StorageConfig.model_validate(request["storage"]).filesystem()
plan = read_step_result_plan(request["root"], request["step"], fs)
assert plan is not None
print(json.dumps({"digest": plan.plan_digest, "roles": sorted(plan.result.output_roles)}))
"""
    request = json.dumps(
        {
            "root": committer.delta_root,
            "step": admission.step_run_id,
            "storage": raw_backend_fs[1].model_dump(mode="json"),
        }
    )
    for seed in ("0", "1", "42"):
        restored = subprocess.run(
            [sys.executable, "-c", program],
            input=request,
            text=True,
            capture_output=True,
            check=True,
            env={**os.environ, "PYTHONHASHSEED": seed},
            timeout=30,
        )
        assert json.loads(restored.stdout) == {
            "digest": plan.plan_digest,
            "roles": sorted(final.output_roles),
        }


def test_completed_step_preserves_unit_and_duplicate_output_order(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, receipts = _prepare(committer, tmp_path)
    for receipt in reversed(receipts):
        committer.accept(receipt)
    with pytest.raises(StoreIntegrityError, match="no completed"):
        results.outputs(admission.step_run_id)
    final = results.finish(admission.step_run_id, duration_seconds=1.5)
    assert final.status == StepStatus.SUCCEEDED
    assert final.disposition == StepDisposition.EXECUTED
    assert (final.total_count, final.succeeded_count) == (2, 2)
    assert results.recover(admission.step_run_id) == final
    actual = results.outputs(admission.step_run_id)
    reader = RetainedReader(committer)
    assert [item.reference for item in actual[0].occurrences] == [
        item.reference
        for receipt in receipts
        for item in reader.read(receipt).outputs[0].occurrences
    ]
    assert len(actual[0].occurrences) == 4


@pytest.mark.parametrize("success", [False, True])
def test_failed_units_are_fenced_before_terminal_visibility(
    raw_backend_fs: Backend, tmp_path: Path, success: bool
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, receipts = _prepare(committer, tmp_path)
    if success:
        committer.accept(receipts[0])
    failures = {1: StepUnitFailure(error="tool exited unsuccessfully")}
    if not success:
        failures[0] = StepUnitFailure(error="worker exited unsuccessfully")
    final = results.finish(admission.step_run_id, failures=failures)
    assert final.status == (StepStatus.PARTIAL if success else StepStatus.FAILED)
    assert final.failed_count == (1 if success else 2)
    for index in failures:
        with pytest.raises(StoreIntegrityError, match="ineligible"):
            committer.accept(receipts[index])
    assert len(results.outputs(admission.step_run_id)) == (1 if success else 0)


def test_reused_units_publish_current_membership_without_new_deliveries(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _committer(raw_backend_fs)
    _, _, sources = _prepare(committer, tmp_path, count=1)
    committer.accept(sources[0])
    results, admission, _ = _prepare(committer, tmp_path, step_run_id="b" * 32)
    verifier = partial(verify_cache_source, reader=RetainedReader(committer))
    for reference in admission.assignments:
        committer.journal.reuse(reference, sources[0], verify_source=verifier)
    final = results.finish(admission.step_run_id)
    assert final.disposition == StepDisposition.CACHE_HIT
    assert final.step_run_id == "b" * 32
    assert final.total_count == 2
    assert [
        item.reference.delivery_id
        for item in results.outputs(admission.step_run_id)[0].occurrences
    ] == [sources[0].delivery.delivery_id] * 4
    assert all(
        committer.journal.state(reference.attempt.assignment).winner is None
        for reference in admission.assignments
    )


@pytest.mark.parametrize("boundary", ["plan", "planned", "terminal", "complete"])
def test_step_completion_recovers_exact_frozen_membership(
    raw_backend_fs: Backend, tmp_path: Path, boundary: str
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, receipts = _prepare(committer, tmp_path, count=1)
    committer.accept(receipts[0])

    def interrupt(event: str) -> None:
        if event == boundary:
            raise InterruptedError(event)

    results.checkpoint = interrupt
    with pytest.raises(InterruptedError, match=boundary):
        results.finish(admission.step_run_id)
    plan = read_step_result_plan(
        committer.delta_root, admission.step_run_id, committer.fs
    )
    assert plan is not None
    reopened = RetainedStepResults(RetainedStepTracker(committer, "synthetic-pipeline"))
    if boundary != "complete":
        assert (
            reopened.tracker.current_state(admission.step_run_id).status
            == StepStatus.RUNNING
        )
        with pytest.raises(StoreIntegrityError, match="not complete"):
            reopened.outputs(admission.step_run_id)
    assert reopened.recover(admission.step_run_id) == plan.result
    assert len(reopened.outputs(admission.step_run_id)[0].occurrences) == 2


def test_whole_step_chooses_newest_eligible_completion_and_flattens_reuse(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _committer(raw_backend_fs)
    older, old_admission, old_receipts = _prepare(committer, tmp_path / "old")
    for receipt in old_receipts:
        committer.accept(receipt)
    older.finish(old_admission.step_run_id)
    newer, new_admission, new_receipts = _prepare(
        committer, tmp_path / "new", step_run_id="b" * 32
    )
    committer.accept(new_receipts[0])
    newer.finish(
        new_admission.step_run_id,
        failures={1: StepUnitFailure(error="new partial failure")},
    )
    current, admission, _ = _prepare(
        committer, tmp_path / "current", step_run_id="c" * 32
    )
    successful = current.lookup(admission.step_run_id, CachePolicy.ALL_SUCCEEDED)
    partial_hit = current.lookup(admission.step_run_id, CachePolicy.STEP_COMPLETED)
    assert successful is not None
    assert partial_hit is not None
    assert successful.source.step_run_id == old_admission.step_run_id
    assert partial_hit.source.step_run_id == new_admission.step_run_id
    current.reuse(partial_hit)
    repeat, repeated, _ = _prepare(committer, tmp_path / "repeat", step_run_id="d" * 32)
    hit = repeat.lookup(repeated.step_run_id, CachePolicy.STEP_COMPLETED)
    assert hit is not None
    assert hit.source.step_run_id == new_admission.step_run_id
    repeat.reuse(hit)
    plan = read_step_result_plan(
        committer.delta_root, repeated.step_run_id, committer.fs
    )
    assert plan is not None
    assert isinstance(plan.selection, WholeStepReuse)
    assert plan.selection.source_step_run_id == new_admission.step_run_id


@pytest.mark.parametrize(
    "boundary", ["reuse_fenced", "plan", "planned", "terminal", "complete"]
)
def test_whole_step_process_death_preserves_only_published_selection(
    raw_backend_fs: Backend, tmp_path: Path, boundary: str
) -> None:
    committer = _committer(raw_backend_fs)
    source, original, receipts = _prepare(committer, tmp_path / "source")
    committer.accept(receipts[0])
    source.finish(
        original.step_run_id, failures={1: StepUnitFailure(error="original failure")}
    )
    current, admission, _ = _prepare(
        committer, tmp_path / "current", step_run_id="b" * 32
    )
    request = json.dumps(
        {
            "root": committer.delta_root,
            "storage": raw_backend_fs[1].model_dump(mode="json"),
            "step": admission.step_run_id,
            "boundary": boundary,
        }
    )
    program = """
import json, os, sys
from fixtures.retained_acceptance import open_committer
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.enums import CachePolicy
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.engine.retained_step_results import RetainedStepResults
r = json.load(sys.stdin)
storage = StorageConfig.model_validate(r["storage"])
committer = open_committer(r["root"], storage.filesystem(), storage)
def die(event):
    if event == r["boundary"]:
        os._exit(73)
results = RetainedStepResults(RetainedStepTracker(committer, "synthetic-pipeline"), checkpoint=die)
hit = results.lookup(r["step"], CachePolicy.STEP_COMPLETED)
assert hit is not None
results.reuse(hit)
"""
    exited = subprocess.run(
        [sys.executable, "-c", program],
        check=False,
        input=request,
        text=True,
        capture_output=True,
        timeout=180,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).parents[2])},
    )
    assert exited.returncode == 73, exited.stderr
    plan = read_step_result_plan(
        committer.delta_root, admission.step_run_id, committer.fs
    )
    if boundary == "reuse_fenced":
        assert plan is None
        assert (
            current.tracker.current_state(admission.step_run_id).status
            == StepStatus.RUNNING
        )
        with pytest.raises(StoreIntegrityError, match="no frozen"):
            current.recover(admission.step_run_id)
        assert current.lookup(admission.step_run_id, CachePolicy.STEP_COMPLETED) is None
        return
    assert plan is not None
    recovered = current.recover(admission.step_run_id)
    assert recovered.status == StepStatus.PARTIAL
    assert recovered.disposition == StepDisposition.CACHE_HIT
    assert current.outputs(admission.step_run_id) == source.outputs(
        original.step_run_id
    )


@pytest.mark.parametrize("cancel_first", [False, True])
def test_whole_step_selection_serializes_with_cancellation(
    raw_backend_fs: Backend, tmp_path: Path, cancel_first: bool
) -> None:
    committer = _committer(raw_backend_fs)
    source, original, receipts = _prepare(committer, tmp_path / "source")
    committer.accept(receipts[0])
    source.finish(
        original.step_run_id, failures={1: StepUnitFailure(error="original failure")}
    )
    current, admission, _ = _prepare(
        committer, tmp_path / "current", step_run_id="b" * 32
    )
    hit = current.lookup(admission.step_run_id, CachePolicy.STEP_COMPLETED)
    assert hit is not None
    request = CancellationAcknowledgement(CancellationStatus.REQUESTED)
    if cancel_first:
        current.record_cancellation(admission.step_run_id, request)
        with pytest.raises(CancelledError):
            current.reuse(hit)
        assert (
            read_step_result_plan(
                committer.delta_root, admission.step_run_id, committer.fs
            )
            is None
        )
        return

    def stop(event: str) -> None:
        if event == "plan":
            raise InterruptedError(event)

    current.checkpoint = stop
    with pytest.raises(InterruptedError):
        current.reuse(hit)
    current.checkpoint = None
    state = current.record_cancellation(admission.step_run_id, request)
    assert state.status == StepStatus.PARTIAL
    assert state.cancellation_status is None


def test_step_cannot_finish_with_unresolved_work(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, receipts = _prepare(committer, tmp_path)
    committer.accept(receipts[0])
    with pytest.raises(StoreIntegrityError, match="no accepted result"):
        results.finish(admission.step_run_id)
    assert (
        results.tracker.current_state(admission.step_run_id).status
        == StepStatus.RUNNING
    )


@pytest.mark.parametrize(
    "cancellation", [CancellationStatus.CONFIRMED, CancellationStatus.UNKNOWN]
)
@pytest.mark.parametrize("all_accepted", [False, True])
def test_cancelled_step_preserves_accepted_units_without_exposing_outputs(
    raw_backend_fs: Backend,
    tmp_path: Path,
    cancellation: CancellationStatus,
    all_accepted: bool,
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, receipts = _prepare(committer, tmp_path)
    committer.accept(receipts[0])
    if all_accepted:
        committer.accept(receipts[1])
    for status in (CancellationStatus.REQUESTED, cancellation):
        results.tracker.record_cancellation(
            admission.step_run_id,
            StepStatus.RUNNING,
            CancellationAcknowledgement(status),
        )

    def interrupt(event: str) -> None:
        if event == "terminal":
            raise InterruptedError(event)

    results.checkpoint = interrupt
    with pytest.raises(InterruptedError):
        results.finish(
            admission.step_run_id,
            failures={}
            if all_accepted
            else {1: StepUnitFailure(error="cancelled before acceptance")},
        )
    assert (
        results.tracker.current_state(admission.step_run_id).status
        == StepStatus.RUNNING
    )
    results.checkpoint = None
    final = results.recover(admission.step_run_id)
    confirmed = cancellation == CancellationStatus.CONFIRMED
    assert final.status == (StepStatus.CANCELLED if confirmed else StepStatus.FAILED)
    assert final.total_count == (0 if confirmed else 2)
    assert final.succeeded_count == (0 if confirmed else 1 + all_accepted)
    assert final.cancellation_status == cancellation
    assert final.disposition is None
    assert not final.output_roles
    assert results.outputs(admission.step_run_id) == ()
    assert RetainedReader(committer).read(receipts[0]).outputs
    if not all_accepted:
        with pytest.raises(StoreIntegrityError, match="ineligible"):
            committer.accept(receipts[1])


@pytest.mark.parametrize("infrastructure", [False, True])
def test_failure_policy_preserves_units_but_withholds_failed_step_outputs(
    raw_backend_fs: Backend, tmp_path: Path, infrastructure: bool
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, receipts = _prepare(committer, tmp_path)
    committer.accept(receipts[0])
    final = results.finish(
        admission.step_run_id,
        failures={1: StepUnitFailure(error="tool failed")},
        failure_policy=FailurePolicy.CONTINUE
        if infrastructure
        else FailurePolicy.FAIL_FAST,
        infrastructure_error="provider transport failed" if infrastructure else None,
    )
    assert final.status == StepStatus.FAILED
    assert (final.total_count, final.succeeded_count, final.failed_count) == (2, 1, 1)
    assert results.outputs(admission.step_run_id) == ()
    assert RetainedReader(committer).read(receipts[0]).outputs
    assert results.recover(admission.step_run_id) == final


def test_requested_cancellation_cannot_publish_terminal_membership(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, receipts = _prepare(committer, tmp_path, count=1)
    committer.accept(receipts[0])
    results.tracker.record_cancellation(
        admission.step_run_id,
        StepStatus.RUNNING,
        CancellationAcknowledgement(CancellationStatus.REQUESTED),
    )
    with pytest.raises(ValueError, match="final cancellation acknowledgement"):
        results.finish(admission.step_run_id)
    assert (
        read_step_result_plan(committer.delta_root, admission.step_run_id, committer.fs)
        is None
    )


def test_frozen_terminal_selection_wins_later_cancellation_after_controller_crash(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, receipts = _prepare(committer, tmp_path, count=1)
    committer.accept(receipts[0])

    def interrupt(event: str) -> None:
        if event == "plan":
            raise InterruptedError(event)

    results.checkpoint = interrupt
    with pytest.raises(InterruptedError):
        results.finish(admission.step_run_id)
    reopened = RetainedStepResults(RetainedStepTracker(committer, "synthetic-pipeline"))
    request = CancellationAcknowledgement(CancellationStatus.REQUESTED)
    with pytest.raises(PersistenceIntegrityError, match="frozen terminal result"):
        reopened.tracker.record_cancellation(
            admission.step_run_id, StepStatus.RUNNING, request
        )
    state = reopened.record_cancellation(admission.step_run_id, request)
    assert state.status == StepStatus.SUCCEEDED
    assert state.cancellation_status is None
    assert state.state_sequence == 2
    assert reopened.outputs(admission.step_run_id)


@pytest.mark.parametrize("membership", [("omitted", "present"), ("omitted", "empty")])
def test_optional_output_aggregation_keeps_original_unit_membership(
    raw_backend_fs: Backend, tmp_path: Path, membership: tuple[str, ...]
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, receipts = _prepare(committer, tmp_path, optional=membership)
    for receipt in receipts:
        committer.accept(receipt)
    final = results.finish(admission.step_run_id)
    assert final.output_roles == {"data"}
    output = results.outputs(admission.step_run_id)
    assert [group.role for group in output] == ["data"]
    assert len(output[0].occurrences) == (2 if "present" in membership else 0)
    assert RetainedReader(committer).read(receipts[0]).outputs == ()


def test_step_records_retry_attempt_without_changing_initial_finite_work(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, old = _prepare(committer, tmp_path, count=1)
    original = admission.assignments[0]
    case = retained_case(tmp_path, kind="alias")
    retry = original.attempt.model_copy(update={"attempt": 1, "generation": 1})
    reference = publish_admission(
        retry,
        case.recipe,
        boundary_projection(case),
        case.boundary,
        step_number=7,
        delta_root=committer.delta_root,
        files_root=committer.files_root,
        fs=committer.fs,
    )
    committer.journal.admit(retry, admission_digest=reference.record_digest)
    with pytest.raises(StoreIntegrityError, match="current admitted"):
        committer.accept(old[0])
    receipt = stage_retained_result(
        f"{committer.delta_root}/staging",
        SuccessfulDelivery(
            delivery=DeliveryIdentity.new(retry),
            computation_complete=True,
            cleanup_complete=True,
        ),
        case.projection,
        case.artifacts,
        fs=committer.fs,
    )
    committer.accept(receipt)
    assert results.finish(admission.step_run_id).status == StepStatus.SUCCEEDED
    plan = read_step_result_plan(
        committer.delta_root, admission.step_run_id, committer.fs
    )
    assert plan is not None
    assert plan.admission == admission
    assert isinstance(plan.selection, CurrentStepUnits)
    assert plan.selection.units[0].admission == reference
    assert plan.selection.units[0].receipt == receipt
    assert results.tracker.read_admission(admission.step_run_id) == admission


@pytest.mark.parametrize(
    "boundary", ["terminal_file", "plan", "planned", "terminal", "complete"]
)
def test_controller_death_preserves_terminal_selection_and_visibility(
    raw_backend_fs: Backend, tmp_path: Path, boundary: str
) -> None:
    committer = _committer(raw_backend_fs)
    _, admission, receipts = _prepare(committer, tmp_path, count=1)
    committer.accept(receipts[0])
    fs, storage, _ = raw_backend_fs
    request = {
        "root": committer.delta_root,
        "storage": storage.model_dump(mode="json"),
        "step_run_id": admission.step_run_id,
        "boundary": boundary,
    }
    process = subprocess.run(
        [sys.executable, "-m", "fixtures.retained_step_results"],
        input=json.dumps(request),
        text=True,
        capture_output=True,
        check=False,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).parents[2])},
        timeout=180,
    )
    assert process.returncode == 41, process.stderr
    reopened = RetainedStepResults(
        RetainedStepTracker(
            open_committer(committer.delta_root, fs, storage), "synthetic-pipeline"
        )
    )
    if boundary != "complete":
        assert (
            reopened.tracker.current_state(admission.step_run_id).status
            == StepStatus.RUNNING
        )
    assert reopened.finish(admission.step_run_id).status == StepStatus.SUCCEEDED
    assert len(reopened.outputs(admission.step_run_id)[0].occurrences) == 2


def test_failed_delivery_source_requires_complete_terminal_and_exact_admission(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    from artisan.storage.core.execution_source import (
        FailedScientificExecutionSource,
        read_execution_source,
    )

    committer = _committer(raw_backend_fs)
    results, admission, receipts = _prepare(committer, tmp_path, count=1)
    delivery = DeliveryIdentity.new(admission.assignments[0].attempt)
    assert read_execution_source(committer, delivery.delivery_id) is None
    final = results.finish(
        admission.step_run_id,
        failures={
            0: StepUnitFailure(error="authored failure", failed_delivery=delivery)
        },
    )
    assert final.status == StepStatus.FAILED
    source = read_execution_source(committer, delivery.delivery_id)
    assert isinstance(source, FailedScientificExecutionSource)
    assert source.unit.failed_delivery == delivery
    assert source.admission.attempt == delivery.attempt
    assert source.unit.admission == admission.assignments[0]
    assert source.plan.result == final
    assert not source.plan.selected_receipts
    assert read_execution_source(committer, receipts[0].delivery.delivery_id) is None
    assert not results.outputs(admission.step_run_id)
    with pytest.raises(StoreIntegrityError, match="ineligible"):
        committer.accept(receipts[0])
    record = (
        admission_directory(committer.delta_root, delivery.attempt) + "/admission.json"
    )
    assert committer.fs.exists(record)
    committer.fs.rm(record)
    with pytest.raises(FileNotFoundError):
        read_execution_source(committer, delivery.delivery_id)


def test_failed_delivery_cannot_replace_a_selected_winner(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    from artisan.storage.core.execution_source import read_execution_source

    committer = _committer(raw_backend_fs)
    results, admission, receipts = _prepare(committer, tmp_path, count=1)
    committer.accept(receipts[0])
    failure = DeliveryIdentity.new(admission.assignments[0].attempt)
    final = results.finish(
        admission.step_run_id,
        failures={
            0: StepUnitFailure(error="later redelivery failed", failed_delivery=failure)
        },
    )
    assert final.status == StepStatus.SUCCEEDED
    assert read_execution_source(committer, failure.delivery_id) is None
    assert (
        read_execution_source(committer, receipts[0].delivery.delivery_id) is not None
    )


def test_failed_source_survives_whole_step_reuse_without_new_execution_identity(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    from artisan.storage.core.execution_source import (
        FailedScientificExecutionSource,
        read_execution_source,
    )

    committer = _committer(raw_backend_fs)
    original, admission, receipts = _prepare(committer, tmp_path / "original")
    committer.accept(receipts[0])
    failure = DeliveryIdentity.new(admission.assignments[1].attempt)
    original.finish(
        admission.step_run_id,
        failures={
            1: StepUnitFailure(error="authored failure", failed_delivery=failure)
        },
    )
    source = read_execution_source(committer, failure.delivery_id)
    assert isinstance(source, FailedScientificExecutionSource)
    current, admitted, _ = _prepare(
        committer, tmp_path / "current", step_run_id="b" * 32
    )
    hit = current.lookup(admitted.step_run_id, CachePolicy.STEP_COMPLETED)
    assert hit is not None
    reused = current.reuse(hit)
    assert reused.disposition == StepDisposition.CACHE_HIT
    assert read_execution_source(committer, failure.delivery_id) == source


@pytest.mark.parametrize("boundary", ["plan", "terminal", "complete"])
def test_failed_delivery_process_death_preserves_exact_source_selection(
    raw_backend_fs: Backend, tmp_path: Path, boundary: str
) -> None:
    from artisan.storage.core.execution_source import (
        FailedScientificExecutionSource,
        read_execution_source,
    )

    committer = _committer(raw_backend_fs)
    _, admission, _ = _prepare(committer, tmp_path, count=1)
    delivery = DeliveryIdentity.new(admission.assignments[0].attempt)
    request = {
        "root": committer.delta_root,
        "storage": raw_backend_fs[1].model_dump(mode="json"),
        "step_run_id": admission.step_run_id,
        "boundary": boundary,
        "failures": {
            0: StepUnitFailure(
                error="authored failure", failed_delivery=delivery
            ).model_dump(mode="json")
        },
    }
    process = subprocess.run(
        [sys.executable, "-m", "fixtures.retained_step_results"],
        input=json.dumps(request),
        text=True,
        capture_output=True,
        check=False,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).parents[2])},
        timeout=180,
    )
    assert process.returncode == 41, process.stderr
    if boundary != "complete":
        assert read_execution_source(committer, delivery.delivery_id) is None
    reopened = RetainedStepResults(
        RetainedStepTracker(
            open_committer(committer.delta_root, raw_backend_fs[0], raw_backend_fs[1]),
            "synthetic-pipeline",
        )
    )
    assert reopened.recover(admission.step_run_id).status == StepStatus.FAILED
    source = read_execution_source(reopened.committer, delivery.delivery_id)
    assert isinstance(source, FailedScientificExecutionSource)
    assert source.unit.failed_delivery == delivery
    assert reopened.recover(admission.step_run_id) == source.plan.result


def test_failed_delivery_rejects_foreign_attempt_and_duplicate_identity(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    from artisan.storage.io.step_result_plan import StepUnitResolution

    committer = _committer(raw_backend_fs)
    _, admission, receipts = _prepare(committer, tmp_path)
    delivery = DeliveryIdentity.new(admission.assignments[0].attempt)
    with pytest.raises(ValueError, match="exact fenced"):
        StepUnitResolution(
            admission=admission.assignments[1],
            decision_sequence=1,
            error="wrong unit",
            failed_delivery=delivery,
        )
    with pytest.raises(ValueError, match="exact fenced"):
        StepUnitResolution(
            admission=admission.assignments[0],
            decision_sequence=1,
            receipt=receipts[0],
            failed_delivery=delivery,
        )
    units = tuple(
        StepUnitResolution(
            admission=reference,
            decision_sequence=1,
            error="failed",
            failed_delivery=delivery.model_copy(update={"attempt": reference.attempt}),
        )
        for reference in admission.assignments
    )
    with pytest.raises(ValueError, match="duplicate delivery"):
        CurrentStepUnits(units=units, failure_policy=FailurePolicy.CONTINUE)
    with pytest.raises(ValueError, match="duplicate delivery"):
        CurrentStepUnits(
            units=(
                StepUnitResolution(
                    admission=admission.assignments[0],
                    decision_sequence=1,
                    error="contradictory failure",
                    failed_delivery=receipts[0].delivery,
                ),
                StepUnitResolution(
                    admission=admission.assignments[1],
                    decision_sequence=1,
                    receipt=receipts[0],
                    reused=True,
                ),
            ),
            failure_policy=FailurePolicy.CONTINUE,
        )


@pytest.mark.parametrize("evidence", ["terminal", "fence", "finite_admission"])
def test_failed_source_rejects_changed_required_authority(
    raw_backend_fs: Backend, tmp_path: Path, evidence: str
) -> None:
    from artisan.schemas.enums import TablePath
    from artisan.storage.core.assignment_schema import ASSIGNMENT_DECISIONS_PATH
    from artisan.storage.core.execution_source import read_execution_source

    committer = _committer(raw_backend_fs)
    results, admission, _ = _prepare(committer, tmp_path, count=1)
    delivery = DeliveryIdentity.new(admission.assignments[0].attempt)
    results.finish(
        admission.step_run_id,
        failures={
            0: StepUnitFailure(error="authored failure", failed_delivery=delivery)
        },
    )
    if evidence == "finite_admission":
        path = f"{committer.delta_root}/_artisan/step-admissions/{admission.step_run_id}.json"
        encoded = json.loads(committer.fs.cat_file(path))
        encoded["step_number"] += 1
        committer.fs.pipe_file(path, json.dumps(encoded).encode())
    else:
        path = f"{committer.delta_root}/{TablePath.STEPS.value if evidence == 'terminal' else ASSIGNMENT_DECISIONS_PATH}"
        rows = pl.read_delta(path, storage_options=committer.storage_options)
        changed = (
            rows.with_columns(pl.lit("changed diagnostic").alias("error"))
            if evidence == "terminal"
            else rows.filter(pl.col("action") != "cancel")
        )
        changed.write_delta(
            path, mode="overwrite", storage_options=committer.storage_options
        )
    with pytest.raises(StoreIntegrityError):
        read_execution_source(committer, delivery.delivery_id)


def test_stale_failed_delivery_does_not_fence_current_admission(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    committer = _committer(raw_backend_fs)
    results, admission, _ = _prepare(committer, tmp_path, count=1)
    attempt = admission.assignments[0].attempt
    wrong = DeliveryIdentity.new(attempt.model_copy(update={"attempt": 9}))
    with pytest.raises(StoreIntegrityError, match="current unit admission"):
        results.finish(
            admission.step_run_id,
            failures={0: StepUnitFailure(error="stale failure", failed_delivery=wrong)},
        )
    state = committer.journal.state(attempt.assignment)
    assert state is not None
    assert not state.cancelled
    assert (
        read_step_result_plan(committer.delta_root, admission.step_run_id, committer.fs)
        is None
    )


def test_failed_source_duplicate_identity_across_completed_owners_rejects(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    from artisan.storage.core.execution_source import read_execution_source

    committer = _committer(raw_backend_fs)
    first, admitted, _ = _prepare(committer, tmp_path / "first", count=1)
    delivery = DeliveryIdentity.new(admitted.assignments[0].attempt)
    first.finish(
        admitted.step_run_id,
        failures={0: StepUnitFailure(error="first failure", failed_delivery=delivery)},
    )
    second, admitted, _ = _prepare(
        committer, tmp_path / "second", count=1, step_run_id="b" * 32
    )
    duplicate = delivery.model_copy(update={"attempt": admitted.assignments[0].attempt})
    second.finish(
        admitted.step_run_id,
        failures={
            0: StepUnitFailure(error="second failure", failed_delivery=duplicate)
        },
    )
    with pytest.raises(StoreIntegrityError, match="multiple completed source owners"):
        read_execution_source(committer, delivery.delivery_id)
