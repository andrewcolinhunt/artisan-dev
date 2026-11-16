"""Curator orchestration verifies sealed results and retains worker ownership."""

from __future__ import annotations

import json
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import pytest
from fixtures.curator_cache import DynamicCurator, stage_observed
from fixtures.retained_acceptance import open_committer

from artisan.execution.assignment_transport import AssignmentDispatch
from artisan.execution.curator_worker import AdmittedCurator
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.engine import retained_curator_executor
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.orchestration.engine.retained_curator_executor import (
    RetainedCuratorExecution,
)
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.runners.assignment import AssignmentSoftware
from artisan.orchestration.runners.curator import CuratorLaunchError
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_admission import CuratorAdmissionReference
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.io.curator_admission import read_curator_admission
from artisan.storage.io.curator_plan import read_curator_plan
from artisan.storage.io.retained_commit import RetainedCommitter


@dataclass
class _Owner:
    state: OwnershipState = OwnershipState.CLOSED
    closes: int = 0

    def ownership_state(self) -> OwnershipState:
        return self.state

    def request_close(self, deadline: float) -> None:
        self.closes += 1


class _Handle:
    def __init__(
        self, admission: CuratorAdmissionReference, staged: StagingResult, owner: _Owner
    ) -> None:
        self.admission = admission
        self.worker_owner = owner
        self.future: Future[StagingResult] = Future()
        self.future.set_result(staged)

    def result(self, timeout: float | None = None) -> StagingResult:
        return self.future.result(timeout)

    def done(self) -> bool:
        return self.future.done()

    def ownership_state(self) -> OwnershipState:
        return self.worker_owner.state

    def cancel(self) -> CancellationAcknowledgement:
        return CancellationAcknowledgement(
            CancellationStatus.REJECTED
            if self.worker_owner.state == OwnershipState.CLOSED
            else CancellationStatus.REQUESTED
        )


@dataclass
class _Software:
    software_digest: str = "e" * 64
    software_cacheable: bool = True


class _Runner:
    name = "synthetic-curator"

    def __init__(self, committer: RetainedCommitter) -> None:
        self.committer = committer
        self.owner = _Owner()
        self.launched = threading.Event()
        self.count = 0
        self.software = _Software()
        self.lie = False
        self.partial_start = False
        self.foreign = False

    def resolve_curator(
        self,
        operation: OperationDefinition,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> _Software:
        return self.software

    def launch_curator(
        self,
        curator: AdmittedCurator,
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> _Handle:
        self.count += 1
        if self.partial_start:
            message = "synthetic startup failure"
            raise CuratorLaunchError(
                message,
                handles=(),
                worker_owners=(self.owner,),
                cancellation=CancellationAcknowledgement(CancellationStatus.UNKNOWN),
            )
        admission = read_curator_admission(
            curator.admission,
            delta_root=self.committer.delta_root,
            fs=self.committer.fs,
        )
        assert isinstance(curator.operation, DynamicCurator)
        worker_committer = open_committer(
            self.committer.delta_root, dispatch.storage.filesystem(), dispatch.storage
        )
        staged, _, _ = stage_observed(
            CuratorResults(
                RetainedStepTracker(worker_committer, "synthetic-pipeline"),
                dispatch.staging_root,
            ),
            admission,
            curator.operation,
        )
        if self.lie:
            staged.success = not staged.success
            staged.artifact_ids = ["f" * 32]
            staged.staging_path = "/not-the-admitted-location"
        reference = curator.admission
        if self.foreign:
            reference = reference.model_copy(update={"step_run_id": "f" * 32})
        handle = _Handle(reference, staged, self.owner)
        self.launched.set()
        return handle


def _execution(
    root: Path,
    *,
    operation: DynamicCurator | None = None,
    runner: _Runner | None = None,
    number: int = 0,
    preserve_staging: bool = False,
) -> RetainedCuratorExecution:
    storage = StorageConfig()
    committer = (
        runner.committer
        if runner is not None
        else open_committer(
            str(root / "store"), storage.filesystem(), storage, initialize=True
        )
    )
    tracker = RetainedStepTracker(committer, "synthetic-pipeline")
    operation = operation or DynamicCurator()
    record = StepStartRecord(
        step_run_id=f"{number + 1:032x}",
        step_number=number,
        step_name="selected-curator",
        operation_class=f"{type(operation).__module__}.{type(operation).__qualname__}",
        params_json=operation.params.model_dump_json(),
        input_refs_json="{}",
        compute_backend="synthetic-curator",
        compute_options_json=json.dumps({"dynamic_output_roles": True}),
        output_roles_json="[]",
        output_types_json="{}",
    )
    execution = RetainedCuratorExecution(
        record,
        operation=operation,
        resolve_inputs=dict,
        tracker=tracker,
        runner=runner or _Runner(committer),
        dispatch=AssignmentDispatch(
            committer.delta_root,
            committer.files_root,
            str(root / "staging"),
            storage,
            root / "workers",
            preserve_staging=preserve_staging,
        ),
        runner_resources=RunnerResources(),
    )
    execution.results.initialize()
    return execution


@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("preserve", [False, True])
def test_closed_result_comes_from_seal_and_cache_keeps_original_producers(
    tmp_path: Path, empty: bool, preserve: bool
) -> None:
    operation = DynamicCurator(params=DynamicCurator.Params(empty=empty))
    first = _execution(tmp_path, operation=operation, preserve_staging=preserve)
    runner = first.runner
    assert isinstance(runner, _Runner)
    runner.lie = True
    with ThreadPoolExecutor() as pool:
        result = first.run(pool)
        assert result.status == StepStatus.SUCCEEDED, result.error
        assert result.output_types == ({} if empty else {"found": "csv"})
        assert result.metadata == {"authored": {"selected": "original-source"}}
        assert first.ownership_state() == OwnershipState.CLOSED
        second = _execution(
            tmp_path,
            operation=operation,
            runner=runner,
            number=1,
            preserve_staging=preserve,
        )
        cached = second.run(pool)
    assert cached.disposition == StepDisposition.CACHE_HIT, cached.error
    assert cached.output_types == result.output_types
    assert runner.count == 1
    assert second.handles == ()
    for execution in (first, second):
        committed = read_curator_plan(
            runner.committer.delta_root,
            execution.record.step_run_id,
            runner.committer.fs,
        )
        assert committed is not None
        files = [
            f"{execution.dispatch.staging_root}/{file.relative_path}"
            for table in committed.tables
            for file in table.files
        ]
        assert files
        assert all(runner.committer.fs.exists(path) == preserve for path in files)
    plan = read_curator_plan(
        runner.committer.delta_root, second.record.step_run_id, runner.committer.fs
    )
    assert plan is not None
    accepted = second.reader.read_result(plan.step_run_id, plan.plan_digest)
    if not empty:
        assert (
            accepted.output("found")[0].reference.step_run_id
            == first.record.step_run_id
        )


def test_sealed_science_failure_cannot_be_changed_by_transport(tmp_path: Path) -> None:
    execution = _execution(
        tmp_path, operation=DynamicCurator(params=DynamicCurator.Params(fail=True))
    )
    assert isinstance(execution.runner, _Runner)
    execution.runner.lie = True
    with ThreadPoolExecutor() as pool:
        result = execution.run(pool)
    assert result.status == StepStatus.FAILED
    assert result.failed_count == 1
    assert "synthetic science failure" in result.error
    plan = read_curator_plan(
        execution.tracker.committer.delta_root,
        result.step_run_id,
        execution.tracker.committer.fs,
    )
    assert plan is not None
    assert plan.table(TablePath.EXECUTIONS.value) is not None
    assert not result.output_roles


def test_pending_owner_exists_before_resolution_failure(tmp_path: Path) -> None:
    execution = _execution(tmp_path)

    def fail() -> dict[str, list[str]]:
        assert (
            execution.tracker.current_state(execution.record.step_run_id).status
            == StepStatus.PENDING
        )
        message = "synthetic input failure"
        raise ValueError(message)

    execution.resolve_inputs = fail
    with ThreadPoolExecutor() as pool:
        result = execution.run(pool)
    assert result.status == StepStatus.FAILED
    assert result.error == "synthetic input failure"
    assert execution.handles == ()


def test_cancel_during_input_resolution_prevents_admission_and_launch(
    tmp_path: Path,
) -> None:
    execution = _execution(tmp_path)
    entered, release = threading.Event(), threading.Event()

    def resolve() -> dict[str, list[str]]:
        entered.set()
        assert release.wait(10)
        return {}

    execution.resolve_inputs = resolve
    with ThreadPoolExecutor() as pool:
        future = execution.submit(pool)
        try:
            assert entered.wait(10)
            assert execution.cancel().status == CancellationStatus.REQUESTED
        finally:
            release.set()
        result = future.result(timeout=10)
    assert result.status == StepStatus.CANCELLED
    assert result.cancellation_status == CancellationStatus.CONFIRMED
    assert execution.handles == ()
    assert execution.ownership_state() == OwnershipState.CLOSED


def test_result_is_not_published_until_shared_worker_closes(tmp_path: Path) -> None:
    execution = _execution(tmp_path)
    runner = execution.runner
    assert isinstance(runner, _Runner)
    runner.owner.state = OwnershipState.PENDING
    with ThreadPoolExecutor() as pool:
        future = execution.submit(pool)
        try:
            assert runner.launched.wait(10)
            assert (
                execution.tracker.current_state(execution.record.step_run_id).status
                == StepStatus.RUNNING
            )
            assert not future.done
            assert execution.ownership_state() == OwnershipState.PENDING
        finally:
            runner.owner.state = OwnershipState.CLOSED
        assert future.result(timeout=10).status == StepStatus.SUCCEEDED


def test_unknown_startup_retains_zero_handle_owner_and_never_claims_cancelled(
    tmp_path: Path,
) -> None:
    execution = _execution(tmp_path)
    runner = execution.runner
    assert isinstance(runner, _Runner)
    runner.partial_start = True
    runner.owner.state = OwnershipState.UNKNOWN
    with ThreadPoolExecutor() as pool:
        result = execution.run(pool)
    assert result.status == StepStatus.FAILED
    assert result.cancellation_status == CancellationStatus.UNKNOWN
    assert execution.worker_owners == (runner.owner,)
    assert runner.owner.closes > 0
    runner.owner.state = OwnershipState.CLOSED
    assert execution.ownership_state() == OwnershipState.UNKNOWN
    assert execution.cancel().status == CancellationStatus.UNKNOWN


@pytest.mark.parametrize("closure", list(OwnershipState))
def test_resolution_failure_retains_probe_without_admitting_curator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, closure: OwnershipState
) -> None:
    monkeypatch.setattr(retained_curator_executor, "CLEANUP_SECONDS", 0.01)
    execution = _execution(tmp_path)
    runner = execution.runner
    assert isinstance(runner, _Runner)
    runner.owner.state = closure

    def fail_resolution(
        operation: OperationDefinition,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> _Software:
        message = "Current software measurement failed"
        raise CuratorLaunchError(
            message,
            handles=(),
            worker_owners=(runner.owner,),
            cancellation=CancellationAcknowledgement(CancellationStatus.REQUESTED),
        )

    monkeypatch.setattr(runner, "resolve_curator", fail_resolution)
    with ThreadPoolExecutor() as pool:
        result = execution.run(pool)
    assert result.status == StepStatus.FAILED
    assert execution.worker_owners == (runner.owner,)
    assert runner.owner.closes > 0
    assert runner.count == 0
    assert not execution.handles
    assert execution._reference is None
    assert not result.output_roles
    unknown = closure != OwnershipState.CLOSED
    assert result.cancellation_status == (
        CancellationStatus.UNKNOWN if unknown else None
    )
    runner.owner.state = OwnershipState.CLOSED
    assert execution.ownership_state() == (
        OwnershipState.UNKNOWN if unknown else OwnershipState.CLOSED
    )


def test_foreign_handle_is_retained_but_cannot_publish_outputs(tmp_path: Path) -> None:
    execution = _execution(tmp_path)
    assert isinstance(execution.runner, _Runner)
    execution.runner.foreign = True
    with ThreadPoolExecutor() as pool:
        result = execution.run(pool)
    assert result.status == StepStatus.FAILED
    assert "another curator admission" in result.error
    assert len(execution.handles) == 1
    assert not result.output_roles
    plan = read_curator_plan(
        execution.tracker.committer.delta_root,
        execution.record.step_run_id,
        execution.tracker.committer.fs,
    )
    assert plan is not None
    assert plan.table(TablePath.EXECUTIONS.value) is None


def test_provider_ownership_poll_failure_finishes_with_unknown_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    execution = _execution(tmp_path)
    assert isinstance(execution.runner, _Runner)

    def fail() -> OwnershipState:
        message = "synthetic provider poll failure"
        raise OSError(message)

    monkeypatch.setattr(execution.runner.owner, "ownership_state", fail)
    with ThreadPoolExecutor() as pool:
        result = execution.run(pool)
    assert result.status == StepStatus.FAILED
    assert result.cancellation_status == CancellationStatus.UNKNOWN
    assert execution.ownership_state() == OwnershipState.UNKNOWN
    assert execution.worker_owners == (execution.runner.owner,)


def test_selected_plan_recovers_after_publication_failure_without_rerun(
    tmp_path: Path,
) -> None:
    execution = _execution(tmp_path)
    failed = False

    def checkpoint(event: str) -> None:
        nonlocal failed
        if event == "planned" and not failed:
            failed = True
            message = "synthetic publication interruption"
            raise OSError(message)

    execution.results.checkpoint = checkpoint
    with ThreadPoolExecutor() as pool:
        result = execution.run(pool)
    assert failed
    assert result.status == StepStatus.SUCCEEDED, result.error
    assert result.disposition == StepDisposition.EXECUTED
    assert isinstance(execution.runner, _Runner)
    assert execution.runner.count == 1


def test_preflight_cannot_return_another_owners_skipped_result(tmp_path: Path) -> None:
    execution = _execution(tmp_path)
    execution.before_resolve = lambda: StepResult(
        step_run_id="f" * 32,
        step_number=0,
        step_name="foreign",
        status=StepStatus.SKIPPED,
    )
    with ThreadPoolExecutor() as pool:
        result = execution.run(pool)
    assert result.status == StepStatus.FAILED
    assert "Pre-work skip differs" in (result.error or "")
    assert result.step_run_id == execution.record.step_run_id
    assert not execution.handles
