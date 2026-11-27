"""Controller submission preserves scientific boundaries and accepted ownership."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from fixtures.composition_operations import (
    Iterate,
    PredictMeasure,
    Transform,
    coordinate,
    structure,
)
from fixtures.retained_acceptance import open_committer
from fixtures.retained_execution import NativeHandle, NativeRunner, NativeSoftware

from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.compiler import compile_definition
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.errors import StoreIntegrityError
from artisan.execution.assignment import Assignment
from artisan.execution.assignment_transport import (
    AdmittedAssignment,
    AssignmentDispatch,
)
from artisan.execution.composition import ExecuteAdapter
from artisan.execution.exceptions import AssignmentExecutionError
from artisan.operations.base import OperationDefinition
from artisan.orchestration.engine import retained_executor
from artisan.orchestration.engine.retained_executor import RetainedStepExecution
from artisan.orchestration.engine.retained_inputs import (
    PreparedAssignmentInputs,
    prepare_step_inputs,
)
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.retained_replay import Definition, prepare_retained_replay
from artisan.orchestration.runners.assignment import (
    AssignmentHandle,
    AssignmentLaunchError,
    AssignmentSoftware,
)
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.enums import CachePolicy, FailurePolicy
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.assignment import (
    AssignmentIdentity,
    DeliveryIdentity,
    SealedDelivery,
)
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_ownership import OwnershipState, WorkerOwner
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.orchestration.step_state import StepState
from artisan.schemas.specs.input_models import PreprocessInput
from artisan.storage.cache.retained_lookup import RetainedCacheHit, lookup_assignment
from artisan.storage.core.retained_reader import AcceptedArtifact, RetainedReader
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_plan import RetainedCommitPlan
from artisan.storage.io.step_result_plan import StepUnitFailure


def _execution(
    root: Path,
    definition: type[CompositeDefinition] | type[OperationDefinition],
    inputs: Mapping[str, Sequence[Artifact | AcceptedArtifact]],
    *,
    step_number: int = 0,
    step_run_id: str | None = None,
    runner: NativeRunner | None = None,
    batch: BatchStrategy | None = None,
    preserve_staging: bool = False,
) -> RetainedStepExecution:
    storage = StorageConfig()
    committer = (
        runner.committer
        if runner is not None
        else open_committer(
            str(root / "store"), storage.filesystem(), storage, initialize=True
        )
    )
    tracker = RetainedStepTracker(committer, "synthetic-pipeline")
    runner = runner or NativeRunner(committer)
    plan = compile_definition(definition)
    identity = step_run_id or f"{step_number + 1:032x}"
    record = StepStartRecord(
        step_run_id=identity,
        step_number=step_number,
        step_name=definition.name,
        operation_class=f"{plan.recipe.definition.module}.{plan.recipe.definition.qualname}",
        params_json=plan.recipe.params_json,
        input_refs_json="{}",
        compute_backend=runner.name,
        compute_options_json="{}",
        output_roles_json=json.dumps(sorted(item.role for item in plan.recipe.outputs)),
        output_types_json=json.dumps(
            {item.role: item.artifact_type for item in plan.recipe.outputs}
        ),
    )
    return RetainedStepExecution(
        record,
        resolve_plan=lambda: plan,
        resolve_inputs=lambda compiled: prepare_step_inputs(
            compiled.recipe,
            inputs,
            step_run_id=identity,
            reader=RetainedReader(committer),
        ),
        tracker=tracker,
        runner=runner,
        dispatch=AssignmentDispatch(
            committer.delta_root,
            committer.files_root,
            str(root / "staging"),
            storage,
            root / "workers",
            preserve_staging=preserve_staging,
        ),
        runner_resources=RunnerResources(),
        batch_strategy=batch or BatchStrategy(),
    )


@pytest.mark.parametrize(
    ("definition", "expected"), [(PredictMeasure, 1), (Iterate, 14)]
)
@pytest.mark.parametrize("preserve", [False, True])
def test_sync_and_async_scientific_compositions(
    tmp_path: Path,
    definition: type[CompositeDefinition],
    expected: int,
    preserve: bool,
) -> None:
    execution = _execution(
        tmp_path,
        definition,
        {"structure": [structure(0)]},
        preserve_staging=preserve,
    )
    with ThreadPoolExecutor() as pool:
        final = (
            execution.run(pool)
            if definition is PredictMeasure
            else execution.submit(pool).result(timeout=30)
        )
    assert final.status == StepStatus.SUCCEEDED, final.model_dump_json()
    assert final.total_count == 1
    outputs = {
        item.role: item.occurrences
        for item in execution.results.outputs(execution.record.step_run_id)
    }
    selected = outputs["structure"][0]
    assert coordinate(selected.artifact) == expected
    metric = outputs["metrics"][0]
    assert execution.reader.associated(selected.reference, "metric") == (metric,)
    receipt = execution.handles[0].result()
    accepted = execution.reader.read(receipt)
    assert execution.tracker.committer.fs.exists(receipt.seal_uri) == preserve
    assert not accepted.support
    assert len(accepted.projection.occurrences) == 3
    if definition is Iterate:
        assert metric.artifact.values == {"selected": 14, "history": [2, 6, 14]}


def test_unqualified_software_can_execute_but_cannot_supply_or_consume_cache(
    tmp_path: Path,
) -> None:
    class ObservedRunner(NativeRunner):
        qualified = False

        def resolve(
            self,
            plan: CompiledPlan,
            runner_resources: RunnerResources,
            *,
            application_modules: tuple[str, ...] = (),
        ) -> NativeSoftware:
            return NativeSoftware(software_cacheable=self.qualified)

    initial = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})
    runner = ObservedRunner(initial.tracker.committer)
    with ThreadPoolExecutor() as pool:
        for index, qualified in enumerate((False, False, True, True, False)):
            runner.qualified = qualified
            execution = _execution(
                tmp_path,
                PredictMeasure,
                {"structure": [structure(0)]},
                step_number=index,
                runner=runner,
            )
            result = execution.run(pool)
            assert result.status == StepStatus.SUCCEEDED, result.error
            assert (result.disposition == StepDisposition.CACHE_HIT) == (index == 3)
            for handle in execution.handles:
                with execution.reader.capture_inputs(handle.result()) as captured:
                    assert captured.record.attempt.software_cacheable is qualified
    assert len(runner.packs) == 4


def test_pending_owner_precedes_operational_failure(tmp_path: Path) -> None:
    execution = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})

    def fail() -> CompiledPlan:
        assert (
            execution.tracker.current_state(execution.record.step_run_id).status
            == StepStatus.PENDING
        )
        message = "prepared software unavailable"
        raise RuntimeError(message)

    execution.resolve_plan = fail
    with ThreadPoolExecutor() as pool:
        final = execution.run(pool)
    assert final.status == StepStatus.FAILED
    assert final.error == "prepared software unavailable"
    assert final.total_count == 0
    assert not execution.handles


def test_empty_input_skips_without_resolving_software(tmp_path: Path) -> None:
    execution = _execution(tmp_path, PredictMeasure, {"structure": []})
    with ThreadPoolExecutor() as pool:
        final = execution.run(pool)
    assert final.status == StepStatus.SKIPPED
    assert isinstance(execution.runner, NativeRunner)
    assert execution.runner.resolutions == 0
    assert not execution.tracker.read_admission(
        execution.record.step_run_id
    ).assignments


def test_preflight_cannot_return_another_owners_skipped_result(tmp_path: Path) -> None:
    execution = _execution(tmp_path, PredictMeasure, {"structure": []})
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


def test_pending_future_does_not_block_submit(tmp_path: Path) -> None:
    execution = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})
    entered, release = threading.Event(), threading.Event()
    original = execution.resolve_plan

    def waiting() -> CompiledPlan:
        entered.set()
        assert release.wait(10)
        return original()

    execution.resolve_plan = waiting
    with ThreadPoolExecutor() as pool:
        future = execution.submit(pool)
        assert entered.wait(5)
        assert future.status == StepStatus.PENDING
        assert not future.done
        release.set()
        assert future.result(timeout=30).status == StepStatus.SUCCEEDED


class _GatedRunner(NativeRunner):
    def __init__(self, source: NativeRunner, *, blocked_index: int = 1) -> None:
        super().__init__(source.committer)
        self.blocked_index = blocked_index
        self.entered = threading.Event()
        self.release = threading.Event()

    def _invoke(
        self, assignment: AdmittedAssignment, dispatch: AssignmentDispatch
    ) -> SealedDelivery:
        if assignment.admission.attempt.assignment.unit_index == self.blocked_index:
            self.entered.set()
            assert self.release.wait(20)
        return super()._invoke(assignment, dispatch)


def test_pack_slot_waits_for_every_member_and_preserves_unit_order(
    tmp_path: Path,
) -> None:
    execution = _execution(
        tmp_path,
        PredictMeasure,
        {"structure": [structure(index) for index in range(4)]},
        batch=BatchStrategy(units_per_worker=2, max_workers=1),
    )
    assert isinstance(execution.runner, NativeRunner)
    runner = _GatedRunner(execution.runner)
    execution.runner = runner
    with ThreadPoolExecutor() as pool:
        future = execution.submit(pool)
        try:
            assert runner.entered.wait(15)
            assert execution.handles[0].done()
            time.sleep(0.1)
            assert runner.packs == [(0, 1)]
            assert not future.done
        finally:
            runner.release.set()
        final = future.result(timeout=30)
    assert final.status == StepStatus.SUCCEEDED
    assert final.total_count == 4
    assert runner.packs == [(0, 1), (2, 3)]
    outputs = next(
        role.occurrences
        for role in execution.results.outputs(execution.record.step_run_id)
        if role.role == "structure"
    )
    assert [coordinate(item.artifact) for item in outputs] == [1, 2, 3, 4]
    assert (
        len({handle.result().delivery.delivery_id for handle in execution.handles}) == 4
    )


class _TransportOwner:
    def __init__(self) -> None:
        self.state = OwnershipState.PENDING
        self.close_on_request = False
        self.requests: list[float] = []

    def ownership_state(self) -> OwnershipState:
        return self.state

    def request_close(self, deadline: float) -> None:
        self.requests.append(deadline)
        if self.close_on_request:
            self.state = OwnershipState.CLOSED


@dataclass(frozen=True)
class _TransportHandle(NativeHandle):
    owner: WorkerOwner

    @property
    def worker_owner(self) -> WorkerOwner:
        return self.owner

    def ownership_state(self) -> OwnershipState:
        return self.owner.ownership_state()


class _TransportRunner(NativeRunner):
    def __init__(self, source: NativeRunner) -> None:
        super().__init__(source.committer)
        self.owner = _TransportOwner()

    def launch(
        self,
        assignments: Sequence[AdmittedAssignment],
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> tuple[NativeHandle, ...]:
        return tuple(
            _TransportHandle(handle.attempt, handle.future, self.owner)
            for handle in super().launch(
                assignments, software=software, dispatch=dispatch
            )
        )


@pytest.mark.parametrize("closure", [OwnershipState.CLOSED, OwnershipState.UNKNOWN])
def test_accepted_unit_retains_pack_capacity_until_transport_closes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, closure: OwnershipState
) -> None:
    execution = _execution(
        tmp_path,
        PredictMeasure,
        {"structure": [structure(0), structure(1)]},
        batch=BatchStrategy(units_per_worker=1, max_workers=1),
    )
    assert isinstance(execution.runner, NativeRunner)
    runner = _TransportRunner(execution.runner)
    execution.runner = runner
    accepted = threading.Event()
    original = execution.tracker.committer.accept

    def accept(receipt: SealedDelivery, *, preserve_staging: bool = False) -> None:
        original(receipt, preserve_staging=preserve_staging)
        accepted.set()

    monkeypatch.setattr(execution.tracker.committer, "accept", accept)
    with ThreadPoolExecutor() as pool:
        future = execution.submit(pool)
        try:
            assert accepted.wait(20)
            assert execution.handles[0].done()
            assert execution.handles[0].ownership_state() == OwnershipState.PENDING
            assert len(execution.reader.completed_plans()) == 1
            time.sleep(0.1)
            assert runner.packs == [(0,)]
            assert not future.done
        finally:
            runner.owner.state = closure
        final = future.result(timeout=30)
    assert execution.ownership_state() == closure
    if closure == OwnershipState.CLOSED:
        assert final.status == StepStatus.SUCCEEDED
        assert runner.packs == [(0,), (1,)]
        assert execution.cancel().status == CancellationStatus.REJECTED
    else:
        assert final.status == StepStatus.FAILED
        assert final.cancellation_status == CancellationStatus.UNKNOWN
        assert runner.packs == [(0,)]
        assert len(execution.handles) == 1
        assert execution.cancel().status == CancellationStatus.UNKNOWN
        assert not execution.results.outputs(execution.record.step_run_id)
        assert len(execution.reader.completed_plans()) == 1
        runner.owner.state = OwnershipState.CLOSED
        assert execution.ownership_state() == OwnershipState.UNKNOWN
        assert execution.cancel().status == CancellationStatus.UNKNOWN


def test_step_cancel_closes_shared_worker_after_all_unit_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    execution = _execution(
        tmp_path,
        PredictMeasure,
        {"structure": [structure(0), structure(1)]},
        batch=BatchStrategy(units_per_worker=2, max_workers=1),
    )
    assert isinstance(execution.runner, NativeRunner)
    runner = _TransportRunner(execution.runner)
    runner.owner.close_on_request = True
    execution.runner = runner
    independent = _TransportOwner()
    accepted = threading.Event()
    original = execution.tracker.committer.accept
    count = 0

    def accept(receipt: SealedDelivery, *, preserve_staging: bool = False) -> None:
        nonlocal count
        original(receipt, preserve_staging=preserve_staging)
        count += 1
        if count == 2:
            accepted.set()

    monkeypatch.setattr(execution.tracker.committer, "accept", accept)
    with ThreadPoolExecutor() as pool:
        future = execution.submit(pool)
        try:
            assert accepted.wait(20)
            assert all(handle.done() for handle in execution.handles)
            assert execution.worker_owners == (runner.owner,)
            assert execution.cancel().status == CancellationStatus.REQUESTED
            final = future.result(timeout=15)
        finally:
            runner.owner.state = OwnershipState.CLOSED
    assert final.status == StepStatus.CANCELLED
    assert final.cancellation_status == CancellationStatus.CONFIRMED
    assert execution.ownership_state() == OwnershipState.CLOSED
    assert runner.owner.requests
    assert not independent.requests
    assert independent.ownership_state() == OwnershipState.PENDING
    assert len(execution.reader.completed_plans()) == 2


def test_terminal_unknown_before_future_completion_still_retains_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    execution = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})
    assert isinstance(execution.runner, NativeRunner)
    runner = _TransportRunner(execution.runner)
    execution.runner = runner
    persisted, release = threading.Event(), threading.Event()
    original_accept = execution.tracker.committer.accept
    original_finish = execution.results.finish

    def accept(receipt: SealedDelivery, *, preserve_staging: bool = False) -> None:
        original_accept(receipt, preserve_staging=preserve_staging)
        runner.owner.state = OwnershipState.UNKNOWN

    def finish(step_run_id: str, **kwargs: Any) -> StepResult:
        result = original_finish(step_run_id, **kwargs)
        persisted.set()
        assert release.wait(15)
        return result

    monkeypatch.setattr(execution.tracker.committer, "accept", accept)
    monkeypatch.setattr(execution.results, "finish", finish)
    with ThreadPoolExecutor() as pool:
        future = execution.submit(pool)
        try:
            assert persisted.wait(20)
            assert not future.done
            assert future.status == StepStatus.FAILED
            assert execution.cancel().status == CancellationStatus.UNKNOWN
            assert execution.worker_owners == (runner.owner,)
        finally:
            release.set()
        assert (
            future.result(timeout=15).cancellation_status == CancellationStatus.UNKNOWN
        )


def test_partial_assignment_cache_dispatches_only_misses(tmp_path: Path) -> None:
    first = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})
    assert isinstance(first.runner, NativeRunner)
    with ThreadPoolExecutor() as pool:
        first.run(pool)
        next_execution = _execution(
            tmp_path,
            PredictMeasure,
            {"structure": [structure(0), structure(5)]},
            step_number=1,
            runner=first.runner,
        )
        final = next_execution.run(pool)
    assert final.status == StepStatus.SUCCEEDED
    assert final.total_count == 2
    assert final.disposition == StepDisposition.EXECUTED
    assert first.runner.packs == [(0,), (1,)]


class _FailingRunner(NativeRunner):
    def _invoke(
        self, assignment: AdmittedAssignment, dispatch: AssignmentDispatch
    ) -> SealedDelivery:
        if assignment.admission.attempt.assignment.unit_index == 1:
            message = "synthetic second-unit failure"
            raise AssignmentExecutionError(
                message, phase="execute", call_path=("second",)
            )
        return super()._invoke(assignment, dispatch)


@pytest.mark.parametrize(
    ("policy", "status"),
    [
        (FailurePolicy.CONTINUE, StepStatus.PARTIAL),
        (FailurePolicy.FAIL_FAST, StepStatus.FAILED),
    ],
)
def test_first_success_survives_second_failure_in_one_pack(
    tmp_path: Path, policy: FailurePolicy, status: StepStatus
) -> None:
    execution = _execution(
        tmp_path,
        PredictMeasure,
        {"structure": [structure(0), structure(1)]},
        batch=BatchStrategy(units_per_worker=2),
    )
    assert isinstance(execution.runner, NativeRunner)
    execution.runner = _FailingRunner(execution.runner.committer)
    execution.failure_policy = policy
    with ThreadPoolExecutor() as pool:
        final = execution.run(pool)
    assert final.status == status
    assert "Call second [execute]" in (final.error or "")
    assert (final.total_count, final.succeeded_count, final.failed_count) == (2, 1, 1)
    if status == StepStatus.FAILED:
        assert not execution.results.outputs(execution.record.step_run_id)
        assert len(execution.reader.completed_plans()) == 1
        return
    outputs = next(
        role.occurrences
        for role in execution.results.outputs(execution.record.step_run_id)
        if role.role == "structure"
    )
    assert [coordinate(item.artifact) for item in outputs] == [1]


def test_step_completed_preserves_partial_before_per_unit_fallback(
    tmp_path: Path,
) -> None:
    inputs = {"structure": [structure(0), structure(1), structure(2)]}
    first = _execution(
        tmp_path, PredictMeasure, inputs, batch=BatchStrategy(units_per_worker=3)
    )
    assert isinstance(first.runner, NativeRunner)
    runner = _FailingRunner(first.runner.committer)
    first.runner = runner
    with ThreadPoolExecutor() as pool:
        partial = first.run(pool)
        assert partial.status == StepStatus.PARTIAL
        reused = _execution(
            tmp_path, PredictMeasure, inputs, step_run_id="b" * 32, runner=runner
        )
        reused.cache_policy = CachePolicy.STEP_COMPLETED
        reused.failure_policy = FailurePolicy.FAIL_FAST
        cached = reused.run(pool)
        assert cached.status == StepStatus.PARTIAL
        assert cached.disposition == StepDisposition.CACHE_HIT
        assert cached.error == partial.error
        assert (cached.total_count, cached.succeeded_count, cached.failed_count) == (
            3,
            2,
            1,
        )
        assert reused.results.outputs(cached.step_run_id) == first.results.outputs(
            partial.step_run_id
        )
        assert runner.packs == [(0, 1, 2)]
        retry = _execution(
            tmp_path, PredictMeasure, inputs, step_run_id="c" * 32, runner=runner
        )
        result = retry.run(pool)
        assert result.status == StepStatus.PARTIAL
        assert result.disposition == StepDisposition.EXECUTED
        assert runner.packs == [(0, 1, 2), (1,)]


@pytest.mark.parametrize("change", ["failed_input", "order", "skip_cache"])
def test_whole_step_reuse_checks_failed_input_order_and_bypass(
    tmp_path: Path, change: str
) -> None:
    values = [structure(0), structure(1), structure(2)]
    first = _execution(
        tmp_path,
        PredictMeasure,
        {"structure": values},
        batch=BatchStrategy(units_per_worker=3),
    )
    assert isinstance(first.runner, NativeRunner)
    runner = _FailingRunner(first.runner.committer)
    first.runner = runner
    with ThreadPoolExecutor() as pool:
        assert first.run(pool).status == StepStatus.PARTIAL
        current_values = (
            [values[0], structure(9), values[2]]
            if change == "failed_input"
            else values[::-1]
            if change == "order"
            else values
        )
        current = _execution(
            tmp_path,
            PredictMeasure,
            {"structure": current_values},
            step_run_id="b" * 32,
            runner=runner,
            batch=BatchStrategy(units_per_worker=3),
        )
        current.cache_policy = CachePolicy.STEP_COMPLETED
        current.skip_cache = change == "skip_cache"
        result = current.run(pool)
        assert result.status == StepStatus.PARTIAL
        assert result.disposition == StepDisposition.EXECUTED
        assert runner.packs[-1] == ((0, 1, 2) if change == "skip_cache" else (1,))


class _CorruptTransportRunner(NativeRunner):
    failure_type: type[Exception] = ValueError

    def _invoke(
        self, assignment: AdmittedAssignment, dispatch: AssignmentDispatch
    ) -> SealedDelivery:
        if assignment.admission.attempt.assignment.unit_index == 1:
            message = "synthetic second-unit transport decode failure"
            raise self.failure_type(message)
        return super()._invoke(assignment, dispatch)


@pytest.mark.parametrize("failure_type", [ValueError, CancelledError])
def test_transport_failure_after_accepted_unit_is_infrastructure_failure(
    tmp_path: Path,
    failure_type: type[Exception],
) -> None:
    execution = _execution(
        tmp_path,
        PredictMeasure,
        {"structure": [structure(0), structure(1)]},
        batch=BatchStrategy(units_per_worker=2),
    )
    assert isinstance(execution.runner, NativeRunner)
    execution.runner = _CorruptTransportRunner(execution.runner.committer)
    execution.runner.failure_type = failure_type
    with ThreadPoolExecutor() as pool:
        final = execution.run(pool)
    assert final.status == StepStatus.FAILED
    assert (final.total_count, final.succeeded_count, final.failed_count) == (2, 1, 1)
    assert "transport decode" in (final.error or "")
    assert not execution.results.outputs(execution.record.step_run_id)
    source = execution.reader.read(execution.handles[0].result())
    assert coordinate(source.output("structure")[0].artifact) == 1
    assert len(execution.reader.completed_plans()) == 1


class _FailingPreprocess(Transform):
    name = "controller_failing_preprocess"

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        if coordinate(inputs.input_artifacts["structure"][0]) == 1:
            message = "synthetic authored preprocess failure"
            raise ValueError(message)
        return super().preprocess(inputs)


def test_authored_preprocess_failure_is_a_scientific_unit_failure(
    tmp_path: Path,
) -> None:
    execution = _execution(
        tmp_path,
        _FailingPreprocess,
        {"structure": [structure(0), structure(1)]},
        batch=BatchStrategy(units_per_worker=2),
    )
    with ThreadPoolExecutor() as pool:
        final = execution.run(pool)
    assert final.status == StepStatus.PARTIAL
    assert "Call <root> [preprocess]" in (final.error or "")
    assert (final.total_count, final.succeeded_count, final.failed_count) == (2, 1, 1)
    with pytest.raises(
        AssignmentExecutionError, match="authored preprocess"
    ) as captured:
        execution.handles[1].result()
    assert captured.value.phase == "preprocess"


class _DelayedDoneHandle:
    def __init__(self, handle: NativeHandle) -> None:
        self.handle = handle
        self.attempt = handle.attempt
        self.polls = 0
        self.reads = 0

    @property
    def worker_owner(self) -> WorkerOwner | None:
        return self.handle.worker_owner

    def result(self, timeout: float | None = None) -> SealedDelivery:
        self.reads += 1
        return self.handle.result(timeout)

    def done(self) -> bool:
        self.polls += 1
        return self.polls > 1

    def cancel(self) -> CancellationAcknowledgement:
        return self.handle.cancel()

    def ownership_state(self) -> OwnershipState:
        return self.handle.ownership_state()


class _CompletedRunner:
    name = "completed-test"

    def __init__(self, delegate: NativeRunner) -> None:
        self.delegate = delegate
        self.handle: _DelayedDoneHandle | None = None

    def resolve(
        self,
        plan: CompiledPlan,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> AssignmentSoftware:
        return self.delegate.resolve(plan, runner_resources)

    def launch(
        self,
        assignments: Sequence[AdmittedAssignment],
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> tuple[AssignmentHandle, ...]:
        assert len(assignments) == 1
        future: Future[SealedDelivery] = Future()
        future.set_result(self.delegate._invoke(assignments[0], dispatch))
        self.handle = _DelayedDoneHandle(
            NativeHandle(assignments[0].admission.attempt, future)
        )
        return (self.handle,)


def test_handle_becoming_done_between_polls_is_collected(tmp_path: Path) -> None:
    execution = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})
    assert isinstance(execution.runner, NativeRunner)
    runner = _CompletedRunner(execution.runner)
    execution.runner = runner
    with ThreadPoolExecutor() as pool:
        assert execution.run(pool).status == StepStatus.SUCCEEDED
    assert runner.handle is not None
    assert runner.handle.reads == 1


def test_cancel_pending_resolution_prevents_late_launch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    execution = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})
    entered, release = threading.Event(), threading.Event()
    original = execution.resolve_plan

    def wait_for_release() -> CompiledPlan:
        entered.set()
        assert release.wait(15)
        return original()

    execution.resolve_plan = wait_for_release
    cancellations: list[CancellationStatus] = []
    record_cancellation = execution.results.record_cancellation

    def observe_cancellation(
        step_run_id: str, outcome: CancellationAcknowledgement
    ) -> StepState:
        cancellations.append(outcome.status)
        return record_cancellation(step_run_id, outcome)

    monkeypatch.setattr(execution.results, "record_cancellation", observe_cancellation)
    with ThreadPoolExecutor() as pool:
        future = execution.submit(pool)
        try:
            assert entered.wait(5)
            assert execution.cancel().status == CancellationStatus.REQUESTED
            assert execution.cancel().status == CancellationStatus.REQUESTED
            assert cancellations == [
                CancellationStatus.REQUESTED,
                CancellationStatus.REQUESTED,
            ]
        finally:
            release.set()
        final = future.result(timeout=15)
    assert final.status == StepStatus.CANCELLED
    assert final.cancellation_status == CancellationStatus.CONFIRMED
    assert not execution.handles


def test_unknown_native_cancellation_retains_owner_and_rejects_late_seal(
    tmp_path: Path,
) -> None:
    execution = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})
    assert isinstance(execution.runner, NativeRunner)
    runner = _GatedRunner(execution.runner, blocked_index=0)
    execution.runner = runner
    with ThreadPoolExecutor() as pool:
        future = execution.submit(pool)
        try:
            assert runner.entered.wait(15)
            assert execution.cancel().status == CancellationStatus.UNKNOWN
            final = future.result(timeout=15)
            assert final.status == StepStatus.FAILED
            assert final.cancellation_status == CancellationStatus.UNKNOWN
            assert not execution.results.outputs(execution.record.step_run_id)
            assert len(execution.handles) == 1
        finally:
            runner.release.set()
        receipt = execution.handles[0].result(timeout=15)
    with pytest.raises(StoreIntegrityError, match="ineligible"):
        execution.tracker.committer.accept(receipt)


class _PartialStartupRunner(_GatedRunner):
    def launch(
        self,
        assignments: Sequence[AdmittedAssignment],
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> tuple[NativeHandle, ...]:
        handles = super().launch(assignments[:1], software=software, dispatch=dispatch)
        assert self.entered.wait(10)
        message = "synthetic second-unit transport startup failure"
        raise AssignmentLaunchError(
            message,
            handles=handles,
            cancellation=CancellationAcknowledgement(CancellationStatus.UNKNOWN),
        )


def test_partial_startup_preserves_handle_and_fences_all_admitted_units(
    tmp_path: Path,
) -> None:
    execution = _execution(
        tmp_path,
        PredictMeasure,
        {"structure": [structure(0), structure(1)]},
        batch=BatchStrategy(units_per_worker=2),
    )
    assert isinstance(execution.runner, NativeRunner)
    runner = _PartialStartupRunner(execution.runner, blocked_index=0)
    execution.runner = runner
    with ThreadPoolExecutor() as pool:
        future = execution.submit(pool)
        try:
            final = future.result(timeout=20)
            assert final.status == StepStatus.FAILED
            assert final.cancellation_status == CancellationStatus.UNKNOWN
            assert final.total_count == 2
            assert len(execution.handles) == 1
            admission = execution.tracker.read_admission(execution.record.step_run_id)
            for reference in admission.assignments:
                state = execution.tracker.committer.journal.state(
                    reference.attempt.assignment
                )
                assert state is not None
                assert state.cancelled
        finally:
            runner.release.set()
        execution.handles[0].result(timeout=15)


@pytest.mark.parametrize("closure", [OwnershipState.CLOSED, OwnershipState.UNKNOWN])
def test_startup_error_retains_worker_without_unit_handles(
    tmp_path: Path, closure: OwnershipState
) -> None:
    execution = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})
    assert isinstance(execution.runner, NativeRunner)
    owner = _TransportOwner()
    owner.close_on_request = closure == OwnershipState.CLOSED
    if closure == OwnershipState.UNKNOWN:
        owner.state = closure

    class StartupRunner(NativeRunner):
        def launch(
            self,
            assignments: Sequence[AdmittedAssignment],
            *,
            software: AssignmentSoftware,
            dispatch: AssignmentDispatch,
        ) -> tuple[NativeHandle, ...]:
            message = "Worker started before first unit submission failed"
            raise AssignmentLaunchError(
                message,
                handles=(),
                worker_owners=(owner,),
                cancellation=CancellationAcknowledgement(
                    CancellationStatus.UNKNOWN
                    if closure == OwnershipState.UNKNOWN
                    else CancellationStatus.REQUESTED
                ),
            )

    execution.runner = StartupRunner(execution.runner.committer)
    with ThreadPoolExecutor() as pool:
        final = execution.run(pool)
    assert final.status == StepStatus.FAILED
    assert not execution.handles
    assert execution.worker_owners == (owner,)
    assert owner.requests
    assert execution.ownership_state() == closure
    assert execution.cancel().status == (
        CancellationStatus.UNKNOWN
        if closure == OwnershipState.UNKNOWN
        else CancellationStatus.REJECTED
    )


def test_pool_rejection_has_durable_pending_failure(tmp_path: Path) -> None:
    execution = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})
    pool = ThreadPoolExecutor()
    pool.shutdown()
    final = execution.submit(pool).result()
    assert final.status == StepStatus.FAILED
    assert "shutdown" in (final.error or "")


@pytest.mark.parametrize("closure", list(OwnershipState))
def test_resolution_failure_retains_probe_until_cleanup_settles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, closure: OwnershipState
) -> None:
    monkeypatch.setattr(retained_executor, "CLEANUP_SECONDS", 0.01)
    execution = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})
    runner = execution.runner
    assert isinstance(runner, NativeRunner)
    owner = _TransportOwner()
    owner.state = closure

    def fail_resolution(
        plan: CompiledPlan,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> NativeSoftware:
        message = "Current software measurement failed"
        raise AssignmentLaunchError(
            message,
            handles=(),
            worker_owners=(owner,),
            cancellation=CancellationAcknowledgement(CancellationStatus.REQUESTED),
        )

    monkeypatch.setattr(runner, "resolve", fail_resolution)
    with ThreadPoolExecutor() as pool:
        result = execution.run(pool)
    assert result.status == StepStatus.FAILED
    assert execution.worker_owners == (owner,)
    assert owner.requests
    assert len(set(owner.requests)) == 1
    assert not execution.handles
    assert not runner.packs
    assert not execution.tracker.committer.journal.all_states()
    with pytest.raises(StoreIntegrityError, match="no completed result membership"):
        execution.results.outputs(execution.record.step_run_id)
    unknown = closure != OwnershipState.CLOSED
    assert result.cancellation_status == (
        CancellationStatus.UNKNOWN if unknown else None
    )
    owner.state = OwnershipState.CLOSED
    assert execution.ownership_state() == (
        OwnershipState.UNKNOWN if unknown else OwnershipState.CLOSED
    )


def test_expired_pending_cancel_remains_unknown_without_late_launch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(retained_executor, "CLEANUP_SECONDS", 0.01)
    execution = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})
    entered, release = threading.Event(), threading.Event()
    original = execution.resolve_plan

    def wait_for_release() -> CompiledPlan:
        entered.set()
        assert release.wait(15)
        return original()

    execution.resolve_plan = wait_for_release
    with ThreadPoolExecutor() as pool:
        future = execution.submit(pool)
        try:
            assert entered.wait(5)
            execution.cancel()
            time.sleep(0.02)
            assert execution.cancel().status == CancellationStatus.UNKNOWN
        finally:
            release.set()
        final = future.result(timeout=15)
    assert final.status == StepStatus.FAILED
    assert final.cancellation_status == CancellationStatus.UNKNOWN
    assert not execution.handles


def test_partial_cache_failure_fences_unselected_units(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})
    assert isinstance(first.runner, NativeRunner)
    with ThreadPoolExecutor() as pool:
        first.run(pool)
        execution = _execution(
            tmp_path,
            PredictMeasure,
            {"structure": [structure(0), structure(2)]},
            step_number=1,
            runner=first.runner,
        )

        def broken_cache(
            admissions: Sequence[AdmissionReference], reader: RetainedReader
        ) -> Iterator[RetainedCacheHit]:
            hit = lookup_assignment(admissions[0], reader=reader)
            assert hit is not None
            yield hit
            message = "synthetic cache read failure"
            raise RuntimeError(message)

        monkeypatch.setattr(retained_executor, "_cache_hits", broken_cache)
        final = execution.run(pool)
    assert final.status == StepStatus.FAILED
    assert (final.total_count, final.succeeded_count, final.failed_count) == (2, 1, 1)
    assert not execution.results.outputs(execution.record.step_run_id)
    assert not execution.handles
    admissions = execution.tracker.read_admission(
        execution.record.step_run_id
    ).assignments
    states = [
        execution.tracker.committer.journal.state(reference.attempt.assignment)
        for reference in admissions
    ]
    assert states[0] is not None
    assert states[0].reuse is not None
    assert states[1] is not None
    assert states[1].cancelled


class _ForeignStartupRunner(NativeRunner):
    def launch(
        self,
        assignments: Sequence[AdmittedAssignment],
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> tuple[NativeHandle, ...]:
        foreign = assignments[0].admission.attempt.model_copy(
            update={
                "assignment": AssignmentIdentity(step_run_id="f" * 32, unit_index=0)
            }
        )
        handle = NativeHandle(foreign, Future())
        message = "synthetic malformed provider ownership"
        raise AssignmentLaunchError(
            message,
            handles=(handle,),
            cancellation=CancellationAcknowledgement(CancellationStatus.CONFIRMED),
        )


def test_foreign_startup_handle_cannot_change_journal_authority(tmp_path: Path) -> None:
    execution = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})
    assert isinstance(execution.runner, NativeRunner)
    execution.runner = _ForeignStartupRunner(execution.runner.committer)
    with ThreadPoolExecutor() as pool:
        final = execution.run(pool)
    assert final.status == StepStatus.FAILED
    assert "foreign" in (final.error or "")
    assert len(execution.handles) == 1
    assert execution.handles[0].done()
    assert (
        execution.tracker.committer.journal.state(
            execution.handles[0].attempt.assignment
        )
        is None
    )


def test_cancel_between_dispatch_completion_and_claim_wins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    execution = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})
    committer = execution.tracker.committer
    original = committer.accept

    def cancel_before_claim(
        receipt: SealedDelivery, *, preserve_staging: bool = False
    ) -> RetainedCommitPlan:
        execution.cancel()
        return original(receipt, preserve_staging=preserve_staging)

    monkeypatch.setattr(committer, "accept", cancel_before_claim)
    with ThreadPoolExecutor() as pool:
        final = execution.run(pool)
    assert final.status == StepStatus.CANCELLED
    assert final.cancellation_status == CancellationStatus.CONFIRMED
    assert not execution.results.outputs(execution.record.step_run_id)
    state = committer.journal.state(execution.handles[0].attempt.assignment)
    assert state is not None
    assert state.winner is None
    assert state.cancelled


def test_partial_admission_failure_fences_already_published_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    execution = _execution(
        tmp_path, PredictMeasure, {"structure": [structure(0), structure(1)]}
    )
    original = retained_executor._publish_assignment

    def fail_second(
        prepared: PreparedAssignmentInputs,
        plan: CompiledPlan,
        record: StepStartRecord,
        software: AssignmentSoftware,
        committer: RetainedCommitter,
    ) -> AdmissionReference:
        if prepared.assignment.unit_index == 1:
            message = "synthetic input publication failure"
            raise RuntimeError(message)
        return original(prepared, plan, record, software, committer)

    monkeypatch.setattr(retained_executor, "_publish_assignment", fail_second)
    with ThreadPoolExecutor() as pool:
        final = execution.run(pool)
    assert final.status == StepStatus.FAILED
    assert final.total_count == 0
    owner = AssignmentIdentity(step_run_id=execution.record.step_run_id, unit_index=0)
    state = execution.tracker.committer.journal.state(owner)
    assert state is not None
    assert state.cancelled
    assert not execution.handles


def _registered(plan: CompiledPlan) -> dict[tuple[str, str], Definition]:
    identity = plan.recipe.definition
    definitions: dict[tuple[str, str], Definition] = {
        (identity.module, identity.qualname): plan.definition
    }
    for call in plan.calls:
        definitions.update(_registered(call.plan))
    return definitions


def test_diagnostic_replay_dispatches_frozen_inputs_without_cache_reuse(
    tmp_path: Path,
) -> None:
    first = _execution(tmp_path, Iterate, {"structure": [structure(0)]})
    assert isinstance(first.runner, NativeRunner)
    with ThreadPoolExecutor() as pool:
        first.run(pool)
        source = first.handles[0].result()
        original_state = first.tracker.current_state(first.record.step_run_id)
        original_states = first.tracker.load_current_states(
            original_state.pipeline_run_id
        )
        original_outputs = first.results.outputs(first.record.step_run_id)
        from artisan.execution.recording.retained_replay import (
            record_replay_configuration,
        )

        record_replay_configuration(
            source,
            {},
            admission=first.reader.completed_plans()[0].admission,
            delta_root=first.tracker.committer.delta_root,
            fs=first.tracker.committer.fs,
        )
        replay = prepare_retained_replay(
            first.tracker.committer,
            source.delivery.delivery_id,
            registered_definitions=_registered(first.resolve_plan()),
            runtime_replacements={},
        )
        diagnostic = RetainedStepExecution.from_replay(
            replay,
            committer=first.tracker.committer,
            runner=first.runner,
            dispatch=first.dispatch,
        )
        final = diagnostic.run(pool)
    assert final.status == StepStatus.SUCCEEDED
    assert final.disposition == StepDisposition.EXECUTED
    assert (
        first.tracker.load_current_states(original_state.pipeline_run_id)
        == original_states
    )
    assert first.results.outputs(first.record.step_run_id) == original_outputs
    assert (
        diagnostic.tracker.current_state(replay.identity.step_run_id).pipeline_run_id
        != original_state.pipeline_run_id
    )
    assert len(diagnostic.handles) == 1
    receipt = diagnostic.handles[0].result()
    assert receipt.delivery.attempt.assignment == replay.identity
    assert (
        receipt.delivery.attempt.replay_of_execution_run_id
        == source.delivery.delivery_id
    )
    assert receipt.delivery.delivery_id != source.delivery.delivery_id
    assert len(first.reader.completed_plans()) == 1
    assert len(first.reader.completed_plans(ordinary=False)) == 2


def test_software_resolution_failure_precedes_available_cache_hit(
    tmp_path: Path,
) -> None:
    first = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})
    assert isinstance(first.runner, NativeRunner)
    with ThreadPoolExecutor() as pool:
        first.run(pool)
        next_execution = _execution(
            tmp_path,
            PredictMeasure,
            {"structure": [structure(0)]},
            step_number=1,
            runner=first.runner,
        )

        class MissingSoftware(NativeRunner):
            def resolve(
                self,
                plan: CompiledPlan,
                runner_resources: RunnerResources,
                *,
                application_modules: tuple[str, ...] = (),
            ) -> AssignmentSoftware:
                assert (
                    next_execution.tracker.current_state(
                        next_execution.record.step_run_id
                    ).status
                    == StepStatus.PENDING
                )
                message = "selected software is unavailable"
                raise ValueError(message)

        next_execution.runner = MissingSoftware(first.runner.committer)
        final = next_execution.run(pool)
    assert final.status == StepStatus.FAILED
    assert final.error == "selected software is unavailable"
    assert final.total_count == 0
    assert not next_execution.handles


class _AdmittedFailureSoftware(NativeSoftware):
    def verify(self, plan: CompiledPlan) -> None:
        plan.instantiate()

    def create_adapter(
        self,
        assignment: Assignment,
        *,
        working_root: Path,
        runtime_env: Mapping[str, str],
    ) -> ExecuteAdapter:
        from artisan.execution.compute.local import LocalExecuteRouter

        return lambda _operation, _path: LocalExecuteRouter()


class _AdmittedFailureRunner(NativeRunner):
    def _invoke(
        self, assignment: AdmittedAssignment, dispatch: AssignmentDispatch
    ) -> SealedDelivery:
        from typing import cast

        from artisan.execution.admitted_worker import execute_admitted_assignment
        from artisan.execution.assignment import PreparedAssignmentSoftware
        from artisan.execution.assignment_reply import AssignmentReply

        try:
            return execute_admitted_assignment(
                assignment,
                cast(PreparedAssignmentSoftware, _AdmittedFailureSoftware()),
                dispatch,
                working_root=dispatch.working_root,
            )
        except AssignmentExecutionError as error:
            encoded = AssignmentReply.failed(
                assignment.attempt, error
            ).model_dump_json()
            return AssignmentReply.model_validate_json(encoded).result(
                assignment.attempt
            )


def test_real_closed_authored_failure_selects_source_without_optional_diagnostics(
    tmp_path: Path,
) -> None:
    from artisan.storage.core.execution_source import (
        FailedScientificExecutionSource,
        read_execution_source,
    )

    execution = _execution(tmp_path, _FailingPreprocess, {"structure": [structure(1)]})
    execution.runner = _AdmittedFailureRunner(execution.tracker.committer)
    with ThreadPoolExecutor() as pool:
        assert execution.run(pool).status == StepStatus.FAILED
    with pytest.raises(AssignmentExecutionError) as observed:
        execution.handles[0].result()
    delivery = observed.value.delivery
    assert delivery is not None
    committer = execution.tracker.committer
    diagnostics = f"{committer.delta_root}/_artisan/diagnostics"
    assert committer.fs.exists(diagnostics)
    committer.fs.rm(diagnostics, recursive=True)
    source = read_execution_source(committer, delivery.delivery_id)
    assert isinstance(source, FailedScientificExecutionSource)
    assert source.unit.failed_delivery == delivery
    assert source.admission.recipe == execution.resolve_plan().recipe
    assert not source.plan.selected_receipts


class _IdentifiedFailureRunner(NativeRunner):
    def __init__(self, committer: RetainedCommitter) -> None:
        super().__init__(committer)
        self.owners: list[_TransportOwner] = []
        self.deliveries: list[DeliveryIdentity] = []
        self.foreign = False

    def launch(
        self,
        assignments: Sequence[AdmittedAssignment],
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> tuple[NativeHandle, ...]:
        from artisan.schemas.execution.assignment import DeliveryIdentity

        handles = []
        for assignment in assignments:
            attempt = assignment.attempt
            delivery = DeliveryIdentity.new(
                attempt.model_copy(update={"attempt": 9}) if self.foreign else attempt
            )
            self.deliveries.append(delivery)
            owner = _TransportOwner()
            self.owners.append(owner)
            future: Future[SealedDelivery] = Future()
            future.set_exception(
                AssignmentExecutionError(
                    "authored failure", phase="preprocess", delivery=delivery
                )
            )
            handles.append(_TransportHandle(attempt, future, owner))
        return tuple(handles)


@pytest.mark.parametrize("closure", [OwnershipState.CLOSED, OwnershipState.UNKNOWN])
def test_failed_source_waits_for_exact_shared_worker_closure(
    tmp_path: Path, closure: OwnershipState
) -> None:
    from artisan.storage.core.execution_source import read_execution_source

    execution = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})
    runner = _IdentifiedFailureRunner(execution.tracker.committer)
    execution.runner = runner
    with ThreadPoolExecutor() as pool:
        future = execution.submit(pool)
        deadline = time.monotonic() + 20
        while not execution._observed_failures and time.monotonic() < deadline:
            time.sleep(0.01)
        try:
            assert execution._observed_failures
            assert not future.done
            assert (
                read_execution_source(
                    runner.committer, runner.deliveries[0].delivery_id
                )
                is None
            )
        finally:
            runner.owners[0].state = closure
        final = future.result(timeout=30)
    assert final.status == StepStatus.FAILED
    assert (
        read_execution_source(runner.committer, runner.deliveries[0].delivery_id)
        is not None
    ) == (closure == OwnershipState.CLOSED)


def test_unrelated_unknown_owner_does_not_erase_independent_closed_failed_source(
    tmp_path: Path,
) -> None:
    from artisan.storage.core.execution_source import read_execution_source

    execution = _execution(
        tmp_path,
        PredictMeasure,
        {"structure": [structure(0), structure(1)]},
        batch=BatchStrategy(units_per_worker=1, max_workers=2),
    )
    runner = _IdentifiedFailureRunner(execution.tracker.committer)
    execution.runner = runner
    with ThreadPoolExecutor() as pool:
        future = execution.submit(pool)
        deadline = time.monotonic() + 20
        while len(execution._observed_failures) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(runner.owners) == 2
        runner.owners[0].state = OwnershipState.CLOSED
        runner.owners[1].state = OwnershipState.UNKNOWN
        final = future.result(timeout=30)
    assert final.cancellation_status == CancellationStatus.UNKNOWN
    assert (
        read_execution_source(runner.committer, runner.deliveries[0].delivery_id)
        is not None
    )
    assert (
        read_execution_source(runner.committer, runner.deliveries[1].delivery_id)
        is None
    )


def test_foreign_failed_delivery_cannot_be_selected_as_source(tmp_path: Path) -> None:
    from artisan.storage.core.execution_source import read_execution_source

    execution = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})
    runner = _IdentifiedFailureRunner(execution.tracker.committer)
    runner.foreign = True
    execution.runner = runner
    with ThreadPoolExecutor() as pool:
        future = execution.submit(pool)
        deadline = time.monotonic() + 20
        while not runner.owners and time.monotonic() < deadline:
            time.sleep(0.01)
        runner.owners[0].state = OwnershipState.CLOSED
        final = future.result(timeout=30)
    assert final.status == StepStatus.FAILED
    assert "another assignment attempt" in (final.error or "")
    assert (
        read_execution_source(runner.committer, runner.deliveries[0].delivery_id)
        is None
    )


@pytest.mark.parametrize("raises", [False, True])
def test_final_failure_ownership_reread_persists_unknown_before_terminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, raises: bool
) -> None:
    from artisan.storage.core.execution_source import read_execution_source

    execution = _execution(tmp_path, PredictMeasure, {"structure": [structure(0)]})
    runner = _IdentifiedFailureRunner(execution.tracker.committer)
    execution.runner = runner
    original = execution._dispatch

    def dispatch(
        assignments: Sequence[AdmittedAssignment], software: AssignmentSoftware
    ) -> dict[int, StepUnitFailure]:
        failures = original(assignments, software)
        if raises:

            def unavailable() -> OwnershipState:
                message = "owner observation unavailable"
                raise OSError(message)

            monkeypatch.setattr(runner.owners[0], "ownership_state", unavailable)
        else:
            runner.owners[0].state = OwnershipState.UNKNOWN
        return failures

    monkeypatch.setattr(execution, "_dispatch", dispatch)
    with ThreadPoolExecutor() as pool:
        future = execution.submit(pool)
        deadline = time.monotonic() + 20
        while not execution._observed_failures and time.monotonic() < deadline:
            time.sleep(0.01)
        runner.owners[0].state = OwnershipState.CLOSED
        final = future.result(timeout=30)
    assert final.cancellation_status == CancellationStatus.UNKNOWN
    assert execution.ownership_state() == OwnershipState.UNKNOWN
    assert (
        read_execution_source(runner.committer, runner.deliveries[0].delivery_id)
        is None
    )
