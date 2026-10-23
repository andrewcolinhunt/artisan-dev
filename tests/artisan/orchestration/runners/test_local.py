"""Tests for LocalRunner."""

from __future__ import annotations

import time
import warnings
from concurrent.futures import Future
from typing import ClassVar
from unittest.mock import MagicMock, patch

import cloudpickle
import pytest
from pydantic import BaseModel, Field

from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.engine.lifecycle_router import LifecycleRouter
from artisan.orchestration.runners.local import LocalLifecycleRouter, LocalRunner
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.utils.process_call import execute_process_call


class _LocalOperation(OperationDefinition):
    """Concrete local operation for serialized router batches."""

    name: ClassVar[str] = "local_test_operation"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {}

    def execute_function(self, inputs, output_dir):
        raise NotImplementedError


@pytest.fixture
def local_runner() -> LocalRunner:
    return LocalRunner(default_max_workers=2)


@pytest.fixture
def mock_operation() -> MagicMock:
    """Mock operation for validate_operation tests."""
    op = MagicMock()
    op.name = "test_op"
    op.batch_strategy.max_workers = None
    op.runner_resources.gpus = 0
    op.runner_resources.extra = {}
    op.compute_provider.active = "local"
    return op


class TestLocalRunnerTraits:
    def test_name(self) -> None:
        assert LocalRunner.name == "local"

    def test_worker_traits_local(self) -> None:
        traits = LocalRunner.worker_traits
        assert traits.worker_id_env_var is None
        assert traits.shared_filesystem is False

    def test_orchestrator_traits_local(self) -> None:
        traits = LocalRunner.orchestrator_traits
        assert traits.shared_filesystem is False
        assert traits.needs_staging_verification is False


class TestLocalRunnerCreateLifecycleRouter:
    def test_exposes_configured_default_workers(
        self, local_runner: LocalRunner
    ) -> None:
        assert local_runner.default_max_workers == 2

    def test_returns_lifecycle_router(self, local_runner: LocalRunner) -> None:
        handle = local_runner.create_lifecycle_router(
            RunnerResources(), BatchStrategy(), step_number=0, job_name="test_op"
        )
        assert isinstance(handle, LifecycleRouter)
        assert isinstance(handle, LocalLifecycleRouter)

    def test_uses_execution_max_workers(self, local_runner: LocalRunner) -> None:
        handle = local_runner.create_lifecycle_router(
            RunnerResources(),
            BatchStrategy(max_workers=8),
            step_number=0,
            job_name="test_op",
        )
        assert handle._max_workers == 8

    def test_gpu_defaults_to_sequential(self, local_runner: LocalRunner) -> None:
        handle = local_runner.create_lifecycle_router(
            RunnerResources(gpus=1),
            BatchStrategy(),
            step_number=0,
            job_name="test_op",
        )
        assert handle._max_workers == 1

    def test_cpu_defaults_to_pool_size(self, local_runner: LocalRunner) -> None:
        handle = local_runner.create_lifecycle_router(
            RunnerResources(gpus=0),
            BatchStrategy(),
            step_number=0,
            job_name="test_op",
        )
        assert handle._max_workers == 2  # fixture default

    def test_explicit_max_workers_overrides_gpu(
        self, local_runner: LocalRunner
    ) -> None:
        handle = local_runner.create_lifecycle_router(
            RunnerResources(gpus=1),
            BatchStrategy(max_workers=3),
            step_number=0,
            job_name="test_op",
        )
        assert handle._max_workers == 3

    def test_explicit_max_workers_overrides_cpu_default(
        self,
        local_runner: LocalRunner,
    ) -> None:
        handle = local_runner.create_lifecycle_router(
            RunnerResources(gpus=0),
            BatchStrategy(max_workers=6),
            step_number=0,
            job_name="test_op",
        )
        assert handle._max_workers == 6

    def test_passes_units_per_worker_to_router(
        self,
        local_runner: LocalRunner,
    ) -> None:
        handle = local_runner.create_lifecycle_router(
            RunnerResources(),
            BatchStrategy(units_per_worker=7),
            step_number=0,
            job_name="test_op",
        )

        assert handle._units_per_worker == 7

    def test_rejects_non_positive_default_workers(self) -> None:
        with pytest.raises(ValueError, match="default_max_workers"):
            LocalRunner(default_max_workers=0)

    def test_rejects_non_positive_operation_workers(
        self,
        local_runner: LocalRunner,
    ) -> None:
        strategy = BatchStrategy.model_construct(max_workers=0, units_per_worker=1)

        with pytest.raises(ValueError, match="max_workers"):
            local_runner.create_lifecycle_router(
                RunnerResources(),
                strategy,
                step_number=0,
                job_name="test_op",
            )


class TestLocalRunnerValidateOperation:
    def test_no_warning_for_default_resources(
        self, local_runner: LocalRunner, mock_operation: MagicMock
    ) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            local_runner.validate_operation(mock_operation)

    def test_no_warning_for_gpu_only(
        self, local_runner: LocalRunner, mock_operation: MagicMock
    ) -> None:
        mock_operation.runner_resources.gpus = 1
        mock_operation.runner_resources.extra = {}
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            local_runner.validate_operation(mock_operation)

    def test_warns_on_extra_kwargs(
        self, local_runner: LocalRunner, mock_operation: MagicMock
    ) -> None:
        mock_operation.runner_resources.extra = {"partition": "gpu"}
        with pytest.warns(UserWarning, match="provider-specific resources"):
            local_runner.validate_operation(mock_operation)

    def test_warns_on_runner_gpus_with_modal_provider(
        self, local_runner: LocalRunner, mock_operation: MagicMock
    ) -> None:
        """runner_resources.gpus serializes the pool — wrong knob for modal."""
        mock_operation.runner_resources.gpus = 1
        mock_operation.compute_provider.active = "modal"
        with pytest.warns(UserWarning, match="compute_resources.gpu"):
            local_runner.validate_operation(mock_operation)

    def test_modal_command_op_passes_validation(
        self, local_runner: LocalRunner, mock_operation: MagicMock
    ) -> None:
        """The unified path validates modal steps — warns at most, never raises."""
        mock_operation.compute_provider.active = "modal"
        local_runner.validate_operation(mock_operation)


class TestLocalLifecycleRouter:
    def test_cancel_confirms_only_after_worker_and_owner_proof(self) -> None:
        handle = LocalLifecycleRouter(max_workers=1, units_per_worker=1)
        ownership = MagicMock(closed=True)
        ownership.request_cancel.return_value = True
        ownership.acknowledgement.return_value = CancellationAcknowledgement(
            CancellationStatus.CONFIRMED, "Native workers and managed commands stopped"
        )
        handle._dispatch_started = True
        handle._scope_expected = True
        handle._ownership = ownership
        assert handle.cancel().status == CancellationStatus.REQUESTED
        handle._done.set()
        assert handle.cancel().status == CancellationStatus.CONFIRMED
        ownership.request_cancel.assert_called_once()

    def test_cancel_is_unknown_when_either_ownership_proof_is_missing(self) -> None:
        handle = LocalLifecycleRouter(max_workers=1, units_per_worker=1)
        handle._dispatch_started = True
        handle._scope_expected = True
        handle._ownership = MagicMock(
            closed=False, reason="Native worker remains alive"
        )
        assert handle.cancel().status == CancellationStatus.REQUESTED
        handle._done.set()
        assert handle.cancel().status == CancellationStatus.UNKNOWN

    def test_local_exit_without_remote_acknowledgement_is_unknown(self) -> None:
        handle = LocalLifecycleRouter(max_workers=1, units_per_worker=1)
        handle._dispatch_started = True
        handle._scope_expected = True
        handle._ownership = MagicMock(closed=True)
        handle._remote_indices = [0]
        handle._results = [UnitResult(False, "cancelled", 1, [])]
        assert handle.cancel().status == CancellationStatus.REQUESTED
        handle._done.set()
        outcome = handle.cancel()
        assert outcome.status == CancellationStatus.UNKNOWN
        assert "remote cancellation evidence" in outcome.message

    def test_remote_acknowledgement_survives_local_completion_race(self) -> None:
        acknowledgement = CancellationAcknowledgement(
            CancellationStatus.CONFIRMED, "named endpoint call stopped"
        )
        handle = LocalLifecycleRouter(max_workers=1, units_per_worker=1)
        handle._dispatch_started = True
        handle._scope_expected = True
        handle._cancel_requested = True
        handle._ownership = MagicMock(closed=True)
        handle._ownership.acknowledgement.return_value = acknowledgement
        handle._remote_indices = [0]
        handle._results = [
            UnitResult(
                False, "cancelled", 1, [], cancellation_acknowledgement=acknowledgement
            )
        ]
        handle._done.set()
        assert handle.cancel() == acknowledgement

    @patch("artisan.orchestration.runners.local.ProcessPoolExecutor")
    def test_pool_creation_failure_returns_one_result_per_unit(
        self,
        mock_executor_class: MagicMock,
    ) -> None:
        message = "process creation denied"
        mock_executor_class.side_effect = OSError(message)
        handle = LocalLifecycleRouter(max_workers=2, units_per_worker=2)

        units = [MagicMock(), MagicMock()]
        for unit in units:
            unit.get_batch_size.return_value = 1
        results = handle.run(units, MagicMock())

        assert len(results) == 2
        assert all(result.success is False for result in results)
        assert all("process creation denied" in result.error for result in results)

    @patch("artisan.orchestration.runners.local.ProcessPoolExecutor")
    def test_collects_batches_in_submission_order(
        self,
        mock_executor_class: MagicMock,
    ) -> None:
        executor = mock_executor_class.return_value
        first: Future[list[UnitResult]] = Future()
        second: Future[list[UnitResult]] = Future()
        first.set_result(
            [
                UnitResult(True, None, 1, ["a"]),
                UnitResult(True, None, 1, ["b"]),
            ]
        )
        second.set_result([UnitResult(True, None, 1, ["c"])])
        executor.submit.side_effect = [first, second]
        units = [
            ExecutionUnit(
                operation=_LocalOperation(),
                execution_spec_id=f"spec-{index}",
            )
            for index in range(3)
        ]
        handle = LocalLifecycleRouter(max_workers=2, units_per_worker=2)

        results = handle.run(units, MagicMock())

        assert [result.execution_run_ids[0] for result in results] == ["a", "b", "c"]
        calls = executor.submit.call_args_list
        assert all(call.args[0] is execute_process_call for call in calls)
        submitted_batches = [
            cloudpickle.loads(call.args[1].payload)[1][0] for call in calls
        ]
        assert submitted_batches == [
            units[:2],
            units[2:],
        ]
        executor.shutdown.assert_called_once_with(wait=False, cancel_futures=False)

    @patch("artisan.orchestration.runners.local.ProcessPoolExecutor")
    def test_partial_submission_failure_preserves_completed_prefix(
        self,
        mock_executor_class: MagicMock,
    ) -> None:
        executor = mock_executor_class.return_value
        completed: Future[list[UnitResult]] = Future()
        completed.set_result([UnitResult(True, None, 1, ["completed"])])
        executor.submit.side_effect = [completed, OSError("submission denied")]
        handle = LocalLifecycleRouter(max_workers=2, units_per_worker=1)

        units = [MagicMock(), MagicMock(), MagicMock()]
        for unit in units:
            unit.get_batch_size.return_value = 1
        results = handle.run(units, MagicMock())

        assert len(results) == 3
        assert results[0].success is True
        assert results[0].execution_run_ids == ["completed"]
        assert all(result.success is False for result in results[1:])
        assert all("submission denied" in result.error for result in results[1:])
        assert executor.submit.call_count == 2

    def test_cloudpickles_locally_defined_operation_for_spawn(self, tmp_path) -> None:
        class NotebookOperation(OperationDefinition):
            name: ClassVar[str] = "notebook_operation"
            inputs: ClassVar[dict[str, InputSpec]] = {}
            outputs: ClassVar[dict[str, OutputSpec]] = {}

            class Params(BaseModel):
                marker: str = Field(description="Execution marker.")

            params: Params

            def execute_function(self, inputs, output_dir):
                raise NotImplementedError

        def _execute_notebook_batch(
            batch: list[ExecutionUnit],
            runtime_env: RuntimeEnvironment,
        ) -> list[UnitResult]:
            del runtime_env
            return [
                UnitResult(
                    success=True,
                    error=None,
                    item_count=1,
                    execution_run_ids=[unit.operation.params.marker],
                )
                for unit in batch
            ]

        units = [
            ExecutionUnit.model_construct(
                operation=NotebookOperation(
                    params=NotebookOperation.Params(marker=marker)
                ),
                inputs={},
                execution_spec_id=f"spec-{marker}",
                step_number=0,
                group_ids=None,
                user_overrides=None,
                step_run_id=None,
            )
            for marker in ("first", "second")
        ]
        runtime_env = RuntimeEnvironment(
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
        )
        handle = LocalLifecycleRouter(max_workers=1, units_per_worker=2)

        with patch(
            "artisan.orchestration.runners.local.execute_unit_batch",
            _execute_notebook_batch,
        ):
            results = handle.run(units, runtime_env)

        assert [result.execution_run_ids for result in results] == [
            ["first"],
            ["second"],
        ]

    @patch("artisan.orchestration.runners.local.ProcessPoolExecutor")
    def test_future_failure_becomes_one_result_per_batched_unit(
        self,
        mock_executor_class: MagicMock,
    ) -> None:
        executor = mock_executor_class.return_value
        failed: Future[list[UnitResult]] = Future()
        failed.set_exception(OSError("worker unavailable"))
        executor.submit.return_value = failed
        handle = LocalLifecycleRouter(max_workers=1, units_per_worker=2)

        units = [MagicMock(), MagicMock()]
        for unit in units:
            unit.get_batch_size.return_value = 1
        results = handle.run(units, MagicMock())

        assert len(results) == 2
        assert all(result.success is False for result in results)
        assert all("worker unavailable" in result.error for result in results)

    def test_empty_dispatch_avoids_creating_pool(self) -> None:
        handle = LocalLifecycleRouter(max_workers=2, units_per_worker=1)

        with patch(
            "artisan.orchestration.runners.local.ProcessPoolExecutor"
        ) as mock_executor_class:
            assert handle.run([], MagicMock()) == []

        mock_executor_class.assert_not_called()


def test_pending_owner_report_does_not_drop_later_cancellation_deadline():
    from artisan.orchestration.engine.native_pool import _NativePoolOwnership
    from artisan.utils.command_supervisor import OwnershipOutcome

    with patch(
        "artisan.orchestration.engine.native_pool.CommandSupervisor.start"
    ) as start:
        ownership = _NativePoolOwnership()
    ownership._command_outcome = OwnershipOutcome("pending")
    ownership.deadline = time.monotonic() + 10
    deadline = time.monotonic() + 0.2
    assert ownership.request_cancel(deadline)
    start.return_value.request_close.assert_called_once_with(deadline)


def test_cancellation_shortens_deadline_during_ordinary_owner_finish():
    import threading

    from artisan.orchestration.engine.native_pool import _NativePoolOwnership
    from artisan.utils.command_supervisor import OwnershipOutcome

    finishing = threading.Event()
    release = threading.Event()
    with patch(
        "artisan.orchestration.engine.native_pool.CommandSupervisor.start"
    ) as start:
        owner = _NativePoolOwnership()
    supervisor = start.return_value
    supervisor.request_finish.side_effect = lambda deadline: finishing.set()
    supervisor.poll.side_effect = lambda: OwnershipOutcome(
        "closed" if release.is_set() else "pending"
    )
    supervisor.finish.return_value = OwnershipOutcome("closed")
    results = []
    thread = threading.Thread(target=lambda: results.append(owner.finish()))
    thread.start()
    try:
        assert finishing.wait(2)
        deadline = time.monotonic() + 0.5
        assert owner.request_cancel(deadline)
        supervisor.request_close.assert_called_once_with(deadline)
        release.set()
        thread.join(2)
        assert not thread.is_alive()
    finally:
        release.set()
        thread.join(2)
    assert results == [True]
    assert owner.deadline == deadline
    supervisor.finish.assert_called_once_with(deadline)
    assert owner.acknowledgement().status == CancellationStatus.CONFIRMED


def test_final_unknown_keeps_sealed_sibling_results_but_blocks_step_acceptance():
    completed = UnitResult(True, None, 1, ["already-sealed-run"])
    future = Future()
    future.set_result([completed])
    with (
        patch(
            "artisan.orchestration.runners.local._NativePoolOwnership"
        ) as owner_class,
        patch(
            "artisan.orchestration.runners.local.ProcessPoolExecutor"
        ) as executor_class,
    ):
        owner = owner_class.return_value
        owner.finish.return_value = False
        owner.reason = "command ownership unknown"
        executor_class.return_value.submit.return_value = future
        handle = LocalLifecycleRouter(max_workers=1, units_per_worker=1)
        results = handle.run([MagicMock()], MagicMock())
    assert results == [completed]
    assert handle.cancellation_acknowledgement.status == CancellationStatus.UNKNOWN


def test_already_failed_pool_future_still_triggers_owner_cleanup():
    from concurrent.futures.process import BrokenProcessPool

    from artisan.orchestration.runners.local import _collect_batch_futures

    future = Future()
    future.set_exception(BrokenProcessPool("worker lost"))
    owner = MagicMock()
    owner.supervisor.attachment = {"secret": "private-test-capability"}
    result = _collect_batch_futures([[MagicMock()]], [future], owner)
    owner.request_failure_cleanup.assert_called_once()
    assert result[0].success is False


def test_bootstrap_future_error_cannot_persist_owner_capability():
    from artisan.orchestration.runners.local import _collect_batch_futures

    future = Future()
    future.set_exception(ValueError("private-test-capability"))
    owner = MagicMock()
    owner.supervisor.attachment = {"secret": "private-test-capability"}
    result = _collect_batch_futures([[MagicMock()]], [future], owner)
    assert "private-test-capability" not in result[0].error
    assert "<redacted>" in result[0].error


def test_submission_failure_starts_cleanup_of_prior_work():
    handle = LocalLifecycleRouter(max_workers=1, units_per_worker=1)
    handle._executor = MagicMock()
    handle._ownership = MagicMock()
    handle._ownership.supervisor.attachment = {"secret": "private-test-capability"}
    running = Future()
    running.set_running_or_notify_cancel()
    handle._executor.submit.side_effect = [running, OSError("cannot submit")]
    futures = handle._submit_batches([[MagicMock()], [MagicMock()]], MagicMock())
    handle._ownership.request_failure_cleanup.assert_called_once()
    assert len(futures) == 2
    assert futures[0] is running
    assert futures[1].result()[0].success is False


def test_cancellation_reaches_owner_during_blocked_serialization():
    import threading

    entered = threading.Event()
    release = threading.Event()
    handle = LocalLifecycleRouter(max_workers=1, units_per_worker=1)
    handle._dispatch_started = True
    handle._scope_expected = True
    handle._executor = MagicMock()
    handle._ownership = MagicMock()
    handle._ownership.supervisor.attachment = {"secret": "private-test-capability"}

    def serialize(*args):
        entered.set()
        assert release.wait(3)

    with patch(
        "artisan.orchestration.runners.local.serialize_process_call",
        side_effect=serialize,
    ):
        thread = threading.Thread(
            target=handle._submit_batches, args=([[MagicMock()]], MagicMock())
        )
        thread.start()
        try:
            assert entered.wait(2)
            started = time.monotonic()
            assert handle.cancel().status == CancellationStatus.REQUESTED
            assert time.monotonic() - started < 0.2
            handle._ownership.request_cancel.assert_called_once()
        finally:
            release.set()
            thread.join(3)
        assert not thread.is_alive()
    handle._executor.submit.assert_not_called()


def test_pending_dispatch_cannot_confirm_empty_workers_or_block_on_shutdown():
    from artisan.orchestration.engine.native_pool import _NativePoolOwnership
    from artisan.utils.command_supervisor import OwnershipOutcome

    with patch(
        "artisan.orchestration.engine.native_pool.CommandSupervisor.start"
    ) as start:
        owner = _NativePoolOwnership()
    start.return_value.poll.return_value = OwnershipOutcome("closed")
    start.return_value.finish.return_value = OwnershipOutcome("closed")
    assert owner.begin_dispatch()
    owner.executor = MagicMock()
    owner.executor._processes = {}
    assert not owner._workers_exited()
    deadline = time.monotonic() + 0.05
    owner.request_cancel(deadline)
    assert owner.finish(failed=True) is False
    assert time.monotonic() - deadline < 0.15
    owner.executor.shutdown.assert_not_called()
    assert owner.acknowledgement().status == CancellationStatus.UNKNOWN
    owner.end_dispatch()
    owner.executor.shutdown.assert_called_once_with(wait=False, cancel_futures=True)
    assert owner.acknowledgement().status == CancellationStatus.UNKNOWN
    assert owner.begin_dispatch() is False


def test_cancellation_reaches_owner_during_blocked_pool_construction():
    import threading

    from artisan.orchestration.engine.native_pool import _NativePoolOwnership
    from artisan.utils.command_supervisor import OwnershipOutcome

    entered = threading.Event()
    release = threading.Event()
    handle = LocalLifecycleRouter(max_workers=1, units_per_worker=1)
    handle._dispatch_started = True
    handle._scope_expected = True
    executor = MagicMock()
    executor._processes = {}

    def construct(**kwargs):
        entered.set()
        assert release.wait(3)
        return executor

    with patch(
        "artisan.orchestration.engine.native_pool.CommandSupervisor.start"
    ) as start:
        owner = _NativePoolOwnership()
    start.return_value.poll.return_value = OwnershipOutcome("closed")
    start.return_value.finish.return_value = OwnershipOutcome("closed")
    with (
        patch(
            "artisan.orchestration.runners.local._NativePoolOwnership",
            return_value=owner,
        ),
        patch(
            "artisan.orchestration.runners.local.ProcessPoolExecutor",
            side_effect=construct,
        ),
    ):
        thread = threading.Thread(
            target=handle._execute_batches, args=([[MagicMock()]], MagicMock())
        )
        thread.start()
        try:
            assert entered.wait(2)
            started = time.monotonic()
            assert handle.cancel().status == CancellationStatus.REQUESTED
            assert time.monotonic() - started < 0.2
            start.return_value.request_close.assert_called_once()
            assert not owner._workers_exited()
        finally:
            release.set()
            thread.join(3)
        assert not thread.is_alive()
    executor.submit.assert_not_called()
    assert owner.acknowledgement().status == CancellationStatus.CONFIRMED


def test_late_worker_dispatch_keeps_original_deadline_and_exact_handles():
    import os

    from artisan.orchestration.engine.native_pool import _NativePoolOwnership

    with patch("artisan.orchestration.engine.native_pool.CommandSupervisor.start"):
        owner = _NativePoolOwnership()
    assert owner.begin_dispatch()
    owner.deadline = time.monotonic() - 1
    owner._finished = True
    read_fd, write_fd = os.pipe()
    worker = MagicMock(sentinel=read_fd)
    owner.executor = MagicMock()
    owner.executor._processes = {123: worker}
    try:
        with patch(
            "artisan.orchestration.engine.native_pool._terminate_process_pool"
        ) as terminate:
            owner.end_dispatch()
        assert terminate.call_args.kwargs["deadline"] == owner.deadline
        assert terminate.call_args.kwargs["processes"] == (worker,)
        assert len(terminate.call_args.kwargs["sentinels"]) == 1
        assert owner._sentinels == {}
        assert not owner.closed
    finally:
        os.close(read_fd)
        os.close(write_fd)
