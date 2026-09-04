"""Tests for LocalRunner."""

from __future__ import annotations

import warnings
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

import pytest

from artisan.orchestration.engine.lifecycle_router import LifecycleRouter
from artisan.orchestration.runners.local import LocalLifecycleRouter, LocalRunner
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.operation_config.runner_resources import RunnerResources


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
    @patch("artisan.orchestration.runners.local.ProcessPoolExecutor")
    def test_pool_creation_failure_returns_one_result_per_unit(
        self,
        mock_executor_class: MagicMock,
    ) -> None:
        message = "process creation denied"
        mock_executor_class.side_effect = OSError(message)
        handle = LocalLifecycleRouter(max_workers=2, units_per_worker=2)

        results = handle.run([MagicMock(), MagicMock()], MagicMock())

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
        units = [MagicMock(), MagicMock(), MagicMock()]
        handle = LocalLifecycleRouter(max_workers=2, units_per_worker=2)

        results = handle.run(units, MagicMock())

        assert [result.execution_run_ids[0] for result in results] == ["a", "b", "c"]
        assert [call.args[1] for call in executor.submit.call_args_list] == [
            units[:2],
            units[2:],
        ]
        executor.shutdown.assert_called_once_with(wait=True, cancel_futures=False)

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

        results = handle.run([MagicMock(), MagicMock()], MagicMock())

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
