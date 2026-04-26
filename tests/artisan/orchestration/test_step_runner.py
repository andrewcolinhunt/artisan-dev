"""Tests for step_runner abstraction in the orchestration layer.

Tests that the Runner namespace and the step_runner= parameter work
correctly across PipelineManager and execute_step. Resolution-only
tests live in runners/test_resolve.py.
"""

from __future__ import annotations

import inspect
from unittest.mock import patch

from artisan.orchestration.runners import Runner
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.runner_resources import RunnerResources


class TestRunnerRouting:
    """Tests for Runner.LOCAL and Runner.SLURM create_dispatch_handle routing."""

    def test_local_runner_returns_dispatch_handle(self):
        from artisan.orchestration.engine.dispatch_handle import DispatchHandle

        runner_resources = RunnerResources()
        batch_strategy = BatchStrategy()
        handle = Runner.LOCAL.create_dispatch_handle(
            runner_resources, batch_strategy, step_number=0, job_name="test_op"
        )
        assert isinstance(handle, DispatchHandle)

    @patch("prefect_submitit.SlurmTaskRunner")
    def test_slurm_runner_returns_dispatch_handle(self, mock_slurm_runner):
        from artisan.orchestration.runners.slurm import SlurmDispatchHandle

        runner_resources = RunnerResources(
            cpus=4,
            memory_gb=8,
            gpus=1,
            time_limit="02:00:00",
            extra={"partition": "gpu"},
        )
        batch_strategy = BatchStrategy(units_per_worker=1)

        handle = Runner.SLURM.create_dispatch_handle(
            runner_resources, batch_strategy, step_number=3, job_name="test_op"
        )

        assert isinstance(handle, SlurmDispatchHandle)
        mock_slurm_runner.assert_called_once()
        call_kwargs = mock_slurm_runner.call_args[1]
        assert call_kwargs["partition"] == "gpu"
        assert call_kwargs["slurm_job_name"] == "s3_test_op"

    @patch("prefect_submitit.SlurmTaskRunner")
    def test_slurm_intra_runner_returns_dispatch_handle(self, mock_slurm_runner):
        from artisan.orchestration.runners.slurm import SlurmDispatchHandle

        runner_resources = RunnerResources(
            cpus=4, memory_gb=8, gpus=1, time_limit="02:00:00"
        )
        batch_strategy = BatchStrategy(units_per_worker=1)

        handle = Runner.SLURM_INTRA.create_dispatch_handle(
            runner_resources, batch_strategy, step_number=1, job_name="test"
        )

        assert isinstance(handle, SlurmDispatchHandle)
        mock_slurm_runner.assert_called_once()
        call_kwargs = mock_slurm_runner.call_args[1]
        assert call_kwargs["execution_mode"] == "srun"


class TestPipelineManagerStepRunnerParam:
    """Tests for step_runner parameter in PipelineManager.run()."""

    def test_run_signature_has_step_runner(self):
        from artisan.orchestration.pipeline_manager import PipelineManager

        sig = inspect.signature(PipelineManager.run)
        params = list(sig.parameters.keys())
        assert "step_runner" in params
        assert "compute_backend" not in params

    def test_run_step_runner_default_is_none(self):
        from artisan.orchestration.pipeline_manager import PipelineManager

        sig = inspect.signature(PipelineManager.run)
        default = sig.parameters["step_runner"].default
        assert default is None


class TestExecuteStepStepRunnerParam:
    """Tests for step_runner in execute_step function."""

    def test_execute_step_signature_has_step_runner(self):
        from artisan.orchestration.engine.step_executor import execute_step

        sig = inspect.signature(execute_step)
        params = list(sig.parameters.keys())
        assert "step_runner" in params
        assert "compute_backend" not in params
