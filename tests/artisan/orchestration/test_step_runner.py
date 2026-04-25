"""Tests for step_runner abstraction in the orchestration layer.

Tests that the Backend namespace, resolve_runner, and the new step_runner=
parameter work correctly across PipelineManager and execute_step.
"""

from __future__ import annotations

import inspect
from unittest.mock import patch

import pytest

from artisan.orchestration.runners import Runner, resolve_runner
from artisan.orchestration.runners.local import LocalRunner
from artisan.orchestration.runners.slurm import SlurmRunner
from artisan.orchestration.runners.slurm_intra import SlurmIntraRunner
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig


class TestBackendRouting:
    """Tests for Runner.LOCAL and Runner.SLURM create_dispatch_handle routing."""

    def test_local_backend_returns_dispatch_handle(self):
        from artisan.orchestration.engine.dispatch_handle import DispatchHandle

        resources = ResourceConfig()
        execution = ExecutionConfig()
        handle = Runner.LOCAL.create_dispatch_handle(
            resources, execution, step_number=0, job_name="test_op"
        )
        assert isinstance(handle, DispatchHandle)

    @patch("prefect_submitit.SlurmTaskRunner")
    def test_slurm_backend_returns_dispatch_handle(self, mock_slurm_runner):
        from artisan.orchestration.runners.slurm import SlurmDispatchHandle

        resources = ResourceConfig(
            cpus=4,
            memory_gb=8,
            gpus=1,
            time_limit="02:00:00",
            extra={"partition": "gpu"},
        )
        execution = ExecutionConfig(units_per_worker=1)

        handle = Runner.SLURM.create_dispatch_handle(
            resources, execution, step_number=3, job_name="test_op"
        )

        assert isinstance(handle, SlurmDispatchHandle)
        mock_slurm_runner.assert_called_once()
        call_kwargs = mock_slurm_runner.call_args[1]
        assert call_kwargs["partition"] == "gpu"
        assert call_kwargs["slurm_job_name"] == "s3_test_op"

    @patch("prefect_submitit.SlurmTaskRunner")
    def test_slurm_intra_backend_returns_dispatch_handle(self, mock_slurm_runner):
        from artisan.orchestration.runners.slurm import SlurmDispatchHandle

        resources = ResourceConfig(cpus=4, memory_gb=8, gpus=1, time_limit="02:00:00")
        execution = ExecutionConfig(units_per_worker=1)

        handle = Runner.SLURM_INTRA.create_dispatch_handle(
            resources, execution, step_number=1, job_name="test"
        )

        assert isinstance(handle, SlurmDispatchHandle)
        mock_slurm_runner.assert_called_once()
        call_kwargs = mock_slurm_runner.call_args[1]
        assert call_kwargs["execution_mode"] == "srun"


class TestResolveBackend:
    """Tests for resolve_runner string/instance/error."""

    def test_resolve_string_local(self):
        assert isinstance(resolve_runner("local"), LocalRunner)

    def test_resolve_string_slurm(self):
        assert isinstance(resolve_runner("slurm"), SlurmRunner)

    def test_passthrough_instance(self):
        step_runner = LocalRunner(default_max_workers=8)
        assert resolve_runner(step_runner) is step_runner

    def test_resolve_string_slurm_intra(self):
        assert isinstance(resolve_runner("slurm_intra"), SlurmIntraRunner)

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown step_runner"):
            resolve_runner("kubernetes")


class TestPipelineManagerBackendParam:
    """Tests for step_runner parameter in PipelineManager.run()."""

    def test_run_signature_has_backend(self):
        from artisan.orchestration.pipeline_manager import PipelineManager

        sig = inspect.signature(PipelineManager.run)
        params = list(sig.parameters.keys())
        assert "step_runner" in params
        assert "compute_backend" not in params

    def test_run_backend_default_is_none(self):
        from artisan.orchestration.pipeline_manager import PipelineManager

        sig = inspect.signature(PipelineManager.run)
        default = sig.parameters["step_runner"].default
        assert default is None


class TestExecuteStepBackendParam:
    """Tests for step_runner in execute_step function."""

    def test_execute_step_signature_has_backend(self):
        from artisan.orchestration.engine.step_executor import execute_step

        sig = inspect.signature(execute_step)
        params = list(sig.parameters.keys())
        assert "step_runner" in params
        assert "compute_backend" not in params
