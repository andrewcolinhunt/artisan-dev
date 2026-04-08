"""Tests for SlurmBackend."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from artisan.orchestration.backends.slurm import SlurmBackend, SlurmDispatchHandle
from artisan.orchestration.engine.dispatch_handle import DispatchHandle
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.operation_config.resource_config import ResourceConfig


class TestSlurmBackendTraits:
    def test_name(self) -> None:
        assert SlurmBackend.name == "slurm"

    def test_worker_traits(self) -> None:
        traits = SlurmBackend.worker_traits
        assert traits.worker_id_env_var == "SLURM_ARRAY_TASK_ID"
        assert traits.shared_filesystem is True
        assert traits.needs_staging_fsync is True

    def test_orchestrator_traits(self) -> None:
        traits = SlurmBackend.orchestrator_traits
        assert traits.shared_filesystem is True
        assert traits.needs_staging_verification is True
        assert traits.staging_verification_timeout == 60.0


class TestSlurmBackendCreateDispatchHandle:
    @patch("prefect_submitit.SlurmTaskRunner")
    def test_returns_slurm_dispatch_handle(self, mock_slurm_runner: MagicMock) -> None:
        backend = SlurmBackend()
        resources = ResourceConfig()
        execution = ExecutionConfig(units_per_worker=1)

        handle = backend.create_dispatch_handle(
            resources, execution, step_number=0, job_name="test_op"
        )
        assert isinstance(handle, DispatchHandle)
        assert isinstance(handle, SlurmDispatchHandle)

    @patch("prefect_submitit.SlurmTaskRunner")
    def test_configures_slurm_task_runner(self, mock_slurm_runner: MagicMock) -> None:
        backend = SlurmBackend()
        resources = ResourceConfig(
            cpus=4,
            memory_gb=8,
            gpus=1,
            time_limit="02:00:00",
            extra={"partition": "gpu"},
        )
        execution = ExecutionConfig(units_per_worker=1)

        backend.create_dispatch_handle(
            resources,
            execution,
            step_number=3,
            job_name="test_op",
            log_folder="/runs/pipeline/logs/slurm",
        )

        mock_slurm_runner.assert_called_once()
        call_kwargs = mock_slurm_runner.call_args[1]
        assert call_kwargs["partition"] == "gpu"
        assert call_kwargs["mem_gb"] == 8
        assert call_kwargs["slurm_gres"] == "gpu:1"
        assert call_kwargs["slurm_job_name"] == "s3_test_op"
        assert call_kwargs["log_folder"] == "/runs/pipeline/logs/slurm"

    @patch("prefect_submitit.SlurmTaskRunner")
    def test_uses_custom_job_name(self, mock_slurm_runner: MagicMock) -> None:
        backend = SlurmBackend()
        resources = ResourceConfig()
        execution = ExecutionConfig(units_per_worker=1)

        handle = backend.create_dispatch_handle(
            resources, execution, step_number=5, job_name="custom_name"
        )

        call_kwargs = mock_slurm_runner.call_args[1]
        assert call_kwargs["slurm_job_name"] == "s5_custom_name"
        assert "log_folder" not in call_kwargs
        assert handle._job_name == "s5_custom_name"


class TestSlurmDispatchHandleCancel:
    @patch("artisan.orchestration.backends.slurm.subprocess")
    def test_cancel_calls_scancel(self, mock_subprocess: MagicMock) -> None:
        handle = SlurmDispatchHandle(
            task_runner=MagicMock(),
            job_name="s3_test_op",
            staging_root="/staging",
            step_number=3,
        )
        handle.cancel()
        mock_subprocess.run.assert_called_once_with(
            ["scancel", "--name", "s3_test_op"],
            check=False,
            capture_output=True,
        )

    @patch("artisan.orchestration.backends.slurm.subprocess")
    def test_cancel_is_idempotent(self, mock_subprocess: MagicMock) -> None:
        handle = SlurmDispatchHandle(
            task_runner=MagicMock(),
            job_name="s1_op",
            staging_root="/staging",
            step_number=1,
        )
        handle.cancel()
        handle.cancel()
        assert mock_subprocess.run.call_count == 2

    @patch("artisan.orchestration.backends.slurm.subprocess")
    def test_cancel_swallows_exceptions(self, mock_subprocess: MagicMock) -> None:
        mock_subprocess.run.side_effect = FileNotFoundError("scancel not found")
        handle = SlurmDispatchHandle(
            task_runner=MagicMock(),
            job_name="s1_op",
            staging_root="/staging",
            step_number=1,
        )
        handle.cancel()  # Should not raise


class TestSlurmBackendCaptureLogs:
    @patch("artisan.orchestration.engine.dispatch._patch_worker_logs")
    def test_capture_logs_calls_patch(self, mock_patch: MagicMock) -> None:
        backend = SlurmBackend()
        results = [
            UnitResult(success=True, error=None, item_count=1, execution_run_ids=[])
        ]
        backend.capture_logs(results, "/staging", "/logs", "test_op", 1)
        mock_patch.assert_called_once_with(results, "/staging", "/logs", "test_op", 1)
