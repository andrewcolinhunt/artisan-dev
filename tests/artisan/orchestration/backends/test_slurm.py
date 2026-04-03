"""Tests for SlurmBackend."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from artisan.orchestration.backends.slurm import SlurmBackend
from artisan.schemas.execution.execution_config import ExecutionConfig
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


class TestSlurmBackendCreateFlow:
    @patch("prefect_submitit.SlurmTaskRunner")
    @patch("prefect.flow")
    @patch("prefect.unmapped")
    def test_create_flow_uses_slurm_task_runner(
        self,
        _mock_unmapped: MagicMock,
        mock_flow: MagicMock,
        mock_slurm_runner: MagicMock,
    ) -> None:
        mock_flow.return_value = lambda fn: fn
        backend = SlurmBackend()

        resources = ResourceConfig(
            cpus=4,
            memory_gb=8,
            gpus=1,
            time_limit="02:00:00",
            extra={"partition": "gpu"},
        )
        execution = ExecutionConfig(units_per_worker=1)

        backend.create_flow(
            resources,
            execution,
            step_number=3,
            job_name="test_op",
            log_folder=Path("/runs/pipeline/logs/slurm"),
        )

        mock_slurm_runner.assert_called_once()
        call_kwargs = mock_slurm_runner.call_args[1]
        assert call_kwargs["partition"] == "gpu"
        assert call_kwargs["mem_gb"] == 8
        assert call_kwargs["slurm_gres"] == "gpu:1"
        assert call_kwargs["slurm_job_name"] == "s3_test_op"
        assert call_kwargs["log_folder"] == "/runs/pipeline/logs/slurm"

    @patch("prefect_submitit.SlurmTaskRunner")
    @patch("prefect.flow")
    @patch("prefect.unmapped")
    def test_create_flow_uses_custom_job_name(
        self,
        _mock_unmapped: MagicMock,
        mock_flow: MagicMock,
        mock_slurm_runner: MagicMock,
    ) -> None:
        mock_flow.return_value = lambda fn: fn
        backend = SlurmBackend()

        resources = ResourceConfig()
        execution = ExecutionConfig(units_per_worker=1)

        backend.create_flow(resources, execution, step_number=5, job_name="custom_name")

        call_kwargs = mock_slurm_runner.call_args[1]
        assert call_kwargs["slurm_job_name"] == "s5_custom_name"
        assert "log_folder" not in call_kwargs


class TestSlurmBackendCaptureLogs:
    @patch("artisan.orchestration.engine.dispatch._patch_worker_logs")
    def test_capture_logs_calls_patch(self, mock_patch: MagicMock) -> None:
        backend = SlurmBackend()
        results = [{"success": True}]
        backend.capture_logs(results, Path("/staging"), Path("/logs"), "test_op")
        mock_patch.assert_called_once_with(
            results, Path("/staging"), Path("/logs"), "test_op"
        )
