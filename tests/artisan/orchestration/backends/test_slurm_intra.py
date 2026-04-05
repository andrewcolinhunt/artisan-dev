"""Tests for SlurmIntraBackend."""

from __future__ import annotations

import warnings
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from artisan.orchestration.backends.slurm import SlurmDispatchHandle
from artisan.orchestration.backends.slurm_intra import SlurmIntraBackend
from artisan.orchestration.engine.dispatch_handle import DispatchHandle
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.operation_config.resource_config import ResourceConfig


class TestSlurmIntraBackendTraits:
    def test_name(self) -> None:
        assert SlurmIntraBackend.name == "slurm_intra"

    def test_worker_traits(self) -> None:
        traits = SlurmIntraBackend.worker_traits
        assert traits.worker_id_env_var == "SLURM_STEP_ID"
        assert traits.shared_filesystem is True
        assert traits.needs_staging_fsync is True

    def test_orchestrator_traits(self) -> None:
        traits = SlurmIntraBackend.orchestrator_traits
        assert traits.shared_filesystem is True
        assert traits.needs_staging_verification is True
        assert traits.staging_verification_timeout == 60.0


class TestSlurmIntraBackendCreateDispatchHandle:
    @patch("prefect_submitit.SlurmTaskRunner")
    def test_returns_slurm_dispatch_handle(self, mock_slurm_runner: MagicMock) -> None:
        backend = SlurmIntraBackend()
        resources = ResourceConfig(cpus=4, memory_gb=8, gpus=1, time_limit="02:00:00")
        execution = ExecutionConfig(units_per_worker=1)

        handle = backend.create_dispatch_handle(
            resources, execution, step_number=3, job_name="test_op"
        )
        assert isinstance(handle, DispatchHandle)
        assert isinstance(handle, SlurmDispatchHandle)

    @patch("prefect_submitit.SlurmTaskRunner")
    def test_uses_srun_execution_mode(self, mock_slurm_runner: MagicMock) -> None:
        backend = SlurmIntraBackend()
        resources = ResourceConfig(cpus=4, memory_gb=8, gpus=1, time_limit="02:00:00")
        execution = ExecutionConfig(units_per_worker=1)

        backend.create_dispatch_handle(
            resources, execution, step_number=3, job_name="test_op"
        )

        mock_slurm_runner.assert_called_once()
        call_kwargs = mock_slurm_runner.call_args[1]
        assert call_kwargs["execution_mode"] == "srun"
        assert call_kwargs["gpus_per_node"] == 1
        assert call_kwargs["cpus_per_task"] == 4
        assert call_kwargs["mem_gb"] == 8
        assert call_kwargs["time_limit"] == "02:00:00"
        assert call_kwargs["units_per_worker"] == 1
        # srun mode does not pass partition, slurm_job_name, or slurm_gres
        assert "partition" not in call_kwargs
        assert "slurm_job_name" not in call_kwargs
        assert "slurm_gres" not in call_kwargs

    @patch("prefect_submitit.SlurmTaskRunner")
    def test_passes_extra_kwargs(self, mock_slurm_runner: MagicMock) -> None:
        backend = SlurmIntraBackend()
        resources = ResourceConfig(extra={"constraint": "a100"})
        execution = ExecutionConfig()

        backend.create_dispatch_handle(
            resources, execution, step_number=1, job_name="test"
        )

        call_kwargs = mock_slurm_runner.call_args[1]
        assert call_kwargs["constraint"] == "a100"


class TestSlurmIntraBackendCaptureLogs:
    @patch("artisan.orchestration.engine.dispatch._patch_worker_logs")
    def test_capture_logs_calls_patch(self, mock_patch: MagicMock) -> None:
        backend = SlurmIntraBackend()
        results = [
            UnitResult(success=True, error=None, item_count=1, execution_run_ids=[])
        ]
        backend.capture_logs(results, Path("/staging"), Path("/logs"), "test_op")
        mock_patch.assert_called_once_with(
            results, Path("/staging"), Path("/logs"), "test_op"
        )


class TestSlurmIntraBackendValidateOperation:
    @patch.dict("os.environ", {}, clear=True)
    def test_warns_when_slurm_job_id_missing(self) -> None:
        backend = SlurmIntraBackend()
        mock_op = MagicMock()
        mock_op.name = "test_op"
        with pytest.warns(UserWarning, match="SLURM_JOB_ID is not set"):
            backend.validate_operation(mock_op)

    @patch.dict("os.environ", {"SLURM_JOB_ID": "12345"})
    def test_no_warning_when_slurm_job_id_present(self) -> None:
        backend = SlurmIntraBackend()
        mock_op = MagicMock()
        mock_op.name = "test_op"
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            backend.validate_operation(mock_op)
