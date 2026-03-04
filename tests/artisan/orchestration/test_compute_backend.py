"""Tests for backend abstraction in the orchestration layer.

Tests that the Backend namespace, resolve_backend, and the new backend=
parameter work correctly across PipelineManager and execute_step.
"""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch

import pytest

from artisan.orchestration.backends import Backend, resolve_backend
from artisan.orchestration.backends.local import LocalBackend
from artisan.orchestration.backends.slurm import SlurmBackend


class TestBackendRouting:
    """Tests for Backend.LOCAL and Backend.SLURM create_flow routing."""

    @patch("prefect.flow")
    @patch("prefect.unmapped")
    def test_local_backend_create_flow_returns_callable(
        self, _mock_unmapped, mock_flow
    ):
        mock_flow.return_value = lambda fn: fn
        op = MagicMock()
        op.name = "test_op"
        op.execution.max_workers = None
        result = Backend.LOCAL.create_flow(op, step_number=0)
        assert callable(result)

    @patch("prefect_submitit.SlurmTaskRunner")
    @patch("prefect.flow")
    @patch("prefect.unmapped")
    def test_slurm_backend_create_flow_uses_slurm_runner(
        self, _mock_unmapped, mock_flow, mock_slurm_runner
    ):
        mock_flow.return_value = lambda fn: fn
        op = MagicMock()
        op.name = "test_op"
        op.resources.partition = "gpu"
        op.resources.time_limit = "02:00:00"
        op.resources.mem_gb = 8
        op.resources.cpus_per_task = 4
        op.resources.gres = "gpu:1"
        op.resources.extra_slurm_kwargs = {}
        op.execution.units_per_worker = 1
        op.execution.job_name = None

        Backend.SLURM.create_flow(op, step_number=3)

        mock_slurm_runner.assert_called_once()
        call_kwargs = mock_slurm_runner.call_args[1]
        assert call_kwargs["partition"] == "gpu"
        assert call_kwargs["slurm_job_name"] == "s3_test_op"


class TestResolveBackend:
    """Tests for resolve_backend string/instance/error."""

    def test_resolve_string_local(self):
        assert isinstance(resolve_backend("local"), LocalBackend)

    def test_resolve_string_slurm(self):
        assert isinstance(resolve_backend("slurm"), SlurmBackend)

    def test_passthrough_instance(self):
        backend = LocalBackend(default_max_workers=8)
        assert resolve_backend(backend) is backend

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown backend"):
            resolve_backend("kubernetes")


class TestPipelineManagerBackendParam:
    """Tests for backend parameter in PipelineManager.run()."""

    def test_run_signature_has_backend(self):
        from artisan.orchestration.pipeline_manager import PipelineManager

        sig = inspect.signature(PipelineManager.run)
        params = list(sig.parameters.keys())
        assert "backend" in params
        assert "compute_backend" not in params

    def test_run_backend_default_is_none(self):
        from artisan.orchestration.pipeline_manager import PipelineManager

        sig = inspect.signature(PipelineManager.run)
        default = sig.parameters["backend"].default
        assert default is None


class TestExecuteStepBackendParam:
    """Tests for backend in execute_step function."""

    def test_execute_step_signature_has_backend(self):
        from artisan.orchestration.engine.step_executor import execute_step

        sig = inspect.signature(execute_step)
        params = list(sig.parameters.keys())
        assert "backend" in params
        assert "compute_backend" not in params
