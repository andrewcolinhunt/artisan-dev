"""Tests for LocalBackend."""

from __future__ import annotations

import warnings
from unittest.mock import MagicMock, patch

import pytest

from artisan.orchestration.backends.local import LocalBackend
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig


@pytest.fixture
def local_backend() -> LocalBackend:
    return LocalBackend(default_max_workers=2)


@pytest.fixture
def mock_operation() -> MagicMock:
    """Mock operation for validate_operation tests."""
    op = MagicMock()
    op.name = "test_op"
    op.execution.max_workers = None
    op.resources.gpus = 0
    op.resources.extra = {}
    return op


class TestLocalBackendTraits:
    def test_name(self) -> None:
        assert LocalBackend.name == "local"

    def test_worker_traits_local(self) -> None:
        traits = LocalBackend.worker_traits
        assert traits.worker_id_env_var is None
        assert traits.shared_filesystem is False
        assert traits.needs_staging_fsync is False

    def test_orchestrator_traits_local(self) -> None:
        traits = LocalBackend.orchestrator_traits
        assert traits.shared_filesystem is False
        assert traits.needs_staging_verification is False


class TestLocalBackendCreateFlow:
    @patch("prefect.flow")
    @patch("prefect.unmapped")
    def test_create_flow_returns_callable(
        self,
        _mock_unmapped: MagicMock,
        mock_flow: MagicMock,
        local_backend: LocalBackend,
    ) -> None:
        mock_flow.return_value = lambda fn: fn
        result = local_backend.create_flow(
            ResourceConfig(), ExecutionConfig(), step_number=0, job_name="test_op"
        )
        assert callable(result)

    @patch("prefect.flow")
    @patch("prefect.unmapped")
    def test_create_flow_uses_execution_max_workers(
        self,
        _mock_unmapped: MagicMock,
        mock_flow: MagicMock,
        local_backend: LocalBackend,
    ) -> None:
        mock_flow.return_value = lambda fn: fn
        local_backend.create_flow(
            ResourceConfig(),
            ExecutionConfig(max_workers=8),
            step_number=0,
            job_name="test_op",
        )
        call_kwargs = mock_flow.call_args[1]
        task_runner = call_kwargs["task_runner"]
        assert task_runner._max_workers == 8

    @patch("prefect.flow")
    @patch("prefect.unmapped")
    def test_create_flow_uses_process_pool_task_runner(
        self,
        _mock_unmapped: MagicMock,
        mock_flow: MagicMock,
        local_backend: LocalBackend,
    ) -> None:
        from prefect.task_runners import ProcessPoolTaskRunner

        mock_flow.return_value = lambda fn: fn
        local_backend.create_flow(
            ResourceConfig(), ExecutionConfig(), step_number=0, job_name="test_op"
        )
        call_kwargs = mock_flow.call_args[1]
        task_runner = call_kwargs["task_runner"]
        assert isinstance(task_runner, ProcessPoolTaskRunner)


class TestLocalBackendValidateOperation:
    def test_no_warning_for_default_resources(
        self, local_backend: LocalBackend, mock_operation: MagicMock
    ) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            local_backend.validate_operation(mock_operation)

    def test_warns_on_gpus(
        self, local_backend: LocalBackend, mock_operation: MagicMock
    ) -> None:
        mock_operation.resources.gpus = 1
        with pytest.warns(UserWarning, match="SLURM-specific resources"):
            local_backend.validate_operation(mock_operation)

    def test_warns_on_extra_kwargs(
        self, local_backend: LocalBackend, mock_operation: MagicMock
    ) -> None:
        mock_operation.resources.extra = {"partition": "gpu"}
        with pytest.warns(UserWarning, match="SLURM-specific resources"):
            local_backend.validate_operation(mock_operation)


class TestLocalBackendCaptureLogs:
    def test_capture_logs_is_noop(self, local_backend: LocalBackend) -> None:
        results = [{"success": True}]
        local_backend.capture_logs(results, MagicMock(), None, "test_op")
