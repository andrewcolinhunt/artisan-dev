"""Tests for LocalBackend."""

from __future__ import annotations

import warnings
from unittest.mock import MagicMock, patch

import pytest

from artisan.orchestration.backends.local import LocalBackend


@pytest.fixture
def local_backend() -> LocalBackend:
    return LocalBackend(default_max_workers=2)


@pytest.fixture
def mock_operation() -> MagicMock:
    op = MagicMock()
    op.name = "test_op"
    op.execution.max_workers = None
    op.resources.gres = None
    op.resources.partition = "cpu"
    op.resources.extra_slurm_kwargs = {}
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
        mock_operation: MagicMock,
    ) -> None:
        mock_flow.return_value = lambda fn: fn
        result = local_backend.create_flow(mock_operation, step_number=0)
        assert callable(result)

    @patch("prefect.flow")
    @patch("prefect.unmapped")
    def test_create_flow_uses_operation_max_workers(
        self,
        _mock_unmapped: MagicMock,
        mock_flow: MagicMock,
        local_backend: LocalBackend,
        mock_operation: MagicMock,
    ) -> None:
        mock_operation.execution.max_workers = 8
        mock_flow.return_value = lambda fn: fn
        local_backend.create_flow(mock_operation, step_number=0)
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
        mock_operation: MagicMock,
    ) -> None:
        from prefect.task_runners import ProcessPoolTaskRunner

        mock_flow.return_value = lambda fn: fn
        local_backend.create_flow(mock_operation, step_number=0)
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

    def test_warns_on_slurm_gres(
        self, local_backend: LocalBackend, mock_operation: MagicMock
    ) -> None:
        mock_operation.resources.gres = "gpu:1"
        with pytest.warns(UserWarning, match="SLURM-specific resources"):
            local_backend.validate_operation(mock_operation)

    def test_warns_on_non_cpu_partition(
        self, local_backend: LocalBackend, mock_operation: MagicMock
    ) -> None:
        mock_operation.resources.partition = "gpu"
        with pytest.warns(UserWarning, match="SLURM-specific resources"):
            local_backend.validate_operation(mock_operation)


class TestLocalBackendCaptureLogs:
    def test_capture_logs_is_noop(self, local_backend: LocalBackend) -> None:
        results = [{"success": True}]
        local_backend.capture_logs(results, MagicMock(), None, "test_op")
