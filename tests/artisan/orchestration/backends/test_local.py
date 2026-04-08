"""Tests for LocalBackend."""

from __future__ import annotations

import warnings
from unittest.mock import MagicMock

import pytest

from artisan.orchestration.backends.local import LocalBackend, LocalDispatchHandle
from artisan.orchestration.engine.dispatch_handle import DispatchHandle
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.execution.unit_result import UnitResult
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


class TestLocalBackendCreateDispatchHandle:
    def test_returns_dispatch_handle(self, local_backend: LocalBackend) -> None:
        handle = local_backend.create_dispatch_handle(
            ResourceConfig(), ExecutionConfig(), step_number=0, job_name="test_op"
        )
        assert isinstance(handle, DispatchHandle)
        assert isinstance(handle, LocalDispatchHandle)

    def test_uses_execution_max_workers(self, local_backend: LocalBackend) -> None:
        handle = local_backend.create_dispatch_handle(
            ResourceConfig(),
            ExecutionConfig(max_workers=8),
            step_number=0,
            job_name="test_op",
        )
        assert handle._task_runner._max_workers == 8

    def test_gpu_defaults_to_sequential(self, local_backend: LocalBackend) -> None:
        handle = local_backend.create_dispatch_handle(
            ResourceConfig(gpus=1),
            ExecutionConfig(),
            step_number=0,
            job_name="test_op",
        )
        assert handle._task_runner._max_workers == 1

    def test_cpu_defaults_to_pool_size(self, local_backend: LocalBackend) -> None:
        handle = local_backend.create_dispatch_handle(
            ResourceConfig(gpus=0),
            ExecutionConfig(),
            step_number=0,
            job_name="test_op",
        )
        assert handle._task_runner._max_workers == 2  # fixture default

    def test_explicit_max_workers_overrides_gpu(
        self, local_backend: LocalBackend
    ) -> None:
        handle = local_backend.create_dispatch_handle(
            ResourceConfig(gpus=1),
            ExecutionConfig(max_workers=3),
            step_number=0,
            job_name="test_op",
        )
        assert handle._task_runner._max_workers == 3

    def test_explicit_max_workers_overrides_cpu_default(
        self,
        local_backend: LocalBackend,
    ) -> None:
        handle = local_backend.create_dispatch_handle(
            ResourceConfig(gpus=0),
            ExecutionConfig(max_workers=6),
            step_number=0,
            job_name="test_op",
        )
        assert handle._task_runner._max_workers == 6


class TestLocalBackendValidateOperation:
    def test_no_warning_for_default_resources(
        self, local_backend: LocalBackend, mock_operation: MagicMock
    ) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            local_backend.validate_operation(mock_operation)

    def test_no_warning_for_gpu_only(
        self, local_backend: LocalBackend, mock_operation: MagicMock
    ) -> None:
        mock_operation.resources.gpus = 1
        mock_operation.resources.extra = {}
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            local_backend.validate_operation(mock_operation)

    def test_warns_on_extra_kwargs(
        self, local_backend: LocalBackend, mock_operation: MagicMock
    ) -> None:
        mock_operation.resources.extra = {"partition": "gpu"}
        with pytest.warns(UserWarning, match="SLURM-specific resources"):
            local_backend.validate_operation(mock_operation)


class TestLocalBackendCaptureLogs:
    def test_capture_logs_is_noop(self, local_backend: LocalBackend) -> None:
        results = [
            UnitResult(success=True, error=None, item_count=1, execution_run_ids=[])
        ]
        local_backend.capture_logs(results, MagicMock(), None, "test_op", 1)
