"""Tests for backend base classes and traits."""

from __future__ import annotations

import pytest

from artisan.orchestration.backends.base import (
    BackendBase,
    OrchestratorTraits,
    WorkerTraits,
)


class TestWorkerTraits:
    def test_default_traits(self) -> None:
        traits = WorkerTraits()
        assert traits.worker_id_env_var is None
        assert traits.shared_filesystem is False
        assert traits.needs_staging_fsync is False

    def test_shared_filesystem_enables_fsync(self) -> None:
        traits = WorkerTraits(shared_filesystem=True)
        assert traits.needs_staging_fsync is True

    def test_frozen(self) -> None:
        traits = WorkerTraits()
        with pytest.raises(AttributeError):
            traits.shared_filesystem = True  # type: ignore[misc]


class TestOrchestratorTraits:
    def test_default_traits(self) -> None:
        traits = OrchestratorTraits()
        assert traits.shared_filesystem is False
        assert traits.staging_verification_timeout == 60.0
        assert traits.needs_staging_verification is False

    def test_shared_filesystem_enables_verification(self) -> None:
        traits = OrchestratorTraits(shared_filesystem=True)
        assert traits.needs_staging_verification is True

    def test_custom_timeout(self) -> None:
        traits = OrchestratorTraits(
            shared_filesystem=True, staging_verification_timeout=120.0
        )
        assert traits.staging_verification_timeout == 120.0

    def test_frozen(self) -> None:
        traits = OrchestratorTraits()
        with pytest.raises(AttributeError):
            traits.shared_filesystem = True  # type: ignore[misc]


class TestInitSubclassValidation:
    def test_missing_name_raises(self) -> None:
        with pytest.raises(TypeError, match="must define 'name'"):

            class BadBackend(BackendBase):
                worker_traits = WorkerTraits()
                orchestrator_traits = OrchestratorTraits()

                def create_dispatch_handle(self, *a, **kw):
                    pass

                def capture_logs(
                    self, results, staging_root, failure_logs_root, op, step
                ):
                    pass

    def test_missing_worker_traits_raises(self) -> None:
        with pytest.raises(TypeError, match="must define 'worker_traits'"):

            class BadBackend(BackendBase):
                name = "bad"
                orchestrator_traits = OrchestratorTraits()

                def create_dispatch_handle(self, *a, **kw):
                    pass

                def capture_logs(
                    self, results, staging_root, failure_logs_root, op, step
                ):
                    pass

    def test_missing_orchestrator_traits_raises(self) -> None:
        with pytest.raises(TypeError, match="must define 'orchestrator_traits'"):

            class BadBackend(BackendBase):
                name = "bad"
                worker_traits = WorkerTraits()

                def create_dispatch_handle(self, *a, **kw):
                    pass

                def capture_logs(
                    self, results, staging_root, failure_logs_root, op, step
                ):
                    pass

    def test_valid_subclass_succeeds(self) -> None:
        from unittest.mock import MagicMock

        class GoodBackend(BackendBase):
            name = "good"
            worker_traits = WorkerTraits()
            orchestrator_traits = OrchestratorTraits()

            def create_dispatch_handle(self, *a, **kw):
                return MagicMock()

            def capture_logs(self, results, staging_root, failure_logs_root, op, step):
                pass

        backend = GoodBackend()
        assert backend.name == "good"
