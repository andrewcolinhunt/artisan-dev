"""Tests for create_execute_router factory."""

from __future__ import annotations

import pytest

from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.compute.routing import create_execute_router
from artisan.schemas.operation_config.compute import (
    ComputeConfig,
    LocalComputeConfig,
    ModalComputeConfig,
)


class TestCreateRouter:
    def test_local_config_creates_local_router(self):
        config = LocalComputeConfig()
        router = create_execute_router(config)
        assert isinstance(router, LocalExecuteRouter)

    def test_modal_config_raises(self):
        """Modal compute runs via tool endpoints, not a router."""
        config = ModalComputeConfig(image="test-image")
        with pytest.raises(ValueError, match="tool endpoints"):
            create_execute_router(config)

    def test_unknown_config_raises(self):
        config = ComputeConfig()
        with pytest.raises(ValueError, match="Unknown compute provider config"):
            create_execute_router(config)
