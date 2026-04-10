"""Tests for create_router factory."""

from __future__ import annotations

import pytest

from artisan.execution.compute.local import LocalComputeRouter
from artisan.execution.compute.modal import ModalComputeRouter
from artisan.execution.compute.routing import create_router
from artisan.schemas.operation_config.compute import (
    ComputeConfig,
    LocalComputeConfig,
    ModalComputeConfig,
)


class TestCreateRouter:
    def test_local_config_creates_local_router(self):
        config = LocalComputeConfig()
        router = create_router(config)
        assert isinstance(router, LocalComputeRouter)

    def test_modal_config_creates_modal_router(self):
        config = ModalComputeConfig(image="test-image")
        router = create_router(config)
        assert isinstance(router, ModalComputeRouter)

    def test_unknown_config_raises(self):
        config = ComputeConfig()
        with pytest.raises(ValueError, match="Unknown compute config"):
            create_router(config)
