"""Tests for create_router factory and route_execute_batch default."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from artisan.execution.compute.local import LocalComputeRouter
from artisan.execution.compute.modal import ModalComputeRouter
from artisan.execution.compute.routing import create_router
from artisan.schemas.operation_config.compute import (
    ComputeConfig,
    LocalComputeConfig,
    ModalComputeConfig,
)
from artisan.schemas.specs.input_models import ExecuteInput


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


class TestRouteExecuteBatchDefault:
    """Tests for the default sequential route_execute_batch on ComputeRouter."""

    def test_loops_route_execute(self):
        """Default route_execute_batch loops route_execute for each input."""
        router = LocalComputeRouter()
        op = MagicMock()
        op.execute.side_effect = [{"r": 0}, {"r": 1}, {"r": 2}]

        inputs = [
            ExecuteInput(execute_dir="/tmp/e0", inputs={"i": 0}),
            ExecuteInput(execute_dir="/tmp/e1", inputs={"i": 1}),
            ExecuteInput(execute_dir="/tmp/e2", inputs={"i": 2}),
        ]

        results = list(router.route_execute_batch(op, inputs, "/tmp/sandbox"))

        assert len(results) == 3
        assert results == [{"r": 0}, {"r": 1}, {"r": 2}]

    def test_captures_per_element_failures(self):
        """Failure at one index yields the exception; others succeed."""
        router = LocalComputeRouter()
        op = MagicMock()
        op.execute.side_effect = [{"r": 0}, ValueError("boom"), {"r": 2}]

        inputs = [
            ExecuteInput(execute_dir="/tmp/e0", inputs={}),
            ExecuteInput(execute_dir="/tmp/e1", inputs={}),
            ExecuteInput(execute_dir="/tmp/e2", inputs={}),
        ]

        results = list(router.route_execute_batch(op, inputs, "/tmp/sandbox"))

        assert results[0] == {"r": 0}
        assert isinstance(results[1], ValueError)
        assert results[2] == {"r": 2}
