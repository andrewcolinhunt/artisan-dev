"""Tests for LocalComputeRouter."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from artisan.execution.compute.local import LocalComputeRouter
from artisan.schemas.specs.input_models import ExecuteInput


class TestLocalComputeRouter:
    def test_passthrough_calls_execute(self):
        """route_execute delegates to operation.execute and returns its result."""
        router = LocalComputeRouter()
        operation = MagicMock()
        operation.execute.return_value = {"key": "value"}

        execute_input = ExecuteInput(
            inputs={},
            execute_dir="/tmp/test",
            log_path="/tmp/test/log",
        )

        result = router.route_execute(operation, execute_input, "/tmp/sandbox")

        operation.execute.assert_called_once_with(execute_input)
        assert result == {"key": "value"}

    def test_passthrough_returns_none(self):
        """route_execute passes through None returns."""
        router = LocalComputeRouter()
        operation = MagicMock()
        operation.execute.return_value = None

        execute_input = ExecuteInput(
            inputs={},
            execute_dir="/tmp/test",
            log_path="/tmp/test/log",
        )

        result = router.route_execute(operation, execute_input, "/tmp/sandbox")
        assert result is None

    def test_passthrough_propagates_exception(self):
        """route_execute does not catch exceptions from execute."""
        router = LocalComputeRouter()
        operation = MagicMock()
        operation.execute.side_effect = RuntimeError("boom")

        execute_input = ExecuteInput(
            inputs={},
            execute_dir="/tmp/test",
            log_path="/tmp/test/log",
        )

        try:
            router.route_execute(operation, execute_input, "/tmp/sandbox")
            pytest.fail("Should have raised")
        except RuntimeError as exc:
            assert str(exc) == "boom"
