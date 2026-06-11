"""Tests for LocalExecuteRouter."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from artisan.execution.compute.local import LocalExecuteRouter
from artisan.schemas.specs.input_models import ExecuteInput


def _function_op(**kwargs) -> MagicMock:
    """Mock function op — invoke_op_work routes on is_command_op()."""
    operation = MagicMock()
    operation.is_command_op.return_value = False
    operation.execute_function.configure_mock(**kwargs)
    return operation


class TestLocalExecuteRouter:
    def test_routes_through_invoke_to_execute_function(self):
        """route_execute performs the function slot and returns its result."""
        router = LocalExecuteRouter()
        operation = _function_op(return_value={"key": "value"})

        execute_input = ExecuteInput(
            inputs={},
            execute_dir="/tmp/test",
            log_path="/tmp/test/log",
        )

        result = router.route_execute(operation, execute_input, "/tmp/sandbox")

        operation.execute_function.assert_called_once_with(execute_input)
        assert result == {"key": "value"}

    def test_passthrough_returns_none(self):
        """route_execute passes through None returns."""
        router = LocalExecuteRouter()
        operation = _function_op(return_value=None)

        execute_input = ExecuteInput(
            inputs={},
            execute_dir="/tmp/test",
            log_path="/tmp/test/log",
        )

        result = router.route_execute(operation, execute_input, "/tmp/sandbox")
        assert result is None

    def test_passthrough_propagates_exception(self):
        """route_execute does not catch exceptions from the slot body."""
        router = LocalExecuteRouter()
        operation = _function_op(side_effect=RuntimeError("boom"))

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
