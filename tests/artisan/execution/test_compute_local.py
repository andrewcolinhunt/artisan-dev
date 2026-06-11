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


def _inputs(n: int) -> list[ExecuteInput]:
    return [
        ExecuteInput(inputs={}, execute_dir=f"/tmp/test/a{i}", log_path="/tmp/log")
        for i in range(n)
    ]


class TestLocalExecuteRouter:
    def test_sequential_loop_preserves_order(self):
        """One invoke per artifact, results positionally aligned."""
        router = LocalExecuteRouter()
        operation = _function_op(side_effect=[{"i": 0}, {"i": 1}, {"i": 2}])

        results = router.route_execute(operation, _inputs(3), "/tmp/sandbox")

        assert results == [{"i": 0}, {"i": 1}, {"i": 2}]
        called_with = [c.args[0] for c in operation.execute_function.call_args_list]
        assert [ei.execute_dir for ei in called_with] == [
            "/tmp/test/a0",
            "/tmp/test/a1",
            "/tmp/test/a2",
        ]

    def test_passthrough_returns_none(self):
        """route_execute passes through None returns."""
        router = LocalExecuteRouter()
        operation = _function_op(return_value=None)

        results = router.route_execute(operation, _inputs(1), "/tmp/sandbox")
        assert results == [None]

    def test_passthrough_propagates_exception(self):
        """route_execute does not catch exceptions from the slot body."""
        router = LocalExecuteRouter()
        operation = _function_op(side_effect=RuntimeError("boom"))

        with pytest.raises(RuntimeError, match="boom"):
            router.route_execute(operation, _inputs(1), "/tmp/sandbox")
