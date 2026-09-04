"""Tests for transport-neutral execution and result validation."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from artisan.orchestration.engine.dispatch import (
    execute_unit,
    execute_unit_batch,
    failure_results_for_units,
    validate_batch_results,
)
from artisan.schemas.execution.unit_result import UnitResult


def _result(**overrides: object) -> UnitResult:
    """Build a UnitResult with sensible defaults."""
    defaults = {
        "success": True,
        "error": None,
        "item_count": 1,
        "execution_run_ids": [],
    }
    return UnitResult(**{**defaults, **overrides})


class TestExecuteUnit:
    def test_creator_result_preserves_unit_cardinality(self) -> None:
        unit = MagicMock()
        unit.operation = MagicMock()
        unit.get_batch_size.return_value = 3
        runtime_env = MagicMock(worker_id_env_var=None)
        flow_result = MagicMock(
            success=True,
            error=None,
            execution_run_id="run-1",
        )

        with (
            patch(
                "artisan.execution.executors.curator.is_curator_operation",
                return_value=False,
            ),
            patch(
                "artisan.execution.executors.creator.run_creator_flow",
                return_value=flow_result,
            ),
        ):
            result = execute_unit(unit, runtime_env)

        assert result.success is True
        assert result.item_count == 3
        assert result.execution_run_ids == ["run-1"]

    def test_operation_exception_becomes_failure(self) -> None:
        unit = MagicMock()
        unit.operation = MagicMock()
        unit.get_batch_size.return_value = 3
        runtime_env = MagicMock(worker_id_env_var=None)

        with (
            patch(
                "artisan.execution.executors.curator.is_curator_operation",
                return_value=False,
            ),
            patch(
                "artisan.execution.executors.creator.run_creator_flow",
                side_effect=ValueError("bad input"),
            ),
        ):
            result = execute_unit(unit, runtime_env)

        assert result.success is False
        assert result.item_count == 3
        assert "ValueError: bad input" in result.error
        assert result.execution_run_ids == []

    def test_keyboard_interrupt_raises_runtime_error(self) -> None:
        unit = MagicMock()
        unit.operation = MagicMock()
        runtime_env = MagicMock(worker_id_env_var=None)

        with (
            patch(
                "artisan.execution.executors.curator.is_curator_operation",
                return_value=False,
            ),
            patch(
                "artisan.execution.executors.creator.run_creator_flow",
                side_effect=KeyboardInterrupt,
            ),
            pytest.raises(RuntimeError, match="SIGINT"),
        ):
            execute_unit(unit, runtime_env)


class TestExecuteUnitBatch:
    def test_returns_one_ordered_result_per_unit(self) -> None:
        units = [MagicMock(), MagicMock(), MagicMock()]
        expected = [
            _result(execution_run_ids=["a"]),
            _result(execution_run_ids=["b"]),
            _result(execution_run_ids=["c"]),
        ]

        with patch(
            "artisan.orchestration.engine.dispatch.execute_unit",
            side_effect=expected,
        ) as mock_execute:
            results = execute_unit_batch(units, MagicMock())

        assert results == expected
        assert [call.args[0] for call in mock_execute.call_args_list] == units


class TestFailureResultsForUnits:
    def test_returns_one_failure_per_unit(self) -> None:
        units = [MagicMock(), MagicMock()]
        units[0].get_batch_size.return_value = 2
        units[1].get_batch_size.return_value = 3

        results = failure_results_for_units(units, OSError("transport down"))

        assert len(results) == 2
        assert [result.item_count for result in results] == [2, 3]
        assert all(result.success is False for result in results)
        assert all("OSError: transport down" in result.error for result in results)


class TestValidateBatchResults:
    def test_returns_valid_ordered_results(self) -> None:
        units = [MagicMock(), MagicMock()]
        expected = [
            _result(execution_run_ids=["first"]),
            _result(execution_run_ids=["second"]),
        ]

        assert validate_batch_results(units, expected) is expected

    @pytest.mark.parametrize("returned_count", [0, 1, 3])
    def test_cardinality_mismatch_fails_entire_batch(
        self,
        returned_count: int,
    ) -> None:
        units = [MagicMock(), MagicMock()]

        results = validate_batch_results(
            units,
            [_result() for _ in range(returned_count)],
        )

        assert len(results) == len(units)
        assert all(result.success is False for result in results)
        assert all("submitted units" in result.error for result in results)

    def test_non_list_fails_entire_batch(self) -> None:
        units = [MagicMock(), MagicMock()]

        results = validate_batch_results(units, (_result(), _result()))

        assert len(results) == 2
        assert all(result.success is False for result in results)
        assert "expected list[UnitResult]" in results[0].error

    def test_invalid_result_type_fails_entire_batch(self) -> None:
        units = [MagicMock(), MagicMock()]

        results = validate_batch_results(units, [_result(), {"success": True}])

        assert len(results) == 2
        assert all(result.success is False for result in results)
        assert "expected UnitResult" in results[0].error
