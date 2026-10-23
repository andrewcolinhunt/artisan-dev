"""Tests for transport-neutral execution and result validation."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import MagicMock, patch

import pytest

from artisan.orchestration.engine.dispatch import (
    execute_unit,
    execute_unit_batch,
    failure_results_for_units,
    validate_batch_results,
)
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
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
        runtime_env = RuntimeEnvironment(delta_root="/delta", staging_root="/staging")
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
        runtime_env = RuntimeEnvironment(delta_root="/delta", staging_root="/staging")

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
        runtime_env = RuntimeEnvironment(delta_root="/delta", staging_root="/staging")

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


class TestWorkerIdentityResolution:
    @pytest.mark.parametrize("curator", [False, True])
    @pytest.mark.parametrize(
        ("runtime_id", "env_var", "env_value", "expected"),
        [
            (0, None, "7", 0),
            (42, None, "7", 42),
            (42, "WORKER_ID", "7", 7),
            (42, "WORKER_ID", None, 0),
            (42, "WORKER_ID", " 07 ", 7),
            (42, "WORKER_ID", str(-(2**31)), -(2**31)),
            (42, "WORKER_ID", str(2**31 - 1), 2**31 - 1),
        ],
    )
    def test_executor_receives_resolved_snapshot(
        self,
        monkeypatch: pytest.MonkeyPatch,
        curator: bool,
        runtime_id: int,
        env_var: str | None,
        env_value: str | None,
        expected: int,
    ) -> None:
        if env_value is None:
            monkeypatch.delenv("WORKER_ID", raising=False)
        else:
            monkeypatch.setenv("WORKER_ID", env_value)
        runtime = RuntimeEnvironment(
            delta_root="/delta",
            staging_root="/staging",
            worker_id=runtime_id,
            worker_id_env_var=env_var,
        )
        unit = MagicMock()
        unit.get_batch_size.return_value = 3
        flow_result = MagicMock(
            success=True,
            error=None,
            execution_run_id="run",
            artifact_ids=["a"],
            cancellation_acknowledgement=None,
        )
        flow = "curator.run_curator_flow" if curator else "creator.run_creator_flow"
        with (
            patch(
                "artisan.execution.executors.curator.is_curator_operation",
                return_value=curator,
            ),
            patch(
                f"artisan.execution.executors.{flow}", return_value=flow_result
            ) as run_flow,
        ):
            result = execute_unit(unit, runtime)
        assert result.success
        received = run_flow.call_args.args[1]
        assert received is not runtime
        assert received.worker_id == expected
        assert received.worker_id_env_var == env_var
        assert runtime.worker_id == runtime_id

    @pytest.mark.parametrize(
        "raw_value", ["", "private-invalid-worker", "2147483648", "-2147483649"]
    )
    def test_invalid_provider_value_fails_aligned_batch_without_leaking_input(
        self,
        monkeypatch: pytest.MonkeyPatch,
        raw_value: str,
    ) -> None:
        monkeypatch.setenv("WORKER_ID", raw_value)
        runtime = RuntimeEnvironment(
            delta_root="/delta",
            staging_root="/staging",
            worker_id=42,
            worker_id_env_var="WORKER_ID",
        )
        units = [MagicMock(), MagicMock()]
        for index, unit in enumerate(units):
            unit.get_batch_size.return_value = index + 2
        with (
            patch("artisan.execution.executors.creator.run_creator_flow") as creator,
            patch("artisan.execution.executors.curator.run_curator_flow") as curator,
        ):
            results = execute_unit_batch(units, runtime)
        creator.assert_not_called()
        curator.assert_not_called()
        assert [result.item_count for result in results] == [2, 3]
        for result in results:
            assert not result.success
            assert result.execution_run_ids == []
            assert "WORKER_ID" in result.error
            assert "signed 32-bit integer" in result.error
            if raw_value:
                assert raw_value not in result.error
            assert "During handling of the above exception" not in result.error
        assert runtime.worker_id == 42

    def test_sequential_calls_keep_earlier_snapshot(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        runtime = RuntimeEnvironment(
            delta_root="/delta",
            staging_root="/staging",
            worker_id=42,
            worker_id_env_var="WORKER_ID",
        )
        unit = MagicMock()
        unit.get_batch_size.return_value = 1
        flow_result = MagicMock(
            success=True,
            error=None,
            execution_run_id="run",
            cancellation_acknowledgement=None,
        )
        with (
            patch(
                "artisan.execution.executors.curator.is_curator_operation",
                return_value=False,
            ),
            patch(
                "artisan.execution.executors.creator.run_creator_flow",
                return_value=flow_result,
            ) as flow,
        ):
            monkeypatch.setenv("WORKER_ID", "7")
            assert execute_unit(unit, runtime).success
            first = flow.call_args.args[1]
            monkeypatch.setenv("WORKER_ID", "8")
            assert execute_unit(unit, runtime).success
            second = flow.call_args.args[1]
        assert first.worker_id == 7
        assert second.worker_id == 8
        assert first is not second
        assert runtime.worker_id == 42

    def test_concurrent_calls_keep_independent_snapshots(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from artisan.execution.recording.parquet_writer import StagingResult

        runtimes = [
            RuntimeEnvironment(
                delta_root="/delta",
                staging_root="/staging",
                worker_id=42,
                worker_id_env_var=name,
            )
            for name in ("WORKER_A", "WORKER_B")
        ]
        monkeypatch.setenv("WORKER_A", "7")
        monkeypatch.setenv("WORKER_B", "8")
        unit = MagicMock()
        unit.get_batch_size.return_value = 1

        executing = Barrier(2)

        def flow(unit: object, runtime: RuntimeEnvironment) -> StagingResult:
            assert all(runtime is not submitted for submitted in runtimes)
            executing.wait(timeout=5)
            return StagingResult(success=True, execution_run_id=str(runtime.worker_id))

        with (
            patch(
                "artisan.execution.executors.curator.is_curator_operation",
                return_value=False,
            ),
            patch(
                "artisan.execution.executors.creator.run_creator_flow", side_effect=flow
            ),
            ThreadPoolExecutor(max_workers=2) as pool,
        ):
            results = list(
                pool.map(lambda runtime: execute_unit(unit, runtime), runtimes)
            )
        assert [result.execution_run_ids for result in results] == [["7"], ["8"]]
        assert [runtime.worker_id for runtime in runtimes] == [42, 42]


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
