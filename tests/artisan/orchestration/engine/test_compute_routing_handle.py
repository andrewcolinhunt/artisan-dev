"""Tests for compute routing dispatch handle and shared router child function."""

from __future__ import annotations

import multiprocessing
import threading
from concurrent.futures.process import BrokenProcessPool
from unittest.mock import MagicMock, call, patch

import pytest

from artisan.orchestration.engine.compute_routing_handle import (
    ComputeRoutingDispatchHandle,
    _run_units_with_shared_router,
)
from artisan.orchestration.engine.dispatch_handle import _HandleState
from artisan.schemas.execution.unit_result import UnitResult


def _result(**overrides) -> UnitResult:
    defaults = {
        "success": True,
        "error": None,
        "item_count": 1,
        "execution_run_ids": [],
    }
    defaults.update(overrides)
    return UnitResult(**defaults)


def _make_staging_result(success=True, error=None, run_id="run_0"):
    """Create a mock StagingResult with the fields read by _run_units_with_shared_router."""
    result = MagicMock()
    result.success = success
    result.error = error
    result.execution_run_id = run_id
    return result


def _make_unit(batch_size=1):
    unit = MagicMock()
    unit.get_batch_size.return_value = batch_size
    return unit


# ---- _run_units_with_shared_router ----


class TestRunUnitsWithSharedRouter:
    @patch("artisan.orchestration.engine.compute_routing_handle.run_creator_flow")
    @patch("artisan.orchestration.engine.compute_routing_handle.create_router")
    def test_shared_router_created_once(self, mock_create, mock_flow):
        """create_router is called once; the same router goes to every unit."""
        mock_router = MagicMock()
        mock_create.return_value = mock_router
        mock_flow.return_value = _make_staging_result()

        units = [_make_unit() for _ in range(3)]
        runtime_env = MagicMock()
        config = MagicMock()

        _run_units_with_shared_router(units, runtime_env, config)

        mock_create.assert_called_once_with(config)
        assert mock_flow.call_count == 3
        for c in mock_flow.call_args_list:
            assert c.kwargs["compute_router"] is mock_router

    @patch("artisan.orchestration.engine.compute_routing_handle.run_creator_flow")
    @patch("artisan.orchestration.engine.compute_routing_handle.create_router")
    def test_results_in_dispatch_order(self, mock_create, mock_flow):
        """Results are returned in the same order as the input units."""
        mock_create.return_value = MagicMock()
        mock_flow.side_effect = [
            _make_staging_result(run_id="r0"),
            _make_staging_result(run_id="r1"),
            _make_staging_result(run_id="r2"),
        ]

        units = [_make_unit() for _ in range(3)]
        results = _run_units_with_shared_router(units, MagicMock(), MagicMock())

        assert len(results) == 3
        assert results[0].execution_run_ids == ["r0"]
        assert results[1].execution_run_ids == ["r1"]
        assert results[2].execution_run_ids == ["r2"]

    @patch("artisan.orchestration.engine.compute_routing_handle.run_creator_flow")
    @patch("artisan.orchestration.engine.compute_routing_handle.create_router")
    def test_cancellation_fills_remaining(self, mock_create, mock_flow):
        """When cancel is set after the first unit, remaining get cancelled results."""
        mock_create.return_value = MagicMock()
        cancel = multiprocessing.Event()

        def _flow_with_cancel(unit, env, **kwargs):
            cancel.set()  # signal cancel after first unit
            return _make_staging_result(run_id="done")

        mock_flow.side_effect = _flow_with_cancel

        units = [_make_unit() for _ in range(3)]
        results = _run_units_with_shared_router(
            units, MagicMock(), MagicMock(), cancel_event=cancel,
        )

        assert len(results) == 3
        assert results[0].success is True
        assert results[1].success is False
        assert results[1].error == "Cancelled"
        assert results[2].success is False
        assert results[2].error == "Cancelled"

    @patch("artisan.orchestration.engine.compute_routing_handle.run_creator_flow")
    @patch("artisan.orchestration.engine.compute_routing_handle.create_router")
    def test_exception_in_one_unit_continues(self, mock_create, mock_flow):
        """A failing unit gets an error result; other units still run."""
        mock_create.return_value = MagicMock()
        mock_flow.side_effect = [
            _make_staging_result(run_id="r0"),
            ValueError("boom"),
            _make_staging_result(run_id="r2"),
        ]

        units = [_make_unit() for _ in range(3)]
        results = _run_units_with_shared_router(units, MagicMock(), MagicMock())

        assert len(results) == 3
        assert results[0].success is True
        assert results[1].success is False
        assert "boom" in results[1].error
        assert results[2].success is True

    @patch("artisan.orchestration.engine.compute_routing_handle.run_creator_flow")
    @patch("artisan.orchestration.engine.compute_routing_handle.create_router")
    def test_router_close_called_on_failure(self, mock_create, mock_flow):
        """Router.close() is called even when a unit raises."""
        mock_router = MagicMock()
        mock_create.return_value = mock_router
        mock_flow.side_effect = RuntimeError("crash")

        units = [_make_unit()]
        _run_units_with_shared_router(units, MagicMock(), MagicMock())

        mock_router.close.assert_called_once()

    @patch("artisan.orchestration.engine.compute_routing_handle.run_creator_flow")
    @patch("artisan.orchestration.engine.compute_routing_handle.create_router")
    def test_router_close_not_called_without_method(self, mock_create, mock_flow):
        """Routers without close() (e.g. LocalComputeRouter) are not called."""
        mock_router = MagicMock(spec=[])  # no attributes at all
        mock_create.return_value = mock_router
        mock_flow.return_value = _make_staging_result()

        units = [_make_unit()]
        _run_units_with_shared_router(units, MagicMock(), MagicMock())
        # No error — hasattr check prevents AttributeError

    @patch("artisan.orchestration.engine.compute_routing_handle.run_creator_flow")
    @patch("artisan.orchestration.engine.compute_routing_handle.create_router")
    def test_batch_size_in_result(self, mock_create, mock_flow):
        """item_count reflects the unit's batch size."""
        mock_create.return_value = MagicMock()
        mock_flow.return_value = _make_staging_result()

        unit = _make_unit(batch_size=5)
        results = _run_units_with_shared_router([unit], MagicMock(), MagicMock())

        assert results[0].item_count == 5


# ---- ComputeRoutingDispatchHandle ----


class TestComputeRoutingDispatchHandle:
    def test_initial_state_is_idle(self):
        handle = ComputeRoutingDispatchHandle(
            compute_config=MagicMock(), cancel_event=None,
        )
        assert handle._state is _HandleState.IDLE

    def test_double_dispatch_raises(self):
        handle = ComputeRoutingDispatchHandle(
            compute_config=MagicMock(), cancel_event=None,
        )
        # First dispatch: mock _start_background to avoid real threads
        with patch.object(handle, "_start_background"):
            handle.dispatch([_make_unit()], MagicMock())

        with pytest.raises(RuntimeError, match="dispatch.*already called"):
            handle.dispatch([_make_unit()], MagicMock())

    def test_cancel_sets_mp_event(self):
        handle = ComputeRoutingDispatchHandle(
            compute_config=MagicMock(), cancel_event=None,
        )
        # Simulate dispatch so _mp_cancel is created
        with patch.object(handle, "_start_background"):
            handle.dispatch([_make_unit()], MagicMock())

        assert not handle._mp_cancel.is_set()
        handle.cancel()
        assert handle._mp_cancel.is_set()

    def test_cancel_before_dispatch_is_noop(self):
        handle = ComputeRoutingDispatchHandle(
            compute_config=MagicMock(), cancel_event=None,
        )
        # _mp_cancel is None before dispatch — cancel should not raise
        handle.cancel()

    @patch(
        "artisan.orchestration.engine.compute_routing_handle"
        "._run_units_with_shared_router"
    )
    @patch(
        "artisan.orchestration.engine.compute_routing_handle.ProcessPoolExecutor"
    )
    def test_end_to_end_with_mocked_pool(self, mock_pool_cls, mock_child_fn):
        """run() returns results from the child process."""
        expected = [_result(), _result(success=False, error="fail")]
        mock_future = MagicMock()
        mock_future.result.return_value = expected

        mock_pool = MagicMock()
        mock_pool.__enter__ = MagicMock(return_value=mock_pool)
        mock_pool.__exit__ = MagicMock(return_value=False)
        mock_pool.submit.return_value = mock_future
        mock_pool_cls.return_value = mock_pool

        handle = ComputeRoutingDispatchHandle(
            compute_config=MagicMock(), cancel_event=None,
        )
        results = handle.run([_make_unit(), _make_unit()], MagicMock())

        assert results == expected

    def test_broken_process_pool_propagates(self):
        """BrokenProcessPool from child crash propagates through collect()."""
        handle = ComputeRoutingDispatchHandle(
            compute_config=MagicMock(), cancel_event=None,
        )
        # Manually simulate what _start_background does on error
        handle._error = BrokenProcessPool("worker died")
        handle._done.set()
        handle._state = _HandleState.DISPATCHED

        with pytest.raises(BrokenProcessPool, match="worker died"):
            handle.collect()
