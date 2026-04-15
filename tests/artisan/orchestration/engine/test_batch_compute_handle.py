"""Tests for BatchComputeDispatchHandle and _process_unit."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest

from artisan.orchestration.engine.batch_compute_handle import (
    BatchComputeDispatchHandle,
    _batch_execute_with_shared_router,
    _process_unit,
)
from artisan.orchestration.engine.dispatch_handle import _HandleState
from artisan.schemas.execution.unit_result import UnitResult


def _make_unit(batch_size: int = 1) -> MagicMock:
    unit = MagicMock()
    unit.get_batch_size.return_value = batch_size
    unit.user_overrides = None
    return unit


class TestBatchComputeDispatchHandle:
    """State machine and dispatch tests."""

    def test_initial_state_is_idle(self):
        handle = BatchComputeDispatchHandle(
            compute_config=MagicMock(),
        )
        assert handle._state is _HandleState.IDLE

    def test_double_dispatch_raises(self):
        handle = BatchComputeDispatchHandle(
            compute_config=MagicMock(),
        )
        handle._state = _HandleState.DISPATCHED
        with pytest.raises(RuntimeError, match="dispatch.*already called"):
            handle.dispatch([], MagicMock())

    def test_cancel_is_noop(self):
        handle = BatchComputeDispatchHandle(
            compute_config=MagicMock(),
        )
        handle.cancel()  # Before dispatch
        handle._state = _HandleState.DISPATCHED
        handle.cancel()  # After dispatch

    @patch("artisan.orchestration.engine.batch_compute_handle.ProcessPoolExecutor")
    def test_cancel_event_interrupts_polling(self, mock_ppe_cls):
        """When cancel event is set, the polling loop raises RuntimeError."""
        cancel = threading.Event()
        handle = BatchComputeDispatchHandle(
            compute_config=MagicMock(),
            cancel_event=cancel,
        )

        mock_pool = MagicMock()
        mock_ppe_cls.return_value.__enter__ = MagicMock(return_value=mock_pool)
        mock_ppe_cls.return_value.__exit__ = MagicMock(return_value=False)

        future = MagicMock()
        future.result.side_effect = TimeoutError
        mock_pool.submit.return_value = future

        cancel.set()

        with patch.object(handle, "_start_background") as mock_bg:
            handle.dispatch([_make_unit()], MagicMock())
            bg_fn = mock_bg.call_args[0][0]
            with pytest.raises(RuntimeError, match="Batch compute interrupted"):
                bg_fn()

    @patch("artisan.orchestration.engine.batch_compute_handle.ProcessPoolExecutor")
    def test_end_to_end_with_mocked_pool(self, mock_ppe_cls):
        """run() returns results from the child process."""
        expected = [
            UnitResult(success=True, error=None, item_count=1, execution_run_ids=[]),
        ]

        mock_pool = MagicMock()
        mock_ppe_cls.return_value.__enter__ = MagicMock(return_value=mock_pool)
        mock_ppe_cls.return_value.__exit__ = MagicMock(return_value=False)

        future = MagicMock()
        future.result.return_value = expected
        mock_pool.submit.return_value = future

        handle = BatchComputeDispatchHandle(
            compute_config=MagicMock(),
            max_workers=2,
        )
        results = handle.run([_make_unit()], MagicMock())

        assert results == expected
        call_args = mock_pool.submit.call_args
        assert call_args[0][0] is _batch_execute_with_shared_router
        # max_workers forwarded
        assert call_args[0][4] == 2


class TestBatchExecuteWithSharedRouter:
    """Tests for the child process function."""

    @patch("artisan.orchestration.engine.batch_compute_handle._process_unit")
    @patch("artisan.orchestration.engine.batch_compute_handle.create_router")
    def test_shared_router_created_once_and_closed(self, mock_create, mock_process):
        mock_router = MagicMock()
        mock_create.return_value = mock_router
        mock_process.return_value = UnitResult(
            success=True, error=None, item_count=1, execution_run_ids=[]
        )

        units = [_make_unit(), _make_unit(), _make_unit()]
        _batch_execute_with_shared_router(
            units, MagicMock(), MagicMock(), max_workers=2
        )

        mock_create.assert_called_once()
        mock_router.close.assert_called_once()

    @patch("artisan.orchestration.engine.batch_compute_handle._process_unit")
    @patch("artisan.orchestration.engine.batch_compute_handle.create_router")
    def test_worker_ids_unique(self, mock_create, mock_process):
        """Each unit gets a distinct worker_id."""
        mock_create.return_value = MagicMock()
        mock_process.return_value = UnitResult(
            success=True, error=None, item_count=1, execution_run_ids=[]
        )

        units = [_make_unit(), _make_unit(), _make_unit()]
        _batch_execute_with_shared_router(
            units, MagicMock(), MagicMock(), max_workers=3
        )

        worker_ids = sorted(
            call.kwargs["worker_id"] for call in mock_process.call_args_list
        )
        assert worker_ids == [0, 1, 2]

    @patch("artisan.orchestration.engine.batch_compute_handle._process_unit")
    @patch("artisan.orchestration.engine.batch_compute_handle.create_router")
    def test_router_closed_on_failure(self, mock_create, mock_process):
        """Router is closed even when a unit raises."""
        mock_router = MagicMock()
        mock_create.return_value = mock_router
        mock_process.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError):
            _batch_execute_with_shared_router([_make_unit()], MagicMock(), MagicMock())

        mock_router.close.assert_called_once()


class TestProcessUnit:
    """Tests for _process_unit."""

    @patch("artisan.orchestration.engine.batch_compute_handle.prep_unit")
    def test_prep_failure_returns_error_result(self, mock_prep):
        """Prep failure returns error UnitResult without staging."""
        mock_prep.side_effect = ValueError("working_root not set")

        result = _process_unit(
            _make_unit(batch_size=2),
            MagicMock(),
            MagicMock(),
            worker_id=0,
        )

        assert not result.success
        assert "working_root" in result.error
        assert result.item_count == 2

    @patch("artisan.orchestration.engine.batch_compute_handle.record_execution_failure")
    @patch("artisan.orchestration.engine.batch_compute_handle.prep_unit")
    def test_execute_failure_records_and_returns_error(self, mock_prep, mock_record):
        """Execute failure records to staging and returns error UnitResult."""
        prepped = MagicMock()
        prepped.operation.per_artifact_dispatch = True
        prepped.timings = {}
        prepped.log_path = "/tmp/log"
        prepped.execution_run_id = "run123"
        mock_prep.return_value = prepped

        router = MagicMock()
        router.route_execute_batch.side_effect = RuntimeError("GPU OOM")

        unit = _make_unit(batch_size=2)
        result = _process_unit(unit, MagicMock(), router, worker_id=0)

        assert not result.success
        assert "GPU OOM" in result.error
        mock_record.assert_called_once()

    @patch("artisan.orchestration.engine.batch_compute_handle.record_execution_success")
    @patch("artisan.orchestration.engine.batch_compute_handle.post_unit")
    @patch("artisan.orchestration.engine.batch_compute_handle.prep_unit")
    def test_success_records_and_returns(self, mock_prep, mock_post, mock_record):
        """Successful lifecycle records to staging and returns success."""
        prepped = MagicMock()
        prepped.operation.per_artifact_dispatch = True
        prepped.timings = {}
        prepped.execution_run_id = "run456"
        mock_prep.return_value = prepped

        lifecycle_result = MagicMock()
        lifecycle_result.artifacts = {"output": []}
        lifecycle_result.edges = []
        mock_post.return_value = lifecycle_result

        router = MagicMock()
        router.route_execute_batch.return_value = [{"r": 0}]

        unit = _make_unit(batch_size=1)
        result = _process_unit(unit, MagicMock(), router, worker_id=0)

        assert result.success
        assert result.execution_run_ids == ["run456"]
        mock_record.assert_called_once()

    @patch("artisan.orchestration.engine.batch_compute_handle.prep_unit")
    def test_per_artifact_dispatch_false_uses_route_execute(self, mock_prep):
        """per_artifact_dispatch=False calls route_execute, not batch."""
        prepped = MagicMock()
        prepped.operation.per_artifact_dispatch = False
        prepped.timings = {}
        prepped.execution_run_id = "run789"
        prepped.artifact_execute_inputs = [MagicMock()]
        mock_prep.return_value = prepped

        router = MagicMock()
        router.route_execute.return_value = None

        mock_post = MagicMock()
        mock_post.return_value = MagicMock(artifacts={}, edges=[])

        with (
            patch(
                "artisan.orchestration.engine.batch_compute_handle.post_unit",
                mock_post,
            ),
            patch(
                "artisan.orchestration.engine.batch_compute_handle.record_execution_success",
            ),
        ):
            result = _process_unit(
                _make_unit(batch_size=2),
                MagicMock(),
                router,
                worker_id=0,
            )

        router.route_execute.assert_called_once()
        router.route_execute_batch.assert_not_called()
        assert result.success
