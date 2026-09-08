"""Tests for native local lifecycle cancellation."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from concurrent.futures import Future
from concurrent.futures.process import BrokenProcessPool
from unittest.mock import MagicMock, patch

import pytest

from artisan.orchestration.runners.local import LocalLifecycleRouter
from artisan.schemas.execution.unit_result import UnitResult


def _success() -> UnitResult:
    return UnitResult(True, None, 1, [])


class TestLocalLifecycleRouterCancel:
    def test_cancel_before_dispatch_is_noop(self) -> None:
        handle = LocalLifecycleRouter(max_workers=1, units_per_worker=1)

        handle.cancel()

        assert handle._cancel_requested is False

    def test_cancel_after_empty_dispatch_is_noop(self) -> None:
        handle = LocalLifecycleRouter(max_workers=1, units_per_worker=1)
        assert handle.run([], MagicMock()) == []

        handle.cancel()

        assert handle._cancel_requested is False

    @patch("artisan.orchestration.runners.local.ProcessPoolExecutor")
    def test_cancel_pending_future_returns_aligned_failure(
        self,
        mock_executor_class: MagicMock,
    ) -> None:
        executor = mock_executor_class.return_value
        pending: Future[list[UnitResult]] = Future()
        executor.submit.return_value = pending
        handle = LocalLifecycleRouter(max_workers=1, units_per_worker=1)
        handle.dispatch([MagicMock()], MagicMock())
        _wait_until(lambda: executor.submit.called)

        handle.cancel()
        handle.cancel()
        assert handle._done.wait(timeout=2)
        results = handle.collect()

        assert len(results) == 1
        assert results[0].success is False
        assert "CancelledError" in results[0].error
        executor.shutdown.assert_any_call(wait=False, cancel_futures=True)
        assert (
            sum(
                call.kwargs.get("wait") is False
                for call in executor.shutdown.call_args_list
            )
            == 1
        )

    @patch("artisan.orchestration.runners.local.ProcessPoolExecutor")
    def test_cancel_terminates_in_flight_worker(
        self,
        mock_executor_class: MagicMock,
    ) -> None:
        executor = mock_executor_class.return_value
        process = MagicMock()
        process.is_alive.return_value = True
        executor._processes = {123: process}
        running: Future[list[UnitResult]] = Future()
        running.set_running_or_notify_cancel()
        executor.submit.return_value = running
        handle = LocalLifecycleRouter(max_workers=1, units_per_worker=1)
        handle.dispatch([MagicMock()], MagicMock())
        _wait_until(lambda: executor.submit.called)

        handle.cancel()
        assert running.cancelled() is False
        process.terminate.assert_called_once_with()
        running.set_exception(BrokenProcessPool("worker terminated"))
        assert handle._done.wait(timeout=2)

        with pytest.raises(BrokenProcessPool, match="worker terminated"):
            handle.collect()
        executor.shutdown.assert_any_call(wait=False, cancel_futures=True)

    @patch("artisan.orchestration.runners.local.ProcessPoolExecutor")
    def test_run_observes_delayed_cancel_event(
        self,
        mock_executor_class: MagicMock,
    ) -> None:
        executor = mock_executor_class.return_value
        running: Future[list[UnitResult]] = Future()
        running.set_running_or_notify_cancel()
        executor.submit.return_value = running
        handle = LocalLifecycleRouter(max_workers=1, units_per_worker=1)
        cancel_event = threading.Event()

        def _cancel_then_finish() -> None:
            time.sleep(0.05)
            cancel_event.set()
            time.sleep(0.15)
            running.set_result([_success()])

        threading.Thread(target=_cancel_then_finish, daemon=True).start()

        results = handle.run([MagicMock(step_run_id=None)], MagicMock(), cancel_event)

        assert results == [_success()]
        assert handle._cancel_requested is True


def _wait_until(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    """Wait for an asynchronous router condition in a bounded loop."""
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            msg = "condition was not met before timeout"
            raise AssertionError(msg)
        time.sleep(0.01)
