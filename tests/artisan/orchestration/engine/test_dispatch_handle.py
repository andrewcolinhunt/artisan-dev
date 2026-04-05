"""Tests for DispatchHandle ABC and state machine."""

from __future__ import annotations

import threading

import pytest

from artisan.orchestration.engine.dispatch_handle import (
    DispatchHandle,
    _HandleState,
)
from artisan.schemas.execution.unit_result import UnitResult


def _result(**overrides: object) -> UnitResult:
    """Build a UnitResult with sensible defaults."""
    defaults = dict(success=True, error=None, item_count=1, execution_run_ids=[])
    return UnitResult(**{**defaults, **overrides})


class _StubHandle(DispatchHandle):
    """Minimal concrete handle for state machine tests."""

    def __init__(self, results: list[UnitResult] | None = None) -> None:
        super().__init__()
        self._stub_results = results or [_result()]
        self.cancel_count = 0
        self.dispatch_called = False

    def dispatch(self, units, runtime_env) -> None:
        self._assert_idle()
        self._state = _HandleState.DISPATCHED
        self.dispatch_called = True
        self._results = self._stub_results
        self._done.set()

    def cancel(self) -> None:
        self.cancel_count += 1


class _SlowStubHandle(DispatchHandle):
    """Stub that doesn't complete until explicitly told to."""

    def __init__(self) -> None:
        super().__init__()
        self.cancel_count = 0

    def dispatch(self, units, runtime_env) -> None:
        self._assert_idle()
        self._state = _HandleState.DISPATCHED
        # Don't set _done or _results — stays in DISPATCHED state

    def cancel(self) -> None:
        self.cancel_count += 1

    def complete(self, results: list[UnitResult]) -> None:
        """Externally signal completion (for test control)."""
        self._results = results
        self._done.set()


class TestDispatchHandleStateMachine:
    def test_dispatch_then_collect(self) -> None:
        handle = _StubHandle()
        handle.dispatch([], None)
        assert handle.is_done()
        results = handle.collect()
        assert len(results) == 1
        assert results[0].success is True

    def test_double_dispatch_raises(self) -> None:
        handle = _StubHandle()
        handle.dispatch([], None)
        with pytest.raises(RuntimeError, match="dispatch.*already called"):
            handle.dispatch([], None)

    def test_collect_before_done_raises(self) -> None:
        handle = _SlowStubHandle()
        handle.dispatch([], None)
        with pytest.raises(RuntimeError, match="before completion"):
            handle.collect()

    def test_cancel_before_dispatch_noop(self) -> None:
        handle = _StubHandle()
        handle.cancel()  # Should not raise
        assert handle.cancel_count == 1

    def test_cancel_after_done_noop(self) -> None:
        handle = _StubHandle()
        handle.dispatch([], None)
        assert handle.is_done()
        handle.cancel()
        assert handle.cancel_count == 1

    def test_cancel_idempotent(self) -> None:
        handle = _StubHandle()
        handle.cancel()
        handle.cancel()
        assert handle.cancel_count == 2

    def test_is_done_false_before_dispatch(self) -> None:
        handle = _StubHandle()
        assert not handle.is_done()


class TestRunTemplateMethod:
    def test_run_calls_dispatch_and_collect(self) -> None:
        handle = _StubHandle(results=[_result(item_count=5)])
        results = handle.run([], None)
        assert handle.dispatch_called
        assert len(results) == 1
        assert results[0].item_count == 5

    def test_run_with_cancel_event(self) -> None:
        handle = _SlowStubHandle()
        cancel_event = threading.Event()
        cancel_event.set()  # Already cancelled

        def _complete_after_cancel():
            """Complete the handle after cancel is called."""
            while handle.cancel_count == 0:
                pass
            handle.complete([_result(success=False, error="Cancelled")])

        t = threading.Thread(target=_complete_after_cancel, daemon=True)
        t.start()

        results = handle.run([], None, cancel_event=cancel_event)
        t.join(timeout=2)

        assert handle.cancel_count >= 1
        assert len(results) == 1
        assert results[0].error == "Cancelled"

    def test_run_propagates_errors(self) -> None:
        class _ErrorHandle(DispatchHandle):
            def dispatch(self, units, runtime_env):
                self._assert_idle()
                self._state = _HandleState.DISPATCHED
                self._error = ValueError("boom")
                self._done.set()

            def cancel(self):
                pass

        handle = _ErrorHandle()
        with pytest.raises(ValueError, match="boom"):
            handle.run([], None)
