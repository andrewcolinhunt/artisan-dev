"""Tests for LifecycleRouter ABC and state machine."""

from __future__ import annotations

import threading

import pytest

from artisan.orchestration.engine.lifecycle_router import (
    LifecycleRouter,
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


class _StubHandle(LifecycleRouter):
    """Minimal concrete handle for state machine tests."""

    def __init__(self, results: list[UnitResult] | None = None) -> None:
        super().__init__()
        self._stub_results = results or [_result()]
        self.cancel_count = 0
        self.dispatch_called = False

    def _dispatch(self, units, runtime_env) -> None:
        self.dispatch_called = True
        self._results = self._stub_results
        self._done.set()

    def cancel(self) -> None:
        self.cancel_count += 1


class _SlowStubHandle(LifecycleRouter):
    """Stub that doesn't complete until explicitly told to."""

    def __init__(self) -> None:
        super().__init__()
        self.cancel_count = 0

    def _dispatch(self, units, runtime_env) -> None:
        # Don't set _done or _results — stays in DISPATCHED state
        return None

    def cancel(self) -> None:
        self.cancel_count += 1

    def complete(self, results: list[UnitResult]) -> None:
        """Externally signal completion (for test control)."""
        self._results = results
        self._done.set()


class TestLifecycleRouterStateMachine:
    def test_dispatch_then_collect(self) -> None:
        handle = _StubHandle()
        handle.dispatch([object()], None)
        assert handle.is_done()
        results = handle.collect()
        assert len(results) == 1
        assert results[0].success is True

    def test_double_dispatch_raises(self) -> None:
        handle = _StubHandle()
        handle.dispatch([object()], None)
        with pytest.raises(RuntimeError, match="dispatch.*already called"):
            handle.dispatch([object()], None)

    def test_collect_before_done_raises(self) -> None:
        handle = _SlowStubHandle()
        handle.dispatch([object()], None)
        with pytest.raises(RuntimeError, match="before completion"):
            handle.collect()

    def test_cancel_before_dispatch_noop(self) -> None:
        handle = _StubHandle()
        handle.cancel()  # Should not raise
        assert handle.cancel_count == 1

    def test_cancel_after_done_noop(self) -> None:
        handle = _StubHandle()
        handle.dispatch([object()], None)
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
        results = handle.run([object()], None)
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

        results = handle.run([object()], None, cancel_event=cancel_event)
        t.join(timeout=2)

        assert handle.cancel_count >= 1
        assert len(results) == 1
        assert results[0].error == "Cancelled"

    def test_run_propagates_errors(self) -> None:
        class _ErrorHandle(LifecycleRouter):
            def _dispatch(self, units, runtime_env):
                self._error = ValueError("boom")
                self._done.set()

            def cancel(self):
                pass

        handle = _ErrorHandle()
        with pytest.raises(ValueError, match="boom"):
            handle.run([object()], None)

    def test_collect_rejects_wrong_result_count(self) -> None:
        handle = _StubHandle(results=[_result()])
        handle.dispatch([object(), object()], None)

        with pytest.raises(RuntimeError, match="1 results for 2 submitted units"):
            handle.collect()

    def test_background_failure_propagates(self) -> None:
        class _BackgroundErrorHandle(LifecycleRouter):
            def _dispatch(self, units, runtime_env):
                def _raise():
                    msg = "transport failed"
                    raise OSError(msg)

                self._start_background(_raise)

            def cancel(self):
                pass

        handle = _BackgroundErrorHandle()
        handle.dispatch([object()], None)
        assert handle._done.wait(timeout=2)

        with pytest.raises(OSError, match="transport failed"):
            handle.collect()

    def test_synchronous_dispatch_failure_marks_router_done(self) -> None:
        class _DispatchErrorHandle(LifecycleRouter):
            def _dispatch(self, units, runtime_env):
                msg = "submission failed"
                raise OSError(msg)

            def cancel(self):
                pass

        handle = _DispatchErrorHandle()

        with pytest.raises(OSError, match="submission failed"):
            handle.dispatch([object()], None)

        assert handle.is_done()


class TestCancelSentinel:
    """run() writes the cancel sentinel before cancelling the router."""

    def test_run_writes_sentinel_on_cancel(self, tmp_path) -> None:
        from types import SimpleNamespace

        from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
        from artisan.utils.path import cancel_sentinel_path

        staging = tmp_path / "staging"
        staging.mkdir()
        runtime_env = RuntimeEnvironment(
            delta_root=str(tmp_path / "delta"),
            working_root=str(tmp_path / "working"),
            staging_root=str(staging),
        )
        unit = SimpleNamespace(step_run_id="step-abc123")

        handle = _SlowStubHandle()
        cancel_event = threading.Event()
        cancel_event.set()

        def _complete_after_cancel():
            while handle.cancel_count == 0:
                pass
            handle.complete([_result()])

        t = threading.Thread(target=_complete_after_cancel, daemon=True)
        t.start()
        handle.run([unit], runtime_env, cancel_event=cancel_event)
        t.join(timeout=2)

        sentinel = cancel_sentinel_path(str(staging), "step-abc123")
        assert runtime_env.storage.filesystem().exists(sentinel)

    def test_failing_sentinel_write_is_logged_not_raised(self, caplog) -> None:
        from unittest.mock import MagicMock

        runtime_env = MagicMock()
        runtime_env.staging_root = "/tmp/staging"
        runtime_env.storage.filesystem.side_effect = RuntimeError("fs down")
        unit = MagicMock()
        unit.step_run_id = "step-abc123"

        handle = _SlowStubHandle()
        cancel_event = threading.Event()
        cancel_event.set()

        def _complete_after_cancel():
            while handle.cancel_count == 0:
                pass
            handle.complete([_result()])

        t = threading.Thread(target=_complete_after_cancel, daemon=True)
        t.start()
        results = handle.run([unit], runtime_env, cancel_event=cancel_event)
        t.join(timeout=2)

        assert len(results) == 1  # cancellation path completed despite the failure
        assert handle.cancel_count >= 1
        assert any("cancel sentinel" in record.message for record in caplog.records)

    def test_no_step_run_id_skips_sentinel(self) -> None:
        """Units without step_run_id (composites) write nothing and don't raise."""
        handle = _SlowStubHandle()
        cancel_event = threading.Event()
        cancel_event.set()

        def _complete_after_cancel():
            while handle.cancel_count == 0:
                pass
            handle.complete([_result()])

        t = threading.Thread(target=_complete_after_cancel, daemon=True)
        t.start()
        unit = type("Unit", (), {"step_run_id": None})()
        results = handle.run([unit], None, cancel_event=cancel_event)
        t.join(timeout=2)
        assert len(results) == 1
