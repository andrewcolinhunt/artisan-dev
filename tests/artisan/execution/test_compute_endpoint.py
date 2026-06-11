"""Tests for EndpointExecuteRouter (HTTP client mocked)."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest.mock import MagicMock, patch

from artisan.execution.compute.endpoint import EndpointExecuteRouter
from artisan.execution.tool_endpoint.client import _cancel_event
from artisan.schemas.specs.input_models import ExecuteInput

_MODULE = "artisan.execution.compute.endpoint"


def _inputs(n: int) -> list[ExecuteInput]:
    return [ExecuteInput(execute_dir=f"/tmp/a{i}") for i in range(n)]


class TestRouteExecute:
    def test_one_call_per_artifact(self):
        """Every artifact gets exactly one endpoint call, results aligned."""
        operation = MagicMock()
        execute_inputs = _inputs(3)
        with patch(f"{_MODULE}.call_endpoint") as call:
            results = EndpointExecuteRouter().route_execute(
                operation, execute_inputs, "/tmp/sandbox"
            )
        assert results == [None, None, None]
        called_with = [c.args[1] for c in call.call_args_list]
        assert sorted(ei.execute_dir for ei in called_with) == [
            "/tmp/a0",
            "/tmp/a1",
            "/tmp/a2",
        ]

    def test_single_input_skips_the_pool(self):
        """A 1-artifact unit runs inline — no thread pool."""
        operation = MagicMock()
        with (
            patch(f"{_MODULE}.call_endpoint"),
            patch(f"{_MODULE}.ThreadPoolExecutor") as pool,
        ):
            results = EndpointExecuteRouter().route_execute(
                operation, _inputs(1), "/tmp/sandbox"
            )
        assert results == [None]
        pool.assert_not_called()

    def test_fan_out_capped_at_max_concurrent_calls(self):
        """Pool size is min(cap, artifacts); results stay aligned."""
        operation = MagicMock()
        seen_order: list[str] = []

        def _record(_op: Any, ei: ExecuteInput) -> None:
            seen_order.append(ei.execute_dir)

        router = EndpointExecuteRouter(max_concurrent_calls=2)
        with (
            patch(f"{_MODULE}.call_endpoint", side_effect=_record),
            patch(f"{_MODULE}.ThreadPoolExecutor", wraps=ThreadPoolExecutor) as pool,
        ):
            results = router.route_execute(operation, _inputs(5), "/tmp/sandbox")
        assert results == [None] * 5
        assert pool.call_args.kwargs["max_workers"] == 2
        assert len(seen_order) == 5

    def test_per_artifact_failure_is_an_exception_entry(self):
        """A failing artifact lands as an Exception entry; siblings complete."""
        operation = MagicMock()
        boom = ValueError("container died")

        def _maybe_fail(_op: Any, ei: ExecuteInput) -> None:
            if ei.execute_dir.endswith("a1"):
                raise boom

        with patch(f"{_MODULE}.call_endpoint", side_effect=_maybe_fail):
            results = EndpointExecuteRouter().route_execute(
                operation, _inputs(3), "/tmp/sandbox"
            )
        assert results[0] is None
        assert results[1] is boom
        assert results[2] is None


class TestCancellation:
    def test_calls_observe_router_cancel_event(self):
        """Each endpoint call runs under cancel_scope with the router's event."""
        router = EndpointExecuteRouter()
        seen: list[threading.Event] = []

        def _capture(_op: Any, _ei: ExecuteInput) -> None:
            event = _cancel_event.get()
            assert event is not None
            seen.append(event)

        with patch(f"{_MODULE}.call_endpoint", side_effect=_capture):
            router.route_execute(MagicMock(), _inputs(1), "/tmp/sandbox")

        assert len(seen) == 1
        assert seen[0] is router._cancel

    def test_watcher_sets_event_when_sentinel_appears(self):
        """cancel_check flipping True trips the cancel event via the watcher."""
        cancelled = threading.Event()
        router = EndpointExecuteRouter(cancel_check=cancelled.is_set)

        def _wait_for_cancel(_op: Any, _ei: ExecuteInput) -> None:
            event = _cancel_event.get()
            assert event is not None
            cancelled.set()  # orchestrator writes the sentinel mid-call
            assert event.wait(timeout=10), "watcher never set the cancel event"

        with (
            patch(f"{_MODULE}._CANCEL_POLL_SECONDS", 0.01),
            patch(f"{_MODULE}.call_endpoint", side_effect=_wait_for_cancel),
        ):
            router.route_execute(MagicMock(), _inputs(1), "/tmp/sandbox")

        assert router._cancel.is_set()

    def test_no_cancel_check_means_no_watcher(self):
        """cancel_check=None: no watcher thread is started."""
        router = EndpointExecuteRouter(cancel_check=None)
        before = threading.active_count()
        with patch(f"{_MODULE}.call_endpoint"):
            router.route_execute(MagicMock(), _inputs(1), "/tmp/sandbox")
        assert threading.active_count() == before
