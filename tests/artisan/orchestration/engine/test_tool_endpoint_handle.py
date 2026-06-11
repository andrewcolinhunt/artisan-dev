"""Tests for ToolEndpointDispatchHandle (lifecycle phases mocked)."""

from __future__ import annotations

import threading
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

from artisan.execution.executors.creator import _ExecuteFailure
from artisan.execution.tool_endpoint.client import _cancel_event
from artisan.orchestration.engine.tool_endpoint_handle import (
    ToolEndpointDispatchHandle,
)
from artisan.schemas.specs.input_models import ExecuteInput

_MODULE = "artisan.orchestration.engine.tool_endpoint_handle"


def _unit() -> MagicMock:
    unit = MagicMock()
    unit.get_batch_size.return_value = 2
    unit.user_overrides = {}
    return unit


def _prepped(operation: Any, n_artifacts: int = 2) -> SimpleNamespace:
    return SimpleNamespace(
        operation=operation,
        artifact_execute_inputs=[
            ExecuteInput(execute_dir=f"/tmp/a{i}") for i in range(n_artifacts)
        ],
        timings={},
        log_path="/tmp/unit.log",
        execution_context=MagicMock(),
        execution_run_id="run-1",
    )


def _run_one_unit(operation: Any, **mocks: Any) -> tuple[Any, dict[str, MagicMock]]:
    """Drive one unit through the handle with the lifecycle mocked."""
    handle = mocks.pop("handle", None) or ToolEndpointDispatchHandle(max_workers=2)
    prepped = mocks.pop("prepped", None) or _prepped(operation)
    with (
        patch(f"{_MODULE}._extract_inputs", return_value={}),
        patch(f"{_MODULE}.prep_unit", return_value=prepped) as prep,
        patch(f"{_MODULE}.post_unit") as post,
        patch(f"{_MODULE}.call_endpoint") as call,
        patch(f"{_MODULE}.record_execution_success") as rec_ok,
        patch(f"{_MODULE}.record_execution_failure") as rec_fail,
        patch(f"{_MODULE}._read_tool_output", return_value=None),
        patch(f"{_MODULE}.serialize_params", return_value={}),
    ):
        post.return_value = SimpleNamespace(artifacts={}, edges=[])
        if "post_side_effect" in mocks:
            post.side_effect = mocks.pop("post_side_effect")
        if "prep_side_effect" in mocks:
            prep.side_effect = mocks.pop("prep_side_effect")
        if "call_side_effect" in mocks:
            call.side_effect = mocks.pop("call_side_effect")
        results = handle.run([_unit()], MagicMock())  # runtime_env stand-in
    return results, {
        "prep": prep,
        "post": post,
        "call": call,
        "rec_ok": rec_ok,
        "rec_fail": rec_fail,
    }


class TestProcessUnit:
    def test_success_runs_all_artifacts_and_records(self):
        operation = MagicMock()

        results, mocks = _run_one_unit(operation)

        assert len(results) == 1
        assert results[0].success is True
        assert mocks["call"].call_count == 2
        raw_results = mocks["post"].call_args.args[1]
        assert raw_results == [None, None]  # endpoint products are files
        mocks["rec_ok"].assert_called_once()
        mocks["rec_fail"].assert_not_called()

    def test_artifacts_execute_concurrently(self):
        barrier = threading.Barrier(2, timeout=5)
        operation = MagicMock()

        results, _ = _run_one_unit(
            operation, call_side_effect=lambda _op, _ei: barrier.wait()
        )

        # both per-artifact calls must be in flight at once to pass the barrier
        assert results[0].success is True

    def test_per_artifact_failure_fails_unit_with_real_error(self):
        """An embedded exception must surface, not vanish into post_unit."""
        operation = MagicMock()

        results, mocks = _run_one_unit(
            operation, call_side_effect=[None, ValueError("container died")]
        )

        assert results[0].success is False
        assert "1/2 artifact executions failed" in results[0].error
        assert "container died" in results[0].error
        mocks["post"].assert_not_called()  # never reaches postprocess
        mocks["rec_fail"].assert_called_once()

    def test_post_failure_records_failure(self):
        operation = MagicMock()

        results, mocks = _run_one_unit(
            operation, post_side_effect=_ExecuteFailure("1/2 artifacts failed")
        )

        assert results[0].success is False
        assert "artifacts failed" in results[0].error
        mocks["rec_fail"].assert_called_once()
        mocks["rec_ok"].assert_not_called()

    def test_prep_failure_skips_recording(self):
        operation = MagicMock()

        results, mocks = _run_one_unit(
            operation, prep_side_effect=RuntimeError("sandbox unavailable")
        )

        assert results[0].success is False
        assert "sandbox unavailable" in results[0].error
        mocks["rec_fail"].assert_not_called()
        mocks["rec_ok"].assert_not_called()


class TestCancellation:
    def test_cancel_scope_carries_handle_event(self):
        """The endpoint call sees the handle's cancel event; cancel() trips it."""
        handle = ToolEndpointDispatchHandle(max_workers=1)
        seen: list[threading.Event] = []

        operation = MagicMock()

        def _capture(_op: Any, _ei: ExecuteInput) -> None:
            event = _cancel_event.get()
            assert event is not None
            seen.append(event)

        _run_one_unit(
            operation,
            handle=handle,
            prepped=_prepped(operation, 1),
            call_side_effect=_capture,
        )

        assert len(seen) == 1
        assert not seen[0].is_set()
        handle.cancel()
        assert seen[0].is_set()
