"""Request ownership gates workspace use and rejects overlapping user code."""

from __future__ import annotations

import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock

import pytest
from fixtures.endpoint_ops import PlainTool

from artisan.errors import ErrorCode
from artisan.execution.tool_endpoint import server
from artisan.execution.tool_endpoint.protocol import ToolRequest
from artisan.execution.tool_endpoint.spec import command_contract
from artisan.schemas.operation_config.endpoint_policy import ToolEndpointDataPolicy
from artisan.utils.command_supervisor import (
    CommandOwnershipError,
    scope_command_observer,
)


def _request(**values) -> ToolRequest:
    return ToolRequest(expected_command_contract=command_contract(PlainTool), **values)


@pytest.mark.parametrize("fail", [False, True])
@pytest.mark.parametrize("stored", [False, True])
def test_unknown_cleanup_blocks_every_workspace_sink(monkeypatch, fail, stored):
    roots = []

    def work(op, inputs, **kwargs):
        output = Path(inputs.execute_dir)
        roots.append(output.parent)
        (output / "partial.txt").write_text("unfinished")
        Path(inputs.log_path).write_text("unverified log")
        if fail:
            message = "execution failed"
            raise ValueError(message)

    def uncertain(error=None):
        if error is not None:
            return False
        raise CommandOwnershipError()

    forbidden = Mock(side_effect=AssertionError("unsafe workspace access"))
    monkeypatch.setattr(server, "invoke_op_work", work)
    monkeypatch.setattr(server, "close_command_scope", uncertain)
    for name in ("_log_tail", "_list_outputs", "_capture_debug", "upload_outputs"):
        monkeypatch.setattr(server, name, forbidden)
    monkeypatch.setattr(server.InlineTransport, "pack_outputs", forbidden)
    try:
        result = server.run_tool_request(
            PlainTool,
            _request(
                debug_capture=True,
                output_store="s3://bucket/results" if stored else None,
            ),
            ToolEndpointDataPolicy(output_allowlist=("s3://bucket/results",)),
        )
        assert len(roots) == 1
        assert roots[0].is_dir()
        assert (roots[0] / "outputs" / "partial.txt").read_text() == "unfinished"
        forbidden.assert_not_called()
        assert result.manifest.error.code == ErrorCode.OP_EXECUTE_FAILED
        assert "workspace retained" in result.manifest.error.message
        if fail:
            assert "execution failed" in result.manifest.error.message
        assert result.manifest.output_names == []
        assert result.manifest.log_tail is None
        assert result.manifest.stored is None
        assert result.manifest.debug_capture.status == "unavailable"
        assert result.output_tar is None
        assert result.debug_tar is None
    finally:
        # The simulated uncertainty owns no real command; the test owns its files.
        for root in roots:
            shutil.rmtree(root, ignore_errors=True)


def test_overlapping_request_rejected_before_instantiation(monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    instantiate = Mock(wraps=server.instantiate_op)
    active_observers = []
    error_result = server._error_result

    def rejected(*args, **kwargs):
        assert scope_command_observer() is active_observers[0]
        return error_result(*args, **kwargs)

    def work(op, inputs, **kwargs):
        active_observers.append(scope_command_observer())
        assert active_observers[0] is not None
        entered.set()
        assert release.wait(10), "test did not release active request"
        (Path(inputs.execute_dir) / "result.txt").write_text("complete")

    monkeypatch.setattr(server, "instantiate_op", instantiate)
    monkeypatch.setattr(server, "invoke_op_work", work)
    monkeypatch.setattr(server, "_error_result", rejected)
    with ThreadPoolExecutor(max_workers=1) as pool:
        active = pool.submit(server.run_tool_request, PlainTool, _request())
        try:
            assert entered.wait(10), "active request never entered user code"
            rejected = server.run_tool_request(PlainTool, _request())
            assert rejected.manifest.error.code == ErrorCode.OP_EXECUTE_FAILED
            assert instantiate.call_count == 1
        finally:
            release.set()
        complete = active.result(timeout=10)
    assert complete.manifest.error is None
    assert complete.manifest.output_names == ["result.txt"]


def test_nested_request_rejected_without_disturbing_outer_scope(monkeypatch):
    nested = []
    instantiate = Mock(wraps=server.instantiate_op)

    def work(op, inputs, **kwargs):
        nested.append(server.run_tool_request(PlainTool, _request()))
        (Path(inputs.execute_dir) / "result.txt").write_text("complete")

    monkeypatch.setattr(server, "instantiate_op", instantiate)
    monkeypatch.setattr(server, "invoke_op_work", work)
    result = server.run_tool_request(PlainTool, _request())
    assert len(nested) == 1
    assert nested[0].manifest.error.code == ErrorCode.OP_EXECUTE_FAILED
    instantiate.assert_called_once()
    assert result.manifest.error is None
    assert result.manifest.output_names == ["result.txt"]
