"""Tests for the tool-endpoint wire models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.errors import ArtisanError, ErrorCode
from artisan.execution.tool_endpoint.protocol import (
    InputRef,
    ResultResponse,
    SubmitResponse,
    ToolManifest,
    ToolRequest,
    WorkerResult,
)


class TestInputRef:
    def test_inline_ref(self):
        ref = InputRef(name="pdb", data=b"ATOM")
        assert ref.uri is None
        assert ref.data == b"ATOM"

    def test_uri_ref(self):
        ref = InputRef(name="pdb", uri="s3://bucket/key.pdb")
        assert ref.data is None


class TestToolRequest:
    def test_defaults(self):
        request = ToolRequest()
        assert request.params == {}
        assert request.inputs == []

    def test_round_trip_with_bytes(self):
        request = ToolRequest(
            params={"n": 1}, inputs=[InputRef(name="f", data=b"\x00\x01")]
        )
        restored = ToolRequest(**request.model_dump())
        assert restored == request


class TestToolManifest:
    def test_defaults(self):
        manifest = ToolManifest()
        assert manifest.output_names == []
        assert manifest.log_tail is None
        assert manifest.error is None

    def test_error_envelope_round_trip(self):
        envelope = ArtisanError(
            code=ErrorCode.OP_EXECUTE_FAILED,
            message="tool exploded",
            error_type="compute",
            operation_name="echo_tool",
        ).envelope
        manifest = ToolManifest(error=envelope)
        restored = ToolManifest(**manifest.model_dump())
        assert restored.error is not None
        assert restored.error.code == "op_execute_failed"
        assert restored.error.error_type == "compute"
        assert restored.error.message == "tool exploded"


class TestWorkerResult:
    def test_round_trip_with_tar(self):
        result = WorkerResult(
            manifest=ToolManifest(output_names=["a.txt"]), output_tar=b"tarbytes"
        )
        restored = WorkerResult(**result.model_dump())
        assert restored == result

    def test_failure_has_no_tar(self):
        result = WorkerResult(manifest=ToolManifest())
        assert result.output_tar is None


class TestResponses:
    def test_submit_response(self):
        assert SubmitResponse(call_id="fc-123").call_id == "fc-123"

    @pytest.mark.parametrize("status", ["pending", "done", "failed", "expired"])
    def test_result_response_statuses(self, status):
        assert ResultResponse(status=status).status == status

    def test_result_response_rejects_unknown_status(self):
        with pytest.raises(ValidationError):
            ResultResponse(status="exploded")
