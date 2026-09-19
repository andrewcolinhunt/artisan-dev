"""Tests for the tool-endpoint wire models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.errors import ArtisanError, ErrorCode
from artisan.execution.tool_endpoint.protocol import (
    CancelResponse,
    InputRef,
    ResultResponse,
    SchemaResponse,
    StoredOutputs,
    SubmitResponse,
    ToolManifest,
    ToolRequest,
    WorkerResult,
)
from artisan.operations.examples import WaitTool
from artisan.registry.resolve import operation_identity
from artisan.schemas.execution.command_record import CommandRecording


class TestInputRef:
    def test_inline_ref(self):
        ref = InputRef(name="pdb", data=b"ATOM")
        assert ref.uri is None
        assert ref.data == b"ATOM"

    def test_uri_ref(self):
        ref = InputRef(
            name="pdb",
            uri="s3://bucket/key.pdb",
            content_digest="a" * 32,
            size_bytes=4,
        )
        assert ref.data is None

    def test_uri_requires_digest_and_size(self):
        with pytest.raises(ValidationError, match="requires content_digest"):
            InputRef(name="pdb", uri="s3://bucket/key.pdb")

    def test_uri_integrity_fields_are_required_together(self):
        with pytest.raises(ValidationError, match="provided together"):
            InputRef(
                name="pdb",
                uri="s3://bucket/key.pdb",
                content_digest="a" * 32,
            )

    def test_rejects_ref_without_data_plane(self):
        with pytest.raises(ValidationError, match="exactly one of uri or data"):
            InputRef(name="pdb")

    def test_rejects_ref_with_both_data_planes(self):
        with pytest.raises(ValidationError, match="exactly one of uri or data"):
            InputRef(name="pdb", uri="s3://bucket/key.pdb", data=b"ATOM")


class TestToolRequest:
    def test_defaults(self):
        request = ToolRequest()
        assert request.params == {}
        assert request.inputs == []
        assert request.output_store is None

    def test_round_trip_with_bytes(self):
        request = ToolRequest(
            params={"n": 1}, inputs=[InputRef(name="f", data=b"\x00\x01")]
        )
        restored = ToolRequest(**request.model_dump())
        assert restored == request

    def test_output_store_round_trips(self):
        request = ToolRequest(output_store="s3://bucket/prefix")
        assert ToolRequest(**request.model_dump()) == request

    def test_deployment_policy_is_not_a_wire_field(self):
        assert set(ToolRequest.model_fields) == {
            "params",
            "inputs",
            "output_store",
            "debug_capture",
        }
        schema = ToolRequest.model_json_schema()
        assert "data_policy" not in schema["properties"]
        assert "input_allowlist" not in schema["properties"]
        assert "output_allowlist" not in schema["properties"]

    def test_deployment_policy_cannot_be_submitted_as_extra_data(self):
        with pytest.raises(ValidationError, match="Extra inputs"):
            ToolRequest.model_validate(
                {
                    "data_policy": {
                        "input_allowlist": ["s3://attacker"],
                        "output_allowlist": ["s3://attacker"],
                    }
                }
            )


class TestToolManifest:
    def test_defaults(self):
        manifest = ToolManifest(
            operation_identity=operation_identity(WaitTool),
            debug_capture=None,
            command_recording=CommandRecording.empty(),
        )
        assert manifest.output_names == []
        assert manifest.stored is None
        assert manifest.log_tail is None
        assert manifest.error is None

    def test_stored_round_trip(self):
        manifest = ToolManifest(
            operation_identity=operation_identity(WaitTool),
            debug_capture=None,
            command_recording=CommandRecording.empty(),
            output_names=["a.txt"],
            stored=StoredOutputs(
                uri="s3://bucket/prefix/my_op/abc123.tar.gz",
                presigned_url="https://bucket.s3.amazonaws.com/signed?sig=x",
            ),
        )
        assert ToolManifest(**manifest.model_dump()) == manifest

    def test_stored_capability_mode_has_no_presigned_url(self):
        # caller-supplied presigned PUT: the caller owns the destination
        stored = StoredOutputs(uri="https://bucket.s3.amazonaws.com/run42.tar.gz")
        assert stored.presigned_url is None
        manifest = ToolManifest(
            operation_identity=operation_identity(WaitTool),
            debug_capture=None,
            command_recording=CommandRecording.empty(),
            stored=stored,
        )
        assert ToolManifest(**manifest.model_dump()) == manifest

    def test_error_envelope_round_trip(self):
        envelope = ArtisanError(
            code=ErrorCode.OP_EXECUTE_FAILED,
            message="tool exploded",
            error_type="compute",
            operation_name="wait_tool",
        ).envelope
        manifest = ToolManifest(
            operation_identity=operation_identity(WaitTool),
            debug_capture=None,
            command_recording=CommandRecording.empty(),
            error=envelope,
        )
        restored = ToolManifest(**manifest.model_dump())
        assert restored.error is not None
        assert restored.error.code == "op_execute_failed"
        assert restored.error.error_type == "compute"
        assert restored.error.message == "tool exploded"


class TestWorkerResult:
    def test_round_trip_with_tar(self):
        result = WorkerResult(
            manifest=ToolManifest(
                operation_identity=operation_identity(WaitTool),
                debug_capture=None,
                command_recording=CommandRecording.empty(),
                output_names=["a.txt"],
            ),
            output_tar=b"tarbytes",
        )
        restored = WorkerResult(**result.model_dump())
        assert restored == result

    def test_failure_has_no_tar(self):
        result = WorkerResult(
            manifest=ToolManifest(
                operation_identity=operation_identity(WaitTool),
                debug_capture=None,
                command_recording=CommandRecording.empty(),
            )
        )
        assert result.output_tar is None

    def test_stored_alone_accepted(self):
        result = WorkerResult(
            manifest=ToolManifest(
                operation_identity=operation_identity(WaitTool),
                debug_capture=None,
                command_recording=CommandRecording.empty(),
                stored=StoredOutputs(uri="s3://b/k.tar.gz"),
            )
        )
        assert result.output_tar is None

    def test_rejects_tar_and_stored_together(self):
        with pytest.raises(ValidationError, match="both an inline tar"):
            WorkerResult(
                manifest=ToolManifest(
                    operation_identity=operation_identity(WaitTool),
                    debug_capture=None,
                    command_recording=CommandRecording.empty(),
                    stored=StoredOutputs(uri="s3://b/k.tar.gz"),
                ),
                output_tar=b"tarbytes",
            )


class TestResponses:
    def test_submit_response(self):
        assert SubmitResponse(call_id="fc-123").call_id == "fc-123"

    @pytest.mark.parametrize(
        "status", ["requested", "confirmed", "rejected", "unknown"]
    )
    def test_cancel_response_statuses(self, status):
        response = CancelResponse(call_id="fc-123", status=status)
        assert response.status.value == status

    def test_schema_response_defaults(self):
        response = SchemaResponse(
            operation="wait_tool",
            operation_identity=operation_identity(WaitTool),
            debug_capture_supported=True,
        )
        assert response.description == ""
        assert response.params_schema == {}
        assert response.inputs == {}

    @pytest.mark.parametrize("status", ["pending", "done", "failed", "expired"])
    def test_result_response_statuses(self, status):
        assert ResultResponse(status=status).status == status

    def test_result_response_rejects_unknown_status(self):
        with pytest.raises(ValidationError):
            ResultResponse(status="exploded")
