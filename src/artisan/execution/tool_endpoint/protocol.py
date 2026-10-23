"""Wire models for endpoint control, outputs, and optional diagnostic archives.

``WorkerResult`` holds the control manifest and independent inline archives.
Control plus both archives share one result-direction byte budget. Each archive
may instead use a ``StoredOutputs`` pointer; ``/download`` selects its plane.
Diagnostic files never become ordinary operation outputs.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from artisan.errors import ArtisanErrorEnvelope
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.replay import OperationIdentity
from artisan.schemas.orchestration.step_lifecycle import CancellationStatus


class InputRef(BaseModel):
    """A tool input file: inline bytes or an authorized remote URI.

    ``name`` is the input role; ``filename`` preserves the original file
    name across the wire — the worker materializes the file under it, so
    ``execute_command`` and operation-owned filename matching see the same
    basename as a local run. Clients send inline bytes as multipart parts keyed by
    ``name``; the endpoint repacks them into ``data`` for the worker hop.
    Authorized ``s3://`` refs use deployment credentials; authorized
    HTTP(S) refs are fetched as bare capabilities. Every URI carries the
    complete-file digest and size that the worker verifies before execution.
    """

    name: str
    filename: str | None = None
    uri: str | None = None
    data: bytes | None = None
    content_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    size_bytes: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def _one_data_plane(self) -> InputRef:
        """Require exactly one inline or referenced input payload."""
        if (self.uri is None) == (self.data is None):
            msg = "InputRef must carry exactly one of uri or data"
            raise ValueError(msg)
        has_digest = self.content_digest is not None
        has_size = self.size_bytes is not None
        if has_digest != has_size:
            msg = "InputRef content_digest and size_bytes must be provided together"
            raise ValueError(msg)
        if self.uri is not None and not has_digest:
            msg = "URI InputRef requires content_digest and size_bytes"
            raise ValueError(msg)
        if self.data is not None and has_digest:
            msg = "Inline InputRef must not carry URI integrity fields"
            raise ValueError(msg)
        return self


class CommandContract(BaseModel):
    """Verified operation declaration and normalized fixed command prefix."""

    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        hide_input_in_errors=True,
        json_schema_extra={
            "oneOf": [
                {
                    "properties": {
                        "kind": {"const": "tool"},
                        "tool_parts": {"minItems": 1},
                    }
                },
                {
                    "properties": {
                        "kind": {"const": "python_function"},
                        "tool_parts": {"maxItems": 0},
                    }
                },
            ],
            "allOf": [
                {
                    "properties": {
                        "operation_identity": {
                            "properties": {"module_digest": {"type": "string"}}
                        }
                    }
                }
            ],
        },
    )

    operation_identity: OperationIdentity
    kind: Literal["tool", "python_function"]
    tool_parts: list[str]

    @model_validator(mode="after")
    def validate_command(self) -> CommandContract:
        """Require a verifiable declaration and bound its control payload."""
        from artisan.execution.tool_endpoint.transport import MAX_INLINE_BYTES

        if (self.kind == "tool") != bool(self.tool_parts):
            msg = "command kind and tool_parts disagree"
            raise ValueError(msg)
        if self.operation_identity.module_digest is None:
            msg = "command operation source identity is unavailable"
            raise ValueError(msg)
        if len(self.model_dump_json().encode("utf-8")) > MAX_INLINE_BYTES:
            msg = "command contract exceeds the control byte limit"
            raise ValueError(msg)
        return self


class ToolRequest(BaseModel):
    """Internal worker payload: validated params + input refs."""

    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    expected_command_contract: CommandContract
    params: dict[str, Any] = Field(default_factory=dict)
    inputs: list[InputRef] = Field(default_factory=list)
    output_store: str | None = None
    debug_capture: bool = False


class StoredOutputs(BaseModel):
    """Point to an ordinary-output or diagnostic archive in object storage.

    Control-plane data: rides the manifest while the bytes stay in the
    store. In prefix mode ``presigned_url`` is minted worker-side at
    completion requesting a 7-day expiry, matching Modal's result retention.
    Credentials or store policy can shorten its usable lifetime. In
    capability mode (caller-supplied presigned PUT) it is None:
    the caller owns the destination and fetches with its own credentials;
    ``uri`` is the PUT URL stripped of its query.
    """

    uri: str
    presigned_url: str | None = None


class DebugCaptureManifest(BaseModel):
    """Separate, opt-in delivery of job-owned diagnostic files."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["complete", "unavailable", "failed"]
    entries: list[str] = Field(default_factory=list)
    stored: StoredOutputs | None = None
    error: str | None = None

    @model_validator(mode="after")
    def validate_delivery(self) -> DebugCaptureManifest:
        """Failed captures cannot advertise a delivered archive."""
        if self.status != "complete" and (self.stored is not None or self.entries):
            msg = "Incomplete diagnostic capture cannot carry files"
            raise ValueError(msg)
        if self.status == "complete" and self.error is not None:
            msg = "Complete diagnostic capture cannot carry an error"
            raise ValueError(msg)
        return self


class ToolManifest(BaseModel):
    """Control payload for a completed tool run — small, always JSON."""

    command_contract: CommandContract | None = None
    command_recording: CommandRecording
    operation_identity: OperationIdentity
    debug_capture: DebugCaptureManifest | None
    output_names: list[str] = Field(default_factory=list)
    stored: StoredOutputs | None = None
    log_tail: str | None = None
    error: ArtisanErrorEnvelope | None = None

    @model_validator(mode="after")
    def validate_command_contract(self) -> ToolManifest:
        """A successful result must attest the actual matching worker command."""
        if self.command_contract is None:
            if self.error is None:
                msg = "successful manifest requires command_contract"
                raise ValueError(msg)
        elif self.command_contract.operation_identity != self.operation_identity:
            msg = "manifest operation identity disagrees with command_contract"
            raise ValueError(msg)
        return self


class WorkerResult(BaseModel):
    """The worker's return value: control and independent inline byte planes."""

    manifest: ToolManifest
    output_tar: bytes | None = None
    debug_tar: bytes | None = None

    @model_validator(mode="after")
    def _one_data_plane(self) -> WorkerResult:
        """Allow at most one output plane and require one for complete diagnostics."""
        if self.output_tar is not None and self.manifest.stored is not None:
            msg = "WorkerResult carries both an inline tar and a stored pointer"
            raise ValueError(msg)
        capture = self.manifest.debug_capture
        if capture is not None and capture.status == "complete":
            if (self.debug_tar is None) == (capture.stored is None):
                msg = "Complete diagnostic capture requires exactly one plane"
                raise ValueError(msg)
        elif self.debug_tar is not None:
            msg = "Diagnostic bytes require a complete capture manifest"
            raise ValueError(msg)
        return self


class SchemaResponse(BaseModel):
    """``GET /schema`` response: the endpoint's request contract.

    ``params_schema`` is the same dict the ``/submit`` validator enforces;
    a parameterless op accepts only an empty object, rejecting extra keys.
    ``inputs`` maps each input role to
    ``{"required": bool, "description": str}``.
    """

    command_contract: CommandContract
    operation: str
    operation_identity: OperationIdentity
    debug_capture_supported: bool
    description: str = ""
    params_schema: dict[str, Any] = Field(default_factory=dict)
    inputs: dict[str, dict[str, Any]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_command_contract(self) -> SchemaResponse:
        """Keep the existing operation identity consistent with the contract."""
        if (
            self.command_contract.operation_identity != self.operation_identity
            or self.operation != self.operation_identity.name
        ):
            msg = "schema operation identity disagrees with command_contract"
            raise ValueError(msg)
        return self


class SubmitResponse(BaseModel):
    """``POST /submit`` response."""

    call_id: str


class CancelResponse(BaseModel):
    """``POST /cancel`` response naming the call and observed outcome."""

    call_id: str
    status: CancellationStatus
    message: str | None = None


class ResultResponse(BaseModel):
    """``GET /result`` response."""

    status: Literal["pending", "done", "failed", "expired"]
    manifest: ToolManifest | None = None
