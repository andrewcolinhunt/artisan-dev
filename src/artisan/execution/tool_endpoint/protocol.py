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
    ``execute_command`` and lineage stem-matching see the same basename as a
    local run. Clients send inline bytes as multipart parts keyed by
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


class ToolRequest(BaseModel):
    """Internal worker payload: validated params + input refs."""

    model_config = ConfigDict(extra="forbid")

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

    command_recording: CommandRecording
    operation_identity: OperationIdentity
    debug_capture: DebugCaptureManifest | None
    output_names: list[str] = Field(default_factory=list)
    stored: StoredOutputs | None = None
    log_tail: str | None = None
    error: ArtisanErrorEnvelope | None = None


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

    operation: str
    operation_identity: OperationIdentity
    debug_capture_supported: bool
    description: str = ""
    params_schema: dict[str, Any] = Field(default_factory=dict)
    inputs: dict[str, dict[str, Any]] = Field(default_factory=dict)


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
