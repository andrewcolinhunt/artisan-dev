"""Wire models for the tool-endpoint protocol.

Control payloads (manifest, status) stay small; bulk files ride the
transport data plane (``transport.py``). Inline mode: the worker's return
value **is** that plane — ``WorkerResult`` wraps the control manifest plus
the inline output tar, bounded by Modal's 100 MB function-call limit;
``/result`` returns the manifest only and ``/download`` streams the tar from
the same retained ``FunctionCall`` result. Stored mode (the request names an
``output_store``): the bytes go to the object store and the manifest carries
a ``StoredOutputs`` pointer instead — the result is pure control.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from artisan.errors import ArtisanErrorEnvelope


class InputRef(BaseModel):
    """A tool input file: inline bytes or an object-store URI.

    ``name`` is the input role; ``filename`` preserves the original file
    name across the wire — the worker materializes the file under it, so
    ``execute_command`` and lineage stem-matching see the same basename as a
    local run. Clients send inline bytes as multipart parts keyed by
    ``name``; the endpoint repacks them into ``data`` for the worker hop.
    ``uri`` refs (e.g. ``s3://bucket/key``) are fetched worker-side via
    fsspec and bypass the inline bound — already-external artifacts
    re-upload nothing.
    """

    name: str
    filename: str | None = None
    uri: str | None = None
    data: bytes | None = None


class ToolRequest(BaseModel):
    """Internal worker payload: validated params + input refs."""

    params: dict[str, Any] = Field(default_factory=dict)
    inputs: list[InputRef] = Field(default_factory=list)
    output_store: str | None = None


class StoredOutputs(BaseModel):
    """Object-store pointer to a completed run's output tarball.

    Control-plane data: rides the manifest while the bytes stay in the
    store. In prefix mode ``presigned_url`` is minted worker-side at
    completion with a 7-day expiry — the SigV4 maximum, matching Modal's
    result retention, so the URL never goes stale while the result is
    alive. In capability mode (caller-supplied presigned PUT) it is None:
    the caller owns the destination and fetches with its own credentials;
    ``uri`` is the PUT URL stripped of its query.
    """

    uri: str
    presigned_url: str | None = None


class ToolManifest(BaseModel):
    """Control payload for a completed tool run — small, always JSON."""

    output_names: list[str] = Field(default_factory=list)
    stored: StoredOutputs | None = None
    log_tail: str | None = None
    error: ArtisanErrorEnvelope | None = None


class WorkerResult(BaseModel):
    """The worker's return value: the manifest plus the inline data plane."""

    manifest: ToolManifest
    output_tar: bytes | None = None

    @model_validator(mode="after")
    def _one_data_plane(self) -> WorkerResult:
        """Outputs ride exactly one plane — inline tar XOR stored pointer."""
        if self.output_tar is not None and self.manifest.stored is not None:
            msg = "WorkerResult carries both an inline tar and a stored pointer"
            raise ValueError(msg)
        return self


class SchemaResponse(BaseModel):
    """``GET /schema`` response: the endpoint's request contract.

    ``params_schema`` is the same dict the ``/submit`` validator enforces;
    empty means the op declares no ``Params``. ``inputs`` maps each input
    role to ``{"required": bool, "description": str}``.
    """

    operation: str
    description: str = ""
    params_schema: dict[str, Any] = Field(default_factory=dict)
    inputs: dict[str, dict[str, Any]] = Field(default_factory=dict)


class SubmitResponse(BaseModel):
    """``POST /submit`` response."""

    call_id: str


class ResultResponse(BaseModel):
    """``GET /result`` response."""

    status: Literal["pending", "done", "failed", "expired"]
    manifest: ToolManifest | None = None
