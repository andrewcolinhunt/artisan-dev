"""Wire models for the tool-endpoint protocol.

Control payloads (manifest, status) stay small; bulk files ride the
``DataTransport`` plane (``transport.py``). In v1 the worker's return value
**is** that plane: ``WorkerResult`` wraps the control manifest plus the
inline output tar, bounded by Modal's 100 MB function-call limit. ``/result``
returns the manifest only; ``/download`` streams the tar from the same
retained ``FunctionCall`` result.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from artisan.errors import ArtisanErrorEnvelope


class InputRef(BaseModel):
    """A tool input file: inline bytes or an object-store URI.

    ``name`` is the input role; ``filename`` preserves the original file
    name across the wire — the worker materializes the file under it, so
    ``build_command`` and lineage stem-matching see the same basename as a
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


class ToolManifest(BaseModel):
    """Control payload for a completed tool run — small, always JSON."""

    output_names: list[str] = Field(default_factory=list)
    log_tail: str | None = None
    error: ArtisanErrorEnvelope | None = None


class WorkerResult(BaseModel):
    """The worker's return value: the manifest plus the v1 inline data plane."""

    manifest: ToolManifest
    output_tar: bytes | None = None


class SubmitResponse(BaseModel):
    """``POST /submit`` response."""

    call_id: str


class ResultResponse(BaseModel):
    """``GET /result`` response."""

    status: Literal["pending", "done", "failed", "expired"]
    manifest: ToolManifest | None = None
