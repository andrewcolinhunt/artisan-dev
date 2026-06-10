"""HTTP client for deployed tool endpoints — the ``compute_provider='modal'`` path.

``call_endpoint`` is invoked by the framework ``execute()``: it submits the
op's params + input files, polls ``/result``, downloads the output tar into
``execute_dir`` (recreating the local layout), and appends the tool-log tail
to ``log_path``. The dispatch handle exposes pipeline cancellation to the
poll loop via ``cancel_scope``.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, NoReturn

import httpx
from pydantic import BaseModel

from artisan.errors import ArtisanError, ArtisanErrorEnvelope, ErrorCode
from artisan.execution.tool_endpoint.protocol import ResultResponse, ToolManifest
from artisan.execution.tool_endpoint.transport import InlineTransport
from artisan.schemas.operation_config.compute import ModalComputeConfig
from artisan.schemas.specs.input_models import ExecuteInput

DEFAULT_AUTH_PREFIX = "MODAL_PROXY"
"""Default env-var prefix for the proxy-auth token pair."""

_HTTP_TIMEOUT = 120.0

_cancel_event: ContextVar[threading.Event | None] = ContextVar(
    "tool_endpoint_cancel", default=None
)


@contextmanager
def cancel_scope(event: threading.Event) -> Iterator[None]:
    """Expose a cancel event to ``call_endpoint`` poll loops in this context."""
    token = _cancel_event.set(event)
    try:
        yield
    finally:
        _cancel_event.reset(token)


def call_endpoint(operation: Any, inputs: ExecuteInput) -> None:
    """Run a tool op's execute on its deployed endpoint.

    Args:
        operation: The tool op instance (provides params + modal config).
        inputs: The per-artifact ExecuteInput (file paths + execute dir).

    Raises:
        ArtisanError: Misconfiguration, HTTP failure, expired result, or a
            tool failure re-raised from the worker's error envelope.
        RuntimeError: When the pipeline cancel event fires mid-poll.
    """
    cfg = operation.compute_provider.modal
    if not isinstance(cfg, ModalComputeConfig):
        raise ArtisanError(
            code=ErrorCode.TOOL_ENDPOINT_MISCONFIGURED,
            message=f"{operation.name} has no compute_provider.modal config",
            error_type="config",
            operation_name=operation.name,
            recovery_hint="CHECK_INPUT",
        )
    base_url = cfg.endpoint_url or _resolve_url(operation.name)
    transport = InlineTransport()
    refs = transport.pack_inputs(_file_inputs(operation.name, inputs.inputs))
    multipart = [
        ("files", (ref.name, ref.data)) for ref in refs if ref.data is not None
    ]
    uris = {ref.name: ref.uri for ref in refs if ref.uri is not None}

    with httpx.Client(
        base_url=base_url,
        headers=_auth_headers(cfg.auth_secret),
        timeout=_HTTP_TIMEOUT,
    ) as client:
        response = client.post(
            "/submit",
            data={
                "params": _params_json(operation),
                "input_uris": json.dumps(uris),
            },
            files=multipart or None,
        )
        _check(response, operation.name)
        call_id = str(response.json()["call_id"])

        manifest = _poll(client, call_id, cfg.poll_interval, operation.name)
        if manifest.log_tail and inputs.log_path:
            _append_log(inputs.log_path, manifest.log_tail)
        if manifest.error is not None:
            _raise_from_envelope(manifest.error, operation.name)
        if manifest.output_names:
            download = client.get("/download", params={"call_id": call_id})
            _check(download, operation.name)
            transport.unpack_outputs(download.content, inputs.execute_dir)


def _poll(
    client: httpx.Client, call_id: str, interval: float, op_name: str
) -> ToolManifest:
    """Poll ``/result`` until the job leaves ``pending``; honor cancellation."""
    cancel = _cancel_event.get()
    while True:
        if cancel is not None and cancel.is_set():
            client.post("/cancel", params={"call_id": call_id})
            msg = f"tool endpoint job {call_id} cancelled"
            raise RuntimeError(msg)
        response = client.get("/result", params={"call_id": call_id})
        _check(response, op_name)
        result = ResultResponse(**response.json())
        if result.status == "pending":
            time.sleep(interval)
            continue
        if result.status == "expired":
            raise ArtisanError(
                code=ErrorCode.OP_EXECUTE_FAILED,
                message=(
                    f"result for call {call_id} expired — Modal retains results 7 days"
                ),
                error_type="compute",
                operation_name=op_name,
                recovery_hint="RETRY_LATER",
            )
        if result.manifest is None:
            raise ArtisanError(
                code=ErrorCode.OP_EXECUTE_FAILED,
                message=f"/result returned {result.status} without a manifest",
                error_type="compute",
                operation_name=op_name,
            )
        return result.manifest


def _file_inputs(op_name: str, prepared: dict[str, Any]) -> dict[str, str]:
    """Validate that prepared inputs are file paths / URIs (v1 contract)."""
    files: dict[str, str] = {}
    for name, value in prepared.items():
        if not isinstance(value, str):
            raise ArtisanError(
                code=ErrorCode.TOOL_ENDPOINT_MISCONFIGURED,
                message=(
                    f"prepared input {name!r} is {type(value).__name__}, not a "
                    "file path — tool ops under modal ship Params + input "
                    "files only; derive scalars in Params or build_command"
                ),
                error_type="config",
                operation_name=op_name,
                field=f"inputs.{name}",
                recovery_hint="CHECK_INPUT",
            )
        files[name] = value
    return files


def _params_json(operation: Any) -> str:
    """The op's nested Params as JSON (the endpoint's typed schema)."""
    params = getattr(operation, "params", None)
    if isinstance(params, BaseModel):
        return params.model_dump_json()
    return "{}"


def _resolve_url(op_name: str) -> str:
    """Web URL of the Artisan-deployed endpoint for this op."""
    import modal

    function = modal.Function.from_name(f"artisan-tool-{op_name}", "endpoint")
    url = function.get_web_url()
    if not url:
        raise ArtisanError(
            code=ErrorCode.TOOL_ENDPOINT_MISCONFIGURED,
            message=f"artisan-tool-{op_name} has no web URL — is it deployed?",
            error_type="config",
            operation_name=op_name,
            hint=f"Run: artisan modal deploy {op_name}",
        )
    return str(url)


def _auth_headers(auth_secret: str | None) -> dict[str, str]:
    """Proxy-auth headers from ``<prefix>_TOKEN_ID`` / ``<prefix>_TOKEN_SECRET``."""
    prefix = auth_secret or DEFAULT_AUTH_PREFIX
    token_id = os.environ.get(f"{prefix}_TOKEN_ID")
    token_secret = os.environ.get(f"{prefix}_TOKEN_SECRET")
    if token_id and token_secret:
        return {"Modal-Key": token_id, "Modal-Secret": token_secret}
    return {}


def _append_log(log_path: str, tail: str) -> None:
    """Append the worker's log tail to the unit log read by recording."""
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    with open(log_path, "a") as f:
        f.write(tail)


def _check(response: httpx.Response, op_name: str) -> None:
    """Raise a compute-typed ArtisanError on a non-2xx endpoint response."""
    if response.status_code >= 400:
        raise ArtisanError(
            code=ErrorCode.OP_EXECUTE_FAILED,
            message=(
                f"tool endpoint returned {response.status_code}: {response.text[:500]}"
            ),
            error_type="compute",
            operation_name=op_name,
        )


def _raise_from_envelope(envelope: ArtisanErrorEnvelope, op_name: str) -> NoReturn:
    """Re-raise the worker's error envelope as a client-side ArtisanError."""
    raise ArtisanError(
        code=envelope.code,
        message=envelope.message,
        error_type=envelope.error_type,
        operation_name=envelope.operation_name or op_name,
        hint=envelope.hint,
        recovery_hint=envelope.recovery_hint,
        doc_uri=envelope.doc_uri,
    )
