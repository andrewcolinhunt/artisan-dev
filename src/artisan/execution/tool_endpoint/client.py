"""HTTP client for deployed tool endpoints — the ``compute_provider='modal'`` path.

``call_endpoint`` runs a command op's execute phase remotely: it submits the
op's params + input files, polls ``/result``, downloads the output tar into
``execute_dir`` (recreating the local layout), and appends the tool-log tail
to ``log_path``. Stored outputs (``output_store`` configured) are fetched
from the object store via the manifest's presigned URL instead of
``/download``. The caller exposes pipeline cancellation to the poll loop
via ``cancel_scope``.
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
from pydantic import ValidationError

from artisan.errors import ArtisanError, ArtisanErrorEnvelope, ErrorCode
from artisan.execution.tool_endpoint._optional import import_modal
from artisan.execution.tool_endpoint.protocol import (
    CancelResponse,
    InputRef,
    ResultResponse,
    ToolManifest,
)
from artisan.execution.tool_endpoint.transport import (
    EndpointTransportError,
    InlineTransport,
)
from artisan.schemas.operation_config.compute import ModalComputeConfig
from artisan.schemas.operation_config.endpoint_policy import _normalize_http_root
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
)
from artisan.schemas.specs.input_models import ExecuteInput
from artisan.utils.env_file import env_or_dotenv

DEFAULT_AUTH_PREFIX = "MODAL_PROXY"
"""Default env-var prefix for the proxy-auth token pair."""

_ENV_HINT = (
    "Create a proxy-auth token (Modal dashboard → Settings → Proxy Auth "
    "Tokens) and put it in a .env file at the repo root:\n"
    "  MODAL_PROXY_TOKEN_ID=wk-...\n"
    "  MODAL_PROXY_TOKEN_SECRET=ws-...\n"
    "(replace the placeholders with your tokens; env vars of the same names "
    "also work)"
)

_HTTP_TIMEOUT = 120.0

_cancel_event: ContextVar[threading.Event | None] = ContextVar(
    "tool_endpoint_cancel", default=None
)


class EndpointCancellationError(RuntimeError):
    """Typed terminal cancellation evidence from one named endpoint call."""

    def __init__(self, acknowledgement: CancellationAcknowledgement) -> None:
        super().__init__(acknowledgement.message or acknowledgement.status.value)
        self.acknowledgement = acknowledgement


@contextmanager
def cancel_scope(event: threading.Event) -> Iterator[None]:
    """Expose a cancel event to ``call_endpoint`` poll loops in this context."""
    token = _cancel_event.set(event)
    try:
        yield
    finally:
        _cancel_event.reset(token)


def call_endpoint(
    operation: Any, inputs: ExecuteInput
) -> CancellationAcknowledgement | None:
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
    cfg = _validated_modal_config(cfg, operation.name)
    base_url, headers = _resolve_target_and_auth(cfg, operation.name)
    transport = InlineTransport()
    refs = _pack_request_inputs(operation.name, inputs, cfg, transport)
    multipart = [
        ("files", (ref.name, ref.data)) for ref in refs if ref.data is not None
    ]
    uris = {ref.name: ref.uri for ref in refs if ref.uri is not None}
    filenames = {ref.name: ref.filename for ref in refs if ref.filename}
    integrity = {
        ref.name: {
            "content_digest": ref.content_digest,
            "size_bytes": ref.size_bytes,
        }
        for ref in refs
        if ref.uri is not None
    }
    output_store = None
    if cfg.output_store is not None:
        try:
            output_store = cfg.data_policy.authorize_output(
                cfg.output_store
            ).transport_target
        except ValueError as exc:
            raise _config_error(
                operation.name,
                str(exc),
            ) from exc

    with httpx.Client(
        base_url=base_url,
        headers=headers,
        timeout=_HTTP_TIMEOUT,
        follow_redirects=False,
    ) as client:
        data = {
            "params": operation.params_json(),
            "input_uris": json.dumps(uris),
            "input_filenames": json.dumps(filenames),
            "input_integrity": json.dumps(integrity),
        }
        if output_store is not None:
            data["output_store"] = output_store
        response = client.post("/submit", data=data, files=multipart or None)
        _check(response, operation.name)
        call_id = str(response.json()["call_id"])

        manifest, cancellation = _poll(
            client,
            call_id,
            cfg.poll_interval,
            operation.name,
        )
        if manifest.log_tail and inputs.log_path:
            _append_log(inputs.log_path, manifest.log_tail)
        if manifest.error is not None:
            _raise_from_envelope(manifest.error, operation.name)
        if manifest.stored is not None:
            if manifest.stored.presigned_url is None:
                # capability-mode pointer — unreachable via this client
                # (the config validator rejects PUT URLs); fail with the
                # contract, not a TypeError inside httpx
                raise ArtisanError(
                    code=ErrorCode.OP_EXECUTE_FAILED,
                    message="stored outputs carry no presigned URL to fetch",
                    error_type="compute",
                    operation_name=operation.name,
                )
            try:
                cfg.data_policy.authorize_output(manifest.stored.uri)
                cfg.data_policy.authorize_output(manifest.stored.presigned_url)
            except ValueError as exc:
                raise _config_error(operation.name, str(exc)) from exc
            try:
                transport.download_outputs(
                    manifest.stored.presigned_url,
                    inputs.execute_dir,
                    policy=cfg.data_policy,
                )
            except (EndpointTransportError, OSError, ValueError) as exc:
                raise ArtisanError(
                    code=ErrorCode.OP_EXECUTE_FAILED,
                    message=str(exc),
                    error_type="compute",
                    operation_name=operation.name,
                ) from exc
        elif manifest.output_names:
            download = client.get("/download", params={"call_id": call_id})
            _check(download, operation.name)
            transport.unpack_outputs(download.content, inputs.execute_dir)
        return cancellation


def _poll(
    client: httpx.Client, call_id: str, interval: float, op_name: str
) -> tuple[ToolManifest, CancellationAcknowledgement | None]:
    """Poll ``/result`` until the job leaves ``pending``; honor cancellation."""
    cancel = _cancel_event.get()
    cancellation: CancellationAcknowledgement | None = None
    while True:
        if cancel is not None and cancel.is_set() and cancellation is None:
            try:
                response = client.post("/cancel", params={"call_id": call_id})
                _check(response, op_name)
                cancelled = CancelResponse.model_validate(response.json())
            except ArtisanError:
                raise
            except Exception as exc:
                acknowledgement = CancellationAcknowledgement(
                    CancellationStatus.UNKNOWN,
                    (
                        f"Could not validate cancellation outcome for call "
                        f"{call_id}: {type(exc).__name__}"
                    ),
                )
                raise EndpointCancellationError(acknowledgement) from exc
            if cancelled.call_id != call_id:
                acknowledgement = CancellationAcknowledgement(
                    CancellationStatus.UNKNOWN,
                    (
                        f"Cancellation response named {cancelled.call_id!r}; "
                        f"expected {call_id!r}"
                    ),
                )
                raise EndpointCancellationError(acknowledgement)
            acknowledgement = CancellationAcknowledgement(
                cancelled.status,
                cancelled.message,
            )
            if acknowledgement.status in {
                CancellationStatus.CONFIRMED,
                CancellationStatus.UNKNOWN,
            }:
                raise EndpointCancellationError(acknowledgement)
            if acknowledgement.status == CancellationStatus.REJECTED:
                cancellation = acknowledgement
            else:
                time.sleep(interval)
                continue
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
        return result.manifest, cancellation


def _file_inputs(op_name: str, prepared: dict[str, Any]) -> dict[str, str]:
    """Validate that prepared inputs are file paths / URIs (v1 contract)."""
    # Deferred: importing artisan.execution.compute.invoke at module level
    # runs the compute package __init__, whose endpoint re-export imports
    # this module back — a cycle for any client-first import order.
    from artisan.execution.compute.invoke import tool_command_inputs

    files: dict[str, str] = {}
    for name, value in tool_command_inputs(prepared).items():
        if not isinstance(value, str):
            raise ArtisanError(
                code=ErrorCode.TOOL_ENDPOINT_MISCONFIGURED,
                message=(
                    f"prepared input {name!r} is {type(value).__name__}, not a "
                    "file path — tool ops under modal ship Params + input "
                    "files only; derive scalars in Params or execute_command"
                ),
                error_type="config",
                operation_name=op_name,
                field=f"inputs.{name}",
                recovery_hint="CHECK_INPUT",
            )
        files[name] = value
    return files


def _resolve_url(op_name: str) -> str:
    """Web URL of the Artisan-deployed endpoint for this op."""
    modal = import_modal()

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


def _resolve_target_and_auth(
    cfg: ModalComputeConfig,
    op_name: str,
) -> tuple[str, dict[str, str]]:
    """Resolve one endpoint target together with credentials scoped to it."""
    if cfg.endpoint_url is None:
        target = _validated_target(_resolve_url(op_name), op_name, authenticated=True)
        prefix = DEFAULT_AUTH_PREFIX if cfg.auth_secret is None else cfg.auth_secret
        if not prefix.strip():
            raise _config_error(
                op_name,
                "auth_secret must be a nonempty variable prefix",
            )
        return target, _auth_headers(prefix, op_name)
    authenticated = cfg.auth_secret is not None
    target = _validated_target(cfg.endpoint_url, op_name, authenticated=authenticated)
    if cfg.auth_secret is None:
        return target, {}
    if not cfg.auth_secret.strip():
        raise _config_error(op_name, "auth_secret must be a nonempty variable prefix")
    return target, _auth_headers(cfg.auth_secret, op_name)


def _validated_modal_config(
    cfg: ModalComputeConfig,
    op_name: str,
) -> ModalComputeConfig:
    """Revalidate configuration copied through Pydantic's unchecked update path."""
    try:
        return ModalComputeConfig.model_validate(
            cfg.model_dump(mode="python", warnings=False)
        )
    except ValidationError as exc:
        detail = "tool endpoint configuration is invalid"
        issues = exc.errors(
            include_url=False,
            include_context=False,
            include_input=False,
        )
        if issues:
            issue = issues[0]
            reason = str(issue["msg"]).removeprefix("Value error, ")
            detail = f"{detail}: {reason}"
        raise _config_error(op_name, detail) from exc
    except (TypeError, ValueError) as exc:
        raise _config_error(op_name, "tool endpoint configuration is invalid") from exc


def _validated_target(url: str, op_name: str, *, authenticated: bool) -> str:
    """Normalize the final endpoint URL, including unvalidated config copies."""
    try:
        target = _normalize_http_root(url)
    except ValueError as exc:
        raise _config_error(op_name, "tool endpoint URL must be an HTTP root") from exc
    if authenticated and not target.startswith("https://"):
        raise _config_error(op_name, "authenticated tool endpoint must use HTTPS")
    return target


def _auth_headers(prefix: str, op_name: str) -> dict[str, str]:
    """Proxy-auth headers from ``<prefix>_TOKEN_ID`` / ``<prefix>_TOKEN_SECRET``.

    Tokens are discovered from the process environment first, then the
    nearest ``.env`` file — so Jupyter kernels and cron jobs work without
    shell-inherited exports.
    """
    token_id = env_or_dotenv(f"{prefix}_TOKEN_ID")
    token_secret = env_or_dotenv(f"{prefix}_TOKEN_SECRET")
    if (
        not token_id
        or not token_id.strip()
        or not token_secret
        or not token_secret.strip()
    ):
        raise _config_error(
            op_name,
            f"proxy-auth token pair for prefix {prefix!r} is missing or incomplete",
            hint=_ENV_HINT if prefix == DEFAULT_AUTH_PREFIX else None,
        )
    return {"Modal-Key": token_id, "Modal-Secret": token_secret}


def _pack_request_inputs(
    op_name: str,
    inputs: ExecuteInput,
    cfg: ModalComputeConfig,
    transport: InlineTransport,
) -> list[InputRef]:
    """Authorize prepared URIs and bind them to D1 integrity descriptors."""
    files = _file_inputs(op_name, inputs.inputs)
    raw_contracts = inputs.metadata.get("external_integrity", {})
    if not isinstance(raw_contracts, dict):
        raise _config_error(op_name, "external input integrity metadata is invalid")
    expected: dict[str, tuple[str, int]] = {}
    authorized_files: dict[str, str] = {}
    for name, source in files.items():
        if "://" not in source:
            authorized_files[name] = source
            continue
        try:
            target = cfg.data_policy.authorize_input(source)
            contract = raw_contracts[source]
            digest = contract["content_digest"]
            size = contract["size_bytes"]
            if (
                not isinstance(digest, str)
                or len(digest) != 32
                or any(char not in "0123456789abcdef" for char in digest)
                or not isinstance(size, int)
                or isinstance(size, bool)
                or size < 0
            ):
                msg = "descriptor is malformed"
                raise ValueError(msg)
        except (KeyError, TypeError, ValueError) as exc:
            raise _config_error(
                op_name,
                f"prepared external input {name!r} is unauthorized or lacks its "
                "verified digest and size",
            ) from exc
        authorized_files[name] = target.transport_target
        descriptor = (digest, size)
        previous = expected.setdefault(target.transport_target, descriptor)
        if previous != descriptor:
            raise _config_error(
                op_name,
                "canonical-equivalent external inputs carry conflicting "
                "digest or size descriptors",
            )
    try:
        return transport.pack_inputs(authorized_files, expected)
    except (OSError, ValueError) as exc:
        raise _config_error(op_name, str(exc)) from exc


def _config_error(
    op_name: str,
    message: str,
    *,
    hint: str | None = None,
) -> ArtisanError:
    """Build one structured endpoint configuration error."""
    return ArtisanError(
        code=ErrorCode.TOOL_ENDPOINT_MISCONFIGURED,
        message=message,
        error_type="config",
        operation_name=op_name,
        hint=hint,
        recovery_hint="CHECK_INPUT",
    )


def _append_log(log_path: str, tail: str) -> None:
    """Append the worker's log tail to the unit log read by recording."""
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    with open(log_path, "a") as f:
        f.write(tail)


def _check(response: httpx.Response, op_name: str) -> None:
    """Raise a compute-typed ArtisanError on a non-2xx endpoint response."""
    # Modal's proxy answers missing/invalid tokens with a fast 401 response
    # (not a connection error); 407 handled defensively.
    if response.status_code in (401, 407):
        raise ArtisanError(
            code=ErrorCode.TOOL_ENDPOINT_MISCONFIGURED,
            message=(f"tool endpoint rejected authentication ({response.status_code})"),
            error_type="config",
            operation_name=op_name,
            hint=_ENV_HINT,
            recovery_hint="CHECK_INPUT",
        )
    if 300 <= response.status_code < 400:
        raise ArtisanError(
            code=ErrorCode.TOOL_ENDPOINT_MISCONFIGURED,
            message=f"tool endpoint redirect refused ({response.status_code})",
            error_type="config",
            operation_name=op_name,
            recovery_hint="CHECK_INPUT",
        )
    if not 200 <= response.status_code < 300:
        raise ArtisanError(
            code=ErrorCode.OP_EXECUTE_FAILED,
            message=f"tool endpoint request failed ({response.status_code})",
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
        field=envelope.field,
        hint=envelope.hint,
        suggestions=envelope.suggestions,
        recovery_hint=envelope.recovery_hint,
        doc_uri=envelope.doc_uri,
    )
