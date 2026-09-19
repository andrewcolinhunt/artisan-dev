"""Worker-side execution of tool requests.

Runs an already resolved operation class inside the worker image, with local
input files and command or Python execution. Returns a manifest with inline
or stored outputs and independent optional diagnostics. No Modal imports.
"""

from __future__ import annotations

import os
import shutil
import tarfile
import tempfile
from typing import Any

import httpx
from pydantic import ValidationError

from artisan.errors import (
    ArtifactIntegrityError,
    ArtisanError,
    ErrorCode,
    ErrorType,
    RecoveryHint,
)
from artisan.execution.compute.invoke import invoke_op_work
from artisan.execution.recording.commands import (
    capture_commands,
    command_snapshot,
    current_recorder,
    invocation_scope,
    sanitize_diagnostic,
)
from artisan.execution.tool_endpoint import transport as transport_module
from artisan.execution.tool_endpoint.protocol import (
    DebugCaptureManifest,
    ToolManifest,
    ToolRequest,
    WorkerResult,
)
from artisan.execution.tool_endpoint.transport import (
    MAX_ARCHIVE_MEMBERS,
    EndpointTransportError,
    InlineTransport,
    upload_outputs,
)
from artisan.execution.transport.log_constants import (
    MAX_TOOL_OUTPUT_BYTES,
    TOOL_OUTPUT_FILENAME,
)
from artisan.operations.base._param_docs import _params_class
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.registry.resolve import operation_identity
from artisan.schemas.operation_config.endpoint_policy import ToolEndpointDataPolicy
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.schemas.specs.input_models import ExecuteInput

_INPUT_RESOLUTION_ERRORS: tuple[type[BaseException], ...] = (
    ValueError,
    OSError,
    shutil.Error,
)
_OUTPUT_BOUNDARY_ERRORS: tuple[type[BaseException], ...] = (
    OSError,
    shutil.Error,
    tarfile.TarError,
    httpx.HTTPError,
)
try:
    from botocore.exceptions import (  # type: ignore[import-untyped]
        BotoCoreError,
        ClientError,
    )
except ModuleNotFoundError:
    pass
else:
    _INPUT_RESOLUTION_ERRORS += (BotoCoreError, ClientError)
    _OUTPUT_BOUNDARY_ERRORS += (BotoCoreError, ClientError)


def run_tool_request(
    op_cls: type[OperationDefinition],
    request: ToolRequest,
    data_policy: ToolEndpointDataPolicy | None = None,
) -> WorkerResult:
    """Run a tool request in a temporary worker workspace and return its result.

    Construct the operation from request parameters, materialize its inputs,
    and run its command or ``execute_as_tool`` Python body locally. Deliver
    ordinary outputs inline or to the requested store. These outputs exclude
    input files and the tool log; the manifest carries a sanitized log tail.

    Opt-in diagnostics capture inputs, outputs, and the full log separately,
    including on failure. Clean up the workspace after delivery or capture,
    and attach fresh request-wide command evidence to ordinary outcomes.

    Args:
        op_cls: Resolved operation class to execute.
        request: Parameters, input references, and output/capture preferences.
        data_policy: Deployment-owned URI permissions. Omission denies remote
            input and output URIs.

    Returns:
        A manifest and independent optional output/diagnostic archives.
        Ordinary validation, input, execution, and delivery failures appear
        as structured errors on the manifest.

    Raises:
        EndpointTransportError: Even the minimal failure manifest exceeds
            the control payload limit.
    """
    with capture_commands(location="endpoint"), invocation_scope():
        try:
            result = _run_tool_request(op_cls, request, data_policy)
        except Exception as exc:
            result = _error_result(
                op_cls,
                ErrorCode.OP_EXECUTE_FAILED,
                str(exc),
                "compute",
                "REPORT_TO_USER",
            )

        if request.debug_capture and result.manifest.debug_capture is None:
            result.manifest.debug_capture = DebugCaptureManifest(
                status="unavailable",
                error="tool workspace was not created",
            )
        return _bound_result(result, op_cls)


def _run_tool_request(
    op_cls: type[OperationDefinition],
    request: ToolRequest,
    data_policy: ToolEndpointDataPolicy | None = None,
) -> WorkerResult:
    """Own request validation, workspace execution, delivery, and cleanup."""
    try:
        policy = ToolEndpointDataPolicy.model_validate(
            data_policy.model_dump(mode="python", warnings=False)
            if data_policy is not None
            else {}
        )
    except (TypeError, ValueError):
        return _error_result(
            op_cls,
            ErrorCode.TOOL_ENDPOINT_MISCONFIGURED,
            "endpoint deployment data policy is invalid",
            "config",
            "REPORT_TO_USER",
        )
    denied = _preflight_request(op_cls, request, policy)
    if denied is not None:
        return denied

    try:
        op = instantiate_op(op_cls, request.params)
        recorder = current_recorder()
        assert recorder is not None
        recorder.add_operation(op)
    except ValidationError:
        # Direct callers bypass /submit, and custom validators can reject
        # schema-valid values. No workspace exists yet to clean up.
        return _error_result(
            op_cls,
            ErrorCode.PARAM_TYPE_MISMATCH,
            "tool parameters failed validation",
            "validation",
            "CHECK_INPUT",
        )

    try:
        job_root = tempfile.mkdtemp(prefix=f"artisan-tool-{op_cls.name}-")
    except OSError:
        return _error_result(
            op_cls,
            ErrorCode.OP_EXECUTE_FAILED,
            "could not create tool workspace",
            "io",
            "RETRY_LATER",
        )
    try:
        try:
            result = _execute_job(op, request, policy, job_root)
        except Exception as exc:
            result = _error_result(
                op_cls,
                ErrorCode.OP_EXECUTE_FAILED,
                str(exc),
                "compute",
                "REPORT_TO_USER",
            )
        if request.debug_capture:
            result = _capture_debug(result, request, policy, job_root)
        return result
    finally:
        shutil.rmtree(job_root, ignore_errors=True)


def _execute_job(
    op: OperationDefinition,
    request: ToolRequest,
    policy: ToolEndpointDataPolicy,
    job_root: str,
) -> WorkerResult:
    """Execute and deliver outputs; the caller captures every workspace outcome."""
    op_cls = type(op)
    inputs_dir = os.path.join(job_root, "inputs")
    outputs_dir = os.path.join(job_root, "outputs")
    try:
        os.makedirs(outputs_dir)
    except OSError:
        return _error_result(
            op_cls,
            ErrorCode.OP_EXECUTE_FAILED,
            "could not create tool output directory",
            "io",
            "RETRY_LATER",
        )

    transport = InlineTransport()
    try:
        inputs = transport.unpack_inputs(
            request.inputs,
            inputs_dir,
            policy=policy,
        )
    except ArtifactIntegrityError as exc:
        return _error_result(
            op_cls,
            ErrorCode.ARTIFACT_INTEGRITY_FAILED,
            str(exc),
            "io",
            "CHECK_INPUT",
        )
    except ImportError:
        return _error_result(
            op_cls,
            ErrorCode.TOOL_ENDPOINT_MISCONFIGURED,
            "input filesystem dependency is unavailable",
            "config",
            "REPORT_TO_USER",
        )
    except EndpointTransportError as exc:
        return _error_result(
            op_cls,
            ErrorCode.INPUT_RESOLUTION_FAILED,
            str(exc),
            "io",
            "CHECK_INPUT",
        )
    except _INPUT_RESOLUTION_ERRORS:
        # malformed ref; a URI that would not resolve (missing object,
        # denied read — s3fs maps these to FileNotFoundError/
        # PermissionError); or a botocore root s3fs returns untranslated
        # (NoCredentialsError, EndpointConnectionError — the R2 bad-creds
        # / bad-endpoint modes, both BotoCoreError, neither an OSError).
        # Earlier inputs may already exist; the final capture retains them.
        return _error_result(
            op_cls,
            ErrorCode.INPUT_RESOLUTION_FAILED,
            "could not resolve tool input",
            "io",
            "CHECK_INPUT",
        )

    log_path = os.path.join(outputs_dir, TOOL_OUTPUT_FILENAME)
    try:
        # The shared primitive — the same invocation as the local execute
        # router, so the two sides cannot drift. The container is the
        # environment; stream so tool progress (ticks, progress bars) is
        # visible live on container stdout (the Modal dashboard log).
        invoke_op_work(
            op,
            ExecuteInput(execute_dir=outputs_dir, inputs=inputs, log_path=log_path),
            environment=LocalEnvironmentSpec(),
            stream_output=True,
        )
    except Exception as exc:
        return _error_result(
            op_cls,
            ErrorCode.OP_EXECUTE_FAILED,
            str(exc),
            "compute",
            "REPORT_TO_USER",
            log_tail=_log_tail(log_path),
        )

    names: list[str] = []
    try:
        names = _list_outputs(outputs_dir)
        stored = None
        if request.output_store and names:
            stored = upload_outputs(
                outputs_dir,
                names,
                request.output_store,
                op_cls.name,
                policy=policy,
            )
        manifest = ToolManifest(
            operation_identity=operation_identity(op_cls),
            debug_capture=None,
            command_recording=command_snapshot(),
            output_names=names,
            stored=stored,
            log_tail=_log_tail(log_path),
        )
        if request.debug_capture:
            manifest.debug_capture = DebugCaptureManifest(
                status="failed",
                error="diagnostic capture failed",
            )
        output_tar = (
            None
            if stored is not None
            else transport.pack_outputs(
                outputs_dir,
                names,
                max_bytes=transport_module.MAX_INLINE_BYTES - _control_size(manifest),
            )
        )
    except (ImportError, NotImplementedError):
        return _error_result(
            op_cls,
            ErrorCode.TOOL_ENDPOINT_MISCONFIGURED,
            "tool output transport is unavailable",
            "config",
            "REPORT_TO_USER",
            output_names=names,
            log_tail=_log_tail(log_path),
        )
    except ValueError:
        return _error_result(
            op_cls,
            ErrorCode.OUTPUT_DELIVERY_FAILED,
            "tool output transport rejected the result",
            "io",
            "CHECK_INPUT",
            output_names=names,
            log_tail=_log_tail(log_path),
        )
    except EndpointTransportError as exc:
        return _error_result(
            op_cls,
            ErrorCode.OUTPUT_DELIVERY_FAILED,
            str(exc),
            "io",
            "RETRY_LATER",
            output_names=names,
            log_tail=_log_tail(log_path),
        )
    except _OUTPUT_BOUNDARY_ERRORS:
        # Compute completed; preserve its output names and log while making
        # the delivery failure explicit to the caller.
        return _error_result(
            op_cls,
            ErrorCode.OUTPUT_DELIVERY_FAILED,
            "tool output transport failed",
            "io",
            "RETRY_LATER",
            output_names=names,
            log_tail=_log_tail(log_path),
        )

    return WorkerResult(manifest=manifest, output_tar=output_tar)


def _control_size(manifest: ToolManifest) -> int:
    """Count the compact UTF-8 control payload in the shared result budget."""
    return len(manifest.model_dump_json().encode("utf-8"))


def _bound_result(
    result: WorkerResult, op_cls: type[OperationDefinition]
) -> WorkerResult:
    """Keep control and both byte planes inside the shared result budget."""
    size = (
        _control_size(result.manifest)
        + len(result.output_tar or b"")
        + len(result.debug_tar or b"")
    )
    if size > transport_module.MAX_INLINE_BYTES:
        if result.manifest.debug_capture is not None:
            result.debug_tar = None
            result.manifest.debug_capture = DebugCaptureManifest(
                status="failed",
                error="diagnostic capture failed",
            )
            size = _control_size(result.manifest) + len(result.output_tar or b"")
            if size <= transport_module.MAX_INLINE_BYTES:
                return result
        failure = _error_result(
            op_cls,
            ErrorCode.OUTPUT_DELIVERY_FAILED,
            "tool result exceeds the aggregate inline byte limit",
            "io",
            "CHECK_INPUT",
        )
        if result.manifest.error is not None:
            failure.manifest.error = result.manifest.error
        if result.manifest.debug_capture is not None:
            failure.manifest.debug_capture = DebugCaptureManifest(
                status="failed",
                error="diagnostic result exceeds the inline byte limit",
            )
        _fit_error_control(failure.manifest)
        return failure
    return result


def _fit_error_control(manifest: ToolManifest) -> None:
    """Fit an error message without losing its code or returning oversized control."""
    error = manifest.error
    assert error is not None
    if _control_size(manifest) <= transport_module.MAX_INLINE_BYTES:
        return
    message = error.message
    marker = " [truncated]"
    manifest.error = error.model_copy(update={"message": marker})
    if _control_size(manifest) > transport_module.MAX_INLINE_BYTES:
        msg = "required endpoint control evidence exceeds the inline byte limit"
        raise EndpointTransportError(msg)
    low, high = 0, len(message)
    while low < high:
        midpoint = (low + high + 1) // 2
        manifest.error = error.model_copy(
            update={"message": message[:midpoint] + marker}
        )
        if _control_size(manifest) <= transport_module.MAX_INLINE_BYTES:
            low = midpoint
        else:
            high = midpoint - 1
    manifest.error = error.model_copy(update={"message": message[:low] + marker})


def _capture_debug(
    result: WorkerResult,
    request: ToolRequest,
    policy: ToolEndpointDataPolicy,
    job_root: str,
) -> WorkerResult:
    """Capture job-owned files before cleanup, preserving the primary outcome."""
    try:
        names = _diagnostic_names(job_root)
        capture = DebugCaptureManifest(status="complete", entries=names)
        result.manifest.debug_capture = capture
        if request.output_store and request.output_store.startswith("s3://"):
            capture.stored = upload_outputs(
                job_root,
                names,
                request.output_store,
                result.manifest.operation_identity.name,
                policy=policy,
            )
        else:
            remaining = (
                transport_module.MAX_INLINE_BYTES
                - _control_size(result.manifest)
                - len(result.output_tar or b"")
            )
            result.debug_tar = InlineTransport().pack_outputs(
                job_root,
                names,
                max_bytes=remaining,
            )
    except Exception:
        result.debug_tar = None
        result.manifest.debug_capture = DebugCaptureManifest(
            status="failed",
            error="diagnostic capture failed",
        )
    return result


def _diagnostic_names(job_root: str) -> list[str]:
    """List only inputs and outputs, rejecting even links inside the job tree."""
    names: list[str] = []

    def fail(error: OSError) -> None:
        raise error

    for prefix in ("inputs", "outputs"):
        directory = os.path.join(job_root, prefix)
        if os.path.islink(directory):
            msg = "Diagnostic directory is a symbolic link"
            raise ValueError(msg)
        for root, dirs, files in os.walk(directory, onerror=fail):
            for name in [*dirs, *files]:
                if os.path.islink(os.path.join(root, name)):
                    msg = "Diagnostic archive contains a symbolic link"
                    raise ValueError(msg)
            for name in files:
                if len(names) >= MAX_ARCHIVE_MEMBERS:
                    msg = "Diagnostic archive exceeds the member limit"
                    raise ValueError(msg)
                names.append(os.path.relpath(os.path.join(root, name), job_root))
    return sorted(names)


def _preflight_request(
    op_cls: type[OperationDefinition],
    request: ToolRequest,
    policy: ToolEndpointDataPolicy,
) -> WorkerResult | None:
    """Authorize every caller URI before construction or workspace effects."""
    for ref in request.inputs:
        if ref.uri is None:
            continue
        if ref.content_digest is None or ref.size_bytes is None:
            return _error_result(
                op_cls,
                ErrorCode.INPUT_RESOLUTION_FAILED,
                "remote input lacks its complete-file integrity contract",
                "validation",
                "CHECK_INPUT",
            )
        try:
            policy.authorize_input(ref.uri)
        except ValueError as exc:
            return _error_result(
                op_cls,
                ErrorCode.INPUT_RESOLUTION_FAILED,
                str(exc),
                "validation",
                "CHECK_INPUT",
            )
    if request.output_store is not None:
        try:
            policy.authorize_output(request.output_store)
        except ValueError as exc:
            return _error_result(
                op_cls,
                ErrorCode.OUTPUT_DELIVERY_FAILED,
                str(exc),
                "validation",
                "CHECK_INPUT",
            )
    return None


def instantiate_op(
    op_cls: type[OperationDefinition], params: dict[str, Any]
) -> OperationDefinition:
    """Instantiate the op from a params dict (nested ``Params`` when present).

    Shared by the endpoint worker (request params) and the ``artisan op
    run`` runner (``--params`` argv) so both sides reconstruct the op
    identically.

    Args:
        op_cls: The operation class to instantiate.
        params: Field values for the op's nested ``Params`` model. Must be
            empty when the operation declares no parameters.

    Returns:
        The operation instance.
    """
    op_any: Any = op_cls  # subclass fields (params, …) are invisible on the base
    params_cls = _params_class(op_cls)
    if params_cls is None:
        if params:
            return op_any.model_validate({"params": params})  # type: ignore[no-any-return]
        return op_any()  # type: ignore[no-any-return]
    return op_any(params=params_cls.model_validate(params))  # type: ignore[no-any-return]


def _error_result(
    op_cls: type[OperationDefinition],
    code: str,
    message: str,
    error_type: ErrorType,
    recovery_hint: RecoveryHint,
    *,
    output_names: list[str] | None = None,
    log_tail: str | None = None,
) -> WorkerResult:
    """Return a WorkerResult carrying an error envelope on the manifest.

    Args:
        op_cls: The actual worker operation class.
        code: Stable ``ErrorCode`` identifier.
        message: Human-readable summary (the underlying exception's text).
        error_type: Coarse envelope category.
        recovery_hint: Next-action signal for an agent.
        output_names: Output files produced before the failure — set only on
            a delivery failure, where the tool ran but its results were not
            delivered.
        log_tail: Tail of the tool log, when a log exists.

    Returns:
        A WorkerResult with no data plane and the error on its manifest.
    """
    return WorkerResult(
        manifest=ToolManifest(
            operation_identity=operation_identity(op_cls),
            debug_capture=None,
            command_recording=command_snapshot(),
            error=ArtisanError(
                code=code,
                message=sanitize_diagnostic(message),
                error_type=error_type,
                operation_name=op_cls.name,
                recovery_hint=recovery_hint,
            ).envelope,
            output_names=output_names or [],
            log_tail=sanitize_diagnostic(log_tail),
        )
    )


def _log_tail(log_path: str) -> str | None:
    """Last ``MAX_TOOL_OUTPUT_BYTES`` of the tool log, or None if unavailable."""
    try:
        size = os.path.getsize(log_path)
    except OSError:
        return None
    try:
        with open(log_path, "rb") as f:
            f.seek(max(0, size - MAX_TOOL_OUTPUT_BYTES))
            return sanitize_diagnostic(f.read().decode("utf-8", errors="replace"))
    except OSError:
        return None


def _list_outputs(outputs_dir: str) -> list[str]:
    """Relative paths of all files under ``outputs_dir``, sorted.

    Exclude the tool log from ordinary outputs. Its tail travels on the
    manifest; opt-in diagnostics can carry the full log separately.
    """

    def raise_walk_error(error: OSError) -> None:
        raise error

    names: list[str] = []
    for root, _dirs, files in os.walk(outputs_dir, onerror=raise_walk_error):
        for fname in files:
            name = os.path.relpath(os.path.join(root, fname), outputs_dir)
            if name == TOOL_OUTPUT_FILENAME:
                continue
            if len(names) >= MAX_ARCHIVE_MEMBERS:
                msg = f"Archive exceeds {MAX_ARCHIVE_MEMBERS} members"
                raise ValueError(msg)
            names.append(name)
    return sorted(names)
