"""Worker-side execution of tool requests.

Runs inside the deployed worker image: resolve the deployed operation
class, materialize input refs, run ``execute_command`` as a local subprocess,
and return the manifest + output tar. No Modal imports — locally testable.
"""

from __future__ import annotations

import importlib
import os
import shutil
import tarfile
import tempfile
from functools import reduce
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
from artisan.execution.tool_endpoint.protocol import (
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
from artisan.schemas.operation_config.endpoint_policy import ToolEndpointDataPolicy
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.schemas.specs.input_models import ExecuteInput
from artisan.utils.external_tools import ExternalToolError

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


def resolve_op(module: str, qualname: str) -> type[OperationDefinition]:
    """Import and return the operation class deployed with this endpoint.

    Args:
        module: Dotted module path (``op_cls.__module__``).
        qualname: Class qualname within the module (``op_cls.__qualname__``).

    Returns:
        The OperationDefinition subclass.

    Raises:
        TypeError: If the resolved object is not an OperationDefinition.
    """
    obj: Any = reduce(getattr, qualname.split("."), importlib.import_module(module))
    if not (isinstance(obj, type) and issubclass(obj, OperationDefinition)):
        msg = f"{module}:{qualname} is not an OperationDefinition"
        raise TypeError(msg)
    return obj


def run_tool_request(
    op_cls: type[OperationDefinition],
    request: ToolRequest,
    data_policy: ToolEndpointDataPolicy | None = None,
) -> WorkerResult:
    """Build the command from the op + request params and run the tool.

    Instantiates ``op_cls`` from the request params, resolves input refs
    into the job's ``inputs/`` dir, runs ``op.execute_command`` as a local
    subprocess with ``cwd=outputs/``, and returns the manifest + output tar
    + tool-log tail. When the request names an ``output_store``, outputs
    are delivered there instead and the manifest carries the stored
    pointer. Param-validation, input-resolution, tool-execution, and
    output-delivery failures each return a structured error envelope on the
    manifest (stable ``code`` + ``recovery_hint``) rather than raising.

    Inputs and outputs live in separate dirs so the tar never sweeps input
    files. The tool log is excluded from the manifest and tar — locally the
    log lives at the sandbox level, not in ``execute_dir``, so shipping it
    in the tar would leak it into ``file_outputs``; the client receives the
    tail via the manifest and appends it to the unit log instead.

    Args:
        op_cls: The deployed operation class.
        request: Validated params + input refs.
        data_policy: Deployment-owned URI permissions. Omission denies every
            remote input and output URI.

    Returns:
        WorkerResult with the control manifest and, on success, the tar.
    """
    try:
        policy = ToolEndpointDataPolicy.model_validate(
            data_policy.model_dump(mode="python", warnings=False)
            if data_policy is not None
            else {}
        )
    except (TypeError, ValueError):
        return _error_result(
            op_cls.name,
            ErrorCode.TOOL_ENDPOINT_MISCONFIGURED,
            "endpoint deployment data policy is invalid",
            "config",
            "REPORT_TO_USER",
        )
    denied = _preflight_request(op_cls.name, request, policy)
    if denied is not None:
        return denied

    try:
        op = instantiate_op(op_cls, request.params)
    except ValidationError:
        # bad params the /submit JSON-schema gate could not express (no-Params
        # ops, custom validators); the agent can fix its own call. Runs before
        # the job dir exists, so no cleanup is owed here.
        return _error_result(
            op_cls.name,
            ErrorCode.PARAM_TYPE_MISMATCH,
            "tool parameters failed validation",
            "validation",
            "CHECK_INPUT",
        )

    try:
        job_root = tempfile.mkdtemp(prefix=f"artisan-tool-{op_cls.name}-")
    except OSError:
        return _error_result(
            op_cls.name,
            ErrorCode.OP_EXECUTE_FAILED,
            "could not create tool workspace",
            "io",
            "RETRY_LATER",
        )
    # Modal reuses warm containers across requests; the job tree must not
    # outlive the call or per-request temp dirs accumulate in the container.
    try:
        inputs_dir = os.path.join(job_root, "inputs")
        outputs_dir = os.path.join(job_root, "outputs")
        try:
            os.makedirs(outputs_dir)
        except OSError:
            return _error_result(
                op_cls.name,
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
                op_cls.name,
                ErrorCode.ARTIFACT_INTEGRITY_FAILED,
                str(exc),
                "io",
                "CHECK_INPUT",
            )
        except ImportError:
            return _error_result(
                op_cls.name,
                ErrorCode.TOOL_ENDPOINT_MISCONFIGURED,
                "input filesystem dependency is unavailable",
                "config",
                "REPORT_TO_USER",
            )
        except EndpointTransportError as exc:
            return _error_result(
                op_cls.name,
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
            # The caller supplied the ref/Secret and can correct it. Fetch
            # precedes compute, so nothing partial exists.
            return _error_result(
                op_cls.name,
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
        except (ExternalToolError, OSError) as exc:
            return _error_result(
                op_cls.name,
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
            output_tar = (
                None
                if stored is not None
                else transport.pack_outputs(outputs_dir, names)
            )
        except (ImportError, NotImplementedError):
            return _error_result(
                op_cls.name,
                ErrorCode.TOOL_ENDPOINT_MISCONFIGURED,
                "tool output transport is unavailable",
                "config",
                "REPORT_TO_USER",
                output_names=names,
                log_tail=_log_tail(log_path),
            )
        except ValueError:
            return _error_result(
                op_cls.name,
                ErrorCode.OUTPUT_DELIVERY_FAILED,
                "tool output transport rejected the result",
                "io",
                "CHECK_INPUT",
                output_names=names,
                log_tail=_log_tail(log_path),
            )
        except EndpointTransportError as exc:
            return _error_result(
                op_cls.name,
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
                op_cls.name,
                ErrorCode.OUTPUT_DELIVERY_FAILED,
                "tool output transport failed",
                "io",
                "RETRY_LATER",
                output_names=names,
                log_tail=_log_tail(log_path),
            )

        return WorkerResult(
            manifest=ToolManifest(
                output_names=names, stored=stored, log_tail=_log_tail(log_path)
            ),
            output_tar=output_tar,
        )
    finally:
        # Result values (tar bytes, log tail, stored pointer) are fully
        # evaluated before finally runs; ignore_errors keeps cleanup from
        # masking the real exception or altering the returned manifest.
        shutil.rmtree(job_root, ignore_errors=True)


def _preflight_request(
    op_name: str,
    request: ToolRequest,
    policy: ToolEndpointDataPolicy,
) -> WorkerResult | None:
    """Authorize every caller URI before construction or workspace effects."""
    for ref in request.inputs:
        if ref.uri is None:
            continue
        if ref.content_digest is None or ref.size_bytes is None:
            return _error_result(
                op_name,
                ErrorCode.INPUT_RESOLUTION_FAILED,
                "remote input lacks its complete-file integrity contract",
                "validation",
                "CHECK_INPUT",
            )
        try:
            policy.authorize_input(ref.uri)
        except ValueError as exc:
            return _error_result(
                op_name,
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
                op_name,
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
    op_name: str,
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
        op_name: The deployed op's name, stamped on the envelope.
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
            error=ArtisanError(
                code=code,
                message=message,
                error_type=error_type,
                operation_name=op_name,
                recovery_hint=recovery_hint,
            ).envelope,
            output_names=output_names or [],
            log_tail=log_tail,
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
            return f.read().decode("utf-8", errors="replace")
    except OSError:
        return None


def _list_outputs(outputs_dir: str) -> list[str]:
    """Relative paths of all files under ``outputs_dir``, sorted.

    Excludes the tool log — it travels as ``log_tail`` on the manifest,
    not on the data plane (local runs keep it outside ``execute_dir``).
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
