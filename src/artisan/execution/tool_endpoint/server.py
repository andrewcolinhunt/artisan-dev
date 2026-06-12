"""Worker-side execution of tool requests.

Runs inside the deployed worker image: resolve the deployed operation
class, materialize input refs, run ``execute_command`` as a local subprocess,
and return the manifest + output tar. No Modal imports — locally testable.
"""

from __future__ import annotations

import importlib
import os
import tempfile
from functools import reduce
from typing import Any

from artisan.errors import ArtisanError, ArtisanErrorEnvelope, ErrorCode
from artisan.execution.compute.invoke import invoke_op_work
from artisan.execution.tool_endpoint.protocol import (
    ToolManifest,
    ToolRequest,
    WorkerResult,
)
from artisan.execution.tool_endpoint.transport import InlineTransport, upload_outputs
from artisan.execution.transport.log_constants import (
    MAX_TOOL_OUTPUT_BYTES,
    TOOL_OUTPUT_FILENAME,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.schemas.specs.input_models import ExecuteInput
from artisan.utils.external_tools import ExternalToolError


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
    op_cls: type[OperationDefinition], request: ToolRequest
) -> WorkerResult:
    """Build the command from the op + request params and run the tool.

    Instantiates ``op_cls`` from the request params, resolves input refs
    into the job's ``inputs/`` dir, runs ``op.execute_command`` as a local
    subprocess with ``cwd=outputs/``, and returns the manifest + output tar
    + tool-log tail. When the request names an ``output_store``, outputs
    are delivered there instead and the manifest carries the stored
    pointer. Tool failures return an ``OP_EXECUTE_FAILED`` envelope.

    Inputs and outputs live in separate dirs so the tar never sweeps input
    files. The tool log is excluded from the manifest and tar — locally the
    log lives at the sandbox level, not in ``execute_dir``, so shipping it
    in the tar would leak it into ``file_outputs``; the client receives the
    tail via the manifest and appends it to the unit log instead.

    Args:
        op_cls: The deployed operation class.
        request: Validated params + input refs.

    Returns:
        WorkerResult with the control manifest and, on success, the tar.
    """
    op = instantiate_op(op_cls, request.params)
    job_root = tempfile.mkdtemp(prefix=f"artisan-tool-{op_cls.name}-")
    inputs_dir = os.path.join(job_root, "inputs")
    outputs_dir = os.path.join(job_root, "outputs")
    os.makedirs(outputs_dir)

    transport = InlineTransport()
    inputs = transport.unpack_inputs(request.inputs, inputs_dir)
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
    except ExternalToolError as exc:
        return WorkerResult(
            manifest=ToolManifest(
                error=_envelope(op_cls.name, exc), log_tail=_log_tail(log_path)
            )
        )
    names = _list_outputs(outputs_dir)
    stored = (
        upload_outputs(outputs_dir, names, request.output_store, op_cls.name)
        if request.output_store and names
        else None
    )
    return WorkerResult(
        manifest=ToolManifest(
            output_names=names, stored=stored, log_tail=_log_tail(log_path)
        ),
        output_tar=None if stored else transport.pack_outputs(outputs_dir, names),
    )


def instantiate_op(
    op_cls: type[OperationDefinition], params: dict[str, Any]
) -> OperationDefinition:
    """Instantiate the op from a params dict (nested ``Params`` when present).

    Shared by the endpoint worker (request params) and the ``artisan op
    run`` runner (``--params`` argv) so both sides reconstruct the op
    identically.

    Args:
        op_cls: The operation class to instantiate.
        params: Field values for the op's nested ``Params`` model, or
            top-level fields when the op declares no ``Params``.

    Returns:
        The operation instance.
    """
    op_any: Any = op_cls  # subclass fields (params, …) are invisible on the base
    params_cls = getattr(op_cls, "Params", None)
    if params_cls is None:
        return op_any(**params)  # type: ignore[no-any-return]
    return op_any(params=params_cls(**params))  # type: ignore[no-any-return]


def _envelope(operation_name: str, exc: ExternalToolError) -> ArtisanErrorEnvelope:
    """Wrap a tool failure in the standard error envelope."""
    return ArtisanError(
        code=ErrorCode.OP_EXECUTE_FAILED,
        message=str(exc),
        error_type="compute",
        operation_name=operation_name,
        recovery_hint="REPORT_TO_USER",
    ).envelope


def _log_tail(log_path: str) -> str | None:
    """Last ``MAX_TOOL_OUTPUT_BYTES`` of the tool log, or None if absent."""
    if not os.path.exists(log_path):
        return None
    size = os.path.getsize(log_path)
    with open(log_path, "rb") as f:
        f.seek(max(0, size - MAX_TOOL_OUTPUT_BYTES))
        return f.read().decode("utf-8", errors="replace")


def _list_outputs(outputs_dir: str) -> list[str]:
    """Relative paths of all files under ``outputs_dir``, sorted.

    Excludes the tool log — it travels as ``log_tail`` on the manifest,
    not on the data plane (local runs keep it outside ``execute_dir``).
    """
    names: list[str] = []
    for root, _dirs, files in os.walk(outputs_dir):
        names.extend(
            os.path.relpath(os.path.join(root, fname), outputs_dir) for fname in files
        )
    return sorted(name for name in names if name != TOOL_OUTPUT_FILENAME)
