"""Call definitions directly without an executor or durable lifecycle."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import nullcontext
from functools import partial
from pathlib import Path
from threading import Lock
from uuid import uuid4

from fsspec import AbstractFileSystem

from artisan.composites.base.compiler import Definition, Parameters, compile_definition
from artisan.execution.composition import ExecuteAdapter
from artisan.execution.compute.base import ExecuteRouter
from artisan.execution.configured_calls import (
    compile_configured_call,
    configured_router,
    current_call_scope,
)
from artisan.execution.external_access import freeze_external_files
from artisan.execution.inputs.local_context import capture_local_context
from artisan.execution.invocation import InvocationPaths, invoke_operation
from artisan.execution.local_execution import execute_local_call, local_inputs
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.external_descriptor import PreparedExternalFiles
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.specs.input_models import FrozenInputContext
from artisan.utils.command_supervisor import has_command_scope, managed_invocation

_call_lock = Lock()


def run_operation(
    definition: Definition,
    *,
    inputs: Mapping[str, Sequence[Artifact]] | None = None,
    params: Parameters = None,
    selection_key: str | None = None,
    work_dir: Path | None = None,
    associations: Mapping[tuple[str, int, str], Sequence[Artifact]] | None = None,
    config_references: Mapping[tuple[str, int], Mapping[str, Artifact]] | None = None,
    external_files: Mapping[str, PreparedExternalFiles] | None = None,
) -> ArtifactResult:
    """Run an operation or static composite in the calling process.

    Native phases use the caller's Python environment. Commands use existing
    software and managed subprocesses. This call neither publishes results nor
    installs environments, and retains no history between calls.

    Args:
        definition: Creator operation or static composite class.
        inputs: Finalized artifacts in ordered role streams. Omitted roles use
            their declared fixed artifact defaults.
        params: Definition parameters, validated against its Params model.
        selection_key: Configuration entry selected inside a managed callback.
        work_dir: Parent directory for files. Required by materialization and
            command adapters. Created files remain until the caller removes them.
        associations: Explicit association membership by original input position
            and artifact type. Omitted membership differs from an empty sequence.
        config_references: Exact configuration referents by original input position.
        external_files: Explicit prepared installations keyed by artifact ID.
            External payloads remain in place and are never materialized.

    Returns:
        Finalized outputs, explicit lineage, and retained reference support.

    Raises:
        RuntimeError: If another call is active in this process, including recursion.
        ValueError: If required inputs, context, paths, or software are unavailable.
    """
    if not _call_lock.acquire(blocking=False):
        msg = "Operation calls must be sequential and nonrecursive"
        raise RuntimeError(msg)
    try:
        scope = current_call_scope()
        plan = None
        adapter_factory = None
        if scope is not None:
            if selection_key is None:
                msg = "A configured callback requires its selection_key"
                raise ValueError(msg)
            plan = compile_configured_call(scope, definition, selection_key, params)

            def adapter_factory() -> ExecuteAdapter:
                router = configured_router(scope, selection_key)
                return lambda _operation, _path: router
        elif selection_key is not None:
            msg = "selection_key requires a configured callback context"
            raise ValueError(msg)
        external_files = freeze_external_files(external_files)
        identity = uuid4().hex
        plan = plan or compile_definition(definition, params=params)
        selected = local_inputs(plan, inputs or {})
        context = FrozenInputContext(
            identity,
            selected,
            associations or {},
            config_references or {},
        )
        root = (
            Path(work_dir).expanduser().absolute() / f"artisan-call-{identity}"
            if work_dir is not None
            else None
        )
        frozen_context, captured = capture_local_context(plan, context)
        return execute_local_call(
            plan,
            context.artifacts,
            partial(_invoke, root=root),
            root,
            identity=identity,
            context=frozen_context,
            captured=captured,
            external_files=external_files,
            adapter_factory=adapter_factory,
        )
    finally:
        _call_lock.release()


def _invoke(
    operation: OperationDefinition,
    context: FrozenInputContext,
    *,
    root: Path | None,
    execute_router: ExecuteRouter,
    batch_size: int,
    materialization_references: Mapping[str, Mapping[str, Artifact]],
    step_number: int | None = None,
    fs: AbstractFileSystem | None = None,
    call_path: tuple[str, ...] = (),
) -> ArtifactResult:
    scope = (
        managed_invocation()
        if has_command_scope() or operation.is_command_op()
        else nullcontext()
    )
    with scope:
        paths = (
            InvocationPaths.create(root / uuid4().hex)
            if root is not None
            else InvocationPaths()
        )
        return invoke_operation(
            operation,
            context,
            paths=paths,
            execute_router=execute_router,
            batch_size=batch_size,
            materialization_references=materialization_references,
            step_number=step_number,
            fs=fs,
        )
