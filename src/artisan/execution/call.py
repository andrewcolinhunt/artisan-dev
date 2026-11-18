"""Call definitions directly without an executor or durable lifecycle."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import nullcontext
from functools import partial
from pathlib import Path
from threading import Lock
from uuid import uuid4

from fsspec import AbstractFileSystem

from artisan.composites.base.compiler import Definition, Parameters
from artisan.execution.compute.base import ExecuteRouter
from artisan.execution.executor_runtime import ExecutorRuntime
from artisan.execution.invocation import InvocationPaths, invoke_operation
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.specs.input_models import FrozenInputContext
from artisan.utils.command_supervisor import has_command_scope, managed_invocation

_call_lock = Lock()


def run_operation(
    definition: Definition,
    *,
    inputs: Mapping[str, Sequence[Artifact]] | None = None,
    params: Parameters = None,
    work_dir: Path | None = None,
    associations: Mapping[tuple[str, int, str], Sequence[Artifact]] | None = None,
    config_references: Mapping[tuple[str, int], Mapping[str, Artifact]] | None = None,
) -> ArtifactResult:
    """Run an operation or static composite in the calling process.

    Native phases use the caller's Python environment. Commands use existing
    software and managed subprocesses. This call neither publishes results nor
    installs environments, and retains no history between calls.

    Args:
        definition: Creator operation or static composite class.
        inputs: Finalized, locally available artifacts in ordered role streams.
        params: Definition parameters, validated against its Params model.
        work_dir: Parent directory for files. Required by materialization and
            command adapters. Created files remain until the caller removes them.
        associations: Explicit association membership by original input position
            and artifact type. Omitted membership differs from an empty sequence.
        config_references: Exact configuration referents by original input position.

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
        identity = uuid4().hex
        context = FrozenInputContext(
            identity,
            inputs or {},
            associations or {},
            config_references or {},
        )
        root = (
            Path(work_dir).expanduser().absolute() / f"artisan-call-{identity}"
            if work_dir is not None
            else None
        )
        return ExecutorRuntime("pixi").run(
            definition,
            context.artifacts,
            params,
            partial(_invoke, root=root),
            root,
            explicit_context=context,
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
