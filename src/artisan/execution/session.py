"""Own local invocation files and command scopes without durable publication."""

from __future__ import annotations

import shutil
import tempfile
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager, nullcontext
from pathlib import Path
from threading import Lock
from types import TracebackType
from typing import TYPE_CHECKING, Literal, Self

from fsspec import AbstractFileSystem

from artisan.composites.base.compiler import Definition, Parameters
from artisan.execution.compute.base import ExecuteRouter
from artisan.execution.invocation import (
    InvocationPaths,
    execute_invocation,
    postprocess_invocation,
    prepare_invocation,
)
from artisan.execution.recording.delivery_diagnostics import (
    DeliveryRecorder,
    InvocationObservation,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.specs.input_models import FrozenInputContext
from artisan.utils.command_supervisor import (
    check_command_scope,
    close_command_scope,
    has_command_scope,
    managed_invocation,
)

if TYPE_CHECKING:
    from artisan.execution.session_runtime import SessionRuntime
    from artisan.utils.command_supervisor import _Invocation


class ExecutionSession:
    """Own sequential local creator/composite calls and temporary artifacts.

    Files remain available between child calls and are removed on confirmed
    session closure unless preservation was requested. Preservation never keeps
    command ownership active. This owner neither installs software nor accesses a store.
    ``run`` accepts live definitions; ``invoke`` is the admitted partition kernel.
    """

    def __init__(
        self,
        *,
        working_root: Path | None = None,
        preserve_working: bool = False,
        diagnostics: DeliveryRecorder | None = None,
        tool_backend: Literal["pixi", "docker", "apptainer"] = "pixi",
    ) -> None:
        if tool_backend not in {"pixi", "docker", "apptainer"}:
            msg = "Unsupported session tool backend"
            raise ValueError(msg)
        self._working_root = working_root
        self._preserve_working = preserve_working
        self._diagnostics = diagnostics
        self._root: Path | None = None
        self._scope: AbstractContextManager[_Invocation] | None = None
        self._invocation: _Invocation | None = None
        self._cleanup_complete = False
        self._tool_backend = tool_backend
        self._runtime: SessionRuntime | None = None
        self._run_lock = Lock()
        self._borrowed = False

    @property
    def cleanup_complete(self) -> bool:
        """Whether this session's command scope has actually finished closing."""
        return self._cleanup_complete

    def __enter__(self) -> Self:
        if self._scope is not None:
            msg = "Execution session is already active"
            raise RuntimeError(msg)
        if self._root is not None:
            msg = "Execution session retains unresolved cleanup ownership"
            raise RuntimeError(msg)
        self._cleanup_complete = False
        self._borrowed = has_command_scope()
        if self._working_root is not None:
            self._working_root.mkdir(parents=True, exist_ok=True)
        root = Path(tempfile.mkdtemp(prefix="artisan-session-", dir=self._working_root))
        scope = managed_invocation()
        try:
            invocation = scope.__enter__()
        except BaseException:
            shutil.rmtree(root)
            raise
        self._root, self._scope = root, scope
        self._invocation = invocation
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._scope is None or self._root is None:
            msg = "Execution session is not active"
            raise RuntimeError(msg)
        closed = False
        try:
            closed = close_command_scope(exc)
        finally:
            try:
                self._scope.__exit__(exc_type, exc, traceback)
                assert self._invocation is not None
                self._cleanup_complete = closed and not self._invocation.uncertain
                if self._cleanup_complete and self._diagnostics is not None:
                    self._diagnostics.cleanup = "closed"
            finally:
                if self._cleanup_complete and not self._preserve_working:
                    shutil.rmtree(self._root)
                if self._cleanup_complete:
                    self._runtime = None
                    self._root = None
                    self._invocation = None
                self._scope = None

    def run(
        self,
        definition: Definition,
        *,
        inputs: Mapping[str, Sequence[Artifact]] | None = None,
        params: Parameters = None,
    ) -> ArtifactResult:
        """Run a creator or composition locally, retaining files until session exit.

        Inputs must be finalized, with local carriers and sufficient captured
        context. Software targets must already be prepared. This method does not
        access a store, install environments, or launch a replacement worker.
        """
        if self._scope is None or self._root is None or self._borrowed:
            msg = "Standalone run requires an active independently owned session"
            raise RuntimeError(msg)
        if not self._run_lock.acquire(blocking=False):
            msg = "Standalone session calls must be sequential and nonrecursive"
            raise RuntimeError(msg)
        try:
            from artisan.execution.session_runtime import SessionRuntime

            check_command_scope(expected=self._invocation)
            if self._runtime is None:
                self._runtime = SessionRuntime(self._tool_backend)
            return self._runtime.run(
                definition, {} if inputs is None else inputs, params, self, self._root
            )
        finally:
            self._run_lock.release()

    @contextmanager
    def invocation(
        self, *, observation: InvocationObservation | None = None
    ) -> Iterator[InvocationPaths]:
        """Open a child scope while retaining its files through session closure."""
        if self._scope is None or self._root is None:
            msg = "Execution session must be entered before invoking operations"
            raise RuntimeError(msg)
        check_command_scope()
        with managed_invocation():
            paths = InvocationPaths.create(self._root / uuid.uuid4().hex)
            failure: BaseException | None = None
            try:
                yield paths
            except BaseException as error:
                failure = error
                raise
            finally:
                closed = close_command_scope(failure)
                if observation is not None:
                    observation.cleanup = "closed" if closed else "unknown"

    def invoke(
        self,
        operation: OperationDefinition,
        context: FrozenInputContext,
        *,
        execute_router: ExecuteRouter,
        batch_size: int,
        materialization_references: Mapping[str, Mapping[str, Artifact]],
        step_number: int | None = None,
        fs: AbstractFileSystem | None = None,
        call_path: tuple[str, ...] = (),
    ) -> ArtifactResult:
        """Run one admitted creator partition and retain its temporary artifacts."""
        observation = (
            self._diagnostics.invocation(operation, context.invocation_id, call_path)
            if self._diagnostics is not None
            else nullcontext(None)
        )
        with observation as observed, self.invocation(observation=observed) as paths:
            if observed is not None:
                observed.log_path = paths.log
            prepared = prepare_invocation(
                operation,
                context,
                paths,
                batch_size=batch_size,
                step_number=step_number,
                materialization_references=materialization_references,
                fs=fs,
                timings=observed.timings if observed is not None else None,
            )
            raw_results = execute_invocation(prepared, execute_router)
            return postprocess_invocation(prepared, raw_results, fs=fs)
