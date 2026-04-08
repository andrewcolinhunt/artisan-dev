"""ExecutionContext builders for creator and curator executors."""

from __future__ import annotations

from datetime import datetime

from fsspec import AbstractFileSystem

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.execution.execution_context import ExecutionContext
from artisan.storage.core.artifact_store import ArtifactStore


def _build_execution_context(
    *,
    execution_run_id: str,
    execution_spec_id: str,
    step_number: int,
    timestamp_start: datetime,
    worker_id: int,
    delta_root: str,
    staging_root: str,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
    operation: OperationDefinition,
    sandbox_path: str | None,
    compute_backend_name: str = "local",
    shared_filesystem: bool = False,
    step_run_id: str | None = None,
    files_root: str | None = None,
) -> ExecutionContext:
    """Build an execution context (shared by creator and curator paths)."""
    artifact_store = ArtifactStore(
        delta_root,
        fs=fs,
        storage_options=storage_options,
        files_root=files_root,
    )
    return ExecutionContext(
        execution_run_id=execution_run_id,
        execution_spec_id=execution_spec_id,
        step_number=step_number,
        timestamp_start=timestamp_start,
        worker_id=worker_id,
        artifact_store=artifact_store,
        staging_root=staging_root,
        fs=fs,
        operation_name=type(operation).name,
        operation=operation,
        sandbox_path=sandbox_path,
        compute_backend=compute_backend_name,
        shared_filesystem=shared_filesystem,
        step_run_id=step_run_id,
    )


def build_creator_execution_context(
    *,
    execution_run_id: str,
    execution_spec_id: str,
    step_number: int,
    timestamp_start: datetime,
    worker_id: int,
    delta_root: str,
    staging_root: str,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
    operation: OperationDefinition,
    sandbox_path: str,
    compute_backend_name: str = "local",
    shared_filesystem: bool = False,
    step_run_id: str | None = None,
    files_root: str | None = None,
) -> ExecutionContext:
    """Build an execution context for a creator operation.

    Creator contexts include a sandbox path for file-based I/O
    during preprocess/execute/postprocess phases.
    """
    return _build_execution_context(
        execution_run_id=execution_run_id,
        execution_spec_id=execution_spec_id,
        step_number=step_number,
        timestamp_start=timestamp_start,
        worker_id=worker_id,
        delta_root=delta_root,
        staging_root=staging_root,
        fs=fs,
        storage_options=storage_options,
        operation=operation,
        sandbox_path=sandbox_path,
        compute_backend_name=compute_backend_name,
        shared_filesystem=shared_filesystem,
        step_run_id=step_run_id,
        files_root=files_root,
    )


def build_curator_execution_context(
    *,
    execution_run_id: str,
    execution_spec_id: str,
    step_number: int,
    timestamp_start: datetime,
    worker_id: int,
    delta_root: str,
    staging_root: str,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
    operation: OperationDefinition,
    compute_backend_name: str = "local",
    shared_filesystem: bool = False,
    step_run_id: str | None = None,
    files_root: str | None = None,
) -> ExecutionContext:
    """Build an execution context for a curator operation.

    Curator contexts have no sandbox path because curators operate
    on in-memory DataFrames rather than filesystem I/O.
    """
    return _build_execution_context(
        execution_run_id=execution_run_id,
        execution_spec_id=execution_spec_id,
        step_number=step_number,
        timestamp_start=timestamp_start,
        worker_id=worker_id,
        delta_root=delta_root,
        staging_root=staging_root,
        fs=fs,
        storage_options=storage_options,
        operation=operation,
        sandbox_path=None,
        compute_backend_name=compute_backend_name,
        shared_filesystem=shared_filesystem,
        step_run_id=step_run_id,
        files_root=files_root,
    )
