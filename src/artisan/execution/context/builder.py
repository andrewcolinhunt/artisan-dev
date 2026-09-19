"""ExecutionContext builder for creator and curator executors."""

from __future__ import annotations

from datetime import datetime

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.execution.execution_context import ExecutionContext
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.storage.core.artifact_store import ArtifactStore


def build_execution_context(
    *,
    execution_run_id: str,
    execution_spec_id: str,
    step_number: int,
    timestamp_start: datetime,
    runtime_env: RuntimeEnvironment,
    operation: OperationDefinition,
    sandbox_path: str | None = None,
    step_run_id: str | None = None,
) -> ExecutionContext:
    """Build an execution context for a creator or curator operation.

    Args:
        execution_run_id: Unique ID for this execution attempt (32-char hex).
        execution_spec_id: Deterministic cache key (32-char hex).
        step_number: Pipeline step number.
        timestamp_start: Execution start time (UTC).
        runtime_env: Resolved worker identity, runtime paths, and storage configuration.
        operation: Fully configured operation this context describes.
        sandbox_path: Sandbox directory for the file-based I/O of a
            creator's preprocess/execute/postprocess phases. None for
            curators, which operate on in-memory DataFrames.
        step_run_id: Identifier of the owning pipeline step run, or None
            for a standalone execution.

    Returns:
        The assembled ExecutionContext for the operation.
    """
    fs = runtime_env.storage.filesystem()
    artifact_store = ArtifactStore(
        runtime_env.delta_root,
        fs=fs,
        storage_options=runtime_env.storage.delta_storage_options(),
        files_root=runtime_env.files_root,
    )
    return ExecutionContext(
        execution_run_id=execution_run_id,
        execution_spec_id=execution_spec_id,
        step_number=step_number,
        timestamp_start=timestamp_start,
        worker_id=runtime_env.worker_id,
        artifact_store=artifact_store,
        staging_root=runtime_env.staging_root,
        fs=fs,
        operation_name=type(operation).name,
        operation=operation,
        sandbox_path=sandbox_path,
        compute_backend=runtime_env.compute_backend_name,
        shared_filesystem=runtime_env.shared_filesystem,
        step_run_id=step_run_id,
    )
