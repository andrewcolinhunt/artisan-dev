"""ExecutionContext builder for creator and curator executors."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.execution.execution_context import ExecutionContext
from artisan.schemas.execution.worker_config import WorkerConfig
from artisan.storage.core.artifact_store import ArtifactStore

if TYPE_CHECKING:
    from artisan.storage.core.curator_view import CuratorView


def build_execution_context(
    *,
    execution_run_id: str,
    execution_spec_id: str,
    step_number: int,
    timestamp_start: datetime,
    worker_config: WorkerConfig,
    operation: OperationDefinition,
    step_run_id: str | None = None,
    curator_view: CuratorView | None = None,
) -> ExecutionContext:
    """Build an execution context for a creator or curator operation.

    Args:
        execution_run_id: Unique ID for this execution attempt (32-char hex).
        execution_spec_id: Deterministic cache key (32-char hex).
        step_number: Pipeline step number.
        timestamp_start: Execution start time (UTC).
        worker_config: Resolved worker identity, runtime paths, and storage configuration.
        operation: Fully configured operation this context describes.
        step_run_id: Identifier of the owning pipeline step run, or None
            for a standalone execution.
        curator_view: Verified fixed store view for an admitted curator.

    Returns:
        The assembled ExecutionContext for the operation.
    """
    fs = worker_config.storage.filesystem()
    artifact_store = ArtifactStore(
        worker_config.delta_root,
        fs=fs,
        storage_options=worker_config.storage.delta_storage_options(),
        files_root=worker_config.files_root,
        _view=curator_view,
    )
    return ExecutionContext(
        execution_run_id=execution_run_id,
        execution_spec_id=execution_spec_id,
        step_number=step_number,
        timestamp_start=timestamp_start,
        worker_id=worker_config.worker_id,
        artifact_store=artifact_store,
        staging_root=worker_config.staging_root,
        fs=fs,
        operation_name=type(operation).name,
        operation=operation,
        compute_backend=worker_config.compute_backend_name,
        step_run_id=step_run_id,
    )
