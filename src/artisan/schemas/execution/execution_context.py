"""Immutable execution identity and resources used for staging records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from fsspec import AbstractFileSystem

if TYPE_CHECKING:
    from artisan.operations.base.operation_definition import OperationDefinition
    from artisan.storage.core.artifact_store import ArtifactStore


@dataclass(frozen=True)
class ExecutionContext:
    """Immutable execution state passed throughout the execution flow.

    All fields are set at execution start and never modified (frozen).

    Attributes:
        execution_run_id: Unique ID for this execution attempt (32-char hex).
        execution_spec_id: Deterministic cache key (32-char hex).
        step_number: Pipeline step number.
        timestamp_start: Execution start time (UTC).
        worker_id: Worker identifier for distributed execution.
        artifact_store: ArtifactStore instance for artifact lookups.
        staging_root: Root URI/path for staging Parquet files.
        fs: Filesystem implementation for staging I/O.
        operation_name: Operation name string.
        operation: Fully configured OperationDefinition instance.
        compute_backend: Resolved step-runner name recorded for provenance.
        step_run_id: Step run ID linking this execution to a specific
            step attempt. None when not associated with a step.
    """

    execution_run_id: str
    execution_spec_id: str
    step_number: int
    timestamp_start: datetime
    worker_id: int
    artifact_store: ArtifactStore
    staging_root: str
    fs: AbstractFileSystem
    operation_name: str
    operation: OperationDefinition
    compute_backend: str = "local"
    step_run_id: str | None = None
