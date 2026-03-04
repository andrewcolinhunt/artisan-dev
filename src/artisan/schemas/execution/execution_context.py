"""Execution context grouping runtime state for a single execution.

``ExecutionContext`` is created once at execution start and threaded
through the entire execution flow, replacing ~13 individual parameters
with a single immutable object.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

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
        staging_root: Root path for staging Parquet files.
        operation_name: Operation name string.
        operation: Fully configured OperationDefinition instance.
        sandbox_path: Creator sandbox directory. None for curators.
        compute_backend: Backend name (e.g. "local", "slurm").
        shared_filesystem: Whether workers share a filesystem with
            the orchestrator.
    """

    execution_run_id: str
    execution_spec_id: str
    step_number: int
    timestamp_start: datetime
    worker_id: int
    artifact_store: ArtifactStore
    staging_root: Path
    operation_name: str
    operation: OperationDefinition
    sandbox_path: Path | None  # None for curator operations
    compute_backend: str = "local"
    shared_filesystem: bool = False
