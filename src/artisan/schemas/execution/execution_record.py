"""ExecutionRecord model for the denormalized execution log.

Each record maps to a row in the executions Delta Lake table and
carries dual identity: ``execution_spec_id`` (deterministic cache
key) and ``execution_run_id`` (unique per attempt, for provenance).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ExecutionRecord(BaseModel):
    """Record of a single execution in the provenance graph.

    Maps directly to a row in the executions Delta Lake table.

    Attributes:
        execution_run_id: Unique ID for this execution attempt (PK).
            Hash of: execution_spec_id + timestamp_start + source_worker.
        execution_spec_id: Deterministic ID for caching.
            Hash of: operation_name + sorted(input_artifact_ids) + merged_params.
        origin_step_number: Pipeline step number where this record was produced (partition key).
        operation_name: From OperationDefinition.name.
        params: Full instantiated parameters.
        user_overrides: Original user-provided parameter overrides.
        timestamp_start: Execution start time.
        timestamp_end: Execution end time.
        source_worker: Worker ID that executed this.
        compute_backend: local or slurm.
        success: Whether execution succeeded (row-level).
        error: Error message if failed (row-level).
        metadata: Additional metadata (JSON-serializable).
    """

    model_config = ConfigDict(
        extra="forbid",
    )

    # Dual identity (reference: v3 "ExecutionRecord has two identities")
    execution_run_id: str = Field(
        ...,
        description="Unique ID for this execution attempt (PK)",
        min_length=32,
        max_length=32,
    )
    execution_spec_id: str = Field(
        ...,
        description="Deterministic ID for caching",
        min_length=32,
        max_length=32,
    )

    step_run_id: str | None = Field(
        default=None,
        description="Step run ID linking this execution to a specific step attempt",
    )

    # Partition and identification
    origin_step_number: int = Field(
        ...,
        ge=0,
        description="Pipeline step number where this record was produced (partition key)",
    )
    operation_name: str = Field(
        ...,
        description="From OperationDefinition.name",
    )
    params: dict[str, Any] = Field(
        default_factory=dict,
        description="Full instantiated parameters",
    )
    user_overrides: dict[str, Any] = Field(
        default_factory=dict,
        description="Original user-provided parameter overrides",
    )

    # Timing
    timestamp_start: datetime = Field(
        ...,
        description="Execution start time",
    )
    timestamp_end: datetime | None = Field(
        default=None,
        description="Execution end time",
    )

    # Worker info
    source_worker: int = Field(
        default=0,
        ge=0,
        description="Worker ID that executed this",
    )
    compute_backend: str = Field(
        default="local",
        description="Where the execution ran",
    )

    # Row-level success/error
    success: bool = Field(
        default=True,
        description="Whether execution succeeded",
    )
    error: str | None = Field(
        default=None,
        description="Error message if execution failed",
    )

    # Extensibility
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata",
    )
