"""Execution edge model for execution-artifact relationships.

Each ``ExecutionEdge`` records that an execution consumed (input) or
produced (output) a specific artifact.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExecutionEdge:
    """Edge between an execution and an artifact.

    Represents what artifacts an execution consumed (direction="input")
    or produced (direction="output"). Stored in execution_edges.parquet.

    Attributes:
        execution_run_id: The execution that consumed/produced the artifact.
        direction: "input" (execution consumed artifact) or "output" (execution produced artifact).
        role: The role name for this input/output.
        artifact_id: The artifact that was consumed/produced.
    """

    execution_run_id: str
    direction: str  # "input" or "output"
    role: str
    artifact_id: str
