"""Runtime environment configuration for worker processes.

``RuntimeEnvironment`` specifies WHERE execution happens (paths, debug
flags), separate from WHAT executes (ExecutionUnit).
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field


class RuntimeEnvironment(BaseModel):
    """Runtime environment configuration.

    This specifies WHERE execution happens, not WHAT executes.
    Typically created from PipelineConfig via _create_runtime_environment() and
    passed to workers.

    The separation of concerns:
    - ExecutionUnit: What to compute (operation, inputs, params, step_number)
    - RuntimeEnvironment: Where to compute (paths, debug flags)

    For curator operations, working_root_path is None since they
    don't require sandboxing or materialization.

    Path semantics:
    - delta_root_path: Read-only access to Delta Lake tables
    - working_root_path: Read-write scratch space (cleaned after execution)
    - staging_root_path: Write-only output for staged Parquet files

    Attributes:
        delta_root_path: Root directory for Delta Lake tables.
            Contains: data/, metrics/, configs/, file_refs/,
            artifact_index/, executions/
        working_root_path: Base directory for execution sandboxes.
            Resolved from $TMPDIR (via tempfile.gettempdir()) by default.
            Each execution creates a sharded subdirectory.
            None for curator operations (no sandbox needed).
        staging_root_path: Where to write staged Parquet files.
            Workers write here; orchestrator commits to Delta Lake.
        preserve_staging: Debug flag - don't cleanup staging after commit.
        preserve_working: Debug flag - don't cleanup sandbox after execution.

    Example:
        >>> env = RuntimeEnvironment(
        ...     delta_root_path=Path("/data/delta"),
        ...     working_root_path=Path("/tmp"),
        ...     staging_root_path=Path("/data/staging"),
        ... )
        >>> # RuntimeEnvironment is typically created once per worker process
        >>> # and reused for many ExecutionUnits
    """

    delta_root_path: Path = Field(
        ...,
        description=(
            "Root directory for Delta Lake tables. "
            "Contains data/, metrics/, configs/, file_refs/, "
            "artifact_index/, executions/"
        ),
    )

    working_root_path: Path | None = Field(
        None,
        description=(
            "Base directory for execution sandboxes. "
            "Each execution creates a sharded subdirectory. "
            "Defaults to $TMPDIR via tempfile.gettempdir(). "
            "None for curator operations (no sandbox needed)."
        ),
    )

    staging_root_path: Path = Field(
        ...,
        description=(
            "Where to write staged Parquet files. "
            "Workers write here; orchestrator commits to Delta Lake"
        ),
    )

    failure_logs_root: Path | None = Field(
        None,
        description=(
            "Where to write human-readable failure log files. "
            "Each failed execution writes a .log file under "
            "step_{N}_{op}/ for easy debugging."
        ),
    )

    # Debug flags
    preserve_staging: bool = Field(
        False,
        description="Don't cleanup staging directory after commit",
    )
    preserve_working: bool = Field(
        False,
        description="Don't cleanup sandbox directory after execution",
    )

    # Backend traits (flattened from WorkerTraits for serialization)
    worker_id_env_var: str | None = Field(
        None,
        description="Environment variable for worker ID (e.g. SLURM_ARRAY_TASK_ID).",
    )
    shared_filesystem: bool = Field(
        False,
        description="Whether workers share a filesystem with the orchestrator.",
    )
    compute_backend_name: str = Field(
        "local",
        description="Backend name for provenance records.",
    )

    model_config = {"frozen": True}  # Config should be immutable
