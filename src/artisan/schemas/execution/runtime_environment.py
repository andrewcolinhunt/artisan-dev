"""Runtime environment configuration for worker processes.

``RuntimeEnvironment`` specifies WHERE execution happens (URIs, debug
flags), separate from WHAT executes (ExecutionUnit).
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from artisan.schemas.execution.storage_config import StorageConfig


class RuntimeEnvironment(BaseModel):
    """Runtime environment configuration.

    This specifies WHERE execution happens, not WHAT executes.
    Typically created from PipelineConfig via _create_runtime_environment() and
    passed to workers.

    The separation of concerns:
    - ExecutionUnit: What to compute (operation, inputs, params, step_number)
    - RuntimeEnvironment: Where to compute (URIs/paths, debug flags)

    For curator operations, working_root is None since they
    don't require sandboxing or materialization.

    Field semantics:
    - delta_root, staging_root, files_root: Cloud-capable — local path or
      s3://bucket/... URI. Use uri_join(), fs.* for I/O.
    - working_root, failure_logs_root: Always local paths. Use os.* for I/O.

    Example:
        >>> env = RuntimeEnvironment(
        ...     delta_root="/data/delta",
        ...     staging_root="/data/staging",
        ...     working_root="/tmp",
        ... )
    """

    delta_root: str = Field(
        ...,
        description=(
            "Root URI for Delta Lake tables. " "Local path or s3://bucket/delta."
        ),
    )

    working_root: str | None = Field(
        None,
        description=(
            "Base directory for execution sandboxes. "
            "Always a local path. "
            "None for curator operations (no sandbox needed)."
        ),
    )

    staging_root: str = Field(
        ...,
        description=(
            "Root URI for staged Parquet files. " "Local path or s3://bucket/staging."
        ),
    )

    files_root: str | None = Field(
        None,
        description=(
            "Root for Artisan-managed external files. "
            "Local path or cloud URI. "
            "None when not configured."
        ),
    )

    failure_logs_root: str | None = Field(
        None,
        description=(
            "Where to write human-readable failure log files. " "Always a local path."
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

    # Storage backend
    storage: StorageConfig = Field(
        default_factory=StorageConfig,
        description="Storage backend configuration for fsspec and delta-rs.",
    )

    model_config = {"frozen": True}  # Config should be immutable
