"""Storage, scratch, cleanup, and identity settings for worker execution."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator

from artisan.schemas.execution.storage_config import StorageConfig
from artisan.utils.application_modules import validate_application_modules


class WorkerConfig(BaseModel):
    """Immutable configuration used by curator execution and replay capture.

    The admitted worker constructs these settings from its dispatch context.
    Software dependencies belong to Environment declarations; this model records
    storage access, scratch locations, cleanup preferences, and worker identity.

    Field semantics:
    - delta_root, staging_root, files_root, failure_logs_root: Local path or
      s3://bucket/... URI. Use uri_join(), fs.* for I/O.
    - working_root: Always a local path. Use os.* for I/O.

    Example:
        >>> config = WorkerConfig(
        ...     delta_root="/data/delta",
        ...     staging_root="/data/staging",
        ...     working_root="/tmp",
        ... )
    """

    application_modules: tuple[str, ...] = Field(
        default=(),
        description="Resolved trusted application modules; no worker environment merge.",
    )

    @field_validator("application_modules", mode="before")
    @classmethod
    def _validate_application_modules(cls, value: object) -> tuple[str, ...]:
        return validate_application_modules(value)

    delta_root: str = Field(
        ...,
        description=(
            "Root URI for Delta Lake tables. Local path or s3://bucket/delta."
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
            "Root URI for staged Parquet files. Local path or s3://bucket/staging."
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
            "Where to write human-readable failure logs: local path or configured storage URI."
        ),
    )

    # Debug flags
    preserve_staging: bool = Field(
        False,
        description="Retain staging after verified commitment; uncommitted staging is always retained",
    )
    preserve_working: bool = Field(
        False,
        description="Don't cleanup sandbox directory after execution",
    )

    # Resolved worker identity and placement metadata.
    worker_id_env_var: str | None = Field(
        None,
        description="Provider environment variable resolved at the worker entry point.",
    )
    worker_id: int = Field(
        default=0,
        strict=True,
        ge=-(2**31),
        le=2**31 - 1,
        description="Resolved numeric worker index; 0 for local execution.",
    )
    shared_filesystem: bool = Field(
        False,
        description="Whether workers share a filesystem with the orchestrator.",
    )
    compute_backend_name: str = Field(
        "local",
        description="Resolved step-runner name for provenance records.",
    )

    # Storage
    storage: StorageConfig = Field(
        default_factory=StorageConfig,
        description="Storage configuration for fsspec and delta-rs.",
    )

    model_config = {"frozen": True}  # Config should be immutable
