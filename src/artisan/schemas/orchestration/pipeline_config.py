"""Pipeline configuration schema for orchestration."""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantic import BaseModel, Field, model_validator

from artisan.schemas.enums import CachePolicy, FailurePolicy
from artisan.schemas.execution.storage_config import StorageConfig


class PipelineConfig(BaseModel):
    """Configuration for a pipeline execution.

    This is the orchestrator-level configuration, set once per pipeline.
    Values are propagated to workers via RuntimeEnvironment.
    """

    name: str = Field(..., description="Pipeline identifier.")
    pipeline_run_id: str = Field(
        default="",
        description="Unique identifier for this pipeline run session.",
    )
    delta_root: Path = Field(..., description="Root path for Delta Lake tables.")
    staging_root: Path = Field(..., description="Root path for worker staging files.")
    working_root: Path = Field(
        default_factory=lambda: Path(tempfile.gettempdir()),
        description="Root path for worker sandboxes.",
    )
    failure_policy: FailurePolicy = Field(
        default=FailurePolicy.CONTINUE,
        description="Default failure handling (CONTINUE or FAIL_FAST).",
    )
    cache_policy: CachePolicy = Field(
        default=CachePolicy.ALL_SUCCEEDED,
        description="Controls when completed steps qualify as cache hits.",
    )
    default_backend: str = Field(
        default="local",
        description="Default backend name for step execution.",
    )
    preserve_staging: bool = Field(
        default=False,
        description="Debug flag to preserve staging files after commit.",
    )
    preserve_working: bool = Field(
        default=False,
        description="Debug flag to preserve sandbox after execution.",
    )
    recover_staging: bool = Field(
        default=True,
        description="Commit leftover staging files from prior crashed runs at pipeline init.",
    )
    skip_cache: bool = Field(
        default=False,
        description="Bypass all cache lookups (step-level and execution-level).",
    )
    files_root: Path | None = Field(
        default=None,
        description=(
            "Root path for Artisan-managed external files. "
            "Defaults to delta_root.parent / 'files'."
        ),
    )
    storage: StorageConfig = Field(
        default_factory=StorageConfig,
        description="Storage backend configuration.",
    )

    model_config = {"frozen": True}

    @model_validator(mode="after")
    def _default_files_root(self) -> PipelineConfig:
        """Derive files_root from delta_root when not explicitly set."""
        if self.files_root is None:
            object.__setattr__(self, "files_root", self.delta_root.parent / "files")
        return self
