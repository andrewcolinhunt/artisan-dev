"""Pipeline configuration schema for orchestration."""

from __future__ import annotations

import tempfile

from pydantic import BaseModel, Field, model_validator

from artisan.schemas.enums import CachePolicy, FailurePolicy
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.utils.path import uri_join, uri_parent


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
    delta_root: str = Field(..., description="Root URI for Delta Lake tables.")
    staging_root: str = Field(..., description="Root URI for worker staging files.")
    working_root: str = Field(
        default_factory=tempfile.gettempdir,
        description="Root path for worker sandboxes. Always local.",
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
    files_root: str | None = Field(
        default=None,
        description=(
            "Root for Artisan-managed external files. "
            "Auto-derived for local deployments; must be set "
            "explicitly for cloud deployments."
        ),
    )
    storage: StorageConfig = Field(
        default_factory=StorageConfig,
        description="Storage backend configuration.",
    )

    model_config = {"frozen": True}

    @model_validator(mode="after")
    def _default_files_root(self) -> PipelineConfig:
        """Derive files_root from delta_root when not explicitly set.

        Only derives for local delta_root values. Cloud deployments
        must set files_root explicitly.
        """
        if self.files_root is None and self.storage.is_local:
            object.__setattr__(
                self, "files_root", uri_join(uri_parent(self.delta_root), "files")
            )
        return self
