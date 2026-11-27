"""Curator operation that imports artifacts from another pipeline's store.

Re-drafts artifacts from a source Delta Lake step as new roots in the
current pipeline, with no cross-pipeline provenance edges.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar

import polars as pl
from pydantic import BaseModel, Field, field_validator

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.artifact_store import ArtifactStore


class IngestPipelineStep(OperationDefinition):
    """Import artifacts from another pipeline's Delta Lake store.

    Reads accepted outputs from an explicit source run at one step or through
    an inclusive boundary, optionally filtered by type. Each invocation reads
    the latest source attempts and re-drafts their artifacts as destination
    roots. External content is copied into destination-owned storage.
    """

    name = "ingest_pipeline_step"
    description = "Import accepted artifacts from another pipeline run"
    version = "2"
    cacheable = False

    inputs: ClassVar[dict[str, InputSpec]] = {}

    outputs: ClassVar[dict[str, OutputSpec]] = {}

    class Params(BaseModel):
        """Cross-pipeline import parameters."""

        source_delta_root: str = Field(
            ..., description="Path to the source pipeline's delta_root"
        )
        source_files_root: str | None = Field(
            default=None,
            min_length=1,
            description="Source managed files root. Defaults to the sibling files "
            "directory for local storage; required for cloud storage.",
        )
        source_run_id: str = Field(..., description="Exact source pipeline run ID")
        source_step: int = Field(
            ..., ge=0, description="Inclusive source step boundary"
        )
        include_prior_steps: bool = Field(
            default=False,
            description="Import the union of accepted outputs through source_step",
        )
        artifact_type: str | None = Field(
            default=None,
            description="Optional filter: import only this artifact type. "
            "If None, imports all selected artifact types.",
        )
        source_storage: StorageConfig = Field(
            default_factory=StorageConfig,
            description="Storage config for the source pipeline. "
            "Default (local) works for local/NFS source pipelines.",
        )

        @field_validator("source_run_id")
        @classmethod
        def validate_source_run_id(cls, value: str) -> str:
            """Require an explicit nonempty source run ID."""
            value = value.strip()
            if not value:
                msg = "source_run_id must not be empty"
                raise ValueError(msg)
            return value

    params: Params

    runner_resources: RunnerResources = RunnerResources(time_limit="00:10:00")  # type: ignore[call-arg]

    batch_strategy: BatchStrategy = BatchStrategy(job_name="ingest_pipeline_step")  # type: ignore[call-arg]

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        """Load artifacts from source store and re-draft them.

        Args:
            inputs: Not used (generative curator — reads from source_delta_root).
            step_number: Step number for re-drafted artifacts.
            artifact_store: Not used (reads from source_delta_root).

        Returns:
            ArtifactResult with imported artifacts keyed by type.
        """
        from artisan.orchestration.engine.source_import import import_configured_outputs

        return import_configured_outputs(self, step_number=step_number)

    def _to_draft(self, artifact: Artifact, step_number: int) -> Artifact:
        """Re-draft an artifact for import into the current pipeline.

        Creates a copy with a new step number and clears the artifact_id
        so finalize() recomputes it. The import metadata deliberately gives
        the new root a distinct semantic identity from its source artifact.

        Args:
            artifact: Source artifact (finalized).
            step_number: Target step number in current pipeline.

        Returns:
            Finalized draft with same content but new step context.
        """
        metadata = dict(artifact.metadata)
        metadata.pop("imported_from_step", None)
        metadata["imported_from"] = {
            "pipeline_run_id": self.params.source_run_id,
            "artifact_id": artifact.artifact_id,
            "origin_step_number": artifact.origin_step_number,
        }
        draft = artifact.model_copy(
            update={
                "artifact_id": None,
                "origin_step_number": step_number,
                "metadata": metadata,
                "materialized_path": None,
            }
        )
        return draft.finalize()
