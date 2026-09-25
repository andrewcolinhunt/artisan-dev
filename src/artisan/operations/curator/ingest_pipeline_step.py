"""Curator operation that imports artifacts from another pipeline's store.

Re-drafts artifacts from a source Delta Lake step as new roots in the
current pipeline, with no cross-pipeline provenance edges.
"""

from __future__ import annotations

from typing import ClassVar

import polars as pl
from pydantic import BaseModel, Field, field_validator

from artisan.errors import ArtifactIntegrityError
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.run_scope import load_run_step_outputs


class IngestPipelineStep(OperationDefinition):
    """Import artifacts from another pipeline's Delta Lake store.

    Reads accepted outputs from an explicit source run at one step or through
    an inclusive boundary, optionally filtered by type. Each invocation reads
    the latest source attempts and re-drafts their artifacts as destination
    roots. External content retains its verified source location.
    """

    # ---------- Metadata ----------
    name = "ingest_pipeline_step"
    description = "Import accepted artifacts from another pipeline run"
    version = "2"
    cacheable = False

    # ---------- Inputs ----------
    inputs: ClassVar[dict[str, InputSpec]] = {}

    # ---------- Outputs ----------
    outputs: ClassVar[dict[str, OutputSpec]] = {}

    # ---------- Parameters ----------
    class Params(BaseModel):
        """Cross-pipeline import parameters."""

        source_delta_root: str = Field(
            ..., description="Path to the source pipeline's delta_root"
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

    # ---------- Resources ----------
    # pydantic Field-based defaults aren't recognized by mypy without the plugin;
    # ignore call-arg errors on these config constructors.
    runner_resources: RunnerResources = RunnerResources(time_limit="00:10:00")  # type: ignore[call-arg]

    # ---------- Execution ----------
    batch_strategy: BatchStrategy = BatchStrategy(job_name="ingest_pipeline_step")  # type: ignore[call-arg]

    # ---------- Lifecycle ----------
    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
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
        fs = self.params.source_storage.filesystem()

        if not fs.exists(self.params.source_delta_root):
            return ArtifactResult(
                success=False,
                error=f"Source delta root does not exist: {self.params.source_delta_root}",
            )

        options = self.params.source_storage.delta_storage_options()
        selected = load_run_step_outputs(
            self.params.source_delta_root,
            pipeline_run_id=self.params.source_run_id,
            step_number=self.params.source_step,
            include_prior_steps=self.params.include_prior_steps,
            fs=fs,
            storage_options=options,
        )
        if self.params.artifact_type is not None:
            selected = selected.filter(
                pl.col("artifact_type") == self.params.artifact_type
            )
        if selected.is_empty():
            mode = "through" if self.params.include_prior_steps else "at"
            return ArtifactResult(
                success=False,
                error=(
                    f"No accepted artifacts found in run {self.params.source_run_id!r} "
                    f"{mode} step {self.params.source_step}"
                    + (
                        f" with type {self.params.artifact_type!r}"
                        if self.params.artifact_type is not None
                        else ""
                    )
                ),
            )
        source_store = ArtifactStore(
            self.params.source_delta_root, fs=fs, storage_options=options
        )
        typed_ids = (
            selected.select("artifact_type", "artifact_id")
            .unique()
            .sort("artifact_type", "artifact_id")
        )
        all_drafts = {
            artifact_type: self._import_type(
                source_store,
                artifact_type,
                group["artifact_id"].to_list(),
                step_number,
            )
            for (artifact_type,), group in typed_ids.group_by(
                "artifact_type", maintain_order=True
            )
        }
        return ArtifactResult(
            success=True,
            artifacts=all_drafts,
            lineage={role: [] for role in all_drafts},
            metadata={
                "ingest_source": {
                    "pipeline_run_id": self.params.source_run_id,
                    "source_step": self.params.source_step,
                    "include_prior_steps": self.params.include_prior_steps,
                    "step_run_ids": sorted(
                        set(selected["current_step_run_id"].to_list())
                    ),
                }
            },
        )

    def _import_type(
        self,
        source_store: ArtifactStore,
        artifact_type: str,
        artifact_ids: list[str],
        target_step_number: int,
    ) -> list[Artifact]:
        """Load and re-draft all artifacts of a type from the source step.

        Args:
            source_store: Read-only store for the source pipeline.
            artifact_type: The artifact type to import.
            artifact_ids: Sorted source artifact IDs selected for this type.
            target_step_number: Step number for the new drafts.

        Returns:
            List of finalized draft artifacts.
        """
        artifacts = source_store.get_artifacts_by_type(artifact_ids, artifact_type)
        missing = sorted(set(artifact_ids) - set(artifacts))
        if missing:
            msg = f"Selected source artifacts are missing content rows: {missing!r}"
            raise ArtifactIntegrityError(msg)
        return [
            self._to_draft(artifacts[artifact_id], target_step_number)
            for artifact_id in artifact_ids
        ]

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
