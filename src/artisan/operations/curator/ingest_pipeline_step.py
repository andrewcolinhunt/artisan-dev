"""Curator operation that imports artifacts from another pipeline's store.

Re-drafts artifacts from a source Delta Lake step as new roots in the
current pipeline, with no cross-pipeline provenance edges.
"""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

import polars as pl
from pydantic import Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.artifact_store import ArtifactStore


class IngestPipelineStep(OperationDefinition):
    """Import artifacts from another pipeline's Delta Lake store.

    Reads artifacts at a specific step in the source pipeline, optionally
    filtered by type, and re-drafts them as new roots in the current
    pipeline. This is a generative curator (no pipeline inputs, dynamic
    outputs).
    """

    # ---------- Metadata ----------
    name = "ingest_pipeline_step"
    description = "Import artifacts from another pipeline step"

    # ---------- Inputs ----------
    inputs: ClassVar[dict[str, InputSpec]] = {}

    # ---------- Outputs ----------
    outputs: ClassVar[dict[str, OutputSpec]] = {}

    # ---------- Parameters ----------
    source_delta_root: Path = Field(
        ..., description="Path to the source pipeline's delta_root"
    )
    source_step: int = Field(
        ..., ge=0, description="Step number to import artifacts from"
    )
    artifact_type: str | None = Field(
        default=None,
        description="Optional filter: import only this artifact type. "
        "If None, imports all types found at the source step.",
    )

    # ---------- Resources ----------
    resources: ResourceConfig = ResourceConfig(time_limit="00:10:00")

    # ---------- Execution ----------
    execution: ExecutionConfig = ExecutionConfig(job_name="ingest_pipeline_step")

    # ---------- Lifecycle ----------
    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],  # noqa: ARG002
        step_number: int,
        artifact_store: ArtifactStore,  # noqa: ARG002
    ) -> ArtifactResult:
        """Load artifacts from source store and re-draft them.

        Args:
            inputs: Not used (generative curator — reads from source_delta_root).
            step_number: Step number for re-drafted artifacts.
            artifact_store: Not used (reads from source_delta_root).

        Returns:
            ArtifactResult with imported artifacts keyed by type.
        """
        source_root = Path(self.source_delta_root)
        if not source_root.exists():
            return ArtifactResult(
                success=False,
                error=f"Source delta root does not exist: {source_root}",
            )

        source_store = ArtifactStore(source_root)
        types_to_import = self._resolve_types(source_store)

        if not types_to_import:
            return ArtifactResult(
                success=False,
                error=f"No artifacts found at step {self.source_step}"
                + (f" with type {self.artifact_type!r}" if self.artifact_type else ""),
            )

        all_drafts: dict[str, list[Artifact]] = {}
        for atype in sorted(types_to_import):
            drafts = self._import_type(source_store, atype, step_number)
            if drafts:
                all_drafts[atype] = drafts

        if not all_drafts:
            return ArtifactResult(
                success=False,
                error=f"No artifacts loaded from step {self.source_step}",
            )

        return ArtifactResult(success=True, artifacts=all_drafts)

    def _resolve_types(self, source_store: ArtifactStore) -> list[str]:
        """Discover which artifact types exist at the source step.

        Args:
            source_store: Read-only store for the source pipeline.

        Returns:
            List of artifact type strings to import.
        """
        if self.artifact_type is not None:
            # User specified a type — check it exists at the step
            ids = source_store.load_artifact_ids_by_type(
                self.artifact_type, step_numbers=[self.source_step]
            )
            return [self.artifact_type] if ids else []

        # Discover all types at the step
        type_map = source_store.load_artifact_type_map()
        step_map = source_store.load_step_number_map()

        types_at_step: set[str] = set()
        for aid, atype in type_map.items():
            if step_map.get(aid) == self.source_step:
                types_at_step.add(atype)

        return list(types_at_step)

    def _import_type(
        self,
        source_store: ArtifactStore,
        artifact_type: str,
        target_step_number: int,
    ) -> list[Artifact]:
        """Load and re-draft all artifacts of a type from the source step.

        Args:
            source_store: Read-only store for the source pipeline.
            artifact_type: The artifact type to import.
            target_step_number: Step number for the new drafts.

        Returns:
            List of finalized draft artifacts.
        """
        artifact_ids = source_store.load_artifact_ids_by_type(
            artifact_type, step_numbers=[self.source_step]
        )
        if not artifact_ids:
            return []

        artifacts = source_store.get_artifacts_by_type(
            list(artifact_ids), artifact_type
        )

        drafts: list[Artifact] = []
        for artifact in artifacts.values():
            draft = self._to_draft(artifact, target_step_number)
            drafts.append(draft)

        return drafts

    @staticmethod
    def _to_draft(artifact: Artifact, step_number: int) -> Artifact:
        """Re-draft an artifact for import into the current pipeline.

        Creates a copy with a new step number and clears the artifact_id
        so finalize() recomputes it. Same content produces same artifact_id.

        Args:
            artifact: Source artifact (finalized).
            step_number: Target step number in current pipeline.

        Returns:
            Finalized draft with same content but new step context.
        """
        draft = artifact.model_copy(
            update={
                "artifact_id": None,
                "origin_step_number": step_number,
                "metadata": {
                    **artifact.metadata,
                    "imported_from_step": artifact.origin_step_number,
                },
                "materialized_path": None,
            }
        )
        return draft.finalize()
