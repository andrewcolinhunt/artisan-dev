"""Curator that concatenates per-worker JSONL bundles into a single file.

Natural post_step target for RecordBundleGenerator: after parallel
workers each produce their own JSONL files, this curator reads them
all and writes a single combined.jsonl in files_root.
"""

from __future__ import annotations

from enum import StrEnum, auto
from typing import TYPE_CHECKING, ClassVar

import polars as pl

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.record_bundle import RecordBundleArtifact
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

if TYPE_CHECKING:
    from artisan.storage.core.artifact_store import ArtifactStore


class ConsolidateRecordBundles(OperationDefinition):
    """Concatenate per-worker JSONL bundles into a single file.

    Reads RecordBundleArtifacts from multiple worker files, concatenates
    all JSONL content into one combined file, and produces new artifacts
    pointing to the combined path. Because external_path is included in
    the content hash, consolidated artifacts get new artifact_ids.

    Input Roles:
        records (record_bundle) -- Per-worker record bundle artifacts

    Output Roles:
        records (record_bundle) -- Consolidated record bundle artifacts
    """

    name = "consolidate_record_bundles"
    description = "Concatenate per-worker JSONL bundles into a single file"

    class InputRole(StrEnum):
        records = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.records: InputSpec(
            artifact_type="record_bundle",
        ),
    }

    class OutputRole(StrEnum):
        records = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.records: OutputSpec(
            artifact_type="record_bundle",
            description="Consolidated record bundle",
            infer_lineage_from={"inputs": ["records"]},
        ),
    }

    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        """Concatenate worker JSONL files and create consolidated artifacts."""
        if artifact_store.files_root is None:
            msg = "files_root required for ConsolidateRecordBundles"
            raise ValueError(msg)

        record_ids = inputs["records"]["artifact_id"].to_list()
        artifacts = artifact_store.get_artifacts_by_type(record_ids, "record_bundle")

        # Find distinct worker files
        worker_files: set[str] = set()
        for art in artifacts.values():
            if art.external_path:
                worker_files.add(art.external_path)

        # Concatenate into combined file
        combined_path = artifact_store.files_root / str(step_number) / "combined.jsonl"
        combined_path.parent.mkdir(parents=True, exist_ok=True)
        with open(combined_path, "w") as out:
            for worker_file in sorted(worker_files):
                with open(worker_file) as f:
                    out.write(f.read())

        # Create new artifacts pointing to combined file
        drafts: list[RecordBundleArtifact] = []
        for art in artifacts.values():
            drafts.append(
                RecordBundleArtifact.draft(
                    record_id=art.record_id,
                    content_hash=art.content_hash,
                    size_bytes=art.size_bytes,
                    step_number=step_number,
                    external_path=str(combined_path),
                    original_name=art.original_name,
                )
            )

        return ArtifactResult(
            success=True,
            artifacts={"records": drafts},
        )
