"""Curator that concatenates per-worker JSONL files into a single file.

Natural post_step target for AppendableGenerator: after parallel
workers each produce their own JSONL files, this curator reads them
all and writes a single combined.jsonl in files_root.
"""

from __future__ import annotations

from enum import StrEnum, auto
from typing import TYPE_CHECKING, ClassVar

import polars as pl

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.appendable import AppendableArtifact
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

if TYPE_CHECKING:
    from artisan.storage.core.artifact_store import ArtifactStore


class ConsolidateAppendables(OperationDefinition):
    """Concatenate per-worker JSONL files into a single file.

    Reads AppendableArtifacts from multiple worker files, concatenates
    all JSONL content into one combined file, and produces new artifacts
    pointing to the combined path. Because external_path is included in
    the content hash, consolidated artifacts get new artifact_ids.

    Input Roles:
        records (appendable) -- Per-worker appendable artifacts

    Output Roles:
        records (appendable) -- Consolidated appendable artifacts
    """

    name = "consolidate_appendables"
    description = "Concatenate per-worker JSONL files into a single file"

    class InputRole(StrEnum):
        records = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.records: InputSpec(
            artifact_type="appendable",
        ),
    }

    class OutputRole(StrEnum):
        records = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.records: OutputSpec(
            artifact_type="appendable",
            description="Consolidated appendable artifacts",
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
            msg = "files_root required for ConsolidateAppendables"
            raise ValueError(msg)

        fs = artifact_store._fs
        record_ids = inputs["records"]["artifact_id"].to_list()
        artifacts = artifact_store.get_artifacts_by_type(record_ids, "appendable")

        # Find distinct worker files
        worker_files: set[str] = set()
        for art in artifacts.values():
            if art.external_path:
                worker_files.add(art.external_path)

        # Concatenate into combined file
        combined_uri = f"{artifact_store.files_root}/{step_number}/combined.jsonl"
        combined_dir = f"{artifact_store.files_root}/{step_number}"
        fs.makedirs(combined_dir, exist_ok=True)
        with fs.open(combined_uri, "w") as out:
            for worker_file in sorted(worker_files):
                with fs.open(worker_file, "r") as f:
                    out.write(f.read())

        # Create new artifacts pointing to combined file
        drafts: list[AppendableArtifact] = []
        for art in artifacts.values():
            drafts.append(
                AppendableArtifact.draft(
                    record_id=art.record_id,
                    content_hash=art.content_hash,
                    size_bytes=art.size_bytes,
                    step_number=step_number,
                    external_path=combined_uri,
                    original_name=art.original_name,
                )
            )

        return ArtifactResult(
            success=True,
            artifacts={"records": drafts},
        )
