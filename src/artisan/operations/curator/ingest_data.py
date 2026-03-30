"""Ingest files as DataArtifacts.

Concrete IngestFiles subclass that reads file content from disk and
produces DataArtifact drafts.
"""

from __future__ import annotations

from enum import StrEnum, auto
from typing import ClassVar

from artisan.operations.curator.ingest_files import IngestFiles
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.specs.output_spec import OutputSpec


class IngestData(IngestFiles):
    """Convert FileRefArtifacts into DataArtifacts by reading file content."""

    # ---------- Metadata ----------
    name = "ingest_data"
    description = "Import files into the provenance graph as data artifacts"

    # ---------- Outputs ----------
    class OutputRole(StrEnum):
        data = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.data: OutputSpec(
            artifact_type="data",
            description="Ingested data artifact",
            infer_lineage_from={"inputs": ["file"]},
        ),
    }

    # ---------- Resources ----------
    resources: ResourceConfig = ResourceConfig(
        cpus=1, memory_gb=4, gpus=0, time_limit="00:10:00"
    )

    # ---------- Execution ----------
    execution: ExecutionConfig = ExecutionConfig(
        artifacts_per_unit=1, units_per_worker=1, job_name="ingest"
    )

    # ---------- Lifecycle ----------
    def convert_file(self, file_ref: FileRefArtifact, step_number: int) -> DataArtifact:
        """Convert a FileRefArtifact into a draft DataArtifact.

        Args:
            file_ref: The file reference to convert.
            step_number: Current pipeline step number.

        Returns:
            A draft DataArtifact with content read from disk.
        """

        content = file_ref.read_content()
        filename = f"{file_ref.original_name}{file_ref.extension or ''}"

        return DataArtifact.draft(
            content=content,
            original_name=filename,
            step_number=step_number,
            external_path=file_ref.path,
        )
