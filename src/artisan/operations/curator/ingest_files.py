"""Abstract base for file ingestion curator operations.

Subclasses implement ``convert_file()`` to turn FileRefArtifacts into
domain-specific artifacts (e.g. DataArtifact).
"""

from __future__ import annotations

from abc import abstractmethod
from enum import StrEnum, auto
from typing import TYPE_CHECKING, ClassVar

import polars as pl

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.specs.input_spec import InputSpec

if TYPE_CHECKING:
    from artisan.storage.core.artifact_store import ArtifactStore


class IngestFiles(OperationDefinition):
    """Abstract base for converting FileRefArtifacts into domain artifacts.

    Subclasses set ``name``, ``outputs``, ``OutputRole``, and implement
    ``convert_file()`` to produce the target artifact type.
    """

    # ---------- Inputs ----------
    class InputRole(StrEnum):
        file = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.file: InputSpec(
            artifact_type=ArtifactTypes.FILE_REF,
            required=True,
            description="File refs to ingest",
        ),
    }

    # ---------- Lifecycle ----------
    @abstractmethod
    def convert_file(self, file_ref: FileRefArtifact, step_number: int) -> Artifact:
        """Convert a single FileRefArtifact into a domain artifact.

        Args:
            file_ref: The file reference to convert.
            step_number: Current pipeline step number.

        Returns:
            A draft artifact of the target type.
        """

    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        """Hydrate FileRefArtifacts and convert them via ``convert_file()``.

        Args:
            inputs: Must contain a ``file`` key with a DataFrame of artifact IDs.
            step_number: Current pipeline step number.
            artifact_store: Store for hydrating FileRefArtifact objects.

        Returns:
            ArtifactResult with drafts keyed by the subclass output role.
        """
        file_df = inputs.get("file")

        if file_df is None or file_df.is_empty():
            return ArtifactResult(
                success=False,
                error="No input files provided",
            )

        file_ref_ids = file_df["artifact_id"].to_list()
        file_refs_by_id = artifact_store.get_artifacts_by_type(
            file_ref_ids, ArtifactTypes.FILE_REF
        )

        output_role = next(iter(self.outputs))

        drafts: list[Artifact] = []
        for fid in file_ref_ids:
            file_ref = file_refs_by_id.get(fid)
            if file_ref is not None:
                drafts.append(self.convert_file(file_ref, step_number))

        if not drafts:
            return ArtifactResult(
                success=False,
                error="No input files provided",
            )

        return ArtifactResult(success=True, artifacts={output_role: drafts})
