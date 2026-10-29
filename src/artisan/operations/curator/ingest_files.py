"""Abstract base for file ingestion curator operations.

Subclasses implement ``convert_file()`` to turn FileRefArtifacts into
domain-specific artifacts (e.g. CSVArtifact).
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterator
from enum import StrEnum, auto
from typing import TYPE_CHECKING, ClassVar, cast

import polars as pl

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.curator_result import (
    ArtifactEmission,
    ArtifactResult,
    ArtifactStreamResult,
)
from artisan.schemas.specs.input_spec import InputSpec

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

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
    def convert_file(
        self,
        file_ref: FileRefArtifact,
        step_number: int,
        *,
        fs: AbstractFileSystem | None = None,
    ) -> Artifact:
        """Convert a single FileRefArtifact into a domain artifact.

        Args:
            file_ref: The file reference to convert.
            step_number: Current pipeline step number.
            fs: Configured filesystem for reading the file.

        Returns:
            A draft artifact of the target type.
        """

    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult | ArtifactStreamResult:
        """Resolve every descriptor, then convert one file per emission.

        Args:
            inputs: Must contain a ``file`` key with a DataFrame of artifact IDs.
            step_number: Current pipeline step number.
            artifact_store: Store for hydrating FileRefArtifact objects.

        Returns:
            A stream of drafts, or an eager failure for empty/unresolved inputs.
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

        if any(fid not in file_refs_by_id for fid in file_ref_ids):
            return ArtifactResult(
                success=False,
                error="Not all provided file refs could be resolved from the artifact store",
            )

        return ArtifactStreamResult(
            items=self._convert_files(
                file_ref_ids, file_refs_by_id, step_number, artifact_store.filesystem
            )
        )

    def _convert_files(
        self,
        file_ref_ids: list[str],
        file_refs_by_id: dict[str, Artifact],
        step_number: int,
        fs: AbstractFileSystem,
    ) -> Iterator[ArtifactEmission]:
        """Preserve ordered occurrences without retaining previous emissions."""
        output_role = next(iter(self.outputs))
        for fid in file_ref_ids:
            yield ArtifactEmission(
                role=output_role,
                artifact=self._convert_occurrence(
                    cast("FileRefArtifact", file_refs_by_id[fid]), step_number, fs
                ),
                sources={"file": [fid]},
            )

    def _convert_occurrence(
        self, file_ref: FileRefArtifact, step_number: int, fs: AbstractFileSystem
    ) -> Artifact:
        """Confine read_content's cache to this conversion's disposable descriptor."""
        disposable = file_ref.model_copy()
        disposable._cached_content = None
        return self.convert_file(disposable, step_number, fs=fs)
