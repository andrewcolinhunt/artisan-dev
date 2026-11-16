"""Abstract base for file ingestion curator operations.

Subclasses implement ``convert_file()`` to turn FileRefArtifacts into
domain-specific artifacts (e.g. CSVArtifact).
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from enum import StrEnum, auto
from typing import TYPE_CHECKING, ClassVar, cast

import polars as pl

from artisan.execution.recording.curator_read_filesystem import CuratorReadFileSystem
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
from artisan.storage.core.curator_view import CuratorView
from artisan.storage.io.curator_external_reads import capture_curator_file

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
        inputs: Mapping[str, pl.DataFrame],
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
        view = getattr(artifact_store, "_view", None)
        if not isinstance(view, CuratorView):
            view = None
        file_refs_by_id = (
            artifact_store._ingest_file_descriptors(file_ref_ids)
            if view is not None
            else artifact_store.get_artifacts_by_type(
                file_ref_ids, ArtifactTypes.FILE_REF
            )
        )

        if any(fid not in file_refs_by_id for fid in file_ref_ids):
            return ArtifactResult(
                success=False,
                error="Not all provided file refs could be resolved from the artifact store",
            )

        return ArtifactStreamResult(
            items=self._convert_files(
                file_ref_ids,
                file_refs_by_id,
                step_number,
                artifact_store._fs if view is not None else artifact_store.filesystem,
                view=view,
            )
        )

    def _convert_files(
        self,
        file_ref_ids: list[str],
        file_refs_by_id: dict[str, Artifact],
        step_number: int,
        fs: AbstractFileSystem,
        *,
        view: CuratorView | None = None,
    ) -> Iterator[ArtifactEmission]:
        """Preserve ordered occurrences without retaining previous emissions."""
        output_role = next(iter(self.outputs))
        for fid in file_ref_ids:
            with _conversion_input(
                cast("FileRefArtifact", file_refs_by_id[fid]), fs, view
            ) as (file_ref, read_fs):
                yield ArtifactEmission(
                    role=output_role,
                    artifact=self._convert_occurrence(file_ref, step_number, read_fs),
                    sources={"file": [fid]},
                )

    def _convert_occurrence(
        self, file_ref: FileRefArtifact, step_number: int, fs: AbstractFileSystem
    ) -> Artifact:
        """Confine read_content's cache to this conversion's disposable descriptor."""
        disposable = file_ref.model_copy()
        disposable._cached_content = None
        return self.convert_file(disposable, step_number, fs=fs)


@contextmanager
def _conversion_input(
    artifact: FileRefArtifact, fs: AbstractFileSystem, view: CuratorView | None
) -> Iterator[tuple[FileRefArtifact, AbstractFileSystem]]:
    """Observe supported reads through staging without changing original paths.

    Direct Python I/O outside the adapter is not intercepted. A bypass-only
    conversion has no observed read and retains the external-input exclusion.
    """
    if view is None:
        yield artifact, fs
        return
    recorder = view._read_recorder
    if recorder is not None:
        recorder.begin_carrier_read()
    with capture_curator_file(view, artifact) as captured:
        adapter = CuratorReadFileSystem(
            fs,
            captured.witness.uri,
            captured.path,
            exclude=lambda: view.exclude_cache("untracked_filesystem_access"),
        )
        try:
            yield captured.artifact, cast("AbstractFileSystem", adapter)
        finally:
            adapter.close()
    if recorder is not None and adapter.read_observed:
        recorder.record_carrier_read(captured.witness)
