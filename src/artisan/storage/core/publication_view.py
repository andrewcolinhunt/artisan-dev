"""Confined, query-owned reads of portable inspection evidence."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from io import BufferedReader, FileIO
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Any, BinaryIO, cast

import polars as pl
from fsspec import AbstractFileSystem

from artisan.errors import ArtifactIntegrityError, StoreIntegrityError
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.admission import ManagedCarrier
from artisan.schemas.execution.retained_result import RetainedTableFile
from artisan.schemas.provenance.projection import BoundaryProjection
from artisan.storage._publication_filesystem import _PublicationFileSystem
from artisan.storage._publication_provenance import (
    curator_artifact_edges,
    curator_execution_edges,
)
from artisan.storage._retained_publication import (
    PublicationExport,
    RetainedInspectionManifest,
    _validate_artifact_index,
)
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_provenance import artifact_edges, execution_edges
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    ARTIFACT_INDEX_SCHEMA,
    ARTIFACT_LOCATIONS_SCHEMA,
    STEPS_SCHEMA,
)
from artisan.storage.io.table_payload import capture_table_payload
from artisan.utils.hashing import compute_stream_digest


class PublicationReadScope:
    """Pin a bundle identity while capturing fresh evidence for each query."""

    def __init__(self, publication: PublicationExport) -> None:
        if publication.directory.is_symlink():
            msg = "Inspection root must not be a symbolic link"
            raise StoreIntegrityError(msg)
        self.publication = PublicationExport(publication.directory.resolve())
        self._digest = self.publication.metadata().inspection_digest
        self._current: ContextVar[_PublicationView | None] = ContextVar(
            "artisan_publication_read", default=None
        )
        self.fs = _InspectionFileSystem(self)

    @property
    def files_root(self) -> str:
        """Return the confined root containing the copied carrier inventory."""
        return str(self.publication.directory)

    @property
    def view(self) -> _PublicationView | None:
        """Return only this caller's active captured view."""
        return self._current.get()

    def _manifest(self) -> RetainedInspectionManifest:
        if self.view is not None:
            return self.view.manifest
        manifest = self.publication.metadata()
        if manifest.inspection_digest != self._digest:
            msg = "Inspection identity changed since the store was opened"
            raise StoreIntegrityError(msg)
        return manifest

    def validate_binding(
        self, root: str, fs: AbstractFileSystem, storage_options: dict[str, str]
    ) -> None:
        """Reject mixing publication visibility with another filesystem or root."""
        if root != self.files_root or fs is not self.fs or storage_options:
            msg = "Inspection query scope belongs to another store binding"
            raise StoreIntegrityError(msg)
        self._manifest()

    def open_store(self) -> ArtifactStore:
        """Reuse the ordinary read facade without granting live store authority."""
        return ArtifactStore(
            self.files_root,
            fs=self.fs,
            files_root=self.files_root,
            _read_scope=self,
        )

    @contextmanager
    def activate(self) -> Iterator[None]:
        """Share one private capture across nested calls, then discard it."""
        if self.view is not None:
            yield
            return
        manifest = self._manifest()
        with TemporaryDirectory(prefix="artisan-publication-query-") as scratch:
            token = self._current.set(_PublicationView(self, manifest, Path(scratch)))
            try:
                yield
            finally:
                self._current.reset(token)


class _InspectionFileSystem(_PublicationFileSystem):
    """Verify whole selected carriers before exposing their read-only streams."""

    def __init__(self, scope: PublicationReadScope) -> None:
        self.scope = scope
        super().__init__(scope.publication.directory)

    def _mapped(self, path: str) -> str:
        self.scope._manifest()
        candidate = Path(path).absolute()
        if not candidate.is_relative_to(self.directory):
            msg = "Path is outside the publication bundle"
            raise PermissionError(msg)
        for part in (candidate, *candidate.parents):
            if part.is_symlink():
                msg = "Inspection paths must not contain symbolic links"
                raise PermissionError(msg)
            if part == self.directory:
                break
        return super()._mapped(path)

    def _open(self, path: str, mode: str = "rb", **kwargs: Any) -> BinaryIO:
        relative = str(Path(self._mapped(path)).relative_to(self.directory))
        carrier = next(
            (
                item
                for item in self.scope._manifest().carriers
                if item.relative_path == relative
            ),
            None,
        )
        stream = super()._open(path, mode, **kwargs)
        if carrier is None:
            return stream
        return _capture_carrier(stream, carrier, path)


def _capture_carrier(stream: BinaryIO, carrier: ManagedCarrier, path: str) -> BinaryIO:
    """Copy and verify before returning an independently read-only descriptor."""
    reader: _CapturedCarrier | None = None
    try:
        with stream, NamedTemporaryFile(mode="w+b") as captured:
            identity = compute_stream_digest(
                stream, destination=cast(BinaryIO, captured)
            )
            if identity != (carrier.digest, carrier.size_bytes):
                msg = "Inspection carrier bytes changed"
                raise ArtifactIntegrityError(msg)
            captured.flush()
            raw = FileIO(captured.name, "r")
            try:
                reader = _CapturedCarrier(raw, path)
            except BaseException:
                raw.close()
                raise
        return reader
    except BaseException:
        if reader is not None:
            reader.close()
        raise


class _CapturedCarrier(BufferedReader):
    """Read unlinked, verified bytes through an independently read-only descriptor."""

    def __init__(self, raw: FileIO, path: str) -> None:
        super().__init__(raw)
        self._path = path

    @property
    def name(self) -> str:
        return self._path

    @property
    def mode(self) -> str:
        return "rb"


class _PublicationView:
    """Read metadata and requested typed payloads from one captured manifest."""

    def __init__(
        self,
        scope: PublicationReadScope,
        manifest: RetainedInspectionManifest,
        scratch: Path,
    ) -> None:
        self.scope = scope
        self.manifest = manifest
        self.scratch = scratch
        self._tables: dict[str, pl.LazyFrame] = {}
        self._index: pl.DataFrame | None = None
        self._curator: pl.DataFrame | None = None

    @property
    def files_root(self) -> str:
        return self.scope.files_root

    def validate_binding(
        self, root: str, fs: AbstractFileSystem, storage_options: dict[str, str]
    ) -> None:
        self.scope.validate_binding(root, fs, storage_options)

    def _capture(self, item: RetainedTableFile, schema: dict[str, Any]) -> pl.LazyFrame:
        if item.table_path not in self._tables:
            captured = capture_table_payload(
                item,
                directory=self.files_root,
                scratch=self.scratch,
                expected_schema=schema,
                fs=self.scope.fs,
            )
            self._tables[item.table_path] = pl.scan_parquet(captured.path)
        return self._tables[item.table_path]

    def _artifact_index(self) -> pl.DataFrame:
        if self._index is None:
            self._index = self._capture(
                self.manifest.artifact_index, ARTIFACT_INDEX_SCHEMA
            ).collect()
            _validate_artifact_index(self.manifest, self._index)
        return self._index

    def scan(self, table: str | TablePath) -> pl.LazyFrame:
        """Scan only the requested metadata or registered artifact table."""
        path = table.value if isinstance(table, TablePath) else table
        if path == TablePath.ARTIFACT_INDEX:
            return self._artifact_index().lazy()
        if path == TablePath.ARTIFACT_EDGES:
            return self.artifact_edges(direct=False)
        if path == TablePath.EXECUTION_EDGES:
            self._curator_edges()
            return (
                pl.concat(
                    [
                        execution_edges(self._scientific_projections()),
                        curator_execution_edges(self.manifest.curator_sources),
                    ]
                )
                .unique()
                .lazy()
            )
        if path == TablePath.ARTIFACT_LOCATIONS:
            return self._locations().lazy()
        if path == TablePath.STEPS:
            return self._lifecycle()
        return self._content(path)

    def _scientific_projections(self) -> dict[str, BoundaryProjection]:
        return {
            source.delivery.delivery_id: source.projection
            for source in self.manifest.sources
            if source.delivery.attempt.replay_of_execution_run_id is None
        }

    def _curator_edges(self) -> pl.DataFrame:
        if self._curator is None:
            frame = self._capture(
                self.manifest.curator_artifact_edges, ARTIFACT_EDGES_SCHEMA
            ).collect()
            self._curator = curator_artifact_edges(
                frame, self.manifest, self._artifact_index()
            )
        return self._curator

    def artifact_edges(self, *, direct: bool) -> pl.LazyFrame:
        return (
            pl.concat(
                [
                    artifact_edges(self._scientific_projections(), direct=direct),
                    self._curator_edges(),
                ]
            )
            .unique()
            .lazy()
        )

    def _locations(self) -> pl.DataFrame:
        carriers = {item.digest: item for item in self.manifest.carriers}
        return pl.DataFrame(
            [
                {
                    "artifact_id": binding.artifact_id,
                    "uri": str(
                        self.scope.publication.directory
                        / carriers[binding.carrier_digest].relative_path
                    ),
                }
                for binding in self.manifest.external_bindings
            ],
            schema=ARTIFACT_LOCATIONS_SCHEMA,
        )

    def _content(self, path: str) -> pl.LazyFrame:
        definition = next(
            (
                item
                for item in ArtifactTypeDef.get_all().values()
                if item.table_path == path
            ),
            None,
        )
        if definition is None:
            msg = "Inspection content type must be registered by the caller"
            raise ValueError(msg)
        item = next(
            (item for item in self.manifest.tables if item.table_path == path), None
        )
        if item is None:
            if (
                self._artifact_index()
                .filter(pl.col("artifact_type") == definition.key)
                .height
            ):
                msg = "Inspection index has no corresponding content payload"
                raise StoreIntegrityError(msg)
            return pl.DataFrame(schema=definition.polars_schema()).lazy()
        frame = self._capture(item, definition.polars_schema())
        index = (
            frame.select("artifact_id", "origin_step_number", "metadata")
            .with_columns(pl.lit(definition.key).alias("artifact_type"))
            .select(list(ARTIFACT_INDEX_SCHEMA))
            .collect()
        )
        expected = self._artifact_index().filter(
            pl.col("artifact_type") == definition.key
        )
        if not index.sort("artifact_id").equals(expected.sort("artifact_id")):
            msg = "Inspection content changed protected index metadata"
            raise StoreIntegrityError(msg)
        return frame

    def _lifecycle(self) -> pl.LazyFrame:
        runs = self.manifest.runs
        if runs is None:
            msg = "Inspection bundle has no run inventory"
            raise ValueError(msg)
        return self._capture(runs.lifecycle, STEPS_SCHEMA)

    def step_names(self, pipeline_run_id: str | None) -> dict[int, str]:
        from artisan.orchestration.engine.publication_runs import _validate_run_rows
        from artisan.orchestration.engine.step_tracker import StepTracker

        attempts = _validate_run_rows(self.manifest, self._lifecycle().collect())
        return {
            state.step_number: state.step_name
            for state in StepTracker._select_current_states(attempts, pipeline_run_id)
        }
