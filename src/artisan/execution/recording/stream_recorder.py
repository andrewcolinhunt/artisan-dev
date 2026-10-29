"""Stage incremental curator outputs without publishing converted prefixes."""

from __future__ import annotations

import posixpath
import uuid
from collections.abc import Iterator
from contextlib import suppress
from copy import deepcopy
from typing import Any

from artisan.errors import StoreIntegrityError
from artisan.execution.recording.parquet_writer import (
    _artifact_index_row,
    _create_staging_path,
)
from artisan.execution.recording.recorder import build_execution_edges
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.external import validate_persistable_uri
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.execution_context import ExecutionContext
from artisan.storage.core.table_schemas import get_schema
from artisan.storage.io.bounded_io import (
    BATCH_BYTES,
    iter_parquet_frames,
    value_size,
    write_parquet_rows,
)
from artisan.storage.io.publication import (
    is_publication_temporary,
    publish_immutable_file,
)
from artisan.utils.path import uri_join


class StreamRecorder:
    """Own one byte budget and unpublished chunks for a curator execution.

    Each completed chunk uses the existing publication-temporary naming rule.
    Only one writer or upload runs at a time, including on object storage.
    Compaction adds a sequential pass to avoid one backend buffer per table.
    """

    def __init__(
        self,
        context: ExecutionContext,
        inputs: dict[str, list[str]],
        *,
        max_bytes: int = BATCH_BYTES,
    ) -> None:
        self.context = context
        self.fs = context.fs
        self.staging_path = _create_staging_path(
            context.staging_root,
            context.execution_run_id,
            context.step_number,
            context.operation_name,
            self.fs,
        )
        if self.fs.exists(self.staging_path):
            for entry in self.fs.ls(self.staging_path, detail=False):
                if not is_publication_temporary(posixpath.basename(str(entry))):
                    msg = "Artifact streaming requires a fresh canonical staging namespace"
                    raise StoreIntegrityError(msg)
        self.promotion_started = False
        self.output_ids: dict[str, list[str]] = {}
        self._max_bytes = max_bytes
        self._buffer_bytes = 0
        self._rows: dict[str, list[dict[str, Any]]] = {}
        self._schemas: dict[str, dict[str, Any]] = {}
        self._chunks: dict[str, list[str]] = {}
        self._candidates: dict[str, str] = {}
        self._temporaries: list[str] = []
        self._edges: set[ArtifactProvenanceEdge] = set()
        self._locations: set[tuple[str, str]] = set()
        self._execution_edges: set[tuple[str, str, str]] = set()
        for row in build_execution_edges(
            context.execution_run_id, inputs, {}
        ).iter_rows(named=True):
            self._add_execution_edge(row)

    def append(
        self, role: str, artifact: Artifact, edges: list[ArtifactProvenanceEdge]
    ) -> None:
        """Serialize one finalized occurrence without retaining its model."""
        artifact_id = artifact.artifact_id
        if artifact_id is None:
            msg = "Cannot stage an unfinished streamed artifact"
            raise ValueError(msg)
        definition = ArtifactTypeDef.get(artifact.artifact_type)
        self._add_row(
            definition.parquet_filename(), artifact.to_row(), definition.polars_schema()
        )
        self._add_table_row(
            TablePath.ARTIFACT_INDEX,
            _artifact_index_row(artifact, self.context.step_number),
        )
        self._add_location(artifact)
        for edge in edges:
            if edge not in self._edges:
                self._edges.add(edge)
                self._add_table_row(TablePath.ARTIFACT_EDGES, edge.model_dump())
        self.output_ids.setdefault(role, []).append(artifact_id)
        self._add_execution_edge(
            {
                "execution_run_id": self.context.execution_run_id,
                "direction": "output",
                "role": role,
                "artifact_id": artifact_id,
            }
        )

    def _add_location(self, artifact: Artifact) -> None:
        if not artifact.EXTERNALLY_BACKED or artifact.artifact_id is None:
            return
        uri = getattr(artifact, next(iter(artifact.LOCATOR_FIELDS)))
        if uri is None:
            return
        validate_persistable_uri(uri)
        key = (artifact.artifact_id, uri)
        if key not in self._locations:
            self._locations.add(key)
            self._add_table_row(
                TablePath.ARTIFACT_LOCATIONS,
                {"artifact_id": artifact.artifact_id, "uri": uri},
            )

    def _add_execution_edge(self, row: dict[str, Any]) -> None:
        key = (row["direction"], row["role"], row["artifact_id"])
        if key not in self._execution_edges:
            self._execution_edges.add(key)
            self._add_table_row(TablePath.EXECUTION_EDGES, row)

    def _add_table_row(self, table: TablePath, row: dict[str, Any]) -> None:
        self._add_row(f"{table.table_name}.parquet", row, get_schema(table))

    def _add_row(
        self, filename: str, row: dict[str, Any], schema: dict[str, Any]
    ) -> None:
        size = value_size(row)
        if self._buffer_bytes and self._buffer_bytes + size > self._max_bytes:
            self.close()
        self._schemas[filename] = schema
        self._rows.setdefault(filename, []).append(deepcopy(row))
        self._buffer_bytes += size
        if self._buffer_bytes >= self._max_bytes:
            self.close()

    def _temporary(self, filename: str) -> str:
        path = uri_join(self.staging_path, f"{filename}.tmp-{uuid.uuid4().hex}")
        self._temporaries.append(path)
        return path

    def close(self) -> None:
        """Flush pending rows, retaining every completed unpublished chunk."""
        for filename, rows in list(self._rows.items()):
            path = self._temporary(filename)
            write_parquet_rows(path, rows, self._schemas[filename], fs=self.fs)
            self._chunks.setdefault(filename, []).append(path)
            del self._rows[filename]
        self._buffer_bytes = 0

    def prepare(self) -> None:
        """Close payloads and compact each table into one unpublished file."""
        self.close()
        for filename, chunks in self._chunks.items():
            if len(chunks) == 1:
                self._candidates[filename] = chunks[0]
                continue
            destination = self._temporary(filename)
            write_parquet_rows(
                destination,
                self._chunk_rows(chunks),
                self._schemas[filename],
                fs=self.fs,
            )
            self._candidates[filename] = destination

    def _chunk_rows(self, chunks: list[str]) -> Iterator[dict[str, Any]]:
        for path in chunks:
            for frame in iter_parquet_frames(path, fs=self.fs):
                yield from frame.iter_rows(named=True)

    def promote(self) -> None:
        """Publish complete payloads, retaining source files until seal confirmation."""
        self.promotion_started = True
        for filename, source in self._candidates.items():
            publish_immutable_file(
                self.fs,
                uri_join(self.staging_path, filename),
                source,
                source_fs=self.fs,
            )

    def cleanup(self) -> None:
        """Best-effort cleanup of this session's temporary copies after success."""
        for path in self._temporaries:
            with suppress(OSError):
                self.fs.rm(path)
