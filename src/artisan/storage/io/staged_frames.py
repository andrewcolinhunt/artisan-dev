"""Bounded reads of immutable staged files and payload-free row evidence."""

from __future__ import annotations

import io
from collections import OrderedDict
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

import polars as pl
import xxhash
from fsspec import AbstractFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.external import sanitized_uri
from artisan.storage.core.table_schemas import get_natural_key, is_global_artifact_table
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest

COMMIT_BATCH_ROWS = 250
COMMIT_BATCH_BYTES = 32 * 1024 * 1024


def json_value(value: Any) -> Any:
    """Normalize scalar storage values for the existing plan digest format."""
    if isinstance(value, bytes):
        return {"$binary": value.hex()}
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return value


def table_rows_digest(
    logical_commit_id: str, table_path: str, rows: Iterable[dict[str, Any]]
) -> str:
    """Hash the existing canonical JSON preimage without collecting its rows."""
    digest = xxhash.xxh3_128()
    digest.update(b'{"logical_commit_id":' + canonical_json_bytes(logical_commit_id))
    digest.update(b',"rows":[')
    separator = b""
    for row in rows:
        digest.update(separator)
        digest.update(canonical_json_bytes(row))
        separator = b","
    digest.update(b'],"table_path":' + canonical_json_bytes(table_path) + b"}")
    return digest.hexdigest()


def effect_row_digest(table_path: str, row: dict[str, Any]) -> str:
    """Hash every comparable row value, excluding reusable artifact origins."""
    return _row_digest(
        {
            name: value
            for name, value in row.items()
            if name != "origin_step_number" or not is_global_artifact_table(table_path)
        }
    )


def _row_digest(row: dict[str, Any]) -> str:
    return compute_content_digest(
        canonical_json_bytes({name: json_value(value) for name, value in row.items()})
    )


def row_digests_plan_key(
    logical_commit_id: str, table_path: str, row_digests: Iterable[str]
) -> str:
    """Hash canonical full-row digests without retaining their payloads."""
    digest = xxhash.xxh3_128()
    digest.update(b'{"logical_commit_id":' + canonical_json_bytes(logical_commit_id))
    digest.update(b',"row_digests":[')
    separator = b""
    for row_digest in row_digests:
        digest.update(separator)
        digest.update(canonical_json_bytes(row_digest))
        separator = b","
    digest.update(b'],"table_path":' + canonical_json_bytes(table_path) + b"}")
    return digest.hexdigest()


@dataclass(frozen=True)
class StagedFile:
    """An original Parquet object whose exact bytes must remain unchanged."""

    fs: AbstractFileSystem
    path: str
    size_bytes: int
    digest: str

    def read(self) -> pl.DataFrame:
        """Verify and decode one original staged object."""
        try:
            with self.fs.open(self.path, "rb") as stream:
                data = stream.read()
        except OSError as exc:
            msg = f"Unreadable staged Parquet object {sanitized_uri(self.path)!r}"
            raise StoreIntegrityError(msg) from exc
        if len(data) != self.size_bytes or compute_content_digest(data) != self.digest:
            msg = f"Staged object changed after planning: {sanitized_uri(self.path)!r}"
            raise StoreIntegrityError(msg)
        try:
            return pl.read_parquet(io.BytesIO(data))
        except pl.exceptions.PolarsError as exc:
            msg = f"Unreadable staged Parquet object {sanitized_uri(self.path)!r}"
            raise StoreIntegrityError(msg) from exc


class TableRowDigests:
    """Retain natural keys and full-row hashes without keeping row payloads."""

    def __init__(self, table_path: str, schema: pl.Schema) -> None:
        self.table_path = table_path
        self.schema = schema
        self.natural_key = get_natural_key(table_path)
        self._rows: dict[tuple[Any, ...], tuple[str, str]] = {}

    @property
    def height(self) -> int:
        """Return the number of distinct planned rows."""
        return len(self._rows)

    def keys(self) -> pl.DataFrame:
        """Return only natural keys, in canonical order."""
        return pl.DataFrame(
            list(self._rows),
            schema={name: self.schema[name] for name in self.natural_key},
            orient="row",
        ).sort(list(self.natural_key))

    def update(self, frame: pl.DataFrame) -> None:
        """Hash a frame, rejecting conflicting or disallowed duplicate keys."""
        self._validate_schema(frame)
        for row in frame.iter_rows(named=True, buffer_size=1):
            self._add_row(row)

    def _validate_schema(self, frame: pl.DataFrame) -> None:
        if frame.schema != self.schema:
            msg = f"Inconsistent staged schema for {self.table_path}"
            raise StoreIntegrityError(msg)

    def _add_row(self, row: dict[str, Any]) -> tuple[Any, ...] | None:
        key = tuple(row[name] for name in self.natural_key)
        full_digest = _row_digest(row)
        previous = self._rows.get(key)
        if previous is not None:
            if not is_global_artifact_table(self.table_path):
                msg = f"Duplicate natural keys in staged table {self.table_path}"
                raise StoreIntegrityError(msg)
            if previous[0] != full_digest:
                msg = f"Conflicting natural keys in staged table {self.table_path}"
                raise StoreIntegrityError(msg)
            return None
        self._rows[key] = (full_digest, effect_row_digest(self.table_path, row))
        return key

    def row_digest(self, key: tuple[Any, ...]) -> str:
        """Return the digest of one row's comparable effect."""
        return self._rows[key][1]

    def plan_key(self, logical_commit_id: str) -> str:
        """Hash comparable row digests in canonical natural-key order."""
        return row_digests_plan_key(
            logical_commit_id,
            self.table_path,
            (self.row_digest(key) for key in self.keys().iter_rows()),
        )


@dataclass(frozen=True)
class _SourceRows:
    file: StagedFile
    row_count: int
    estimated_size_bytes: int
    rows: tuple[tuple[int, tuple[Any, ...], int], ...]


class StagedTable(TableRowDigests):
    """Read bounded batches directly from verified original Parquet objects."""

    def __init__(self, table_path: str, schema: pl.Schema) -> None:
        super().__init__(table_path, schema)
        self._sources: list[_SourceRows] = []
        self._largest_row = 1

    @property
    def row_group_size(self) -> int:
        """Keep the Parquet writer's row groups within the payload budget too."""
        return max(1, min(COMMIT_BATCH_ROWS, COMMIT_BATCH_BYTES // self._largest_row))

    def add(self, frame: pl.DataFrame, source: StagedFile) -> None:
        """Index one staged shard without retaining its frame or copying payloads."""
        self._validate_schema(frame)
        rows = []
        for index, row in enumerate(frame.iter_rows(named=True, buffer_size=1)):
            key = self._add_row(row)
            if key is not None:
                size = int(frame.slice(index, 1).estimated_size())
                self._largest_row = max(self._largest_row, size)
                rows.append((index, key, size))
        self._sources.append(
            _SourceRows(source, frame.height, int(frame.estimated_size()), tuple(rows))
        )

    def batches(
        self, selected: set[tuple[Any, ...]] | None = None
    ) -> Iterator[pl.DataFrame]:
        """Coalesce small sources while bounding their full backing allocations."""
        for chunk in self._source_chunks(selected):
            if len(chunk) == 1:
                source, rows = chunk[0]
                frame = self._read_source(source)
                yield from self._source_batches(frame, rows)
                del frame
                continue
            frames = []
            for source, rows in chunk:
                frame = self._read_source(source)
                frames.append(frame[[index for index, _ in rows]])
                del frame
            yield pl.concat(frames)
            del frames

    def _source_chunks(
        self, selected: set[tuple[Any, ...]] | None
    ) -> Iterator[list[tuple[_SourceRows, list[tuple[int, int]]]]]:
        chunk: list[tuple[_SourceRows, list[tuple[int, int]]]] = []
        size = count = 0
        for source in self._sources:
            rows = [
                (index, row_size)
                for index, key, row_size in source.rows
                if selected is None or key in selected
            ]
            if not rows:
                continue
            if chunk and (
                size + source.estimated_size_bytes > COMMIT_BATCH_BYTES
                or count + len(rows) > COMMIT_BATCH_ROWS
            ):
                yield chunk
                chunk, size, count = [], 0, 0
            chunk.append((source, rows))
            size += source.estimated_size_bytes
            count += len(rows)
        if chunk:
            yield chunk

    def _read_source(self, source: _SourceRows) -> pl.DataFrame:
        frame = source.file.read()
        self._validate_schema(frame)
        if frame.height != source.row_count:
            msg = f"Staged row count changed for {self.table_path}"
            raise StoreIntegrityError(msg)
        return frame

    @staticmethod
    def _source_batches(
        frame: pl.DataFrame, rows: list[tuple[int, int]]
    ) -> Iterator[pl.DataFrame]:
        indices: list[int] = []
        size = 0
        for index, row_size in rows:
            if indices and (
                size + row_size > COMMIT_BATCH_BYTES
                or len(indices) >= COMMIT_BATCH_ROWS
            ):
                yield frame[indices]
                indices, size = [], 0
            indices.append(index)
            size += row_size
        if indices:
            yield frame[indices]

    def plan_key(self, logical_commit_id: str, *, digest_version: int = 2) -> str:
        """Hash metadata for new plans or canonical payloads for existing plans."""
        if digest_version == 2:
            return super().plan_key(logical_commit_id)
        if digest_version == 1:
            return table_rows_digest(
                logical_commit_id, self.table_path, self._legacy_rows()
            )
        msg = f"Unsupported table digest version: {digest_version}"
        raise ValueError(msg)

    def _legacy_rows(self) -> Iterator[dict[str, Any]]:
        """Revisit original shards in key order with a bounded source cache."""
        locations = {
            key: (source_index, row_index)
            for source_index, source in enumerate(self._sources)
            for row_index, key, _ in source.rows
        }
        cache: OrderedDict[int, pl.DataFrame] = OrderedDict()
        size = 0
        for key in self.keys().iter_rows():
            source_index, row_index = locations[key]
            source = self._sources[source_index]
            if source_index not in cache:
                while cache and size + source.estimated_size_bytes > COMMIT_BATCH_BYTES:
                    removed = next(iter(cache))
                    del cache[removed]
                    size -= self._sources[removed].estimated_size_bytes
                cache[source_index] = self._read_source(source)
                size += source.estimated_size_bytes
            cache.move_to_end(source_index)
            row = cache[source_index].row(row_index, named=True)
            if is_global_artifact_table(self.table_path):
                row.pop("origin_step_number", None)
            yield {name: json_value(value) for name, value in row.items()}
            del row

    def collect(self) -> pl.DataFrame:
        """Materialize a table for callers explicitly needing a DataFrame."""
        return (
            pl.concat(self.batches())
            if self.height
            else pl.DataFrame(schema=self.schema)
        )


class StagedFrames(Mapping[str, pl.DataFrame]):
    """Expose staged sources to readers and a bounded path to the committer."""

    def __init__(self) -> None:
        self.tables: dict[str, StagedTable] = {}

    def add(self, table_path: str, frame: pl.DataFrame, source: StagedFile) -> None:
        """Index one captured shard without retaining its DataFrame."""
        if table_path not in self.tables:
            self.tables[table_path] = StagedTable(table_path, frame.schema)
        self.tables[table_path].add(frame, source)

    def __getitem__(self, table_path: str) -> pl.DataFrame:
        return self.tables[table_path].collect()

    def __iter__(self) -> Iterator[str]:
        return iter(self.tables)

    def __len__(self) -> int:
        return len(self.tables)
