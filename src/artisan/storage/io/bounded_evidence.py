"""Disk-backed table evidence with compact keys and exact canonical hashes."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, BinaryIO
from uuid import uuid4

import polars as pl
import pyarrow.parquet as pq
import xxhash

from artisan.errors import StoreIntegrityError
from artisan.storage.core.table_schemas import (
    get_natural_key,
    is_global_artifact_table,
)
from artisan.storage.io.bounded_io import iter_parquet_frames, parquet_schema
from artisan.utils.hashing import (
    STREAM_CHUNK_BYTES,
    canonical_json_bytes,
    compute_content_digest,
)


@dataclass(frozen=True, slots=True)
class _RowReference:
    """One representative's typed source location and canonical spool segment."""

    path: str
    ordinal: int
    row_group: int
    group_ordinal: int
    offset: int
    length: int
    full_digest: str
    effect_digest: str
    origin: Any


class DiskTable:
    """Index verified local Parquet without retaining decoded table payloads.

    The caller owns ``scratch_dir`` and the captured source files. Rows are
    emitted in source order, while canonical hashing sorts only their keys.
    """

    def __init__(
        self,
        table_path: str,
        paths: Sequence[str],
        scratch_dir: str,
        *,
        schema: dict[str, Any] | None = None,
        natural_key: tuple[str, ...] | None = None,
    ) -> None:
        self.table_path = table_path
        self.paths = tuple(paths)
        self.schema = dict(schema if schema is not None else parquet_schema(paths[0]))
        self.natural_key = natural_key or get_natural_key(table_path)
        self._comparable_columns = [
            name
            for name in self.schema
            if not (
                is_global_artifact_table(table_path) and name == "origin_step_number"
            )
        ]
        # Python datetime rows lose sub-microsecond precision.
        self._requires_typed_duplicates = any(
            isinstance(dtype, pl.Datetime) and dtype.time_unit == "ns"
            for dtype in self.schema.values()
        )
        self._rows: dict[tuple[Any, ...], _RowReference] = {}
        self._selected: dict[str, set[int]] = {path: set() for path in self.paths}
        self._spool_path = str(Path(scratch_dir) / f"canonical-{uuid4().hex}.json")
        self._index()
        key_schema = {name: self.schema[name] for name in self.natural_key}
        key_frame = pl.DataFrame(list(self._rows), schema=key_schema, orient="row")
        self.keys = tuple(key_frame.sort(list(self.natural_key)).iter_rows())

    @property
    def row_count(self) -> int:
        """Return the count after exact global-artifact deduplication."""
        return len(self._rows)

    def _index(self) -> None:
        if any(name not in self.schema for name in self.natural_key):
            msg = f"Missing natural key {self.natural_key!r} in staged table {self.table_path}"
            raise StoreIntegrityError(msg)
        with open(self._spool_path, "w+b") as spool:
            for path in self.paths:
                if parquet_schema(path) != self.schema:
                    msg = f"Conflicting captured schemas for table {self.table_path}"
                    raise StoreIntegrityError(msg)
                self._index_file(path, spool)

    def _index_file(self, path: str, spool: BinaryIO) -> None:
        metadata = pq.read_metadata(path)
        group = 0
        group_start = 0
        ordinal = 0
        for frame in iter_parquet_frames(path):
            for index, row in enumerate(frame.iter_rows(named=True)):
                while ordinal >= group_start + metadata.row_group(group).num_rows:
                    group_start += metadata.row_group(group).num_rows
                    group += 1
                self._index_row(
                    path,
                    ordinal,
                    group,
                    ordinal - group_start,
                    row,
                    spool,
                    frame,
                    index,
                )
                ordinal += 1

    def _index_row(
        self,
        path: str,
        ordinal: int,
        group: int,
        group_ordinal: int,
        row: dict[str, Any],
        spool: BinaryIO,
        frame: pl.DataFrame,
        row_index: int,
    ) -> None:
        key = tuple(row[name] for name in self.natural_key)
        encoded = _canonical_row(row, self._comparable_columns)
        full_digest = compute_content_digest(_canonical_row(row, list(self.schema)))
        if key in self._rows:
            self._validate_duplicate(
                key, row, encoded, full_digest, spool, frame.slice(row_index, 1)
            )
            return
        offset = spool.tell()
        spool.write(encoded)
        self._rows[key] = _RowReference(
            path,
            ordinal,
            group,
            group_ordinal,
            offset,
            len(encoded),
            full_digest,
            compute_content_digest(encoded),
            row.get("origin_step_number"),
        )
        self._selected[path].add(ordinal)

    def _validate_duplicate(
        self,
        key: tuple[Any, ...],
        row: dict[str, Any],
        encoded: bytes,
        full_digest: str,
        spool: BinaryIO,
        typed_row: pl.DataFrame,
    ) -> None:
        if not is_global_artifact_table(self.table_path):
            msg = f"Duplicate natural keys in staged table {self.table_path}"
            raise StoreIntegrityError(msg)
        reference = self._rows[key]
        if (
            not self._requires_typed_duplicates
            and reference.full_digest == full_digest
            and reference.origin == row.get("origin_step_number")
            and _segment_matches(spool, reference, encoded)
        ):
            return
        variants = (
            pl.concat([self._get_frame(key), typed_row])
            .select(pl.struct(list(self.schema)).n_unique())
            .item()
        )
        if variants != 1:
            msg = f"Conflicting natural keys in staged table {self.table_path}"
            raise StoreIntegrityError(msg)

    def iter_frames(
        self,
        columns: list[str] | None = None,
        *,
        keys: set[tuple[Any, ...]] | None = None,
    ) -> Iterator[pl.DataFrame]:
        """Yield bounded typed batches, keeping only first representative rows."""
        selected = self._selected if keys is None else self._selection(keys)
        for path in self.paths:
            wanted = selected.get(path, set())
            if not wanted:
                continue
            ordinal = 0
            for frame in iter_parquet_frames(path, columns=columns):
                keep = [i for i in range(frame.height) if ordinal + i in wanted]
                ordinal += frame.height
                if keep:
                    yield frame[keep]

    def _selection(self, keys: set[tuple[Any, ...]]) -> dict[str, set[int]]:
        selected: dict[str, set[int]] = {}
        for key in keys:
            reference = self._rows[key]
            selected.setdefault(reference.path, set()).add(reference.ordinal)
        return selected

    def get_row(self, key: tuple[Any, ...]) -> dict[str, Any]:
        """Read one representative from its original typed Parquet row group."""
        return self._get_frame(key).row(0, named=True)

    def _get_frame(self, key: tuple[Any, ...]) -> pl.DataFrame:
        reference = self._rows[key]
        ordinal = 0
        for frame in iter_parquet_frames(
            reference.path, row_groups=[reference.row_group]
        ):
            if ordinal + frame.height > reference.group_ordinal:
                return frame.slice(reference.group_ordinal - ordinal, 1)
            ordinal += frame.height
        msg = f"Captured row disappeared from {self.table_path}"
        raise StoreIntegrityError(msg)

    def row_digest(self, key: tuple[Any, ...], *, comparable: bool = False) -> str:
        """Return compact canonical evidence, omitting global origin when requested."""
        reference = self._rows[key]
        return reference.effect_digest if comparable else reference.full_digest

    def matches_row(
        self, key: tuple[Any, ...], row: dict[str, Any], *, comparable: bool = False
    ) -> bool:
        """Compare exactly, retaining Polars equality when JSON encodings differ."""
        reference = self._rows[key]
        encoded = _canonical_row(row, self._comparable_columns)
        digest_columns = self._comparable_columns if comparable else list(self.schema)
        digest = compute_content_digest(_canonical_row(row, digest_columns))
        same_origin = comparable or reference.origin == row.get("origin_step_number")
        if digest == self.row_digest(key, comparable=comparable) and same_origin:
            with open(self._spool_path, "rb") as spool:
                if _segment_matches(spool, reference, encoded):
                    return True
        schema = {name: self.schema[name] for name in digest_columns}
        return _typed_equal(self.get_row(key), row, schema)

    def rows_equal(
        self,
        key: tuple[Any, ...],
        other: DiskTable,
        other_key: tuple[Any, ...] | None = None,
        *,
        comparable: bool = False,
    ) -> bool:
        """Compare two representatives without collecting either table."""
        return self.matches_row(
            key, other.get_row(other_key or key), comparable=comparable
        )

    def canonical_key(
        self, logical_commit_id: str, table_path: str | None = None
    ) -> str:
        """Hash exactly the legacy canonical table wrapper and sorted row bytes."""
        hasher = xxhash.xxh3_128()
        hasher.update(
            b'{"logical_commit_id":' + canonical_json_bytes(logical_commit_id)
        )
        hasher.update(b',"rows":[')
        with open(self._spool_path, "rb") as spool:
            for index, key in enumerate(self.keys):
                if index:
                    hasher.update(b",")
                reference = self._rows[key]
                spool.seek(reference.offset)
                remaining = reference.length
                while remaining:
                    chunk = spool.read(min(remaining, STREAM_CHUNK_BYTES))
                    if not chunk:
                        msg = "Truncated canonical evidence spool"
                        raise StoreIntegrityError(msg)
                    hasher.update(chunk)
                    remaining -= len(chunk)
        hasher.update(
            b'],"table_path":' + canonical_json_bytes(table_path or self.table_path)
        )
        hasher.update(b"}")
        return hasher.hexdigest()


def _canonical_row(row: dict[str, Any], columns: list[str]) -> bytes:
    return canonical_json_bytes({name: _json_value(row[name]) for name in columns})


def _json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"$binary": value.hex()}
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return value


def _segment_matches(
    stream: BinaryIO, reference: _RowReference, encoded: bytes
) -> bool:
    if reference.length != len(encoded):
        return False
    previous = stream.tell()
    try:
        stream.seek(reference.offset)
        for start in range(0, len(encoded), STREAM_CHUNK_BYTES):
            expected = encoded[start : start + STREAM_CHUNK_BYTES]
            if stream.read(len(expected)) != expected:
                return False
        return True
    finally:
        stream.seek(previous)


def _typed_equal(
    left: dict[str, Any], right: dict[str, Any], schema: dict[str, Any]
) -> bool:
    rows = [{name: row[name] for name in schema} for row in (left, right)]
    frame = pl.DataFrame(rows, schema=schema)
    return bool(frame.select(pl.struct(list(schema)).n_unique()).item() == 1)
