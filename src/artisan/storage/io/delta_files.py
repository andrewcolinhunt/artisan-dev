"""Sequential active-file reads and atomic Delta transactions for bounded effects."""

from __future__ import annotations

import json
import math
import os
import posixpath
import tempfile
import time
import uuid
from bisect import bisect_left
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, cast
from urllib.parse import unquote

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable
from deltalake.transaction import AddAction, CommitProperties
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.storage.io.bounded_io import (
    BATCH_BYTES,
    BATCH_ROWS,
    FILE_BYTES,
    _batch_size,
    row_batches,
)
from artisan.storage.io.publication import publish_immutable_file
from artisan.utils.hashing import STREAM_CHUNK_BYTES
from artisan.utils.path import uri_join


@dataclass(frozen=True)
class _ActiveFile:
    path: str
    partitions: dict[str, Any]


def _delta_type(dtype: pa.DataType) -> pa.DataType:
    if pa.types.is_large_string(dtype) or pa.types.is_string_view(dtype):
        return pa.string()
    if pa.types.is_large_binary(dtype) or pa.types.is_binary_view(dtype):
        return pa.binary()
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        field = dtype.value_field
        return pa.list_(
            pa.field("element", _delta_type(field.type), field.nullable, field.metadata)
        )
    if pa.types.is_struct(dtype):
        return pa.struct(
            [
                pa.field(f.name, _delta_type(f.type), f.nullable, f.metadata)
                for f in dtype
            ]
        )
    return dtype


def _arrow_schema(schema: dict[str, Any]) -> pa.Schema:
    original = pl.DataFrame(schema=schema).to_arrow().schema
    return _normalize_schema(original)


def _normalize_schema(schema: pa.Schema) -> pa.Schema:
    """Normalize Arrow string/list representations without erasing constraints."""
    return pa.schema(
        [pa.field(f.name, _delta_type(f.type), f.nullable, f.metadata) for f in schema],
        metadata=schema.metadata,
    )


def _relative_path(path: str) -> str:
    """Decode Delta's URI path while rejecting traversal and ambiguous separators."""
    decoded = unquote(path)
    if (
        not decoded
        or "\\" in decoded
        or "\x00" in decoded
        or posixpath.isabs(decoded)
        or any(part in {"", ".", ".."} for part in decoded.split("/"))
        or "://" in decoded
    ):
        msg = "Delta active file escapes the registered table"
        raise StoreIntegrityError(msg)
    return decoded


class DeltaSnapshot:
    """A fixed Delta version whose active files are decoded sequentially."""

    def __init__(
        self,
        path: str,
        schema: dict[str, Any],
        *,
        fs: AbstractFileSystem,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        self.path = path
        self.schema = dict(schema)
        self.fs = fs
        self.delta = DeltaTable(path, storage_options=storage_options or {})
        self.arrow_schema = pa.schema(self.delta.schema().to_arrow())
        self.partitions = self.delta.metadata().partition_columns
        self._validate_contract()
        actions = cast(
            pl.DataFrame, pl.from_arrow(self.delta.get_add_actions(flatten=True))
        )
        self.files = tuple(
            _ActiveFile(
                row["path"],
                {name: row[f"partition.{name}"] for name in self.partitions},
            )
            for row in actions.select(
                "path", *(f"partition.{name}" for name in self.partitions)
            ).iter_rows(named=True)
        )
        for item in self.files:
            relative = _relative_path(item.path)
            if isinstance(fs, LocalFileSystem) and os.path.commonpath(
                [
                    os.path.realpath(fs._strip_protocol(path)),
                    os.path.realpath(fs._strip_protocol(uri_join(path, relative))),
                ]
            ) != os.path.realpath(fs._strip_protocol(path)):
                msg = "Delta active file escapes the registered table"
                raise StoreIntegrityError(msg)

    def _validate_contract(self) -> None:
        expected = _arrow_schema(self.schema)
        if not self.arrow_schema.equals(expected, check_metadata=True):
            msg = f"Delta table {posixpath.basename(self.path)!r} has an unexpected schema"
            raise StoreIntegrityError(msg)
        if not set(self.schema) - set(self.partitions):
            msg = "Delta table must contain at least one non-partition column"
            raise StoreIntegrityError(msg)
        if self.partitions not in ([], ["origin_step_number"]):
            msg = "Bounded Delta I/O requires the registered origin partition contract"
            raise StoreIntegrityError(msg)
        protocol = self.delta.protocol()
        features = set(protocol.reader_features or ()) | set(
            protocol.writer_features or ()
        )
        ordinary = (
            protocol.min_reader_version == 1
            and protocol.min_writer_version == 2
            and not features
        )
        timestamp_ntz = (
            protocol.min_reader_version == 3
            and protocol.min_writer_version == 7
            and set(protocol.reader_features or ()) == {"timestampNtz"}
            and set(protocol.writer_features or ()) == {"timestampNtz"}
        )
        if not (ordinary or timestamp_ntz) or self.delta.metadata().configuration:
            msg = "Unsupported Delta protocol, features, or table configuration"
            raise StoreIntegrityError(msg)

    def iter_located(
        self,
        columns: list[str] | None = None,
        *,
        rows_by_file: dict[str, set[int]] | None = None,
    ) -> Iterator[tuple[str, list[int], pl.DataFrame]]:
        """Yield batches with file ordinals, projecting before payload decoding."""
        selected = list(self.schema) if columns is None else columns
        if (
            not selected
            or len(set(selected)) != len(selected)
            or set(selected) - set(self.schema)
        ):
            msg = "Delta projection must contain distinct registered columns"
            raise ValueError(msg)
        if rows_by_file is not None and set(rows_by_file) - {
            item.path for item in self.files
        }:
            msg = "Delta row selection refers to a file outside this snapshot"
            raise StoreIntegrityError(msg)
        physical = [name for name in selected if name not in self.partitions]
        for item in self.files:
            wanted = (
                None if rows_by_file is None else rows_by_file.get(item.path, set())
            )
            if wanted is not None and not wanted:
                continue
            try:
                yield from self._file_frames(item, selected, physical, wanted)
            except (OSError, pa.ArrowException, pl.exceptions.PolarsError) as exc:
                msg = f"Unreadable Delta data file {posixpath.basename(item.path)!r}"
                raise StoreIntegrityError(msg) from exc

    def _file_frames(
        self,
        item: _ActiveFile,
        selected: list[str],
        physical: list[str],
        wanted: set[int] | None,
    ) -> Iterator[tuple[str, list[int], pl.DataFrame]]:
        path = uri_join(self.path, _relative_path(item.path))
        expected = pa.schema(
            [f for f in self.arrow_schema if f.name not in self.partitions]
        )
        with (
            self.fs.open(path, "rb", cache_type="none") as stream,
            pq.ParquetFile(
                stream,
                buffer_size=STREAM_CHUNK_BYTES,
                pre_buffer=False,
                memory_map=False,
            ) as parquet,
        ):
            if not _normalize_schema(parquet.schema_arrow).equals(
                expected, check_metadata=True
            ):
                msg = "Delta data file has an unexpected physical schema"
                raise StoreIntegrityError(msg)
            if wanted is not None and any(
                type(i) is not int or i < 0 or i >= parquet.metadata.num_rows
                for i in wanted
            ):
                msg = "Delta row selection contains an invalid file ordinal"
                raise StoreIntegrityError(msg)
            ordered = sorted(wanted) if wanted is not None else None
            start = 0
            for group in range(parquet.num_row_groups):
                end = start + parquet.metadata.row_group(group).num_rows
                if ordered is None or bisect_left(ordered, start) != bisect_left(
                    ordered, end
                ):
                    yield from self._group_frames(
                        parquet, group, start, end, item, selected, physical, wanted
                    )
                start = end

    def _group_frames(
        self,
        parquet: pq.ParquetFile,
        group: int,
        start: int,
        end: int,
        item: _ActiveFile,
        selected: list[str],
        physical: list[str],
        wanted: set[int] | None,
    ) -> Iterator[tuple[str, list[int], pl.DataFrame]]:
        if physical:
            frames = (
                cast(pl.DataFrame, pl.from_arrow(batch, rechunk=False))
                for batch in parquet.iter_batches(
                    batch_size=_batch_size(parquet, group, physical, BATCH_BYTES),
                    row_groups=[group],
                    columns=physical,
                    use_threads=False,
                )
            )
        else:
            frames = (
                pl.DataFrame(
                    {
                        name: pl.Series(
                            name,
                            [item.partitions[name]] * min(BATCH_ROWS, end - offset),
                            dtype=self.schema[name],
                        )
                        for name in selected
                    }
                )
                for offset in range(start, end, BATCH_ROWS)
            )
        ordinal = start
        for batch in frames:
            positions = list(range(ordinal, ordinal + batch.height))
            ordinal += batch.height
            frame = batch
            if wanted is not None:
                keep = [i for i, position in enumerate(positions) if position in wanted]
                positions = [positions[i] for i in keep]
                frame = frame[keep]
            if not positions:
                continue
            frame = frame.with_columns(
                pl.lit(item.partitions[name]).cast(self.schema[name]).alias(name)
                for name in self.partitions
                if name in selected
            ).select(selected)
            yield item.path, positions, frame

    def iter_frames(
        self,
        columns: list[str] | None = None,
        *,
        rows_by_file: dict[str, set[int]] | None = None,
    ) -> Iterator[pl.DataFrame]:
        """Yield bounded frames without retaining earlier files or batches."""
        for _, _, frame in self.iter_located(columns, rows_by_file=rows_by_file):
            yield frame


class _Statistics:
    """Scalar retry-key statistics without inspecting binary payload columns."""

    def __init__(self, columns: list[str]) -> None:
        self.count = 0
        self.minimum: dict[str, Any] = {}
        self.maximum: dict[str, Any] = {}
        self.nulls = dict.fromkeys(columns, 0)
        self.unsupported: set[str] = set()

    def add(self, frame: pl.DataFrame) -> None:
        self.count += frame.height
        for name in self.nulls:
            self.nulls[name] += frame[name].null_count()
            for value in frame[name]:
                if value is None or name in self.unsupported:
                    continue
                if not isinstance(value, (str, int, float, bool, date, datetime)) or (
                    isinstance(value, float) and not math.isfinite(value)
                ):
                    self.unsupported.add(name)
                    self.minimum.pop(name, None)
                    self.maximum.pop(name, None)
                    continue
                self.minimum[name] = min(value, self.minimum.get(name, value))
                self.maximum[name] = max(value, self.maximum.get(name, value))

    def encode(self) -> str:
        return json.dumps(
            {
                "numRecords": self.count,
                "minValues": self.minimum,
                "maxValues": self.maximum,
                "nullCount": self.nulls,
            },
            default=_statistics_value,
            allow_nan=False,
        )


def _statistics_value(value: date | datetime) -> str:
    """Match Delta's UTC timestamp statistics, including timestamp-without-zone."""
    if isinstance(value, datetime):
        utc = (
            value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        )
        return utc.isoformat(timespec="microseconds").replace("+00:00", "Z")
    return value.isoformat()


def append_table_files(
    snapshot: DeltaSnapshot,
    frames: Iterable[pl.DataFrame],
    *,
    step_number: int,
    statistics_columns: list[str],
    target_bytes: int = FILE_BYTES,
) -> int:
    """Write bounded files then register the complete effect in one transaction."""
    if target_bytes <= 0:
        msg = "Delta file target must be positive"
        raise ValueError(msg)
    if set(statistics_columns) - set(snapshot.schema):
        msg = "Delta statistics must reference registered columns"
        raise ValueError(msg)
    actions: list[AddAction] = []
    partitions = {name: str(step_number) for name in snapshot.partitions}
    schema = pa.schema(
        [field for field in snapshot.arrow_schema if field.name not in partitions]
    )
    with tempfile.TemporaryDirectory(prefix="artisan-delta-") as scratch:
        writer: pq.ParquetWriter | None = None
        size = 0
        local = ""
        relative = ""
        statistics = _Statistics([])
        try:
            for frame in frames:
                if dict(frame.schema) != snapshot.schema:
                    msg = "Delta effect has an unexpected schema"
                    raise StoreIntegrityError(msg)
                for batch in row_batches(
                    frame.iter_rows(named=True),
                    max_bytes=min(BATCH_BYTES, target_bytes),
                ):
                    block = pl.DataFrame(batch, schema=snapshot.schema)
                    if partitions and set(block["origin_step_number"]) != {step_number}:
                        msg = "Delta effect contains an unexpected origin partition"
                        raise StoreIntegrityError(msg)
                    if writer is None:
                        relative = uri_join(
                            *(f"{name}={value}" for name, value in partitions.items()),
                            f"part-{uuid.uuid4().hex}.parquet",
                        )
                        local = os.path.join(scratch, posixpath.basename(relative))
                        writer = pq.ParquetWriter(
                            local,
                            schema,
                            compression="zstd",
                            use_dictionary=False,
                            data_page_size=STREAM_CHUNK_BYTES,
                            write_batch_size=1,
                        )
                        statistics = _Statistics(
                            [
                                name
                                for name in statistics_columns
                                if name not in partitions
                            ]
                        )
                        size = 0
                    table = (
                        block.select(schema.names).to_arrow().cast(schema, safe=True)
                    )
                    writer.write_table(table, row_group_size=table.num_rows)
                    statistics.add(block)
                    size += table.nbytes
                    if size >= target_bytes:
                        writer.close()
                        writer = None
                        actions.append(
                            _publish_file(
                                snapshot,
                                local,
                                relative,
                                partitions,
                                schema,
                                statistics,
                            )
                        )
            if writer is not None:
                writer.close()
                writer = None
                actions.append(
                    _publish_file(
                        snapshot, local, relative, partitions, schema, statistics
                    )
                )
        finally:
            if writer is not None:
                writer.close()
        if actions:
            snapshot.delta.create_write_transaction(
                actions,
                mode="append",
                schema=snapshot.delta.schema(),
                partition_by=snapshot.partitions,
                commit_properties=CommitProperties(max_commit_retries=0),
            )
    return sum(json.loads(action.stats)["numRecords"] for action in actions)


def _publish_file(
    snapshot: DeltaSnapshot,
    local: str,
    relative: str,
    partitions: dict[str, str],
    schema: pa.Schema,
    statistics: _Statistics,
) -> AddAction:
    with pq.ParquetFile(local) as parquet:
        if (
            not parquet.schema_arrow.equals(schema, check_metadata=True)
            or parquet.metadata.num_rows != statistics.count
        ):
            msg = "Completed Delta data file failed schema or row-count verification"
            raise StoreIntegrityError(msg)
    size = os.path.getsize(local)
    publish_immutable_file(
        snapshot.fs,
        uri_join(snapshot.path, relative),
        local,
        source_fs=LocalFileSystem(),
    )
    # The destination is confirmed immutable; this writer owns the scratch copy.
    os.unlink(local)
    return AddAction(
        path=relative,
        size=size,
        partition_values=partitions,
        modification_time=int(time.time() * 1000),
        data_change=True,
        stats=statistics.encode(),
    )
