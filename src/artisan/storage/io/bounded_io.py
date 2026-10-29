"""Sequential Parquet I/O with bounded payload buffers and private snapshots."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any, cast

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.external import sanitized_uri
from artisan.utils.hashing import STREAM_CHUNK_BYTES, compute_stream_digest

BATCH_BYTES = 16 * 1024 * 1024
BATCH_ROWS = 4096
FILE_BYTES = 128 * 1024 * 1024


@dataclass(frozen=True)
class CapturedParquet:
    """Verified private file; its owner controls the scratch-directory lifetime."""

    path: str
    digest: str
    size_bytes: int
    row_count: int
    schema: dict[str, Any]


def capture_parquet(
    path: str, fs: AbstractFileSystem, destination: str
) -> CapturedParquet:
    """Copy mutable staging into private local storage and inspect its footer."""
    try:
        with (
            fs.open(path, "rb", cache_type="none") as source,
            open(destination, "xb") as target,
        ):
            digest, size = compute_stream_digest(source, destination=target)
        with pq.ParquetFile(destination, pre_buffer=False) as parquet:
            schema = _polars_schema(parquet.schema_arrow)
            count = parquet.metadata.num_rows
        return CapturedParquet(destination, digest, size, count, schema)
    except (OSError, ValueError, pa.ArrowException) as exc:
        msg = f"Unreadable staged Parquet or failed scratch capture for {sanitized_uri(path)}"
        raise StoreIntegrityError(msg) from exc


def parquet_schema(
    path: str, *, fs: AbstractFileSystem | None = None
) -> dict[str, Any]:
    """Read logical column types without decoding any payload rows."""
    try:
        filesystem = fs or LocalFileSystem()
        with (
            filesystem.open(path, "rb", cache_type="none") as stream,
            pq.ParquetFile(stream, pre_buffer=False) as parquet,
        ):
            return _polars_schema(parquet.schema_arrow)
    except (OSError, pa.ArrowException) as exc:
        msg = f"Unreadable Parquet object {sanitized_uri(path)}"
        raise StoreIntegrityError(msg) from exc


def _polars_schema(schema: pa.Schema) -> dict[str, Any]:
    return dict(
        cast(
            pl.DataFrame, pl.from_arrow(pa.Table.from_batches([], schema=schema))
        ).schema
    )


def iter_parquet_frames(
    path: str,
    *,
    fs: AbstractFileSystem | None = None,
    columns: list[str] | None = None,
    row_groups: list[int] | None = None,
    max_bytes: int = BATCH_BYTES,
) -> Iterator[pl.DataFrame]:
    """Read one row group at a time, disabling payload prefetch and threads.

    Unknown or oversized encodings use one-row batches. A returned batch bound
    cannot eliminate the decoder's largest page or dictionary working set.
    """
    try:
        filesystem = fs or LocalFileSystem()
        with (
            filesystem.open(path, "rb", cache_type="none") as stream,
            pq.ParquetFile(
                stream,
                buffer_size=STREAM_CHUNK_BYTES,
                pre_buffer=False,
                memory_map=False,
            ) as parquet,
        ):
            for index in (
                range(parquet.num_row_groups) if row_groups is None else row_groups
            ):
                size = _batch_size(parquet, index, columns, max_bytes)
                for batch in parquet.iter_batches(
                    batch_size=size,
                    row_groups=[index],
                    columns=columns,
                    use_threads=False,
                ):
                    yield cast(pl.DataFrame, pl.from_arrow(batch, rechunk=False))
    except (OSError, pa.ArrowException) as exc:
        msg = f"Unreadable Parquet object {sanitized_uri(path)}"
        raise StoreIntegrityError(msg) from exc


def _batch_size(
    parquet: pq.ParquetFile,
    index: int,
    columns: list[str] | None,
    max_bytes: int,
) -> int:
    """Prove a flat, plain-encoded group fits before decoding several rows."""
    group = parquet.metadata.row_group(index)
    selected = set(columns) if columns is not None else set(parquet.schema_arrow.names)
    size = 0
    for field in parquet.schema_arrow:
        if field.name in selected and pa.types.is_nested(field.type):
            return 1
    for number in range(group.num_columns):
        column = group.column(number)
        if column.path_in_schema not in selected:
            continue
        if set(column.encodings) - {"PLAIN", "RLE", "BIT_PACKED"}:
            return 1
        size += column.total_uncompressed_size + 16 * group.num_rows
    return BATCH_ROWS if size <= max_bytes else 1


def value_size(value: Any) -> int:
    """Count variable payload bytes conservatively before batching rows."""
    if isinstance(value, str):
        return len(value.encode("utf-8")) + 8
    if isinstance(value, (bytes, bytearray, memoryview)):
        return len(value) + 8
    if isinstance(value, dict):
        return sum(value_size(key) + value_size(item) for key, item in value.items())
    if isinstance(value, (tuple, list)):
        return sum(value_size(item) for item in value) + 8 * len(value)
    return 16


def row_batches(
    rows: Iterable[dict[str, Any]],
    *,
    max_bytes: int = BATCH_BYTES,
    max_rows: int = BATCH_ROWS,
) -> Iterator[list[dict[str, Any]]]:
    """Group rows by bytes; process an oversized individual row alone."""
    if max_bytes <= 0 or max_rows <= 0:
        msg = "Parquet batch limits must be positive"
        raise ValueError(msg)
    batch: list[dict[str, Any]] = []
    size = 0
    for row in rows:
        row_size = value_size(row)
        if batch and (size + row_size > max_bytes or len(batch) >= max_rows):
            yield batch
            batch = []
            size = 0
        batch.append(row)
        size += row_size
        if size >= max_bytes or len(batch) >= max_rows:
            yield batch
            batch = []
            size = 0
    if batch:
        yield batch


def write_parquet_rows(
    path: str,
    rows: Iterable[dict[str, Any]],
    schema: dict[str, Any],
    *,
    fs: AbstractFileSystem | None = None,
    max_bytes: int = BATCH_BYTES,
    metadata: dict[bytes, bytes] | None = None,
) -> int:
    """Write one registered payload file with byte-bounded plain row groups."""
    filesystem = fs or LocalFileSystem()
    arrow_schema = pl.DataFrame(schema=schema).to_arrow().schema
    if metadata:
        arrow_schema = arrow_schema.with_metadata(metadata)
    count = 0
    with (
        filesystem.open(path, "wb", block_size=5 * 1024 * 1024) as stream,
        pq.ParquetWriter(
            stream,
            arrow_schema,
            compression="zstd",
            use_dictionary=False,
            data_page_size=STREAM_CHUNK_BYTES,
            write_batch_size=1,
        ) as writer,
    ):
        for rows_batch in row_batches(rows, max_bytes=max_bytes):
            table = pl.DataFrame(rows_batch, schema=schema).to_arrow()
            writer.write_table(table, row_group_size=table.num_rows)
            count += table.num_rows
    return count
