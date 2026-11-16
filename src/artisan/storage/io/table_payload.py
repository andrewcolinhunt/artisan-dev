"""Immutable typed Parquet payloads shared by admission and retained results."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.execution.retained_result import RetainedTableFile
from artisan.storage.io.bounded_io import (
    CapturedParquet,
    capture_parquet,
    write_parquet_rows,
)
from artisan.storage.io.publication import publish_immutable_file
from artisan.utils.hashing import compute_stream_digest
from artisan.utils.path import uri_join


def stage_table_payload(
    table_path: str,
    filename: str,
    rows: Iterator[dict[str, Any]],
    schema: dict[str, Any],
    directory: str,
    scratch: Path,
    fs: AbstractFileSystem,
) -> RetainedTableFile:
    """Publish one exact registered table payload with bounded row writing."""
    path = scratch / filename
    count = write_parquet_rows(str(path), rows, schema)
    with path.open("rb") as stream:
        digest, size = compute_stream_digest(stream)
    publish_immutable_file(
        fs, uri_join(directory, filename), str(path), source_fs=LocalFileSystem()
    )
    return RetainedTableFile(
        table_path=table_path,
        filename=filename,
        digest=digest,
        size_bytes=size,
        row_count=count,
    )


def capture_table_payload(
    item: RetainedTableFile,
    *,
    directory: str,
    scratch: Path,
    expected_schema: dict[str, Any],
    fs: AbstractFileSystem,
) -> CapturedParquet:
    """Capture exact registered table bytes before inspecting their content."""
    captured = capture_parquet(
        uri_join(directory, item.filename), fs, str(scratch / item.filename)
    )
    if (captured.digest, captured.size_bytes, captured.row_count, captured.schema) != (
        item.digest,
        item.size_bytes,
        item.row_count,
        expected_schema,
    ):
        msg = "Retained Parquet payload changed or has the wrong schema"
        raise StoreIntegrityError(msg)
    return captured
