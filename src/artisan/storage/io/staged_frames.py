"""Disk-backed snapshots of staged rows for bounded commit preparation."""

from __future__ import annotations

import io
import tempfile
import weakref
from collections.abc import Iterable, Iterator, Mapping
from datetime import datetime
from enum import Enum
from typing import Any

import polars as pl
import xxhash

from artisan.errors import StoreIntegrityError
from artisan.storage.core.table_schemas import get_natural_key, is_global_artifact_table
from artisan.utils.hashing import canonical_json_bytes

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


class StagedTable:
    """Own a temporary snapshot, retaining only keys and disk offsets in RAM.

    Payload memory is bounded by one input shard or output batch (at least one
    row). The original schema and logical-commit plan format are preserved.
    """

    def __init__(self, table_path: str, schema: pl.Schema) -> None:
        self.table_path = table_path
        self.schema = schema
        self.natural_key = get_natural_key(table_path)
        self._rows: dict[tuple[Any, ...], tuple[int, int]] = {}
        # The snapshot must outlive preparation, including failed commit attempts.
        self._stream = tempfile.TemporaryFile()  # noqa: SIM115
        self._cleanup = weakref.finalize(self, self._stream.close)

    @property
    def height(self) -> int:
        """Return the number of distinct planned rows."""
        return len(self._rows)

    @property
    def row_group_size(self) -> int:
        """Keep the Parquet writer's row groups within the payload budget too."""
        largest = max((length for _, length in self._rows.values()), default=1)
        return max(1, min(COMMIT_BATCH_ROWS, COMMIT_BATCH_BYTES // largest))

    def keys(self) -> pl.DataFrame:
        """Return only natural keys, in canonical order."""
        return pl.DataFrame(
            list(self._rows),
            schema={name: self.schema[name] for name in self.natural_key},
            orient="row",
        ).sort(list(self.natural_key))

    def add(self, frame: pl.DataFrame) -> None:
        """Capture a shard and reject conflicting or disallowed duplicate keys."""
        if frame.schema != self.schema:
            msg = f"Inconsistent staged schema for {self.table_path}"
            raise StoreIntegrityError(msg)
        for index, key in enumerate(frame.select(self.natural_key).iter_rows()):
            previous = self._rows.get(key)
            if previous is not None:
                if not is_global_artifact_table(self.table_path):
                    msg = f"Duplicate natural keys in staged table {self.table_path}"
                    raise StoreIntegrityError(msg)
                if not self._read_row(previous).equals(frame.slice(index, 1)):
                    msg = f"Conflicting natural keys in staged table {self.table_path}"
                    raise StoreIntegrityError(msg)
                continue
            payload = frame.slice(index, 1).serialize()
            offset = self._stream.seek(0, io.SEEK_END)
            self._stream.write(payload)
            self._rows[key] = (offset, len(payload))

    def _read_row(self, location: tuple[int, int]) -> pl.DataFrame:
        offset, length = location
        self._stream.seek(offset)
        return pl.DataFrame.deserialize(io.BytesIO(self._stream.read(length)))

    def batches(
        self, selected: set[tuple[Any, ...]] | None = None
    ) -> Iterator[pl.DataFrame]:
        """Read canonical rows in batches bounded by serialized bytes and count."""
        frames: list[pl.DataFrame] = []
        size = 0
        for key in self.keys().iter_rows():
            if selected is not None and key not in selected:
                continue
            location = self._rows[key]
            length = location[1]
            if frames and (
                size + length > COMMIT_BATCH_BYTES or len(frames) >= COMMIT_BATCH_ROWS
            ):
                yield pl.concat(frames)
                frames, size = [], 0
            frames.append(self._read_row(location))
            size += length
        if frames:
            yield pl.concat(frames)

    def plan_key(self, logical_commit_id: str) -> str:
        """Digest canonical rows with bounded payload memory."""

        def rows() -> Iterator[dict[str, Any]]:
            for frame in self.batches():
                for row in frame.iter_rows(named=True):
                    if is_global_artifact_table(self.table_path):
                        row.pop("origin_step_number", None)
                    yield {name: json_value(value) for name, value in row.items()}

        return table_rows_digest(logical_commit_id, self.table_path, rows())

    def collect(self) -> pl.DataFrame:
        """Materialize a table for callers explicitly needing a DataFrame."""
        return (
            pl.concat(self.batches())
            if self.height
            else pl.DataFrame(schema=self.schema)
        )


class StagedFrames(Mapping[str, pl.DataFrame]):
    """Expose snapshots to readers, with a streaming path for the committer."""

    def __init__(self) -> None:
        self.tables: dict[str, StagedTable] = {}

    def add(self, table_path: str, frame: pl.DataFrame) -> None:
        """Spill one captured shard without retaining its DataFrame."""
        if table_path not in self.tables:
            self.tables[table_path] = StagedTable(table_path, frame.schema)
        self.tables[table_path].add(frame)

    def __getitem__(self, table_path: str) -> pl.DataFrame:
        return self.tables[table_path].collect()

    def __iter__(self) -> Iterator[str]:
        return iter(self.tables)

    def __len__(self) -> int:
        return len(self.tables)
