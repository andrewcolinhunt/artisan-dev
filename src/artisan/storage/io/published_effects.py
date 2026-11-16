"""Publish and capture exact controller-owned table effects."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from types import MappingProxyType

from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.storage.core.table_schemas import get_physical_schema_for_path
from artisan.storage.io.bounded_evidence import DiskTable
from artisan.storage.io.bounded_io import capture_parquet
from artisan.storage.io.commit_contract import LogicalEffect
from artisan.storage.io.publication import publish_immutable_file
from artisan.storage.io.retained_plan import RetainedPlannedFile, RetainedPlannedTable
from artisan.utils.hashing import compute_content_digest
from artisan.utils.path import uri_join


def publish_table_effect(
    logical_id: str,
    table_path: str,
    source: str,
    directory: str,
    scratch: str,
    fs: AbstractFileSystem,
    *,
    relative_directory: str = "tables",
) -> RetainedPlannedTable:
    """Publish exact bounded table evidence and its protected origins."""
    relative = (
        f"{relative_directory}/{compute_content_digest(table_path.encode())}.parquet"
    )
    captured = capture_parquet(
        source, LocalFileSystem(), str(Path(scratch) / Path(relative).name)
    )
    table = DiskTable(table_path, [captured.path], scratch)
    file = RetainedPlannedFile(
        relative_path=relative,
        size_bytes=captured.size_bytes,
        digest=captured.digest,
        row_count=captured.row_count,
        schema_signature=tuple(
            (name, str(dtype)) for name, dtype in captured.schema.items()
        ),
    )
    effect = RetainedPlannedTable(
        table_path=table_path,
        natural_key=table.natural_key,
        row_keys=table.keys,
        row_count=table.row_count,
        table_plan_key=table.canonical_key(logical_id),
        files=(file,),
        row_origins=(
            tuple(table.get_row(key)["origin_step_number"] for key in table.keys)
            if "origin_step_number" in table.schema
            else ()
        ),
    )
    publish_immutable_file(
        fs, uri_join(directory, relative), captured.path, source_fs=LocalFileSystem()
    )
    return effect


@contextmanager
def capture_table_effects(
    plan: LogicalEffect,
    effects: Sequence[RetainedPlannedTable],
    *,
    directory: str,
    fs: AbstractFileSystem,
) -> Iterator[Mapping[str, DiskTable]]:
    """Recheck exact persisted spools after the caller validates plan authority."""
    with TemporaryDirectory(prefix="artisan-published-effects-") as scratch:
        tables: dict[str, DiskTable] = {}
        for table in effects:
            paths: list[str] = []
            schema = {
                name: dtype
                for name, dtype in get_physical_schema_for_path(
                    table.table_path
                ).items()
                if name != "logical_commit_id"
            }
            for file in table.files:
                captured = capture_parquet(
                    uri_join(directory, file.relative_path),
                    fs,
                    str(Path(scratch) / f"{len(tables)}-{len(paths)}.parquet"),
                )
                if (
                    captured.digest,
                    captured.size_bytes,
                    captured.row_count,
                    captured.schema,
                ) != (file.digest, file.size_bytes, file.row_count, schema) or tuple(
                    (name, str(dtype)) for name, dtype in captured.schema.items()
                ) != file.schema_signature:
                    msg = "Retained plan table evidence changed"
                    raise StoreIntegrityError(msg)
                paths.append(captured.path)
            evidence = DiskTable(table.table_path, paths, scratch)
            if (
                evidence.keys != table.row_keys
                or evidence.row_count != table.row_count
                or evidence.canonical_key(plan.logical_commit_id)
                != table.table_plan_key
                or any(
                    evidence.get_row(key)["origin_step_number"]
                    != plan.expected_origin(table.table_path, key)
                    for key in table.row_keys
                    if "origin_step_number" in evidence.schema
                )
            ):
                msg = "Retained table effects differ from the published plan"
                raise StoreIntegrityError(msg)
            tables[table.table_path] = evidence
        yield MappingProxyType(tables)
