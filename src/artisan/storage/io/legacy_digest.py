"""Bounded verification of canonical payload digests from existing commit plans."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import polars as pl
from deltalake import DeltaTable

from artisan.errors import StoreIntegrityError
from artisan.storage.core.table_schemas import (
    get_physical_schema_for_path,
    is_global_artifact_table,
)
from artisan.storage.io.staged_frames import json_value, table_rows_digest
from artisan.utils.path import uri_join

_SOURCE_FILE = "_artisan_legacy_source_file"


def legacy_table_plan_key(
    delta_root: str,
    table_path: str,
    natural_key: tuple[str, ...],
    keys: pl.DataFrame,
    logical_commit_id: str,
    visible_owners: set[str],
    storage_options: dict[str, str] | None,
) -> str:
    """Hash an existing plan's sorted payload rows without staging or snapshots.

    Only the key-to-file index is retained. Old digests require payloads in key
    order, so a file can be revisited when its rows interleave with other files.
    Each revisit streams one selected row instead of collecting the file.
    """
    try:
        delta = DeltaTable(
            uri_join(delta_root, table_path), storage_options=storage_options
        )
        schema = pl.scan_delta(delta).collect_schema()
        if dict(schema) != get_physical_schema_for_path(table_path):
            msg = f"Delta table {table_path!r} has an unexpected schema"
            raise StoreIntegrityError(msg)
        if keys.is_empty():
            return table_rows_digest(logical_commit_id, table_path, ())
        source = _LegacyTable(
            table_path,
            natural_key,
            schema,
            delta.metadata().partition_columns,
            storage_options,
        )
        locations = source.locations(delta.file_uris(), keys, visible_owners)
        return table_rows_digest(
            logical_commit_id, table_path, source.rows(locations, visible_owners)
        )
    except StoreIntegrityError:
        raise
    except Exception as exc:
        msg = f"Unreadable Delta table {table_path!r}"
        raise StoreIntegrityError(msg) from exc


@dataclass(frozen=True)
class _LegacyTable:
    table_path: str
    natural_key: tuple[str, ...]
    schema: pl.Schema
    partition_columns: list[str]
    storage_options: dict[str, str] | None

    def scan(self, paths: list[str]) -> pl.LazyFrame:
        """Read active files with the Delta schema and partition column types."""
        return pl.scan_parquet(
            paths,
            schema={
                name: dtype
                for name, dtype in self.schema.items()
                if name not in self.partition_columns
            },
            hive_schema=(
                {name: self.schema[name] for name in self.partition_columns}
                if self.partition_columns
                else None
            ),
            hive_partitioning=bool(self.partition_columns),
            include_file_paths=_SOURCE_FILE,
            storage_options=self.storage_options,
            parallel="none",
            low_memory=True,
            cache=False,
        )

    def visible(self, scan: pl.LazyFrame, owners: set[str]) -> pl.LazyFrame:
        """Keep committed owners, leaving ownerless cache reuse rows intact."""
        if "logical_commit_id" in self.schema:
            return scan.filter(pl.col("logical_commit_id").is_in(sorted(owners)))
        return scan

    def locations(
        self, paths: list[str], keys: pl.DataFrame, owners: set[str]
    ) -> pl.DataFrame:
        """Locate exactly one visible physical row for every planned key."""
        locations = (
            self.visible(self.scan(paths), owners)
            .join(keys.lazy(), on=list(self.natural_key), how="semi", nulls_equal=True)
            .select(*self.natural_key, _SOURCE_FILE)
            .sort(list(self.natural_key))
            .collect(engine="streaming")
        )
        if (
            locations.height != keys.height
            or locations.select(self.natural_key).n_unique() != keys.height
        ):
            msg = f"Legacy commit has missing or duplicate {self.table_path} rows"
            raise StoreIntegrityError(msg)
        return locations

    def rows(
        self, locations: pl.DataFrame, owners: set[str]
    ) -> Iterator[dict[str, Any]]:
        """Yield canonical ownerless effects in the already sorted key order."""
        columns = [
            name
            for name in self.schema
            if name != "logical_commit_id"
            and (
                name != "origin_step_number"
                or not is_global_artifact_table(self.table_path)
            )
        ]
        for location in locations.iter_rows(named=True):
            predicate = pl.all_horizontal(
                pl.col(name).eq_missing(pl.lit(location[name], dtype=self.schema[name]))
                for name in self.natural_key
            )
            scan = self.visible(self.scan([location[_SOURCE_FILE]]), owners)
            batches = (
                scan.filter(predicate)
                .select(columns)
                .collect_batches(chunk_size=1, engine="streaming", lazy=True)
            )
            for batch in batches:
                for row in batch.iter_rows(named=True, buffer_size=1):
                    yield {name: json_value(value) for name, value in row.items()}
                    del row
                del batch
