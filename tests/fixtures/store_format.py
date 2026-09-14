"""Test helpers for constructing the strict format-2 store boundary."""

from __future__ import annotations

import polars as pl
from fsspec import AbstractFileSystem

from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.storage.core.store_format import publish_store_manifest
from artisan.storage.core.table_schemas import FRAMEWORK_SCHEMAS, get_physical_schema
from artisan.utils.path import uri_join


def publish_test_store(
    delta_root: str,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
) -> None:
    """Create every coordinated physical table, then publish the manifest."""
    for table in FRAMEWORK_SCHEMAS:
        table_path = uri_join(delta_root, table)
        if not fs.exists(table_path):
            pl.DataFrame(schema=get_physical_schema(table)).write_delta(
                table_path,
                storage_options=storage_options,
            )
    for type_def in ArtifactTypeDef.get_all().values():
        table_path = uri_join(delta_root, type_def.table_path)
        if not fs.exists(table_path):
            pl.DataFrame(
                schema={**type_def.polars_schema(), "logical_commit_id": pl.String}
            ).write_delta(table_path, storage_options=storage_options)
    publish_store_manifest(delta_root, fs)
