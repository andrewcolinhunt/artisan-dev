"""Test helpers for constructing the strict format-2 store boundary."""

from __future__ import annotations

import polars as pl
from fsspec import AbstractFileSystem

from artisan.schemas.enums import TablePath
from artisan.storage.core.store_format import publish_store_manifest
from artisan.storage.core.table_schemas import CACHE_REUSE_SCHEMA
from artisan.utils.path import uri_join


def publish_test_store(
    delta_root: str,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
) -> None:
    """Create the required empty D2 relation, then publish the manifest."""
    table_path = uri_join(delta_root, TablePath.CACHE_REUSE)
    if not fs.exists(table_path):
        pl.DataFrame(schema=CACHE_REUSE_SCHEMA).write_delta(
            table_path,
            storage_options=storage_options,
        )
    publish_store_manifest(delta_root, fs)
