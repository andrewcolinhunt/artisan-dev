"""Test helpers for constructing the strict format-2 store boundary."""

from __future__ import annotations

import polars as pl
from fsspec import AbstractFileSystem

from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.storage.core.store_format import publish_store_manifest
from artisan.storage.core.table_schemas import FRAMEWORK_SCHEMAS, get_physical_schema
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.commit_plan import build_commit_plan
from artisan.storage.io.staging import StagingManager
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


def commit_test_tables(
    delta_root: str,
    staging_root: str,
    fs: AbstractFileSystem,
    tables: dict[str, pl.DataFrame],
    *,
    step_run_id: str,
    operation_name: str = "seed",
    storage_options: dict[str, str] | None = None,
) -> None:
    """Publish a format-2 store and seed tables through one logical commit."""
    publish_test_store(delta_root, fs, storage_options)
    staging = StagingManager(staging_root, fs)
    for table_path, frame in tables.items():
        staging.stage_orchestrator_dataframe(
            frame,
            table_path,
            commit_kind="input_registration",
            step_run_id=step_run_id,
            step_number=0,
            operation_name=operation_name,
        )
    plan = build_commit_plan(
        delta_root=delta_root,
        staging_root=staging_root,
        fs=fs,
        commit_kind="input_registration",
        step_run_id=step_run_id,
        step_number=0,
        operation_name=operation_name,
    )
    DeltaCommitter(
        delta_root,
        staging,
        fs=fs,
        storage_options=storage_options,
    ).commit_logical(plan)
