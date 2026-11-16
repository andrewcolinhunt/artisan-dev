"""Apply and verify exact table effects for validated logical plans."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.storage.core.committed_scan import scan_plan_keys, verify_snapshot_effect
from artisan.storage.core.table_schemas import is_global_artifact_table
from artisan.storage.io.bounded_evidence import DiskTable
from artisan.storage.io.commit_contract import LogicalEffect, TableEffect
from artisan.storage.io.delta_files import DeltaSnapshot, append_table_files


def apply_table_effect(
    plan: LogicalEffect,
    table: TableEffect,
    expected: DiskTable,
    controls: pl.DataFrame,
    *,
    read_snapshot: Callable[[str], DeltaSnapshot],
) -> int:
    """Write only missing eligible keys, then prove the complete owned effect."""
    snapshot = read_snapshot(table.table_path)
    schema = pl.Schema(snapshot.schema)
    missing = missing_effect_keys(plan, table, expected, controls, snapshot)
    written = 0
    if missing:
        frames = (
            _inject_owner(frame, table.table_path, plan.logical_commit_id).cast(schema)
            for frame in expected.iter_frames(keys=missing)
        )
        stats = list(table.natural_key)
        if "logical_commit_id" in schema:
            stats.append("logical_commit_id")
        written = append_table_files(
            snapshot, frames, step_number=plan.step_number, statistics_columns=stats
        )
        snapshot = read_snapshot(table.table_path)
    verify_snapshot_effect(plan, table.table_path, snapshot, controls)
    return written


def missing_effect_keys(
    plan: LogicalEffect,
    table: TableEffect,
    expected: DiskTable,
    controls: pl.DataFrame,
    snapshot: DeltaSnapshot,
) -> set[tuple[Any, ...]]:
    """Reject conflicting ownership and payloads before deciding what to append."""
    selected = scan_plan_keys(plan, table, snapshot, controls)
    for frame in snapshot.iter_frames(rows_by_file=selected.matched):
        for row in frame.iter_rows(named=True):
            key = tuple(row[name] for name in table.natural_key)
            if not expected.matches_row(
                key, row, comparable=is_global_artifact_table(table.table_path)
            ):
                msg = f"Natural key has conflicting values in {table.table_path}"
                raise StoreIntegrityError(msg)
    if not is_global_artifact_table(table.table_path) and (
        0 < len(selected.satisfied) < table.row_count
    ):
        msg = f"Only part of the planned effect exists in {table.table_path}"
        raise StoreIntegrityError(msg)
    if expected.canonical_key(plan.logical_commit_id) != table.table_plan_key:
        msg = f"Table plan key disagrees for {table.table_path}"
        raise StoreIntegrityError(msg)
    return set(table.row_keys) - selected.satisfied


def _inject_owner(
    frame: pl.DataFrame, table_path: str, logical_commit_id: str
) -> pl.DataFrame:
    if table_path == TablePath.CACHE_REUSE.value:
        return frame
    return frame.with_columns(
        pl.lit(logical_commit_id).cast(pl.String).alias("logical_commit_id")
    )
