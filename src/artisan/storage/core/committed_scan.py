"""Single completion-aware read boundary for Artisan Delta tables."""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from typing import Any

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import (
    is_global_artifact_table,
)
from artisan.storage.io.bounded_evidence import DiskTable
from artisan.storage.io.bounded_io import write_parquet_rows
from artisan.storage.io.commit_contract import LogicalEffect, TableEffect
from artisan.storage.io.delta_files import DeltaSnapshot
from artisan.storage.io.plan_evidence import canonical_table_plan_key


def _visibility_expression(table_path: str, controls: pl.DataFrame) -> pl.Expr:
    complete = _complete_ids(controls)
    if table_path == TablePath.CACHE_REUSE.value:
        return (pl.lit("step_result:") + pl.col("current_step_run_id")).is_in(complete)
    owner = pl.col("logical_commit_id")
    if table_path == TablePath.STEPS.value:
        lifecycle = owner.is_null() & pl.col("status").is_in(
            ["pending", "running", "failed", "cancelled", "skipped"]
        )
        terminal = (
            owner == pl.lit("step_result:") + pl.col("step_run_id")
        ) & owner.is_in(complete)
        return lifecycle | terminal
    return owner.is_in(complete)


def filter_committed_rows(
    physical: pl.DataFrame,
    table_path: str,
    controls: pl.DataFrame,
) -> pl.DataFrame:
    """Apply logical visibility without a historical integrity audit."""
    return physical.filter(_visibility_expression(table_path, controls))


def audit_table_owners(
    physical: pl.DataFrame,
    table_path: str,
    controls: pl.DataFrame,
) -> None:
    """Audit physical owner/lifecycle metadata, including tables without plans."""
    if physical.is_empty():
        return
    if table_path == TablePath.CACHE_REUSE.value:
        owners = physical.select(
            (pl.lit("step_result:") + pl.col("current_step_run_id")).alias(
                "logical_commit_id"
            )
        )
        if owners["logical_commit_id"].null_count():
            msg = f"Table {table_path!r} contains unowned rows"
            raise StoreIntegrityError(msg)
        _require_known_owners(owners, controls, table_path)
        return
    if "logical_commit_id" not in physical:
        msg = f"Table {table_path!r} lacks commit ownership"
        raise StoreIntegrityError(msg)
    owned = physical.filter(pl.col("logical_commit_id").is_not_null())
    _require_known_owners(owned.select("logical_commit_id"), controls, table_path)
    if table_path == TablePath.STEPS.value:
        invalid_owner = owned.filter(
            pl.col("logical_commit_id")
            != (pl.lit("step_result:") + pl.col("step_run_id"))
        )
        invalid_lifecycle = physical.filter(
            pl.col("logical_commit_id").is_null()
            & ~pl.col("status")
            .is_in(["pending", "running", "failed", "cancelled", "skipped"])
            .fill_null(False)
        )
        if not invalid_owner.is_empty() or not invalid_lifecycle.is_empty():
            msg = "Step terminal ownership is inconsistent"
            raise StoreIntegrityError(msg)
    elif physical["logical_commit_id"].null_count():
        msg = f"Table {table_path!r} contains unowned rows"
        raise StoreIntegrityError(msg)


def verify_plan_effect(
    plan: LogicalEffect,
    table_path: str,
    physical: pl.DataFrame,
    controls: pl.DataFrame,
) -> None:
    """Prove one plan's exact effect using supplied frames, without any I/O.

    The plan's own rows may still be planned. Reusable global artifact rows
    must belong to this plan or a completed owner. Calling this for an
    undeclared table detects unexpected rows during explicit store audits.
    """
    table = plan.table(table_path)
    owned = _owned_rows(plan, table_path, physical)
    if table is None:
        if not owned.is_empty():
            msg = f"Complete commit {plan.logical_commit_id} has unplanned {table_path} rows"
            raise StoreIntegrityError(msg)
        return
    keys = list(table.natural_key)
    expected = pl.DataFrame(
        [dict(zip(keys, key, strict=True)) for key in table.row_keys],
        schema={column: physical.schema[column] for column in keys},
    )
    extras = owned.join(expected, on=keys, how="anti", nulls_equal=True)
    if not extras.is_empty():
        msg = (
            f"Complete commit {plan.logical_commit_id} owns unplanned {table_path} rows"
        )
        raise StoreIntegrityError(msg)
    if "origin_step_number" in owned:
        origins = expected.with_columns(
            pl.Series(
                "expected_origin",
                [plan.expected_origin(table_path, key) for key in table.row_keys],
                dtype=pl.Int64,
            )
        )
        joined = owned.join(origins, on=keys, how="left", nulls_equal=True)
        if joined.filter(
            ~pl.col("origin_step_number").eq_missing(pl.col("expected_origin"))
        ).height:
            msg = (
                f"Artifact origin disagrees with owning commit {plan.logical_commit_id}"
            )
            raise StoreIntegrityError(msg)
    rows = owned
    if is_global_artifact_table(table_path):
        rows = physical.filter(
            (pl.col("logical_commit_id") == plan.logical_commit_id)
            | pl.col("logical_commit_id").is_in(_complete_ids(controls))
        ).join(expected, on=keys, how="inner", nulls_equal=True)
    duplicates = rows.group_by(keys).len().filter(pl.col("len") != 1)
    if not duplicates.is_empty():
        msg = f"Duplicate natural key in table {table_path!r}"
        raise StoreIntegrityError(msg)
    if rows.height != table.row_count:
        msg = f"Complete commit {plan.logical_commit_id} is missing {table_path} rows"
        raise StoreIntegrityError(msg)
    ownerless = rows.drop("logical_commit_id") if "logical_commit_id" in rows else rows
    if (
        canonical_table_plan_key(plan.logical_commit_id, table_path, ownerless)
        != table.table_plan_key
    ):
        msg = f"Complete commit {plan.logical_commit_id} has conflicting {table_path} rows"
        raise StoreIntegrityError(msg)


def _owned_rows(
    plan: LogicalEffect, table_path: str, physical: pl.DataFrame
) -> pl.DataFrame:
    if table_path == TablePath.CACHE_REUSE.value:
        if plan.commit_kind != "step_result":
            return physical.head(0)
        return physical.filter(pl.col("current_step_run_id") == plan.step_run_id)
    return physical.filter(pl.col("logical_commit_id") == plan.logical_commit_id)


@dataclass
class EffectKeys:
    """Compact row locations for all matching values and eligible representatives."""

    matched: dict[str, set[int]]
    eligible: dict[str, set[int]]
    satisfied: set[tuple[Any, ...]]


def scan_plan_keys(
    plan: LogicalEffect,
    table: TableEffect,
    snapshot: DeltaSnapshot,
    controls: pl.DataFrame,
) -> EffectKeys:
    """Check owners and complete membership before reading matching payloads."""
    wanted = set(table.row_keys)
    complete = set(_complete_ids(controls))
    cache = table.table_path == TablePath.CACHE_REUSE.value
    owner_column = "current_step_run_id" if cache else "logical_commit_id"
    columns = list(dict.fromkeys([*table.natural_key, owner_column]))
    if "origin_step_number" in snapshot.schema:
        columns.append("origin_step_number")
    result = EffectKeys({}, {}, set())
    global_artifacts = is_global_artifact_table(table.table_path)
    for path, positions, frame in snapshot.iter_located(columns):
        for position, row in zip(positions, frame.iter_rows(named=True), strict=True):
            key = tuple(row[column] for column in table.natural_key)
            owner = row[owner_column]
            owned = owner == (plan.step_run_id if cache else plan.logical_commit_id)
            if owned:
                if key not in wanted:
                    msg = f"Commit {plan.logical_commit_id} owns unplanned {table.table_path} rows"
                    raise StoreIntegrityError(msg)
                if "origin_step_number" in row and row[
                    "origin_step_number"
                ] != plan.expected_origin(table.table_path, key):
                    msg = f"Artifact origin disagrees with owning commit {plan.logical_commit_id}"
                    raise StoreIntegrityError(msg)
            if key not in wanted:
                continue
            if owner is None:
                msg = f"Table {table.table_path!r} contains unowned rows"
                raise StoreIntegrityError(msg)
            if not global_artifacts and not owned:
                msg = f"Natural key already belongs to another commit in {table.table_path}"
                raise StoreIntegrityError(msg)
            result.matched.setdefault(path, set()).add(position)
            if owned or (global_artifacts and owner in complete):
                if key in result.satisfied:
                    msg = f"Duplicate natural key in table {table.table_path!r}"
                    raise StoreIntegrityError(msg)
                result.satisfied.add(key)
                result.eligible.setdefault(path, set()).add(position)
    return result


def verify_snapshot_effect(
    plan: LogicalEffect,
    table_path: str,
    snapshot: DeltaSnapshot,
    controls: pl.DataFrame,
) -> None:
    """Verify a complete effect through projected keys and a bounded disk spool."""
    table = plan.table(table_path)
    if table is None:
        owner = (
            "current_step_run_id"
            if table_path == TablePath.CACHE_REUSE.value
            else "logical_commit_id"
        )
        for frame in snapshot.iter_frames([owner]):
            if not _owned_rows(plan, table_path, frame).is_empty():
                msg = f"Complete commit {plan.logical_commit_id} has unplanned {table_path} rows"
                raise StoreIntegrityError(msg)
        return
    selection = scan_plan_keys(plan, table, snapshot, controls)
    if len(selection.satisfied) != table.row_count:
        msg = f"Complete commit {plan.logical_commit_id} is missing {table_path} rows"
        raise StoreIntegrityError(msg)
    schema = {
        name: dtype
        for name, dtype in snapshot.schema.items()
        if name != "logical_commit_id"
    }
    with tempfile.TemporaryDirectory(prefix="artisan-verify-") as scratch:
        path = os.path.join(scratch, "effect.parquet")
        rows = (
            {name: row[name] for name in schema}
            for frame in snapshot.iter_frames(rows_by_file=selection.eligible)
            for row in frame.iter_rows(named=True)
        )
        write_parquet_rows(path, rows, schema)
        effect = DiskTable(table_path, [path], scratch)
        if effect.canonical_key(plan.logical_commit_id) != table.table_plan_key:
            msg = f"Complete commit {plan.logical_commit_id} has conflicting {table_path} rows"
            raise StoreIntegrityError(msg)
        foreign = {
            filename: positions - selection.eligible.get(filename, set())
            for filename, positions in selection.matched.items()
        }
        for frame in snapshot.iter_frames(rows_by_file=foreign):
            for row in frame.iter_rows(named=True):
                key = tuple(row[name] for name in table.natural_key)
                if not effect.matches_row(
                    key, row, comparable=is_global_artifact_table(table_path)
                ):
                    msg = f"Natural key has conflicting values in {table_path}"
                    raise StoreIntegrityError(msg)


def _complete_ids(controls: pl.DataFrame) -> list[str]:
    return controls.filter(pl.col("state") == "complete")["logical_commit_id"].to_list()


def _require_known_owners(
    owners: pl.DataFrame,
    controls: pl.DataFrame,
    table_path: str,
) -> None:
    if owners.is_empty():
        return
    known = set(controls["logical_commit_id"].to_list())
    found = set(owners["logical_commit_id"].drop_nulls().to_list())
    unknown = sorted(found - known)
    if unknown:
        msg = f"Table {table_path!r} contains rows with unknown owners {unknown!r}"
        raise StoreIntegrityError(msg)
