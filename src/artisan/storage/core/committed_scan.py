"""Single completion-aware read boundary for Artisan Delta tables."""

from __future__ import annotations

from typing import Any

import polars as pl
from fsspec import AbstractFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.storage.core.store_format import assert_store_format
from artisan.storage.core.table_schemas import is_global_artifact_table
from artisan.storage.io.commit_plan import (
    canonical_table_plan_key,
    logical_commit_identity,
    read_commit_plan,
)
from artisan.utils.path import uri_join

_CONTROL_STATES = frozenset({"planned", "complete", "abandoned"})


def scan_committed(
    delta_root: str,
    table: str | TablePath,
    *,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
) -> pl.LazyFrame:
    """Eagerly read and verify committed rows, then expose a lazy frame.

    Subsequent filters operate on the materialized result; they do not defer
    Delta reads or push predicates into the physical table scan.
    """
    return read_committed(
        delta_root,
        table,
        fs=fs,
        storage_options=storage_options,
    ).lazy()


def read_committed(
    delta_root: str,
    table: str | TablePath,
    *,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Read, filter, and integrity-check one physical Delta table."""
    options = storage_options or {}
    assert_store_format(delta_root, fs, options)
    table_path = table.value if isinstance(table, TablePath) else table
    if table_path == TablePath.LOGICAL_COMMITS.value:
        return read_logical_commits(delta_root, fs=fs, storage_options=options)
    path = uri_join(delta_root, table_path)
    if not fs.exists(path):
        msg = f"Missing required table {table_path!r}"
        raise StoreIntegrityError(msg)
    try:
        physical = pl.scan_delta(path, storage_options=options).collect()
    except Exception as exc:
        msg = f"Unreadable Delta table {table_path!r}"
        raise StoreIntegrityError(msg) from exc
    controls = read_logical_commits(
        delta_root,
        fs=fs,
        storage_options=options,
    )
    visible = filter_committed_rows(physical, table_path, controls)
    _validate_complete_effects(
        delta_root,
        table_path,
        physical,
        controls,
        fs,
    )
    if "logical_commit_id" in visible.columns:
        return visible.drop("logical_commit_id")
    return visible


def read_logical_commits(
    delta_root: str,
    *,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Read and validate the logical-commit control table directly."""
    path = uri_join(delta_root, TablePath.LOGICAL_COMMITS)
    if not fs.exists(path):
        msg = "Missing logical commit control table"
        raise StoreIntegrityError(msg)
    try:
        controls = pl.scan_delta(
            path,
            storage_options=storage_options or {},
        ).collect()
    except Exception as exc:
        msg = "Unreadable logical commit control table"
        raise StoreIntegrityError(msg) from exc
    if controls.is_empty():
        return controls
    invalid = controls.filter(~pl.col("state").is_in(_CONTROL_STATES))
    duplicates = controls.group_by("logical_commit_id").len().filter(pl.col("len") != 1)
    malformed = controls.filter(
        ((pl.col("state") == "complete") & pl.col("completed_at").is_null())
        | ((pl.col("state") != "complete") & pl.col("completed_at").is_not_null())
        | ((pl.col("state") == "abandoned") & pl.col("abandon_reason").is_null())
        | ((pl.col("state") != "abandoned") & pl.col("abandon_reason").is_not_null())
    )
    if not invalid.is_empty() or not duplicates.is_empty() or not malformed.is_empty():
        msg = "Logical commit control rows are inconsistent"
        raise StoreIntegrityError(msg)
    for row in controls.iter_rows(named=True):
        try:
            expected_id = logical_commit_identity(
                row["commit_kind"], row["step_run_id"], row["execution_run_id"]
            )
        except ValueError as exc:
            msg = "Logical commit ownership is invalid"
            raise StoreIntegrityError(msg) from exc
        if row["logical_commit_id"] != expected_id:
            msg = "Logical commit ID does not match its owner"
            raise StoreIntegrityError(msg)
    return controls


def filter_committed_rows(
    physical: pl.DataFrame,
    table_path: str,
    controls: pl.DataFrame,
) -> pl.DataFrame:
    """Apply the one shared completion filter to already-read physical rows."""
    if physical.is_empty():
        return physical
    if table_path == TablePath.CACHE_REUSE.value:
        owners = physical.select(
            (pl.lit("step_result:") + pl.col("current_step_run_id")).alias(
                "logical_commit_id"
            )
        )
        _require_known_owners(owners, controls, table_path)
        complete_ids = _complete_ids(controls)
        return physical.filter(
            (pl.lit("step_result:") + pl.col("current_step_run_id")).is_in(complete_ids)
        )
    if "logical_commit_id" not in physical.columns:
        msg = f"Table {table_path!r} lacks commit ownership"
        raise StoreIntegrityError(msg)
    owned = physical.filter(pl.col("logical_commit_id").is_not_null())
    _require_known_owners(owned.select("logical_commit_id"), controls, table_path)
    if table_path == TablePath.STEPS.value:
        invalid_owner = owned.filter(
            pl.col("logical_commit_id")
            != (pl.lit("step_result:") + pl.col("step_run_id"))
        )
        unowned_success = physical.filter(
            pl.col("logical_commit_id").is_null()
            & pl.col("status").is_in(["succeeded", "partial"])
        )
        if not invalid_owner.is_empty() or not unowned_success.is_empty():
            msg = "Step terminal ownership is inconsistent"
            raise StoreIntegrityError(msg)
        return physical.filter(
            pl.col("logical_commit_id").is_null()
            | pl.col("logical_commit_id").is_in(_complete_ids(controls))
        )
    if physical["logical_commit_id"].null_count():
        msg = f"Table {table_path!r} contains unowned rows"
        raise StoreIntegrityError(msg)
    return physical.filter(pl.col("logical_commit_id").is_in(_complete_ids(controls)))


def _validate_complete_effects(
    delta_root: str,
    table_path: str,
    physical: pl.DataFrame,
    controls: pl.DataFrame,
    fs: AbstractFileSystem,
) -> None:
    """Prove every completed plan's effect for the requested table."""
    complete_plans: dict[str, Any] = {}
    for control in controls.filter(pl.col("state") == "complete").iter_rows(named=True):
        plan = read_commit_plan(
            delta_root,
            fs,
            control["step_run_id"],
            control["commit_kind"],
            control["execution_run_id"],
        )
        if (
            plan.logical_commit_id != control["logical_commit_id"]
            or plan.plan_digest != control["plan_digest"]
            or plan.execution_run_id != control["execution_run_id"]
        ):
            msg = f"Completion evidence disagrees for {control['logical_commit_id']}"
            raise StoreIntegrityError(msg)
        complete_plans[plan.logical_commit_id] = plan
        table = plan.table(table_path)
        if table is None:
            continue
        rows = _planned_physical_rows(
            physical,
            table_path,
            table.natural_key,
            table.row_keys,
            plan.logical_commit_id,
            controls,
        )
        if rows.height != table.row_count:
            msg = (
                f"Complete commit {plan.logical_commit_id} is missing {table_path} rows"
            )
            raise StoreIntegrityError(msg)
        ownerless = (
            rows.drop("logical_commit_id") if "logical_commit_id" in rows else rows
        )
        if (
            canonical_table_plan_key(plan.logical_commit_id, table_path, ownerless)
            != table.table_plan_key
        ):
            msg = (
                f"Complete commit {plan.logical_commit_id} has conflicting "
                f"{table_path} rows"
            )
            raise StoreIntegrityError(msg)
    _reject_unplanned_complete_rows(physical, table_path, complete_plans)


def _reject_unplanned_complete_rows(
    physical: pl.DataFrame,
    table_path: str,
    complete_plans: dict[str, Any],
) -> None:
    """Reject rows a completed owner never declared in its immutable plan."""
    for logical_commit_id, plan in complete_plans.items():
        if table_path == TablePath.CACHE_REUSE.value:
            owned = physical.filter(
                pl.col("current_step_run_id")
                == logical_commit_id.removeprefix("step_result:")
            )
        elif "logical_commit_id" in physical.columns:
            owned = physical.filter(pl.col("logical_commit_id") == logical_commit_id)
        else:
            continue
        if owned.is_empty():
            continue
        if is_global_artifact_table(table_path) and "origin_step_number" in owned:
            invalid_origin = owned.filter(
                pl.col("origin_step_number").is_null()
                | (pl.col("origin_step_number") != plan.step_number)
            )
            if not invalid_origin.is_empty():
                msg = (
                    f"Artifact origin disagrees with owning commit {logical_commit_id}"
                )
                raise StoreIntegrityError(msg)
        table = plan.table(table_path)
        if table is None:
            msg = f"Complete commit {logical_commit_id} has unplanned {table_path} rows"
            raise StoreIntegrityError(msg)
        key_schema = {column: physical.schema[column] for column in table.natural_key}
        expected = pl.DataFrame(
            [dict(zip(table.natural_key, key, strict=True)) for key in table.row_keys],
            schema=key_schema,
        )
        extras = owned.join(
            expected,
            on=list(table.natural_key),
            how="anti",
            nulls_equal=True,
        )
        if not extras.is_empty():
            msg = (
                f"Complete commit {logical_commit_id} owns unplanned {table_path} rows"
            )
            raise StoreIntegrityError(msg)


def _planned_physical_rows(
    physical: pl.DataFrame,
    table_path: str,
    natural_key: tuple[str, ...],
    row_keys: tuple[tuple[Any, ...], ...],
    logical_commit_id: str,
    controls: pl.DataFrame,
) -> pl.DataFrame:
    key_schema = {column: physical.schema[column] for column in natural_key}
    expected = pl.DataFrame(
        [dict(zip(natural_key, key, strict=True)) for key in row_keys],
        schema=key_schema,
    )
    if table_path == TablePath.CACHE_REUSE.value:
        rows = physical.filter(
            pl.col("current_step_run_id") == logical_commit_id.split(":", 1)[1]
        )
    elif is_global_artifact_table(table_path):
        rows = filter_committed_rows(physical, table_path, controls)
        rows = rows.join(
            expected,
            on=list(natural_key),
            how="inner",
            nulls_equal=True,
        )
    else:
        rows = physical.filter(pl.col("logical_commit_id") == logical_commit_id)
    duplicates = rows.group_by(list(natural_key)).len().filter(pl.col("len") != 1)
    if not duplicates.is_empty():
        msg = f"Duplicate natural key in table {table_path!r}"
        raise StoreIntegrityError(msg)
    return rows


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
