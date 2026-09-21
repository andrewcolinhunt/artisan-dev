"""Single completion-aware read boundary for Artisan Delta tables."""

from __future__ import annotations

import polars as pl
from fsspec import AbstractFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.storage.core.store_format import assert_store_manifest
from artisan.storage.core.table_schemas import (
    get_physical_schema_for_path,
    is_global_artifact_table,
)
from artisan.storage.io.commit_plan import (
    CommitPlan,
    canonical_table_plan_key,
    logical_commit_identity,
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
    """Scan visible rows lazily, trusting durable logical completion records."""
    options = storage_options or {}
    assert_store_manifest(delta_root, fs)
    table_path = table.value if isinstance(table, TablePath) else table
    if table_path == TablePath.LOGICAL_COMMITS.value:
        return read_logical_commits(delta_root, fs=fs, storage_options=options).lazy()
    physical = _scan_physical(delta_root, table_path, fs, options)
    controls = read_logical_commits(delta_root, fs=fs, storage_options=options)
    visible = physical.filter(_visibility_expression(table_path, controls))
    if "logical_commit_id" in physical.collect_schema().names():
        return visible.drop("logical_commit_id")
    return visible


def read_committed(
    delta_root: str,
    table: str | TablePath,
    *,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Collect the shared visibility scan without auditing historical payloads."""
    return scan_committed(
        delta_root, table, fs=fs, storage_options=storage_options
    ).collect()


def _scan_physical(
    delta_root: str,
    table_path: str,
    fs: AbstractFileSystem,
    storage_options: dict[str, str],
) -> pl.LazyFrame:
    path = uri_join(delta_root, table_path)
    if not fs.exists(path):
        msg = f"Missing required table {table_path!r}"
        raise StoreIntegrityError(msg)
    try:
        scan = pl.scan_delta(path, storage_options=storage_options)
        schema = dict(scan.collect_schema())
    except Exception as exc:
        msg = f"Unreadable Delta table {table_path!r}"
        raise StoreIntegrityError(msg) from exc
    if schema != get_physical_schema_for_path(table_path):
        msg = f"Delta table {table_path!r} has an unexpected schema"
        raise StoreIntegrityError(msg)
    return scan


def read_logical_commits(
    delta_root: str,
    *,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Read and validate the logical-commit control table directly."""
    assert_store_manifest(delta_root, fs)
    controls = _scan_physical(
        delta_root, TablePath.LOGICAL_COMMITS.value, fs, storage_options or {}
    ).collect()
    if controls.is_empty():
        return controls
    required = [
        "logical_commit_id",
        "commit_kind",
        "step_run_id",
        "state",
        "plan_digest",
        "created_at",
    ]
    invalid = controls.filter(
        ~pl.col("state").is_in(_CONTROL_STATES)
        | pl.any_horizontal(pl.col(name).is_null() for name in required)
    )
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
                row["commit_kind"], row["step_run_id"], row["recovery_batch_id"]
            )
        except ValueError as exc:
            msg = "Logical commit ownership is invalid"
            raise StoreIntegrityError(msg) from exc
        if row["logical_commit_id"] != expected_id:
            msg = "Logical commit ID does not match its owner"
            raise StoreIntegrityError(msg)
    return controls


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
    plan: CommitPlan,
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
    if (
        "origin_step_number" in owned
        and not owned.filter(
            pl.col("origin_step_number").is_null()
            | (pl.col("origin_step_number") != plan.step_number)
        ).is_empty()
    ):
        msg = f"Artifact origin disagrees with owning commit {plan.logical_commit_id}"
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
    plan: CommitPlan, table_path: str, physical: pl.DataFrame
) -> pl.DataFrame:
    if table_path == TablePath.CACHE_REUSE.value:
        if plan.commit_kind != "step_result":
            return physical.head(0)
        return physical.filter(pl.col("current_step_run_id") == plan.step_run_id)
    return physical.filter(pl.col("logical_commit_id") == plan.logical_commit_id)


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
