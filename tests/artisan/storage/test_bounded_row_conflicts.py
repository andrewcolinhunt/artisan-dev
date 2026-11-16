"""Shared exact-effect checks inspect every physical copy, including planned owners.

These tests exercise the lower structural writer, not domain authorization.
Real acceptance and recovery authority are covered by retained-store tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import (
    ARTIFACT_INDEX_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
    get_physical_schema_for_path,
)
from artisan.storage.io.bounded_evidence import DiskTable
from artisan.storage.io.commit_contract import TableEffect
from artisan.storage.io.delta_files import DeltaSnapshot
from artisan.storage.io.logical_control import planned_control
from artisan.storage.io.table_effect import apply_table_effect


@dataclass(frozen=True)
class _Table:
    table_path: str
    natural_key: tuple[str, ...]
    row_keys: tuple[tuple[Any, ...], ...]
    row_count: int
    table_plan_key: str


@dataclass(frozen=True)
class _Plan:
    effect: _Table
    logical_commit_id: str = "retained_result:" + "c" * 32
    commit_kind: str = "retained_result"
    step_run_id: str = "d" * 32
    recovery_batch_id: str | None = None
    step_number: int = 0
    plan_digest: str = "e" * 32

    def table(self, table_path: str) -> TableEffect | None:
        return self.effect if table_path == self.effect.table_path else None

    def expected_origin(self, table_path: str, key: tuple[Any, ...]) -> int:
        return 0


def _effect(
    tmp_path: Path, table_path: str, rows: pl.DataFrame
) -> tuple[_Plan, DiskTable]:
    source = tmp_path / "expected.parquet"
    rows.write_parquet(source)
    expected = DiskTable(table_path, [str(source)], str(tmp_path))
    owner = "retained_result:" + "c" * 32
    return _Plan(
        _Table(
            table_path,
            expected.natural_key,
            expected.keys,
            expected.row_count,
            expected.canonical_key(owner),
        )
    ), expected


@pytest.mark.parametrize(
    ("owners", "conflicting", "error"),
    [
        (("pending",), False, None),
        (("pending", "pending"), False, None),
        (("current", "pending"), False, None),
        (("pending", "current"), False, None),
        (("complete", "pending"), False, None),
        (("current", "current"), False, "Duplicate natural key"),
        (("complete", "complete"), False, "Duplicate natural key"),
        (("current", "complete"), False, "Duplicate natural key"),
        (("pending",), True, "conflicting values"),
        (("current", "pending"), True, "conflicting values"),
        (("complete", "pending"), True, "conflicting values"),
        (("unowned",), False, "unowned rows"),
    ],
)
def test_effect_checks_all_global_copies(
    backend_fs,
    tmp_path: Path,
    owners: tuple[str, ...],
    conflicting: bool,
    error: str | None,
) -> None:
    fs, storage, root = backend_fs
    table_path = TablePath.ARTIFACT_INDEX.value
    rows = pl.DataFrame(
        {
            "artifact_id": ["a" * 32, "b" * 32],
            "artifact_type": ["metric"] * 2,
            "origin_step_number": [0, 0],
            "metadata": ["{}"] * 2,
        },
        schema=ARTIFACT_INDEX_SCHEMA,
    )
    plan, expected = _effect(tmp_path, table_path, rows)
    controls = planned_control(plan)
    owner_ids = {"current": plan.logical_commit_id, "unowned": None}
    for state in set(owners) & {"pending", "complete"}:
        owner = "retained_result:" + ("f" if state == "pending" else "1") * 32
        owner_ids[state] = owner
        control = planned_control(plan).with_columns(
            pl.lit(owner).alias("logical_commit_id")
        )
        if state == "complete":
            control = control.with_columns(
                pl.lit("complete").alias("state"),
                pl.col("created_at").alias("completed_at"),
            )
        controls = pl.concat([controls, control])
    copies = [
        rows.head(1).with_columns(
            pl.lit(owner_ids[state], dtype=pl.String).alias("logical_commit_id")
        )
        for state in owners
    ]
    if conflicting:
        copies[-1] = copies[-1].with_columns(
            pl.lit('{"changed":true}').alias("metadata")
        )
    path = f"{root}/structural/{table_path}"
    pl.concat(copies).write_delta(path, storage_options=storage.delta_storage_options())

    def snapshot(requested: str) -> DeltaSnapshot:
        assert requested == table_path
        return DeltaSnapshot(
            path,
            get_physical_schema_for_path(table_path),
            fs=fs,
            storage_options=storage.delta_storage_options(),
        )

    if error is not None:
        with pytest.raises(StoreIntegrityError, match=error):
            apply_table_effect(
                plan, plan.effect, expected, controls, read_snapshot=snapshot
            )
        return
    apply_table_effect(plan, plan.effect, expected, controls, read_snapshot=snapshot)
    assert (
        apply_table_effect(
            plan, plan.effect, expected, controls, read_snapshot=snapshot
        )
        == 0
    )


@pytest.mark.parametrize("state", ["planned", "complete"])
def test_effect_rejects_foreign_non_global_owner(
    backend_fs, tmp_path: Path, state: str
) -> None:
    fs, storage, root = backend_fs
    table_path = TablePath.EXECUTION_EDGES.value
    rows = pl.DataFrame(
        {
            "execution_run_id": ["f" * 32],
            "direction": ["output"],
            "role": ["metric"],
            "artifact_id": ["a" * 32],
        },
        schema=EXECUTION_EDGES_SCHEMA,
    )
    plan, expected = _effect(tmp_path, table_path, rows)
    owner = "retained_result:" + "1" * 32
    foreign = planned_control(plan).with_columns(
        pl.lit(owner).alias("logical_commit_id"), pl.lit(state).alias("state")
    )
    if state == "complete":
        foreign = foreign.with_columns(pl.col("created_at").alias("completed_at"))
    controls = pl.concat([planned_control(plan), foreign])
    path = f"{root}/structural/{table_path}"
    rows.with_columns(pl.lit(owner).alias("logical_commit_id")).write_delta(
        path, storage_options=storage.delta_storage_options()
    )

    def snapshot(requested: str) -> DeltaSnapshot:
        assert requested == table_path
        return DeltaSnapshot(
            path,
            get_physical_schema_for_path(table_path),
            fs=fs,
            storage_options=storage.delta_storage_options(),
        )

    with pytest.raises(StoreIntegrityError, match="belongs to another commit"):
        apply_table_effect(
            plan, plan.effect, expected, controls, read_snapshot=snapshot
        )
