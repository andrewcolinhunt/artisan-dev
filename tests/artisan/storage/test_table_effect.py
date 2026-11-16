"""Exact effect verification protects per-row origins across both storage backends."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from deltalake import write_deltalake

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.storage.core.committed_scan import (
    verify_plan_effect,
    verify_snapshot_effect,
)
from artisan.storage.core.table_schemas import (
    ARTIFACT_INDEX_SCHEMA,
    LOGICAL_COMMITS_SCHEMA,
    get_physical_schema_for_path,
)
from artisan.storage.io.bounded_evidence import DiskTable
from artisan.storage.io.commit_contract import TableEffect
from artisan.storage.io.delta_files import DeltaSnapshot, _arrow_schema
from artisan.storage.io.logical_control import complete_control, planned_control
from artisan.storage.io.table_effect import apply_table_effect


@dataclass(frozen=True)
class _Table:
    table_path: str
    natural_key: tuple[str, ...]
    row_keys: tuple[tuple[Any, ...], ...]
    row_count: int
    table_plan_key: str


@dataclass(frozen=True)
class _OriginPlan:
    """Exercise the shared writer's structural contract before domain authorization."""

    effect: _Table
    logical_commit_id: str = "retained_result:" + "a" * 32
    commit_kind: str = "retained_result"
    step_run_id: str = "b" * 32
    recovery_batch_id: str | None = None
    step_number: int = 7
    plan_digest: str = "c" * 32

    def table(self, table_path: str) -> TableEffect | None:
        return self.effect if table_path == self.effect.table_path else None

    def expected_origin(self, table_path: str, key: tuple[Any, ...]) -> int | None:
        assert table_path == self.effect.table_path
        return {("d" * 32,): None, ("e" * 32,): 3}[key]


@pytest.mark.parametrize("prior_owner", [False, True])
def test_table_effect_preserves_and_protects_exact_mixed_origins(
    backend_fs, tmp_path: Path, prior_owner: bool
) -> None:
    fs, storage, root = backend_fs
    options = storage.delta_storage_options()
    table_path = TablePath.ARTIFACT_INDEX.value
    physical_schema = get_physical_schema_for_path(table_path)
    path = f"{root}/candidate/{table_path}"
    rows = pl.DataFrame(
        {
            "artifact_id": ["d" * 32, "e" * 32],
            "artifact_type": ["metric", "metric"],
            "origin_step_number": [None, 3],
            "metadata": ["{}", "{}"],
        },
        schema=ARTIFACT_INDEX_SCHEMA,
    )
    source = tmp_path / "index.parquet"
    rows.write_parquet(source)
    expected = DiskTable(table_path, [str(source)], str(tmp_path))
    plan = _OriginPlan(
        _Table(
            table_path,
            expected.natural_key,
            expected.keys,
            expected.row_count,
            expected.canonical_key("retained_result:" + "a" * 32),
        )
    )
    controls = planned_control(plan)
    existing = pl.DataFrame(schema=physical_schema)
    if prior_owner:
        existing = rows.head(1).with_columns(
            pl.lit(99, dtype=pl.Int32).alias("origin_step_number"),
            pl.lit("input_registration:" + "f" * 32).alias("logical_commit_id"),
        )
        controls = pl.concat(
            [
                controls,
                controls.with_columns(
                    pl.lit("input_registration:" + "f" * 32).alias("logical_commit_id"),
                    pl.lit("input_registration").alias("commit_kind"),
                    pl.lit("f" * 32).alias("step_run_id"),
                    pl.lit("complete").alias("state"),
                    pl.col("created_at").alias("completed_at"),
                ),
            ]
        )
    existing.write_delta(path, storage_options=options)

    def snapshot(requested: str) -> DeltaSnapshot:
        assert requested == table_path
        return DeltaSnapshot(path, physical_schema, fs=fs, storage_options=options)

    written = apply_table_effect(
        plan, plan.effect, expected, controls, read_snapshot=snapshot
    )
    assert written == (1 if prior_owner else 2)
    physical = pl.read_delta(path, storage_options=options)
    assert physical.sort("artifact_id")["origin_step_number"].to_list() == [
        99 if prior_owner else None,
        3,
    ]
    verify_plan_effect(plan, table_path, physical, controls)
    assert (
        apply_table_effect(
            plan, plan.effect, expected, controls, read_snapshot=snapshot
        )
        == 0
    )

    physical.with_columns(
        pl.when(pl.col("artifact_id") == "e" * 32)
        .then(pl.lit(4, dtype=pl.Int32))
        .otherwise(pl.col("origin_step_number"))
        .alias("origin_step_number")
    ).write_delta(path, mode="overwrite", storage_options=options)
    with pytest.raises(StoreIntegrityError, match="Artifact origin disagrees"):
        verify_snapshot_effect(plan, table_path, snapshot(table_path), controls)
    with pytest.raises(StoreIntegrityError, match="Artifact origin disagrees"):
        verify_plan_effect(
            plan, table_path, pl.read_delta(path, storage_options=options), controls
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"plan_digest": "1" * 32},
        {"commit_kind": "input_registration"},
        {"step_run_id": "2" * 32},
        {"recovery_batch_id": "3" * 32},
    ],
)
def test_logical_completion_requires_every_immutable_owner_field(
    backend_fs, changes: dict[str, str]
) -> None:
    _, storage, root = backend_fs
    options = storage.delta_storage_options()
    plan = _OriginPlan(_Table("artifacts/index", (), (), 0, "d" * 32))
    path = f"{root}/candidate/{TablePath.LOGICAL_COMMITS.value}"
    write_deltalake(
        path,
        planned_control(plan).to_arrow().cast(_arrow_schema(LOGICAL_COMMITS_SCHEMA)),
        storage_options=options,
    )
    with pytest.raises(StoreIntegrityError, match="not conditional"):
        complete_control(replace(plan, **changes), path, storage_options=options)
    assert pl.read_delta(path, storage_options=options)["state"].to_list() == [
        "planned"
    ]
    complete_control(plan, path, storage_options=options)
    assert pl.read_delta(path, storage_options=options)["state"].to_list() == [
        "complete"
    ]
