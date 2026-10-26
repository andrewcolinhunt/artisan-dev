"""Legacy plan verification from committed files after staging is gone."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from deltalake import DeltaTable, write_deltalake

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import (
    get_natural_key,
    get_physical_schema_for_path,
)
from artisan.storage.io.commit import _delta_arrow
from artisan.storage.io.commit_plan import canonical_table_plan_key
from artisan.storage.io.legacy_digest import legacy_table_plan_key

METRICS = "artifacts/metrics"
OWNER = "input_registration:" + "a" * 32


def _digest(root: Path, table: str, keys: pl.DataFrame) -> str:
    return legacy_table_plan_key(
        str(root), table, get_natural_key(table), keys, OWNER, {OWNER}, {}
    )


def _write(
    root: Path, table: str, rows: pl.DataFrame, partitions: list[str] | None = None
) -> None:
    write_deltalake(
        str(root / table), _delta_arrow(rows), mode="append", partition_by=partitions
    )


@pytest.mark.parametrize("table", [METRICS, TablePath.CACHE_REUSE.value])
def test_legacy_digest_empty_table_has_no_active_files(
    tmp_path: Path, table: str
) -> None:
    physical = pl.DataFrame(schema=get_physical_schema_for_path(table))
    partitions = ["origin_step_number"] if table == METRICS else None
    _write(tmp_path, table, physical, partitions)
    assert DeltaTable(str(tmp_path / table)).file_uris() == []
    expected = physical.drop("logical_commit_id") if table == METRICS else physical
    before = set(tmp_path.rglob("*"))

    assert _digest(tmp_path, table, physical.select(get_natural_key(table))) == (
        canonical_table_plan_key(OWNER, table, expected)
    )
    assert set(tmp_path.rglob("*")) == before


def test_legacy_digest_empty_table_still_validates_schema(tmp_path: Path) -> None:
    table = TablePath.CACHE_REUSE.value
    physical = pl.DataFrame(schema=get_physical_schema_for_path(table))
    _write(
        tmp_path,
        table,
        physical.with_columns(pl.lit(None).cast(pl.String).alias("extra")),
    )

    with pytest.raises(StoreIntegrityError, match="unexpected schema"):
        _digest(tmp_path, table, physical)


def test_legacy_digest_partitioned_payloads_match_without_collecting_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    expected = pl.DataFrame(
        [
            {
                "artifact_id": f"{number:032x}",
                "origin_step_number": number % 2,
                "content": bytes([number]) * 1024,
                "original_name": "résultat.json",
                "extension": ".json",
                "metadata": None,
            }
            for number in [3, 0, 2, 1]
        ],
        schema=MetricArtifact.POLARS_SCHEMA,
    )
    physical = expected.with_columns(pl.lit(OWNER).alias("logical_commit_id"))
    _write(tmp_path, METRICS, physical, ["origin_step_number"])
    hidden = physical.with_columns(
        pl.lit("pending").alias("logical_commit_id"),
        pl.lit(b"uncommitted").alias("content"),
    )
    _write(tmp_path, METRICS, hidden, ["origin_step_number"])
    original_collect = pl.LazyFrame.collect

    def metadata_only_collect(frame: pl.LazyFrame, *args: Any, **kwargs: Any) -> Any:
        assert "content" not in frame.collect_schema()
        return original_collect(frame, *args, **kwargs)

    expected_digest = canonical_table_plan_key(OWNER, METRICS, expected)
    before = set(tmp_path.rglob("*"))
    monkeypatch.setattr(pl.LazyFrame, "collect", metadata_only_collect)
    assert _digest(tmp_path, METRICS, physical.select("artifact_id")) == expected_digest
    assert set(tmp_path.rglob("*")) == before


def test_legacy_digest_ownerless_cache_reuse_matches_selected_keys(
    tmp_path: Path,
) -> None:
    table = TablePath.CACHE_REUSE.value
    expected = pl.DataFrame(
        {"current_step_run_id": ["step", "step"], "cached_execution_run_id": ["z", "a"]}
    )
    _write(tmp_path, table, expected)
    _write(
        tmp_path,
        table,
        expected.with_columns(pl.lit("other").alias("current_step_run_id")),
    )

    assert _digest(tmp_path, table, expected) == canonical_table_plan_key(
        OWNER, table, expected
    )


def test_legacy_digest_nullable_composite_keys_match(tmp_path: Path) -> None:
    table = TablePath.EXECUTION_EDGES.value
    physical = pl.DataFrame(
        {
            "execution_run_id": ["execution", "execution"],
            "direction": ["output", "output"],
            "role": ["named", None],
            "artifact_id": ["z", "a"],
            "logical_commit_id": [OWNER, OWNER],
        },
        schema=get_physical_schema_for_path(table),
    )
    _write(tmp_path, table, physical)
    expected = physical.drop("logical_commit_id")

    assert _digest(tmp_path, table, expected) == canonical_table_plan_key(
        OWNER, table, expected
    )


@pytest.mark.parametrize("damage", ["missing", "duplicate"])
def test_legacy_digest_rejects_missing_or_duplicate_rows(
    tmp_path: Path, damage: str
) -> None:
    table = TablePath.CACHE_REUSE.value
    expected = pl.DataFrame(
        {"current_step_run_id": ["step", "step"], "cached_execution_run_id": ["z", "a"]}
    )
    physical = (
        expected.head(1)
        if damage == "missing"
        else pl.concat([expected, expected.head(1)])
    )
    _write(tmp_path, table, physical)

    with pytest.raises(StoreIntegrityError, match="missing or duplicate"):
        _digest(tmp_path, table, expected)


def test_legacy_digest_timestamps_preserve_canonical_values(tmp_path: Path) -> None:
    table = TablePath.EXECUTIONS.value
    schema = get_physical_schema_for_path(table)
    physical = pl.DataFrame(
        [
            dict.fromkeys(schema)
            | {
                "execution_run_id": str(number),
                "timestamp_start": datetime(2025, 1, 1, 0, 0, number, tzinfo=UTC),
                "origin_step_number": number,
                "logical_commit_id": OWNER,
            }
            for number in [1, 0]
        ],
        schema=schema,
    )
    _write(tmp_path, table, physical, ["origin_step_number"])
    expected = physical.drop("logical_commit_id")

    assert _digest(tmp_path, table, expected.select(get_natural_key(table))) == (
        canonical_table_plan_key(OWNER, table, expected)
    )
