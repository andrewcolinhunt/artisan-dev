"""Disk table evidence preserves canonical plans and typed duplicate semantics."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest

from artisan.errors import StoreIntegrityError
from artisan.storage.io.bounded_evidence import DiskTable
from artisan.storage.io.bounded_io import write_parquet_rows
from artisan.storage.io.commit_plan import canonical_table_plan_key

SCHEMA = {
    "artifact_id": pl.String,
    "origin_step_number": pl.Int32,
    "content": pl.Binary,
    "moment": pl.Datetime("us", "UTC"),
    "value": pl.Float64,
    "tags": pl.List(pl.String),
}
TABLE_PATH = "artifacts/custom_payloads"


def _row(key="a", *, value=1.0, origin=3):
    return {
        "artifact_id": key,
        "origin_step_number": origin,
        "content": b"\x00\xffpayload",
        "moment": datetime(2026, 10, 28, 12, 3, tzinfo=UTC),
        "value": value,
        "tags": ["snowman \u2603", None],
    }


def _table(tmp_path, rows, *, paths=1):
    filenames = []
    for index in range(paths):
        path = str(tmp_path / f"part-{index}.parquet")
        write_parquet_rows(path, rows[index::paths], SCHEMA, max_bytes=256)
        filenames.append(path)
    return DiskTable(TABLE_PATH, filenames, str(tmp_path)), filenames


@pytest.mark.parametrize("value", [0.0, -0.0, float("nan"), float("inf"), None])
def test_canonical_key_matches_legacy_with_typed_special_values(tmp_path, value):
    rows = [_row("z", value=value), _row(None), _row("a")]
    table, paths = _table(tmp_path, rows, paths=2)
    source_order = pl.concat([pl.read_parquet(path) for path in paths])

    assert table.canonical_key("commit:one") == canonical_table_plan_key(
        "commit:one", TABLE_PATH, source_order
    )
    assert table.keys == ((None,), ("a",), ("z",))


@pytest.mark.parametrize("values", [(0.0, -0.0), (float("nan"), float("nan"))])
def test_global_duplicate_uses_typed_equality_and_first_representative(
    tmp_path, values
):
    rows = [_row(value=values[0]), _row(value=values[1])]
    table, _paths = _table(tmp_path, rows)

    assert table.row_count == 1
    assert table.canonical_key("owner") == canonical_table_plan_key(
        "owner", TABLE_PATH, pl.DataFrame(rows, schema=SCHEMA)
    )
    assert sum(frame.height for frame in table.iter_frames()) == 1


@pytest.mark.parametrize("different", [_row(value=2.0), _row(origin=4)])
def test_global_duplicate_rejects_value_or_origin_conflict(tmp_path, different):
    with pytest.raises(StoreIntegrityError, match="Conflicting natural keys"):
        _table(tmp_path, [_row(), different])


def test_cross_commit_comparison_omits_origin_only_when_requested(tmp_path):
    table, _paths = _table(tmp_path, [_row(value=-0.0)])

    assert table.matches_row(("a",), _row(value=0.0))
    assert not table.matches_row(("a",), _row(value=0.0, origin=8))
    assert table.matches_row(("a",), _row(value=0.0, origin=8), comparable=True)
    assert not table.matches_row(("a",), _row(value=2.0), comparable=True)


def test_duplicate_non_global_keys_fail(tmp_path):
    path = str(tmp_path / "steps.parquet")
    schema = {"step_run_id": pl.String, "state_sequence": pl.Int64}
    rows = [{"step_run_id": "a", "state_sequence": 1}] * 2
    write_parquet_rows(path, rows, schema)

    with pytest.raises(StoreIntegrityError, match="Duplicate natural keys"):
        DiskTable("orchestration/steps", [path], str(tmp_path))


def test_duplicate_nanosecond_timestamps_preserve_typed_conflict(tmp_path):
    schema = {"artifact_id": pl.String, "moment": pl.Datetime("ns")}
    frame = pl.DataFrame({"artifact_id": ["a", "a"], "moment": [1, 2]}).cast(schema)
    path = str(tmp_path / "nanoseconds.parquet")
    frame.write_parquet(path)

    with pytest.raises(StoreIntegrityError, match="Conflicting natural keys"):
        canonical_table_plan_key("owner", TABLE_PATH, frame)
    with pytest.raises(StoreIntegrityError, match="Conflicting natural keys"):
        DiskTable(TABLE_PATH, [path], str(tmp_path))


def test_projection_and_selection_keep_source_order_without_eager_reads(
    tmp_path, monkeypatch
):
    rows = [_row("z"), _row("a"), _row("a"), _row("b")]
    table, _paths = _table(tmp_path, rows)

    def forbid_eager(*_args, **_kwargs):
        msg = "eager Parquet read"
        raise AssertionError(msg)

    monkeypatch.setattr(pl, "read_parquet", forbid_eager)
    selected = list(table.iter_frames(columns=["artifact_id"], keys={("z",), ("b",)}))

    assert pl.concat(selected)["artifact_id"].to_list() == ["z", "b"]
    assert all(frame.columns == ["artifact_id"] for frame in selected)
    assert table.get_row(("b",)) == rows[-1]
