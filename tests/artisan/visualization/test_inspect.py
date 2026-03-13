"""Tests for artisan.visualization.inspect."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from artisan.utils.dicts import flatten_dict as _flatten_dict
from artisan.visualization.inspect import (
    _build_details,
    inspect_data,
    inspect_metrics,
    inspect_pipeline,
    inspect_step,
)

# ======================================================================
# Fixtures: write minimal Delta tables to tmp_path
# ======================================================================

# Schemas match the subset of columns each inspect function reads.
# All columns typed to avoid Null dtype errors in write_delta.

STEPS_SCHEMA = {
    "step_run_id": pl.String,
    "step_spec_id": pl.String,
    "pipeline_run_id": pl.String,
    "step_number": pl.Int32,
    "step_name": pl.String,
    "status": pl.String,
    "operation_class": pl.String,
    "params_json": pl.String,
    "input_refs_json": pl.String,
    "compute_backend": pl.String,
    "compute_options_json": pl.String,
    "output_roles_json": pl.String,
    "output_types_json": pl.String,
    "total_count": pl.Int32,
    "succeeded_count": pl.Int32,
    "failed_count": pl.Int32,
    "timestamp": pl.String,
    "duration_seconds": pl.Float64,
    "error": pl.String,
    "dispatch_error": pl.String,
    "commit_error": pl.String,
    "metadata": pl.String,
}

INDEX_SCHEMA = {
    "artifact_id": pl.String,
    "artifact_type": pl.String,
    "origin_step_number": pl.Int32,
    "metadata": pl.String,
}

DATA_SCHEMA = {
    "artifact_id": pl.String,
    "origin_step_number": pl.Int32,
    "content": pl.Binary,
    "original_name": pl.String,
    "extension": pl.String,
    "size_bytes": pl.Int64,
    "columns": pl.String,
    "row_count": pl.Int32,
    "metadata": pl.String,
    "external_path": pl.String,
}

METRICS_SCHEMA = {
    "artifact_id": pl.String,
    "origin_step_number": pl.Int32,
    "content": pl.Binary,
    "original_name": pl.String,
    "extension": pl.String,
    "metadata": pl.String,
    "external_path": pl.String,
}


def _write_delta(
    delta_root: Path, rel_path: str, rows: list[dict], schema: dict
) -> None:
    table_path = delta_root / rel_path
    table_path.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(rows, schema=schema)
    df.write_delta(str(table_path))


def _write_steps(delta_root: Path, rows: list[dict]) -> None:
    _write_delta(delta_root, "orchestration/steps", rows, STEPS_SCHEMA)


def _write_index(delta_root: Path, rows: list[dict]) -> None:
    _write_delta(delta_root, "artifacts/index", rows, INDEX_SCHEMA)


def _write_data(delta_root: Path, rows: list[dict]) -> None:
    _write_delta(delta_root, "artifacts/data", rows, DATA_SCHEMA)


def _write_metrics(delta_root: Path, rows: list[dict]) -> None:
    _write_delta(delta_root, "artifacts/metrics", rows, METRICS_SCHEMA)


def _csv_bytes(header: str, data_rows: list[str]) -> bytes:
    return ("\n".join([header, *data_rows])).encode("utf-8")


def _metric_bytes(values: dict) -> bytes:
    return json.dumps(values).encode("utf-8")


def _step_row(
    *,
    step_number: int,
    step_name: str,
    pipeline_run_id: str = "run1",
    operation_class: str = "SomeOp",
    succeeded_count: int = 3,
    total_count: int = 3,
    duration_seconds: float = 0.5,
) -> dict:
    """Build a minimal completed step row."""
    return {
        "step_run_id": f"sr{step_number}",
        "step_spec_id": f"ss{step_number}",
        "pipeline_run_id": pipeline_run_id,
        "step_number": step_number,
        "step_name": step_name,
        "status": "completed",
        "operation_class": operation_class,
        "params_json": "{}",
        "input_refs_json": "{}",
        "compute_backend": "local",
        "compute_options_json": "{}",
        "output_roles_json": "[]",
        "output_types_json": "{}",
        "total_count": total_count,
        "succeeded_count": succeeded_count,
        "failed_count": total_count - succeeded_count,
        "timestamp": "2026-01-01T00:00:00",
        "duration_seconds": duration_seconds,
        "error": "",
        "dispatch_error": None,
        "commit_error": None,
        "metadata": "{}",
    }


# ======================================================================
# inspect_pipeline tests
# ======================================================================


def test_inspect_pipeline_basic(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    _write_steps(
        delta_root,
        [
            _step_row(step_number=0, step_name="data_generator"),
            _step_row(
                step_number=1, step_name="metric_calculator", duration_seconds=0.3
            ),
        ],
    )
    _write_index(
        delta_root,
        [
            {
                "artifact_id": "a1",
                "artifact_type": "data",
                "origin_step_number": 0,
                "metadata": "{}",
            },
            {
                "artifact_id": "a2",
                "artifact_type": "data",
                "origin_step_number": 0,
                "metadata": "{}",
            },
            {
                "artifact_id": "a3",
                "artifact_type": "data",
                "origin_step_number": 0,
                "metadata": "{}",
            },
            {
                "artifact_id": "m1",
                "artifact_type": "metric",
                "origin_step_number": 1,
                "metadata": "{}",
            },
            {
                "artifact_id": "m2",
                "artifact_type": "metric",
                "origin_step_number": 1,
                "metadata": "{}",
            },
            {
                "artifact_id": "m3",
                "artifact_type": "metric",
                "origin_step_number": 1,
                "metadata": "{}",
            },
        ],
    )

    result = inspect_pipeline(delta_root)
    assert result.shape[0] == 2
    assert result.columns == ["step", "operation", "status", "produced", "duration"]
    assert result["produced"][0] == "3 data"
    assert result["produced"][1] == "3 metric"


def test_inspect_pipeline_filter_step(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    _write_steps(
        delta_root,
        [
            _step_row(
                step_number=0,
                step_name="filter",
                operation_class="artisan.operations.curator.Filter",
                total_count=5,
                succeeded_count=2,
            ),
        ],
    )

    result = inspect_pipeline(delta_root)
    assert result["produced"][0] == "2 passed"


def test_inspect_pipeline_skipped_steps(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    skipped_row = _step_row(step_number=1, step_name="data_transformer")
    skipped_row["status"] = "skipped"
    skipped_row["succeeded_count"] = 0
    skipped_row["duration_seconds"] = 0.0
    _write_steps(
        delta_root,
        [
            _step_row(step_number=0, step_name="data_generator"),
            skipped_row,
        ],
    )
    _write_index(
        delta_root,
        [
            {
                "artifact_id": "a1",
                "artifact_type": "data",
                "origin_step_number": 0,
                "metadata": "{}",
            },
        ],
    )

    result = inspect_pipeline(delta_root)
    assert result.shape[0] == 2
    assert result["status"][0] == "ok"
    assert result["status"][1] == "skipped"
    assert result["produced"][1] == "-"
    assert result["duration"][1] == "-"


def test_inspect_pipeline_cancelled_steps(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    cancelled_row = _step_row(step_number=1, step_name="data_transformer")
    cancelled_row["status"] = "cancelled"
    cancelled_row["succeeded_count"] = None
    cancelled_row["duration_seconds"] = None
    _write_steps(
        delta_root,
        [
            _step_row(step_number=0, step_name="data_generator"),
            cancelled_row,
        ],
    )
    _write_index(
        delta_root,
        [
            {
                "artifact_id": "a1",
                "artifact_type": "data",
                "origin_step_number": 0,
                "metadata": "{}",
            },
        ],
    )

    result = inspect_pipeline(delta_root)
    assert result.shape[0] == 2
    assert result["status"][0] == "ok"
    assert result["status"][1] == "cancelled"
    assert result["produced"][1] == "-"
    assert result["duration"][1] == "-"


def test_inspect_pipeline_no_steps_raises(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    with pytest.raises(FileNotFoundError):
        inspect_pipeline(delta_root)


# ======================================================================
# inspect_step tests
# ======================================================================


def test_inspect_step_data_artifacts(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    _write_index(
        delta_root,
        [
            {
                "artifact_id": "a1",
                "artifact_type": "data",
                "origin_step_number": 0,
                "metadata": "{}",
            },
            {
                "artifact_id": "a2",
                "artifact_type": "data",
                "origin_step_number": 0,
                "metadata": "{}",
            },
        ],
    )
    _write_data(
        delta_root,
        [
            {
                "artifact_id": "a1",
                "origin_step_number": 0,
                "content": _csv_bytes("x,y,z", ["1,2,3"] * 10),
                "original_name": "dataset_00000",
                "extension": ".csv",
                "size_bytes": 100,
                "columns": json.dumps(["x", "y", "z"]),
                "row_count": 10,
                "metadata": "{}",
                "external_path": "",
            },
            {
                "artifact_id": "a2",
                "origin_step_number": 0,
                "content": _csv_bytes("x,y,z", ["4,5,6"] * 5),
                "original_name": "dataset_00001",
                "extension": ".csv",
                "size_bytes": 50,
                "columns": json.dumps(["x", "y", "z"]),
                "row_count": 5,
                "metadata": "{}",
                "external_path": "",
            },
        ],
    )

    result = inspect_step(delta_root, step_number=0)
    assert result.shape[0] == 2
    assert result.columns == ["name", "artifact_type", "step", "details"]
    details = result.sort("name")["details"].to_list()
    assert details[0] == "10 rows, 3 cols"
    assert details[1] == "5 rows, 3 cols"


def test_inspect_step_metric_artifacts(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    _write_index(
        delta_root,
        [
            {
                "artifact_id": "m1",
                "artifact_type": "metric",
                "origin_step_number": 1,
                "metadata": "{}",
            },
        ],
    )
    _write_metrics(
        delta_root,
        [
            {
                "artifact_id": "m1",
                "origin_step_number": 1,
                "content": _metric_bytes({"mean_score": 0.5, "std_score": 0.1}),
                "original_name": "d0_metrics",
                "extension": ".json",
                "metadata": "{}",
                "external_path": "",
            },
        ],
    )

    result = inspect_step(delta_root, step_number=1)
    assert result.shape[0] == 1
    assert "mean_score" in result["details"][0]


def test_inspect_step_empty(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    _write_index(
        delta_root,
        [
            {
                "artifact_id": "a1",
                "artifact_type": "data",
                "origin_step_number": 0,
                "metadata": "{}",
            },
        ],
    )

    result = inspect_step(delta_root, step_number=99)
    assert result.is_empty()
    assert result.columns == ["name", "artifact_type", "step", "details"]


# ======================================================================
# inspect_metrics tests
# ======================================================================


def test_inspect_metrics_basic(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    _write_metrics(
        delta_root,
        [
            {
                "artifact_id": f"m{i}",
                "origin_step_number": 1,
                "content": _metric_bytes(
                    {
                        "mean_score": 0.1 * i,
                        "std_score": 0.01 * i,
                        "min_score": 0.05 * i,
                        "max_score": 0.2 * i,
                    }
                ),
                "original_name": f"d{i}_metrics",
                "extension": ".json",
                "metadata": "{}",
                "external_path": "",
            }
            for i in range(3)
        ],
    )

    result = inspect_metrics(delta_root)
    assert result.shape == (3, 6)  # name, step, + 4 metric columns
    assert "mean_score" in result.columns
    assert "std_score" in result.columns


def test_inspect_metrics_rounding(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    _write_metrics(
        delta_root,
        [
            {
                "artifact_id": "m1",
                "origin_step_number": 1,
                "content": _metric_bytes({"score": 0.123456789}),
                "original_name": "d0_metrics",
                "extension": ".json",
                "metadata": "{}",
                "external_path": "",
            },
        ],
    )

    result = inspect_metrics(delta_root, round_digits=3)
    assert result["score"][0] == 0.123


def test_inspect_metrics_nested(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    _write_metrics(
        delta_root,
        [
            {
                "artifact_id": "m1",
                "origin_step_number": 1,
                "content": _metric_bytes({"a": {"b": 1.0, "c": 2.0}}),
                "original_name": "d0_metrics",
                "extension": ".json",
                "metadata": "{}",
                "external_path": "",
            },
        ],
    )

    result = inspect_metrics(delta_root)
    assert "a.b" in result.columns
    assert "a.c" in result.columns


def test_inspect_metrics_filter_step(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    _write_metrics(
        delta_root,
        [
            {
                "artifact_id": "m1",
                "origin_step_number": 1,
                "content": _metric_bytes({"score": 0.5}),
                "original_name": "d0_metrics",
                "extension": ".json",
                "metadata": "{}",
                "external_path": "",
            },
            {
                "artifact_id": "m2",
                "origin_step_number": 2,
                "content": _metric_bytes({"score": 0.8}),
                "original_name": "d1_metrics",
                "extension": ".json",
                "metadata": "{}",
                "external_path": "",
            },
        ],
    )

    result = inspect_metrics(delta_root, step_number=1)
    assert result.shape[0] == 1
    assert result["name"][0] == "d0"


def test_inspect_metrics_no_table_raises(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    with pytest.raises(FileNotFoundError):
        inspect_metrics(delta_root)


# ======================================================================
# inspect_data tests
# ======================================================================


def test_inspect_data_by_name(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    csv = _csv_bytes("x,y,z", ["1,2,3", "4,5,6"])
    _write_data(
        delta_root,
        [
            {
                "artifact_id": "a1",
                "origin_step_number": 0,
                "content": csv,
                "original_name": "my_data",
                "extension": ".csv",
                "size_bytes": len(csv),
                "columns": json.dumps(["x", "y", "z"]),
                "row_count": 2,
                "metadata": "{}",
                "external_path": "",
            },
        ],
    )

    result = inspect_data(delta_root, name="my_data")
    assert result.shape == (2, 3)
    assert result.columns == ["x", "y", "z"]


def test_inspect_data_by_step(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    csv0 = _csv_bytes("a,b", ["1,2"])
    csv1 = _csv_bytes("a,b", ["3,4"])
    _write_data(
        delta_root,
        [
            {
                "artifact_id": "a1",
                "origin_step_number": 0,
                "content": csv0,
                "original_name": "d0",
                "extension": ".csv",
                "size_bytes": len(csv0),
                "columns": json.dumps(["a", "b"]),
                "row_count": 1,
                "metadata": "{}",
                "external_path": "",
            },
            {
                "artifact_id": "a2",
                "origin_step_number": 1,
                "content": csv1,
                "original_name": "d1",
                "extension": ".csv",
                "size_bytes": len(csv1),
                "columns": json.dumps(["a", "b"]),
                "row_count": 1,
                "metadata": "{}",
                "external_path": "",
            },
        ],
    )

    result = inspect_data(delta_root, step_number=0)
    assert result.shape[0] == 1
    assert "_source" in result.columns


def test_inspect_data_not_found(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    _write_data(
        delta_root,
        [
            {
                "artifact_id": "a1",
                "origin_step_number": 0,
                "content": _csv_bytes("x", ["1"]),
                "original_name": "exists",
                "extension": ".csv",
                "size_bytes": 10,
                "columns": json.dumps(["x"]),
                "row_count": 1,
                "metadata": "{}",
                "external_path": "",
            },
        ],
    )

    with pytest.raises(ValueError, match="No matching"):
        inspect_data(delta_root, name="nonexistent")


# ======================================================================
# _flatten_dict tests
# ======================================================================


# ======================================================================
# _build_details tests
# ======================================================================


def test_build_details_data_rows_and_cols() -> None:
    row = {"row_count": 10, "columns": json.dumps(["x", "y", "z"])}
    assert _build_details("data", row) == "10 rows, 3 cols"


def test_build_details_data_size_fallback() -> None:
    row = {"row_count": None, "columns": None, "size_bytes": 1_500_000}
    assert _build_details("data", row) == "1.5 MB"


def test_build_details_data_no_info() -> None:
    row = {"row_count": None, "columns": None, "size_bytes": None}
    assert _build_details("data", row) == "-"


def test_build_details_file_ref_size() -> None:
    row = {"size_bytes": 2_500}
    assert _build_details("file_ref", row) == "2.5 KB"


def test_build_details_metric_keys() -> None:
    content = json.dumps({"mean": 0.5, "std": 0.1}).encode("utf-8")
    row = {"content": content}
    assert _build_details("metric", row) == "mean, std"


def test_build_details_config_params() -> None:
    content = json.dumps({"lr": 0.01, "epochs": 10, "batch": 32}).encode("utf-8")
    row = {"content": content}
    assert _build_details("config", row) == "3 params"


def test_build_details_unknown_type() -> None:
    assert _build_details("unknown", {}) == "-"


def test_flatten_dict_simple() -> None:
    assert _flatten_dict({"a": 1, "b": 2}) == {"a": 1, "b": 2}


def test_flatten_dict_nested() -> None:
    assert _flatten_dict({"a": {"b": 1}}) == {"a.b": 1}


def test_flatten_dict_deeply_nested() -> None:
    result = _flatten_dict({"a": {"b": {"c": 3}}})
    assert result == {"a.b.c": 3}
