"""Tests for artisan.visualization.inspect."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest
from fixtures.cache_isolation_store import build_cache_isolation_store
from fixtures.execution_records import executions_df
from fixtures.logical_commit_store import commit_test_inputs, commit_test_step
from fixtures.store_format import publish_test_store
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import ArtisanError, ErrorCode
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import (
    CACHE_REUSE_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
    EXECUTIONS_SCHEMA,
)
from artisan.utils.log_paths import failure_log_relative_path
from artisan.visualization.inspect import (
    _build_details,
    inspect_data,
    inspect_failures,
    inspect_metrics,
    inspect_pipeline,
    inspect_step,
)

# ======================================================================
# Fixtures: write minimal Delta tables to tmp_path
# ======================================================================

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
}

METRICS_SCHEMA = {
    "artifact_id": pl.String,
    "origin_step_number": pl.Int32,
    "content": pl.Binary,
    "original_name": pl.String,
    "extension": pl.String,
    "metadata": pl.String,
}


@pytest.fixture(autouse=True)
def _format_root(tmp_path: Path) -> None:
    publish_test_store(str(tmp_path / "delta"), LocalFileSystem())


def _write_delta(
    delta_root: Path, rel_path: str, rows: list[dict], schema: dict
) -> None:
    df = pl.DataFrame(rows, schema=schema)
    if df.is_empty():
        return
    table_path = rel_path.value if isinstance(rel_path, TablePath) else rel_path
    commit_test_inputs(
        delta_root,
        delta_root.parent / "staging",
        {table_path: df},
    )


def _write_steps(delta_root: Path, rows: list[dict]) -> None:
    for row in rows:
        execution = {
            "execution_run_id": f"exec-{row['step_run_id']}",
            "execution_spec_id": f"execution-spec-{row['step_number']}",
            "step_run_id": row["step_run_id"],
            "origin_step_number": row["step_number"],
            "operation_name": row["step_name"],
            "params": "{}",
            "user_overrides": "{}",
            "timestamp_start": datetime(2026, 1, 1, tzinfo=UTC),
            "timestamp_end": datetime(2026, 1, 1, tzinfo=UTC),
            "source_worker": 0,
            "compute_backend": "local",
            "success": row["status"] in {"succeeded", "partial"},
            "error": row.get("error"),
            "error_envelope": None,
            "tool_output": None,
            "worker_log": None,
            "metadata": "{}",
        }
        tables = {}
        if row["status"] not in {"pending", "skipped", "cancelled"}:
            tables[TablePath.EXECUTIONS.value] = pl.DataFrame(
                [execution], schema=EXECUTIONS_SCHEMA
            )
        commit_test_step(
            delta_root,
            delta_root.parent / "staging",
            [row],
            tables,
        )


def _write_index(delta_root: Path, rows: list[dict]) -> None:
    _write_delta(delta_root, "artifacts/index", rows, INDEX_SCHEMA)
    edges = [
        {
            "execution_run_id": f"exec-sr{row['origin_step_number']}",
            "direction": "output",
            "role": "output",
            "artifact_id": row["artifact_id"],
        }
        for row in rows
    ]
    _write_delta(
        delta_root,
        TablePath.EXECUTION_EDGES,
        edges,
        EXECUTION_EDGES_SCHEMA,
    )


def _write_data(delta_root: Path, rows: list[dict]) -> None:
    _write_delta(delta_root, "artifacts/data", rows, DATA_SCHEMA)


def _write_metrics(delta_root: Path, rows: list[dict]) -> None:
    _write_delta(delta_root, "artifacts/metrics", rows, METRICS_SCHEMA)


def _write_executions(delta_root: Path, df: pl.DataFrame) -> None:
    rows = df.to_dicts()
    for index, row in enumerate(rows):
        step_run_id = (
            row["step_run_id"] or f"fixture-step-{index}-{row['execution_run_id']}"
        )
        step_number = int(row["origin_step_number"])
        row["step_run_id"] = step_run_id
        row["timestamp_start"] = row["timestamp_start"] or datetime(
            2026, 1, 1, tzinfo=UTC
        )
        terminal = _step_row(
            step_number=step_number,
            step_name=str(row["operation_name"]),
            pipeline_run_id="other-run" if step_run_id == "sr_other" else "run1",
            succeeded_count=1 if row["success"] else 0,
            total_count=1,
        )
        terminal["step_run_id"] = step_run_id
        terminal["error"] = row["error"]
        commit_test_step(
            delta_root,
            delta_root.parent / "staging",
            [terminal],
            {TablePath.EXECUTIONS.value: pl.DataFrame([row], schema=EXECUTIONS_SCHEMA)},
        )


def _envelope_json(**overrides) -> str:
    """Serialize an ArtisanError envelope dict the way the recorder does."""
    kwargs = {
        "code": ErrorCode.OP_EXECUTE_FAILED,
        "message": "boom",
        "error_type": "compute",
        "recovery_hint": "REPORT_TO_USER",
    }
    kwargs.update(overrides)
    return json.dumps(ArtisanError(**kwargs).to_dict())


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
    """Build a minimal authoritative terminal step row."""
    failed_count = total_count - succeeded_count
    if failed_count == 0:
        status = "succeeded"
    elif succeeded_count:
        status = "partial"
    else:
        status = "failed"
    return {
        "step_run_id": f"sr{step_number}",
        "step_spec_id": f"ss{step_number}",
        "pipeline_run_id": pipeline_run_id,
        "step_number": step_number,
        "step_name": step_name,
        "status": status,
        "state_sequence": 2,
        "disposition": "executed" if status != "failed" else None,
        "cancellation_status": None,
        "logical_commit_id": None,
        "operation_class": operation_class,
        "params_json": "{}",
        "input_refs_json": "{}",
        "compute_backend": "local",
        "compute_options_json": "{}",
        "output_roles_json": "[]",
        "output_types_json": "{}",
        "total_count": total_count,
        "succeeded_count": succeeded_count,
        "failed_count": failed_count,
        "timestamp": datetime(2026, 1, 1, tzinfo=UTC),
        "duration_seconds": duration_seconds,
        "error": "step failed" if status == "failed" else None,
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
    assert result["status"][0] == "succeeded"
    assert result["status"][1] == "skipped"
    assert result["produced"][1] == "-"
    assert result["duration"][1] == "-"


def test_inspect_pipeline_cancelled_steps(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    cancelled_row = _step_row(step_number=1, step_name="data_transformer")
    cancelled_row["status"] = "cancelled"
    cancelled_row["total_count"] = 0
    cancelled_row["succeeded_count"] = 0
    cancelled_row["failed_count"] = 0
    cancelled_row["disposition"] = None
    cancelled_row["cancellation_status"] = "confirmed"
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
    assert result["status"][0] == "succeeded"
    assert result["status"][1] == "cancelled"
    assert result["produced"][1] == "-"
    assert result["duration"][1] == "-"


def test_inspect_pipeline_empty_store_is_empty(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    assert inspect_pipeline(delta_root).is_empty()


def test_inspect_pipeline_failed_steps(tmp_path: Path) -> None:
    """A whole-step failure shows as status='failed', not hidden."""
    delta_root = tmp_path / "delta"
    failed_row = _step_row(step_number=1, step_name="transform")
    failed_row["status"] = "failed"
    failed_row["disposition"] = None
    failed_row["total_count"] = 1
    failed_row["succeeded_count"] = 0
    failed_row["failed_count"] = 1
    failed_row["error"] = "step failed"
    _write_steps(
        delta_root,
        [
            _step_row(step_number=0, step_name="data_generator"),
            failed_row,
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
    assert result["status"][0] == "succeeded"
    assert result["status"][1] == "failed"
    assert result["produced"][1] == "-"
    assert result["duration"][1] == "-"


def test_inspect_pipeline_all_units_failed_preserves_failed(tmp_path: Path) -> None:
    """A step with every unit failed preserves authoritative failed status."""
    delta_root = tmp_path / "delta"
    row = _step_row(
        step_number=0, step_name="transform", total_count=1, succeeded_count=0
    )
    assert row["status"] == "failed"
    assert row["failed_count"] == 1
    _write_steps(delta_root, [row])

    result = inspect_pipeline(delta_root)
    assert result["status"][0] == "failed"


def test_inspect_pipeline_mixed_units_preserves_partial(
    tmp_path: Path,
) -> None:
    """A mixed step preserves authoritative partial status."""
    delta_root = tmp_path / "delta"
    row = _step_row(
        step_number=0, step_name="transform", total_count=3, succeeded_count=2
    )
    assert row["failed_count"] == 1
    _write_steps(delta_root, [row])

    result = inspect_pipeline(delta_root)
    assert result["status"][0] == "partial"


def test_inspect_pipeline_filter_uses_authoritative_succeeded(tmp_path: Path) -> None:
    """Filtered-out artifacts do not become failed execution units."""
    delta_root = tmp_path / "delta"
    row = _step_row(
        step_number=0,
        step_name="filter",
        operation_class="artisan.operations.curator.Filter",
        total_count=2,
        succeeded_count=2,
    )
    assert row["failed_count"] == 0
    _write_steps(delta_root, [row])

    result = inspect_pipeline(delta_root)
    assert result["status"][0] == "succeeded"


# ======================================================================
# inspect_failures tests
# ======================================================================


def test_inspect_failures_structured(tmp_path: Path) -> None:
    """A failed execution with an envelope surfaces its structured fields."""
    delta_root = tmp_path / "delta"
    _write_executions(
        delta_root,
        executions_df(
            execution_run_id=["run_fail"],
            origin_step_number=[1],
            operation_name=["transform"],
            success=[False],
            error=["boom traceback"],
            error_envelope=[
                _envelope_json(field="params.scale", suggestions=["scale_factor"])
            ],
        ),
    )

    result = inspect_failures(delta_root)
    assert result.shape[0] == 1
    assert result.columns == [
        "step",
        "operation",
        "execution_run_id",
        "timestamp_start",
        "code",
        "recovery_hint",
        "field",
        "suggestions",
        "error",
        "log",
    ]
    row = result.to_dicts()[0]
    assert row["step"] == 1
    assert row["operation"] == "transform"
    assert row["code"] == "op_execute_failed"
    assert row["recovery_hint"] == "REPORT_TO_USER"
    assert row["field"] == "params.scale"
    assert row["suggestions"] == ["scale_factor"]
    assert row["error"] == "boom traceback"
    assert row["log"] == failure_log_relative_path("run_fail", row["timestamp_start"])


def test_inspect_failures_unstructured_degrades(tmp_path: Path) -> None:
    """A failure with no envelope shows string + log, null structured fields."""
    delta_root = tmp_path / "delta"
    _write_executions(
        delta_root,
        executions_df(
            execution_run_id=["run_plain"],
            origin_step_number=[2],
            operation_name=["transform"],
            success=[False],
            error=["ValueError: bad"],
            error_envelope=[None],
        ),
    )

    result = inspect_failures(delta_root)
    row = result.to_dicts()[0]
    assert row["code"] is None
    assert row["recovery_hint"] is None
    assert row["field"] is None
    assert row["suggestions"] is None
    assert row["error"] == "ValueError: bad"
    assert row["log"] == failure_log_relative_path("run_plain", row["timestamp_start"])


def test_inspect_failures_only_failed_rows(tmp_path: Path) -> None:
    """Successful executions are excluded."""
    delta_root = tmp_path / "delta"
    _write_executions(
        delta_root,
        executions_df(
            execution_run_id=["ok1", "fail1", "ok2"],
            origin_step_number=[0, 1, 2],
            operation_name=["a", "b", "c"],
            success=[True, False, True],
            error=[None, "boom", None],
            error_envelope=[None, _envelope_json(), None],
        ),
    )

    result = inspect_failures(delta_root)
    assert result.shape[0] == 1
    assert result["execution_run_id"][0] == "fail1"


def test_inspect_failures_empty_when_no_failures(tmp_path: Path) -> None:
    """All-success table yields a fixed-schema empty frame."""
    delta_root = tmp_path / "delta"
    _write_executions(
        delta_root,
        executions_df(
            execution_run_id=["ok1"],
            origin_step_number=[0],
            operation_name=["a"],
            success=[True],
        ),
    )

    result = inspect_failures(delta_root)
    assert result.is_empty()
    assert result.columns == [
        "step",
        "operation",
        "execution_run_id",
        "timestamp_start",
        "code",
        "recovery_hint",
        "field",
        "suggestions",
        "error",
        "log",
    ]


def test_inspect_failures_pipeline_run_id_filter(tmp_path: Path) -> None:
    """pipeline_run_id filters via the steps join on step_run_id."""
    delta_root = tmp_path / "delta"
    _write_executions(
        delta_root,
        executions_df(
            execution_run_id=["in_run", "other_run"],
            step_run_id=["sr1", "sr_other"],
            origin_step_number=[1, 1],
            operation_name=["transform", "transform"],
            success=[False, False],
            error=["a", "b"],
            error_envelope=[_envelope_json(), _envelope_json()],
        ),
    )

    result = inspect_failures(delta_root, pipeline_run_id="run1")
    assert result.shape[0] == 1
    assert result["execution_run_id"][0] == "in_run"


def test_inspect_failures_empty_store_is_empty(tmp_path: Path) -> None:
    """An initialized store without executions returns the empty schema."""
    delta_root = tmp_path / "delta"
    assert inspect_failures(delta_root).is_empty()


def test_inspect_failures_steps_but_no_executions_returns_empty(tmp_path: Path) -> None:
    """A real store where nothing executed yet yields the empty frame, not a raise."""
    delta_root = tmp_path / "delta"
    _write_steps(delta_root, [_step_row(step_number=0, step_name="data_generator")])

    result = inspect_failures(delta_root)
    assert result.is_empty()
    assert result.columns == [
        "step",
        "operation",
        "execution_run_id",
        "timestamp_start",
        "code",
        "recovery_hint",
        "field",
        "suggestions",
        "error",
        "log",
    ]


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


def test_inspect_metrics_empty_store_is_empty(tmp_path: Path) -> None:
    delta_root = tmp_path / "delta"
    assert inspect_metrics(delta_root).is_empty()


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


# ======================================================================
# storage_options forwarding tests
# ======================================================================


def test_inspect_pipeline_forwards_storage_options(tmp_path: Path) -> None:
    """inspect_pipeline forwards storage_options to pl.scan_delta."""
    delta_root = tmp_path / "delta"
    _write_steps(
        delta_root,
        [_step_row(step_number=0, step_name="data_generator")],
    )
    opts = {"key": "val"}

    with patch(
        "artisan.visualization.inspect.pl.scan_delta",
        wraps=pl.scan_delta,
    ) as mock_scan:
        inspect_pipeline(delta_root, storage_options=opts)
        mock_scan.assert_called()
        _, kwargs = mock_scan.call_args_list[0]
        assert kwargs.get("storage_options") == opts


def test_run_scoped_inspection_projects_cached_outputs_without_cross_run_leaks(
    tmp_path: Path,
) -> None:
    store = build_cache_isolation_store(tmp_path)

    pipeline = inspect_pipeline(
        store.root,
        pipeline_run_id=store.current_run,
    )
    cached_step = inspect_step(
        store.root,
        5,
        pipeline_run_id=store.current_run,
    )
    metrics = inspect_metrics(
        store.root,
        5,
        pipeline_run_id=store.current_run,
    )
    data = inspect_data(
        store.root,
        name="shared",
        step_number=0,
        pipeline_run_id=store.current_run,
    )

    assert pipeline["step"].to_list() == [0, 5]
    assert pipeline["produced"].to_list() == ["1 data", "1 metric"]
    assert cached_step["name"].to_list() == ["source_metric"]
    assert metrics.select("step", "score").row(0) == (5, 0.9)
    assert data["value"].to_list() == [1]


def test_inspect_pipeline_defaults_to_latest_run_by_lifecycle_time(
    tmp_path: Path,
) -> None:
    store = build_cache_isolation_store(tmp_path)

    pipeline = inspect_pipeline(store.root)

    assert pipeline["operation"].to_list() == ["other_data", "other_metric"]


def test_inspect_pipeline_counts_distinct_artifacts_across_roles(
    tmp_path: Path,
) -> None:
    store = build_cache_isolation_store(tmp_path)
    commit_test_inputs(
        store.root,
        tmp_path / "extra-staging",
        {
            TablePath.EXECUTION_EDGES.value: pl.DataFrame(
                [
                    {
                        "execution_run_id": store.current_data_execution,
                        "direction": "output",
                        "role": "alternate",
                        "artifact_id": store.data_id,
                    }
                ],
                schema=EXECUTION_EDGES_SCHEMA,
            )
        },
    )

    pipeline = inspect_pipeline(store.root, pipeline_run_id=store.current_run)

    assert pipeline.filter(pl.col("step") == 0)["produced"].item() == "1 data"


def test_inspect_metrics_preserves_reuse_at_multiple_current_steps(
    tmp_path: Path,
) -> None:
    store = build_cache_isolation_store(tmp_path)
    repeated_step_id = "e" * 32
    repeated = _step_row(
        step_number=6,
        step_name="current_metric_cached_again",
        pipeline_run_id=store.current_run,
        succeeded_count=1,
        total_count=1,
    )
    repeated["step_run_id"] = repeated_step_id
    repeated["disposition"] = "cache_hit"
    commit_test_step(
        store.root,
        tmp_path / "repeated-staging",
        [repeated],
        {
            TablePath.CACHE_REUSE.value: pl.DataFrame(
                [
                    {
                        "current_step_run_id": repeated_step_id,
                        "cached_execution_run_id": store.source_metric_execution,
                    }
                ],
                schema=CACHE_REUSE_SCHEMA,
            )
        },
    )

    metrics = inspect_metrics(store.root, pipeline_run_id=store.current_run)

    assert metrics.select("step", "score").rows() == [(5, 0.9), (6, 0.9)]


def test_inspect_failures_uses_current_step_and_source_log_path(tmp_path: Path) -> None:
    store = build_cache_isolation_store(tmp_path)
    failure_id = "f" * 32
    failed_step_id = "d" * 32
    failed_step = _step_row(
        step_number=1,
        step_name="source_failure",
        pipeline_run_id=store.source_run,
        succeeded_count=0,
        total_count=1,
    )
    failed_step["step_run_id"] = failed_step_id
    failure = {
        "execution_run_id": failure_id,
        "execution_spec_id": "failure-spec",
        "step_run_id": failed_step_id,
        "origin_step_number": 1,
        "operation_name": "source_failure",
        "params": "{}",
        "user_overrides": "{}",
        "timestamp_start": datetime(2026, 1, 1, tzinfo=UTC),
        "timestamp_end": datetime(2026, 1, 1, tzinfo=UTC),
        "source_worker": 0,
        "compute_backend": "local",
        "success": False,
        "error": "boom",
        "error_envelope": None,
        "tool_output": None,
        "worker_log": None,
        "metadata": "{}",
    }
    commit_test_step(
        store.root,
        tmp_path / "failure-staging",
        [failed_step],
        {TablePath.EXECUTIONS.value: pl.DataFrame([failure], schema=EXECUTIONS_SCHEMA)},
    )
    current_step_id = "e" * 32
    current_step = _step_row(
        step_number=6,
        step_name="current_failure_cached",
        pipeline_run_id=store.current_run,
        succeeded_count=1,
        total_count=1,
    )
    current_step["step_run_id"] = current_step_id
    current_step["disposition"] = "cache_hit"
    commit_test_step(
        store.root,
        tmp_path / "failure-reuse-staging",
        [current_step],
        {
            TablePath.CACHE_REUSE.value: pl.DataFrame(
                [
                    {
                        "current_step_run_id": current_step_id,
                        "cached_execution_run_id": failure_id,
                    }
                ],
                schema=CACHE_REUSE_SCHEMA,
            )
        },
    )

    failures = inspect_failures(store.root, pipeline_run_id=store.current_run)

    assert failures.select("step", "operation", "log").row(0) == (
        6,
        "source_failure",
        failure_log_relative_path(failure_id, datetime(2026, 1, 1, tzinfo=UTC)),
    )


@pytest.mark.parametrize("success", [True, False])
def test_inspect_commands_reads_exact_committed_execution(tmp_path, success):
    from artisan.schemas.execution.command_record import CommandRecording
    from artisan.visualization import inspect_commands

    delta_root = tmp_path / "delta"
    recording = CommandRecording.empty() if success else CommandRecording.unavailable()
    _write_executions(
        delta_root,
        pl.DataFrame(
            [
                {
                    "execution_run_id": "inspect-commands-run",
                    "execution_spec_id": "commands-spec",
                    "origin_step_number": 0,
                    "success": success,
                    "command_recording": recording.model_dump_json(),
                }
            ],
            schema=EXECUTIONS_SCHEMA,
        ),
    )
    assert (
        inspect_commands(str(delta_root), "inspect-commands-run")
        == recording.model_dump()
    )
    with pytest.raises(FileNotFoundError):
        inspect_commands(str(delta_root), "missing")


def test_inspect_worker_log_falls_back_to_exact_embedded_evidence(tmp_path):
    from artisan.storage.io.publication import publish_immutable_bytes
    from artisan.utils.log_paths import worker_log_path
    from artisan.visualization import inspect_worker_log

    root = tmp_path / "delta"
    _write_executions(
        root,
        executions_df(
            execution_run_id=["worker-one", "worker-two"],
            origin_step_number=[0, 1],
            success=[True, True],
            worker_log=["embedded one", "embedded two"],
        ),
    )
    assert inspect_worker_log(str(root), "worker-one") == "embedded one"
    publish_immutable_bytes(
        LocalFileSystem(), worker_log_path(str(root), "worker-one"), b"provider one"
    )
    assert inspect_worker_log(str(root), "worker-one") == "provider one"
    assert inspect_worker_log(str(root), "worker-two") == "embedded two"


@pytest.mark.parametrize("raw", [None, "{}", '{"status":"complete"}', "not-json"])
def test_inspect_commands_rejects_malformed_canonical_evidence(tmp_path, raw):
    from artisan.errors import StoreIntegrityError
    from artisan.visualization import inspect_commands

    delta_root = tmp_path / "delta"
    _write_executions(
        delta_root,
        pl.DataFrame(
            [
                {
                    "execution_run_id": "broken-recording",
                    "execution_spec_id": "commands-spec",
                    "origin_step_number": 0,
                    "success": True,
                    "command_recording": raw,
                }
            ],
            schema=EXECUTIONS_SCHEMA,
        ),
    )
    with pytest.raises(StoreIntegrityError, match="canonical command recording"):
        inspect_commands(str(delta_root), "broken-recording")


def test_inspect_commands_does_not_read_uncommitted_staging(tmp_path):
    from artisan.schemas.execution.command_record import CommandRecording
    from artisan.visualization import inspect_commands

    staging = tmp_path / "staging"
    staging.mkdir()
    executions_df(
        execution_run_id=["uncommitted"],
        command_recording=[CommandRecording.empty().model_dump_json()],
    ).write_parquet(staging / "executions.parquet")
    with pytest.raises(FileNotFoundError):
        inspect_commands(str(tmp_path / "delta"), "uncommitted")


def test_inspect_failures_orders_source_times_and_id_ties(tmp_path: Path) -> None:
    from datetime import timedelta

    start = datetime(2026, 9, 19, tzinfo=UTC)
    root = tmp_path / "delta"
    _write_executions(
        root,
        executions_df(
            execution_run_id=["z", "a", "newer", "older"],
            origin_step_number=[0, 1, 2, 3],
            operation_name=["op"] * 4,
            success=[False] * 4,
            timestamp_start=[
                start,
                start,
                start + timedelta(seconds=1),
                start - timedelta(seconds=1),
            ],
        ),
    )
    report = inspect_failures(root)
    assert report["execution_run_id"].to_list() == ["older", "a", "z", "newer"]
    assert report.schema["timestamp_start"] == pl.Datetime("us", "UTC")
