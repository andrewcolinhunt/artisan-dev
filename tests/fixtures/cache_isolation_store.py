"""Rich format-2 store fixture for run-isolation reader tests."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    ARTIFACT_INDEX_SCHEMA,
    CACHE_REUSE_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
    EXECUTIONS_SCHEMA,
)
from artisan.utils.hashing import digest_utf8
from fixtures.logical_commit_store import commit_test_step


@dataclass(frozen=True, slots=True)
class CacheIsolationStore:
    """Identifiers in a direct, cached, and unrelated three-run store."""

    root: Path
    source_run: str
    current_run: str
    other_run: str
    source_metric_execution: str
    current_data_execution: str
    other_data_execution: str
    data_id: str
    metric_id: str
    other_data_id: str
    other_metric_id: str
    current_cache_step_id: str


def build_cache_isolation_store(
    tmp_path: Path,
    *,
    reuse_source_metric_at_step_zero: bool = False,
) -> CacheIsolationStore:
    """Create direct/reused membership with same-number cross-run noise."""
    root = tmp_path / "cache-isolation-delta"
    staging_root = tmp_path / "cache-isolation-staging"

    source_run = "source-run"
    current_run = "current-run"
    other_run = "other-run"
    step_ids = {
        (source_run, 0): digest_utf8("source-run:step:0"),
        (source_run, 1): digest_utf8("source-run:step:1"),
        (current_run, 0): digest_utf8("current-run:step:0"),
        (current_run, 5): digest_utf8("current-run:step:5"),
        (other_run, 0): digest_utf8("other-run:step:0"),
        (other_run, 5): digest_utf8("other-run:step:5"),
    }
    execution_ids = {
        "source_data": digest_utf8("source-run:data"),
        "source_metric": digest_utf8("source-run:metric"),
        "current_data": digest_utf8("current-run:data"),
        "other_data": digest_utf8("other-run:data"),
        "other_metric": digest_utf8("other-run:metric"),
    }

    data = DataArtifact.draft(b"value\n1\n", "shared.csv", 0).finalize()
    metric = MetricArtifact.draft({"score": 0.9}, "source_metric.json", 1).finalize()
    other_data = DataArtifact.draft(b"value\n2\n", "other.csv", 0).finalize()
    other_metric = MetricArtifact.draft(
        {"score": 0.1}, "other_metric.json", 5
    ).finalize()
    base = datetime(2026, 9, 1, tzinfo=UTC)
    step_specs = [
        (source_run, 0, "source_data", execution_ids["source_data"], data, None),
        (source_run, 1, "source_metric", execution_ids["source_metric"], metric, data),
        (current_run, 0, "current_data", execution_ids["current_data"], None, data),
        (current_run, 5, "current_metric_cached", None, None, None),
        (other_run, 0, "other_data", execution_ids["other_data"], other_data, None),
        (
            other_run,
            5,
            "other_metric",
            execution_ids["other_metric"],
            other_metric,
            other_data,
        ),
    ]
    for index, (run_id, number, name, execution_id, produced, source) in enumerate(
        step_specs
    ):
        rows = _step_rows(
            step_ids[(run_id, number)],
            run_id,
            number,
            name,
            base + timedelta(seconds=index),
        )
        tables: dict[str, pl.DataFrame] = {}
        if produced is not None:
            tables[
                "artifacts/data"
                if produced.artifact_type == "data"
                else "artifacts/metrics"
            ] = pl.DataFrame([produced.to_row()], schema=type(produced).POLARS_SCHEMA)
            tables[TablePath.ARTIFACT_INDEX.value] = pl.DataFrame(
                [
                    {
                        "artifact_id": produced.artifact_id,
                        "artifact_type": produced.artifact_type,
                        "origin_step_number": produced.origin_step_number,
                        "metadata": json.dumps(produced.metadata),
                    }
                ],
                schema=ARTIFACT_INDEX_SCHEMA,
            )
        if execution_id is not None:
            timing = {
                "source_data": 10.0,
                "source_metric": 99.0,
                "current_data": 2.0,
                "other_data": 88.0,
                "other_metric": 77.0,
            }[name]
            tables[TablePath.EXECUTIONS.value] = pl.DataFrame(
                [
                    _execution(
                        execution_id, step_ids[(run_id, number)], number, name, timing
                    )
                ],
                schema=EXECUTIONS_SCHEMA,
            )
            output = produced if produced is not None else source
            edge_rows = []
            if source is not None and produced is not None:
                edge_rows.append(
                    _edge(execution_id, "input", "data", source.artifact_id)
                )
            if output is not None:
                edge_rows.append(
                    _edge(
                        execution_id, "output", output.artifact_type, output.artifact_id
                    )
                )
            tables[TablePath.EXECUTION_EDGES.value] = pl.DataFrame(
                edge_rows, schema=EXECUTION_EDGES_SCHEMA
            )
            if source is not None and produced is not None:
                tables[TablePath.ARTIFACT_EDGES.value] = pl.DataFrame(
                    [
                        _artifact_edge(
                            execution_id, source.artifact_id, produced.artifact_id
                        )
                    ],
                    schema=ARTIFACT_EDGES_SCHEMA,
                )
            if (
                reuse_source_metric_at_step_zero
                and run_id == current_run
                and number == 0
            ):
                tables[TablePath.CACHE_REUSE.value] = pl.DataFrame(
                    [
                        {
                            "current_step_run_id": step_ids[(current_run, 0)],
                            "cached_execution_run_id": execution_ids["source_metric"],
                        }
                    ],
                    schema=CACHE_REUSE_SCHEMA,
                )
        else:
            tables[TablePath.CACHE_REUSE.value] = pl.DataFrame(
                [
                    {
                        "current_step_run_id": step_ids[(current_run, 5)],
                        "cached_execution_run_id": execution_ids["source_metric"],
                    }
                ],
                schema=CACHE_REUSE_SCHEMA,
            )
        commit_test_step(root, staging_root, rows, tables)

    return CacheIsolationStore(
        root=root,
        source_run=source_run,
        current_run=current_run,
        other_run=other_run,
        source_metric_execution=execution_ids["source_metric"],
        current_data_execution=execution_ids["current_data"],
        other_data_execution=execution_ids["other_data"],
        data_id=data.artifact_id,
        metric_id=metric.artifact_id,
        other_data_id=other_data.artifact_id,
        other_metric_id=other_metric.artifact_id,
        current_cache_step_id=step_ids[(current_run, 5)],
    )


def _step_rows(
    step_id: str,
    run_id: str,
    number: int,
    name: str,
    timestamp: datetime,
) -> list[dict[str, object]]:
    base = {
        "step_run_id": step_id,
        "step_spec_id": None,
        "pipeline_run_id": run_id,
        "step_number": number,
        "step_name": name,
        "status": "pending",
        "state_sequence": 0,
        "disposition": None,
        "cancellation_status": None,
        "operation_class": "example.Operation",
        "params_json": "{}",
        "input_refs_json": "{}",
        "compute_backend": "local",
        "compute_options_json": "{}",
        "output_roles_json": "[]",
        "output_types_json": "{}",
        "total_count": None,
        "succeeded_count": None,
        "failed_count": None,
        "timestamp": timestamp - timedelta(microseconds=2),
        "duration_seconds": None,
        "error": None,
        "metadata": None,
    }
    running = {
        **base,
        "status": "running",
        "state_sequence": 1,
        "timestamp": timestamp - timedelta(microseconds=1),
    }
    succeeded = {
        **running,
        "step_spec_id": digest_utf8(f"{run_id}:spec:{number}"),
        "status": "succeeded",
        "state_sequence": 2,
        "disposition": "cache_hit" if "cached" in name else "executed",
        "total_count": 1,
        "succeeded_count": 1,
        "failed_count": 0,
        "timestamp": timestamp,
        "duration_seconds": 1.0,
        "metadata": json.dumps({"timings": {"total": 1.0}}),
    }
    return [base, running, succeeded]


def _execution(
    execution_id: str,
    step_id: str,
    step_number: int,
    name: str,
    timing: float,
) -> dict[str, object]:
    now = datetime(2026, 9, 1, tzinfo=UTC)
    return {
        "execution_run_id": execution_id,
        "execution_spec_id": digest_utf8(f"{execution_id}:spec"),
        "step_run_id": step_id,
        "origin_step_number": step_number,
        "operation_name": name,
        "params": "{}",
        "user_overrides": "{}",
        "timestamp_start": now,
        "timestamp_end": now,
        "source_worker": 0,
        "compute_backend": "local",
        "success": True,
        "error": None,
        "error_envelope": None,
        "tool_output": None,
        "worker_log": None,
        "metadata": json.dumps({"timings": {"total": timing}}),
    }


def _edge(
    execution_id: str,
    direction: str,
    role: str,
    artifact_id: str,
) -> dict[str, str]:
    return {
        "execution_run_id": execution_id,
        "direction": direction,
        "role": role,
        "artifact_id": artifact_id,
    }


def _artifact_edge(
    execution_id: str,
    source_id: str,
    target_id: str,
) -> dict[str, object]:
    return {
        "execution_run_id": execution_id,
        "source_artifact_id": source_id,
        "target_artifact_id": target_id,
        "source_artifact_type": "data",
        "target_artifact_type": "metric",
        "source_role": "data",
        "target_role": "metric",
        "group_id": None,
        "step_boundary": True,
    }
