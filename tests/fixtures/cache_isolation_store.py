"""Rich format-2 store fixture for run-isolation reader tests."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
from fsspec.implementations.local import LocalFileSystem

from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    ARTIFACT_INDEX_SCHEMA,
    CACHE_REUSE_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
    EXECUTIONS_SCHEMA,
    STEPS_SCHEMA,
)
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.staging import StagingManager
from artisan.utils.hashing import digest_utf8


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


def build_cache_isolation_store(tmp_path: Path) -> CacheIsolationStore:
    """Create direct/reused membership with same-number cross-run noise."""
    root = tmp_path / "cache-isolation-delta"
    fs = LocalFileSystem()
    DeltaCommitter(
        str(root),
        StagingManager(str(tmp_path / "cache-isolation-staging"), fs),
        fs=fs,
    ).initialize_tables()

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
    artifacts = [data, metric, other_data, other_metric]
    for artifact_type, typed in (
        ("data", [data, other_data]),
        ("metric", [metric, other_metric]),
    ):
        schema = type(typed[0]).POLARS_SCHEMA
        relative = "artifacts/data" if artifact_type == "data" else "artifacts/metrics"
        pl.DataFrame(
            [artifact.to_row() for artifact in typed], schema=schema
        ).write_delta(
            str(root / relative),
            mode="append",
        )
    pl.DataFrame(
        [
            {
                "artifact_id": artifact.artifact_id,
                "artifact_type": artifact.artifact_type,
                "origin_step_number": artifact.origin_step_number,
                "metadata": json.dumps(artifact.metadata),
            }
            for artifact in artifacts
        ],
        schema=ARTIFACT_INDEX_SCHEMA,
    ).write_delta(str(root / TablePath.ARTIFACT_INDEX), mode="append")

    base = datetime(2026, 9, 1, tzinfo=UTC)
    step_rows = [
        row
        for index, (run_id, number, name) in enumerate(
            [
                (source_run, 0, "source_data"),
                (source_run, 1, "source_metric"),
                (current_run, 0, "current_data"),
                (current_run, 5, "current_metric_cached"),
                (other_run, 0, "other_data"),
                (other_run, 5, "other_metric"),
            ]
        )
        for row in _step_rows(
            step_ids[(run_id, number)],
            run_id,
            number,
            name,
            base + timedelta(seconds=index),
        )
    ]
    pl.DataFrame(step_rows, schema=STEPS_SCHEMA).write_delta(
        str(root / TablePath.STEPS), mode="append"
    )
    execution_rows = [
        _execution(
            execution_ids["source_data"],
            step_ids[(source_run, 0)],
            0,
            "source_data",
            10.0,
        ),
        _execution(
            execution_ids["source_metric"],
            step_ids[(source_run, 1)],
            1,
            "source_metric",
            99.0,
        ),
        _execution(
            execution_ids["current_data"],
            step_ids[(current_run, 0)],
            0,
            "current_data",
            2.0,
        ),
        _execution(
            execution_ids["other_data"], step_ids[(other_run, 0)], 0, "other_data", 88.0
        ),
        _execution(
            execution_ids["other_metric"],
            step_ids[(other_run, 5)],
            5,
            "other_metric",
            77.0,
        ),
    ]
    pl.DataFrame(execution_rows, schema=EXECUTIONS_SCHEMA).write_delta(
        str(root / TablePath.EXECUTIONS), mode="append"
    )
    edges = [
        _edge(execution_ids["source_data"], "output", "data", data.artifact_id),
        _edge(execution_ids["source_metric"], "input", "data", data.artifact_id),
        _edge(execution_ids["source_metric"], "output", "metric", metric.artifact_id),
        _edge(execution_ids["current_data"], "output", "data", data.artifact_id),
        _edge(execution_ids["other_data"], "output", "data", other_data.artifact_id),
        _edge(execution_ids["other_metric"], "input", "data", other_data.artifact_id),
        _edge(
            execution_ids["other_metric"], "output", "metric", other_metric.artifact_id
        ),
    ]
    pl.DataFrame(edges, schema=EXECUTION_EDGES_SCHEMA).write_delta(
        str(root / TablePath.EXECUTION_EDGES), mode="append"
    )
    artifact_edges = [
        _artifact_edge(
            execution_ids["source_metric"], data.artifact_id, metric.artifact_id
        ),
        _artifact_edge(
            execution_ids["other_metric"],
            other_data.artifact_id,
            other_metric.artifact_id,
        ),
    ]
    pl.DataFrame(artifact_edges, schema=ARTIFACT_EDGES_SCHEMA).write_delta(
        str(root / TablePath.ARTIFACT_EDGES), mode="append"
    )
    pl.DataFrame(
        [
            {
                "current_step_run_id": step_ids[(current_run, 5)],
                "cached_execution_run_id": execution_ids["source_metric"],
            }
        ],
        schema=CACHE_REUSE_SCHEMA,
    ).write_delta(str(root / TablePath.CACHE_REUSE), mode="append")

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
        "logical_commit_id": None,
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
