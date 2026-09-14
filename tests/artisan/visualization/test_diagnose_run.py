"""Tests for the diagnose_run composite reader."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest
from fixtures.logical_commit_store import commit_test_step

from artisan.errors import IncompatibleStoreError
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    ARTIFACT_INDEX_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
    EXECUTIONS_SCHEMA,
)
from artisan.visualization.inspect import RunDiagnosis, diagnose_run

A, B = "a" * 32, "b" * 32


def _step_rows(
    run_id: str, number: int, name: str, status: str, minute: int
) -> list[dict]:
    timestamp = datetime(2026, 7, 1, tzinfo=UTC) + timedelta(minutes=minute)
    pending = {
        "step_run_id": f"{run_id}-step-{number}",
        "step_spec_id": None,
        "pipeline_run_id": run_id,
        "step_number": number,
        "step_name": name,
        "status": "pending",
        "state_sequence": 0,
        "disposition": None,
        "cancellation_status": None,
        "operation_class": "DataGenerator",
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
        **pending,
        "status": "running",
        "state_sequence": 1,
        "timestamp": timestamp - timedelta(microseconds=1),
    }
    if status == "running":
        return [pending, running]
    terminal = {
        **running,
        "step_spec_id": f"spec-{number}",
        "status": status,
        "state_sequence": 2,
        "disposition": "executed" if status == "succeeded" else None,
        "total_count": 1,
        "succeeded_count": 1 if status == "succeeded" else 0,
        "failed_count": 0 if status == "succeeded" else 1,
        "timestamp": timestamp,
        "duration_seconds": 1.0,
        "error": "step failed" if status == "failed" else None,
        "metadata": "{}",
    }
    return [pending, running, terminal]


@pytest.fixture
def failed_store(tmp_path: Path) -> Path:
    """Seed run-1 (a failed transform) plus an older failed run-0."""
    envelope = {
        "error_type": "validation",
        "code": "artifact_validation_failed",
        "message": "bad row",
        "recovery_hint": "CHECK_INPUT",
        "field": "params.x",
        "suggestions": [],
    }
    exec_row = dict.fromkeys(EXECUTIONS_SCHEMA)
    exec_row.update(
        execution_run_id="exec-2",
        execution_spec_id="espec",
        step_run_id="run-1-step-2",
        origin_step_number=2,
        operation_name="transform",
        params="{}",
        user_overrides="{}",
        timestamp_start=datetime(2026, 7, 1, tzinfo=UTC),
        source_worker=0,
        compute_backend="local",
        success=False,
        error="ValueError: bad row",
        error_envelope=json.dumps(envelope),
        tool_output="",
        worker_log="",
        metadata="{}",
    )
    execution_edges = pl.DataFrame(
        {
            "execution_run_id": ["exec-2"],
            "direction": ["output"],
            "role": ["data"],
            "artifact_id": [B],
        },
        schema=EXECUTION_EDGES_SCHEMA,
    )

    artifact_index = pl.DataFrame(
        {
            "artifact_id": [B],
            "artifact_type": ["data"],
            "origin_step_number": [2],
            "metadata": ["{}"],
        },
        schema=ARTIFACT_INDEX_SCHEMA,
    )

    artifact_edges = pl.DataFrame(
        {
            "execution_run_id": ["exec-2"],
            "source_artifact_id": [A],
            "target_artifact_id": [B],
            "source_artifact_type": ["data"],
            "target_artifact_type": ["data"],
            "source_role": ["input"],
            "target_role": ["output"],
            "group_id": [None],
            "step_boundary": [True],
        },
        schema=ARTIFACT_EDGES_SCHEMA,
    )
    staging_root = str(tmp_path / "staging")
    for run_id, number, name, status, minute in (
        ("run-0", 1, "generate", "failed", 0),
        ("run-1", 1, "generate", "succeeded", 10),
    ):
        commit_test_step(
            tmp_path,
            staging_root,
            _step_rows(run_id, number, name, status, minute),
            {},
        )
    commit_test_step(
        tmp_path,
        staging_root,
        _step_rows("run-1", 2, "transform", "failed", 20),
        {
            TablePath.EXECUTIONS.value: pl.DataFrame(
                [exec_row], schema=EXECUTIONS_SCHEMA
            ),
            TablePath.EXECUTION_EDGES.value: execution_edges,
            TablePath.ARTIFACT_INDEX.value: artifact_index,
            TablePath.ARTIFACT_EDGES.value: artifact_edges,
        },
    )
    return tmp_path


class TestDiagnoseRun:
    def test_failed_steps_and_status(self, failed_store) -> None:
        diag = diagnose_run(str(failed_store), "run-1")
        assert isinstance(diag, RunDiagnosis)
        assert diag.last_status == "failed"
        assert len(diag.failed_steps) == 1
        assert diag.failed_steps[0]["code"] == "artifact_validation_failed"

    def test_similar_runs_excludes_self(self, failed_store) -> None:
        diag = diagnose_run(str(failed_store), "run-1")
        ids = {r["pipeline_run_id"] for r in diag.similar_runs}
        assert ids == {"run-0"}

    def test_suggested_actions_from_recovery_hint(self, failed_store) -> None:
        diag = diagnose_run(str(failed_store), "run-1")
        assert any("inputs and parameters" in a for a in diag.suggested_actions)

    def test_upstream_edges_walked(self, failed_store) -> None:
        diag = diagnose_run(str(failed_store), "run-1")
        assert {"source_artifact_id": A, "target_artifact_id": B} in diag.upstream_edges

    def test_missing_executions_raises(self, tmp_path) -> None:
        """A root without a format-2 contract fails before inspection."""
        with pytest.raises(IncompatibleStoreError):
            diagnose_run(str(tmp_path), "run-1")

    def test_steps_but_no_executions_degrades(self, tmp_path) -> None:
        """A real store with no executions recorded yields an empty diagnosis."""
        steps = _step_rows("run-1", 1, "generate", "running", 0)
        commit_test_step(
            tmp_path,
            str(tmp_path / "staging"),
            steps,
            {},
        )

        diag = diagnose_run(str(tmp_path), "run-1")
        assert isinstance(diag, RunDiagnosis)
        assert diag.failed_steps == []
        assert diag.last_status == "running"
        assert diag.suggested_actions == []
