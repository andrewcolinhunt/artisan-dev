"""Tests for the diagnose_run composite reader."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    ARTIFACT_INDEX_SCHEMA,
    EXECUTIONS_SCHEMA,
    STEPS_SCHEMA,
)
from artisan.visualization.inspect import RunDiagnosis, diagnose_run

A, B = "a" * 32, "b" * 32


def _step_row(run_id: str, number: int, name: str, status: str, minute: int) -> dict:
    return {
        "step_run_id": f"{run_id}-step-{number}",
        "step_spec_id": f"spec-{number}",
        "pipeline_run_id": run_id,
        "step_number": number,
        "step_name": name,
        "status": status,
        "operation_class": "DataGenerator",
        "params_json": "{}",
        "input_refs_json": "{}",
        "compute_backend": "local",
        "compute_options_json": "{}",
        "output_roles_json": "[]",
        "output_types_json": "[]",
        "total_count": 1,
        "succeeded_count": 1,
        "failed_count": 1 if status == "failed" else 0,
        "timestamp": datetime(2026, 7, 1, tzinfo=UTC) + timedelta(minutes=minute),
        "duration_seconds": 1.0,
        "error": None,
        "dispatch_error": None,
        "commit_error": None,
        "metadata": "{}",
    }


@pytest.fixture
def failed_store(tmp_path: Path) -> Path:
    """Seed run-1 (a failed transform) plus an older failed run-0."""
    steps = [
        _step_row("run-0", 1, "generate", "failed", 0),
        _step_row("run-1", 1, "generate", "completed", 10),
        _step_row("run-1", 2, "transform", "failed", 20),
    ]
    pl.DataFrame(steps, schema=STEPS_SCHEMA).write_delta(
        str(tmp_path / TablePath.STEPS)
    )

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
    pl.DataFrame([exec_row], schema=EXECUTIONS_SCHEMA).write_delta(
        str(tmp_path / TablePath.EXECUTIONS)
    )

    pl.DataFrame(
        {
            "artifact_id": [B],
            "artifact_type": ["data"],
            "origin_step_number": [2],
            "metadata": ["{}"],
        },
        schema=ARTIFACT_INDEX_SCHEMA,
    ).write_delta(str(tmp_path / TablePath.ARTIFACT_INDEX))

    pl.DataFrame(
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
    ).write_delta(str(tmp_path / TablePath.ARTIFACT_EDGES))
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
        """A bogus root (no executions, no steps) raises for store_not_found."""
        with pytest.raises(FileNotFoundError):
            diagnose_run(str(tmp_path), "run-1")

    def test_steps_but_no_executions_degrades(self, tmp_path) -> None:
        """A real store with no executions recorded yields an empty diagnosis."""
        steps = [_step_row("run-1", 1, "generate", "running", 0)]
        pl.DataFrame(steps, schema=STEPS_SCHEMA).write_delta(
            str(tmp_path / TablePath.STEPS)
        )

        diag = diagnose_run(str(tmp_path), "run-1")
        assert isinstance(diag, RunDiagnosis)
        assert diag.failed_steps == []
        assert diag.last_status == "running"
        assert diag.suggested_actions == []
