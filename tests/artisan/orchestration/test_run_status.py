"""Tests for the run_status composite reader and step-number resolution."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from artisan.orchestration.run_status import (
    RunStatus,
    resolve_step_number,
    run_status,
)
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import STEPS_SCHEMA


def _seed_steps(
    root: Path,
    run_id: str,
    steps: list[tuple[int, str, str]],
) -> None:
    """Write running+terminal step rows from (number, name, status) triples."""
    rows = []
    t0 = datetime(2026, 7, 1, tzinfo=UTC)
    for i, (number, name, status) in enumerate(steps):
        for j, row_status in enumerate(["running", status]):
            rows.append(
                {
                    "step_run_id": f"{run_id}-step-{number}",
                    "step_spec_id": f"spec-{number}",
                    "pipeline_run_id": run_id,
                    "step_number": number,
                    "step_name": name,
                    "status": row_status,
                    "operation_class": "DataGenerator",
                    "params_json": "{}",
                    "input_refs_json": "{}",
                    "compute_backend": "local",
                    "compute_options_json": "{}",
                    "output_roles_json": "[]",
                    "output_types_json": "[]",
                    "total_count": 1,
                    "succeeded_count": 1,
                    "failed_count": 0,
                    "timestamp": t0 + timedelta(minutes=10 * i + j),
                    "duration_seconds": 1.5,
                    "error": None,
                    "dispatch_error": None,
                    "commit_error": None,
                    "metadata": "{}",
                }
            )
    pl.DataFrame(rows, schema=STEPS_SCHEMA).write_delta(str(root / TablePath.STEPS))


class TestRunStatus:
    def test_rollup_and_steps(self, tmp_path) -> None:
        _seed_steps(
            tmp_path,
            "run-x",
            [(1, "generate", "completed"), (2, "transform", "completed")],
        )
        status = run_status(str(tmp_path), "run-x")
        assert isinstance(status, RunStatus)
        assert status.pipeline_run_id == "run-x"
        assert status.step_count == 2
        assert status.last_status == "completed"
        assert status.started_at is not None
        assert [s.name for s in status.steps] == ["generate", "transform"]
        assert all(s.status == "ok" for s in status.steps)

    def test_failed_step_surfaces(self, tmp_path) -> None:
        _seed_steps(
            tmp_path,
            "run-f",
            [(1, "generate", "completed"), (2, "boom", "failed")],
        )
        status = run_status(str(tmp_path), "run-f")
        by_name = {s.name: s.status for s in status.steps}
        assert by_name["boom"] == "failed"

    def test_unknown_run_is_empty(self, tmp_path) -> None:
        _seed_steps(tmp_path, "run-x", [(1, "generate", "completed")])
        status = run_status(str(tmp_path), "nope")
        assert status.steps == []
        assert status.last_status is None
        assert status.step_count == 0

    def test_missing_table_raises(self, tmp_path) -> None:
        with pytest.raises(FileNotFoundError):
            run_status(str(tmp_path), "run-x")


class TestResolveStepNumber:
    def test_resolves_known_step(self, tmp_path) -> None:
        _seed_steps(
            tmp_path,
            "run-x",
            [(1, "generate", "completed"), (2, "transform", "completed")],
        )
        assert resolve_step_number(str(tmp_path), "run-x", "transform") == 2

    def test_unknown_step_returns_none(self, tmp_path) -> None:
        _seed_steps(tmp_path, "run-x", [(1, "generate", "completed")])
        assert resolve_step_number(str(tmp_path), "run-x", "nope") is None

    def test_missing_table_raises(self, tmp_path) -> None:
        with pytest.raises(FileNotFoundError):
            resolve_step_number(str(tmp_path), "run-x", "generate")
