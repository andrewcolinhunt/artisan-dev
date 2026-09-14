"""Tests for the run_status composite reader and step-number resolution."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from fixtures.store_format import publish_test_store
from fsspec.implementations.local import LocalFileSystem

from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.orchestration.run_status import (
    RunStatus,
    resolve_step_number,
    run_status,
)
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.table_schemas import EXECUTIONS_SCHEMA


def _seed_steps(
    root: Path,
    run_id: str,
    steps: list[tuple],
) -> None:
    """Write full pending-to-terminal histories from step specs.

    Each spec is ``(number, name, status)`` or, to exercise the count-aware
    status, ``(number, name, status, succeeded, failed)`` — counts default
    to ``1`` succeeded / ``0`` failed.
    """
    publish_test_store(str(root), LocalFileSystem())
    executions_path = root / TablePath.EXECUTIONS.value
    if not executions_path.exists():
        pl.DataFrame(schema=EXECUTIONS_SCHEMA).write_delta(str(executions_path))
    tracker = StepTracker(str(root), run_id)
    for spec in steps:
        number, name, status = spec[:3]
        terminal = StepStatus(status)
        succeeded = spec[3] if len(spec) > 3 else int(terminal == StepStatus.SUCCEEDED)
        failed = spec[4] if len(spec) > 4 else int(terminal == StepStatus.FAILED)
        step_run_id = f"{run_id}-step-{number}"
        step_spec_id = f"spec-{number}"
        tracker.create_attempt(
            StepStartRecord(
                step_run_id=step_run_id,
                step_spec_id=step_spec_id,
                step_number=number,
                step_name=name,
                operation_class="DataGenerator",
                params_json="{}",
                input_refs_json="{}",
                compute_backend="local",
                compute_options_json="{}",
                output_roles_json="[]",
                output_types_json="{}",
            )
        )
        tracker.transition(
            step_run_id,
            StepStatus.PENDING,
            StepStatus.RUNNING,
            step_spec_id=step_spec_id,
        )
        tracker.transition(
            step_run_id,
            StepStatus.RUNNING,
            terminal,
            step_spec_id=step_spec_id,
            result=StepResult(
                step_name=name,
                step_number=number,
                status=terminal,
                disposition=(
                    StepDisposition.EXECUTED
                    if terminal in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}
                    else None
                ),
                error="test failure" if terminal == StepStatus.FAILED else None,
                total_count=succeeded + failed,
                succeeded_count=succeeded,
                failed_count=failed,
                step_run_id=step_run_id,
            ),
        )


class TestRunStatus:
    def test_rollup_and_steps(self, tmp_path) -> None:
        _seed_steps(
            tmp_path,
            "run-x",
            [(1, "generate", "succeeded"), (2, "transform", "succeeded")],
        )
        status = run_status(str(tmp_path), "run-x")
        assert isinstance(status, RunStatus)
        assert status.pipeline_run_id == "run-x"
        assert status.step_count == 2
        assert status.last_status == StepStatus.SUCCEEDED
        assert status.started_at is not None
        assert [s.name for s in status.steps] == ["generate", "transform"]
        assert all(s.status == StepStatus.SUCCEEDED for s in status.steps)

    def test_failed_step_surfaces(self, tmp_path) -> None:
        _seed_steps(
            tmp_path,
            "run-f",
            [(1, "generate", "succeeded"), (2, "boom", "failed")],
        )
        status = run_status(str(tmp_path), "run-f")
        by_name = {s.name: s.status for s in status.steps}
        assert by_name["boom"] == StepStatus.FAILED

    def test_failed_state_surfaces_without_reader_reclassification(
        self, tmp_path
    ) -> None:
        """Readers expose the authoritative failed state directly."""
        _seed_steps(
            tmp_path,
            "run-c",
            [(1, "generate", "succeeded"), (2, "transform", "failed", 0, 1)],
        )
        status = run_status(str(tmp_path), "run-c")
        by_name = {s.name: s.status for s in status.steps}
        assert by_name["transform"] == StepStatus.FAILED
        assert by_name["generate"] == StepStatus.SUCCEEDED
        assert status.last_status == StepStatus.FAILED

    def test_partial_state_surfaces_directly(self, tmp_path) -> None:
        """A mixed-count terminal step exposes partial without derivation."""
        _seed_steps(
            tmp_path,
            "run-p",
            [(1, "transform", "partial", 2, 1)],
        )
        status = run_status(str(tmp_path), "run-p")
        assert status.steps[0].status == StepStatus.PARTIAL
        assert status.last_status == StepStatus.PARTIAL

    def test_unknown_run_is_empty(self, tmp_path) -> None:
        _seed_steps(tmp_path, "run-x", [(1, "generate", "succeeded")])
        status = run_status(str(tmp_path), "nope")
        assert status.steps == []
        assert status.last_status is None
        assert status.step_count == 0

    def test_missing_steps_table_raises(self, tmp_path) -> None:
        with pytest.raises(FileNotFoundError):
            run_status(str(tmp_path), "run-x")


class TestResolveStepNumber:
    def test_resolves_known_step(self, tmp_path) -> None:
        _seed_steps(
            tmp_path,
            "run-x",
            [(1, "generate", "succeeded"), (2, "transform", "succeeded")],
        )
        assert resolve_step_number(str(tmp_path), "run-x", "transform") == 2

    def test_unknown_step_returns_none(self, tmp_path) -> None:
        _seed_steps(tmp_path, "run-x", [(1, "generate", "succeeded")])
        assert resolve_step_number(str(tmp_path), "run-x", "nope") is None

    def test_empty_steps_table_returns_none(self, tmp_path) -> None:
        assert resolve_step_number(str(tmp_path), "run-x", "generate") is None
