"""Tests for the run_status composite reader and step-number resolution."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.retained_inspection import commit_inspection_attempt
from fixtures.store_format import publish_test_store
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import IncompatibleStoreError
from artisan.orchestration.run_status import RunStatus, resolve_step_number, run_status
from artisan.schemas.orchestration.step_lifecycle import StepStatus


def _seed_steps(root: Path, run_id: str, steps: list[tuple[int, str, str]]) -> None:
    """Publish actual finite attempts for each requested lifecycle result."""
    for number, name, status in steps:
        commit_inspection_attempt(
            root, run_id=run_id, number=number, name=name, status=status
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
            [(1, "generate", "succeeded"), (2, "transform", "failed")],
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
            [(1, "transform", "partial")],
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

    def test_missing_store_manifest_raises(self, tmp_path) -> None:
        with pytest.raises(IncompatibleStoreError, match="no domain manifest"):
            run_status(str(tmp_path / "missing"), "run-x")


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
        publish_test_store(str(tmp_path), LocalFileSystem())
        assert resolve_step_number(str(tmp_path), "run-x", "generate") is None
