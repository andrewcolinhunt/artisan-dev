"""Tests for StepTracker cancelled-step handling."""

from __future__ import annotations

import polars as pl

from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas.orchestration.step_start_record import StepStartRecord


def _make_start_record(
    step_number: int = 1,
    step_spec_id: str = "spec-abc",
) -> StepStartRecord:
    return StepStartRecord(
        step_run_id=f"run-{step_number}",
        step_spec_id=step_spec_id,
        step_number=step_number,
        step_name=f"step_{step_number}",
        operation_class="tests.MockOp",
        params_json="{}",
        input_refs_json="{}",
        compute_backend="local",
        compute_options_json="{}",
        output_roles_json='["output"]',
        output_types_json='{"output": "data"}',
    )


class TestRecordStepCancelled:
    """Tests for record_step_cancelled."""

    def test_writes_cancelled_row(self, tmp_path):
        tracker = StepTracker(tmp_path / "delta", pipeline_run_id="run-1")
        record = _make_start_record()
        tracker.record_step_cancelled(record, reason="user cancelled")

        df = pl.read_delta(str(tmp_path / "delta" / "orchestration" / "steps"))
        assert len(df) == 1
        row = df.row(0, named=True)
        assert row["status"] == "cancelled"
        assert row["error"] == "user cancelled"
        assert row["step_spec_id"] == "spec-abc"
        assert row["total_count"] is None
        assert row["succeeded_count"] is None
        assert row["failed_count"] is None

    def test_default_reason(self, tmp_path):
        tracker = StepTracker(tmp_path / "delta", pipeline_run_id="run-1")
        record = _make_start_record()
        tracker.record_step_cancelled(record)

        df = pl.read_delta(str(tmp_path / "delta" / "orchestration" / "steps"))
        assert df.row(0, named=True)["error"] == "Pipeline cancelled"


class TestCancelledExcludedFromCache:
    """check_cache should not return cancelled steps."""

    def test_check_cache_excludes_cancelled(self, tmp_path):
        tracker = StepTracker(tmp_path / "delta", pipeline_run_id="run-1")
        record = _make_start_record(step_spec_id="spec-xyz")
        tracker.record_step_cancelled(record)

        assert tracker.check_cache("spec-xyz") is None


class TestCancelledExcludedFromLoadCompleted:
    """load_completed_steps should not include cancelled steps."""

    def test_load_completed_excludes_cancelled(self, tmp_path):
        tracker = StepTracker(tmp_path / "delta", pipeline_run_id="run-1")
        record = _make_start_record(step_spec_id="spec-xyz")
        tracker.record_step_cancelled(record)

        steps = tracker.load_completed_steps(pipeline_run_id="run-1")
        assert len(steps) == 0
