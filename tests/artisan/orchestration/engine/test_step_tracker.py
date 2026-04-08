"""Tests for StepTracker cancelled-step handling and storage_options."""

from __future__ import annotations

from unittest.mock import patch

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
        tracker = StepTracker(str(tmp_path / "delta"), pipeline_run_id="run-1")
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
        tracker = StepTracker(str(tmp_path / "delta"), pipeline_run_id="run-1")
        record = _make_start_record()
        tracker.record_step_cancelled(record)

        df = pl.read_delta(str(tmp_path / "delta" / "orchestration" / "steps"))
        assert df.row(0, named=True)["error"] == "Pipeline cancelled"


class TestCancelledExcludedFromCache:
    """check_cache should not return cancelled steps."""

    def test_check_cache_excludes_cancelled(self, tmp_path):
        tracker = StepTracker(str(tmp_path / "delta"), pipeline_run_id="run-1")
        record = _make_start_record(step_spec_id="spec-xyz")
        tracker.record_step_cancelled(record)

        assert tracker.check_cache("spec-xyz") is None


class TestCancelledExcludedFromLoadCompleted:
    """load_completed_steps should not include cancelled steps."""

    def test_load_completed_excludes_cancelled(self, tmp_path):
        tracker = StepTracker(str(tmp_path / "delta"), pipeline_run_id="run-1")
        record = _make_start_record(step_spec_id="spec-xyz")
        tracker.record_step_cancelled(record)

        steps = tracker.load_completed_steps(pipeline_run_id="run-1")
        assert len(steps) == 0


class TestStorageOptionsForwarding:
    """storage_options should be forwarded to scan_delta and write_delta."""

    def test_scan_delta_receives_storage_options(self, tmp_path):
        """check_cache forwards storage_options to pl.scan_delta."""
        opts = {"key": "val"}
        tracker = StepTracker(
            str(tmp_path / "delta"), pipeline_run_id="run-1", storage_options=opts
        )
        record = _make_start_record()
        tracker.record_step_start(record)

        with patch(
            "artisan.orchestration.engine.step_tracker.pl.scan_delta",
            wraps=pl.scan_delta,
        ) as mock_scan:
            tracker.check_cache("spec-abc")
            mock_scan.assert_called()
            _, kwargs = mock_scan.call_args
            assert kwargs.get("storage_options") == opts

    def test_storage_options_stored_on_instance(self, tmp_path):
        """storage_options is stored and accessible for write_delta calls."""
        opts = {"key": "val"}
        tracker = StepTracker(
            str(tmp_path / "delta"), pipeline_run_id="run-1", storage_options=opts
        )
        assert tracker._storage_options == opts

    def test_none_storage_options_default(self, tmp_path):
        """storage_options defaults to None when not provided."""
        tracker = StepTracker(str(tmp_path / "delta"), pipeline_run_id="run-1")
        assert tracker._storage_options is None
