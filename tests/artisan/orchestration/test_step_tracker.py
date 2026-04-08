"""Tests for StepTracker delta table operations."""

from __future__ import annotations

from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas.enums import CachePolicy
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord


def _make_start_record(
    step_number: int = 0,
    step_name: str = "Ingest",
    step_run_id: str = "run_001",
    step_spec_id: str = "spec_001",
) -> StepStartRecord:
    """Create a StepStartRecord for testing."""
    return StepStartRecord(
        step_run_id=step_run_id,
        step_spec_id=step_spec_id,
        step_number=step_number,
        step_name=step_name,
        operation_class="artisan.operations.curator.filter.Filter",
        params_json="{}",
        input_refs_json="null",
        compute_backend="local",
        compute_options_json="{}",
        output_roles_json='["file"]',
        output_types_json='{"file": "file_ref"}',
    )


def _make_step_result(
    step_number: int = 0,
    step_name: str = "Ingest",
    succeeded_count: int = 5,
    failed_count: int = 0,
) -> StepResult:
    """Create a StepResult for testing."""
    return StepResult(
        step_name=step_name,
        step_number=step_number,
        success=failed_count == 0,
        total_count=succeeded_count + failed_count,
        succeeded_count=succeeded_count,
        failed_count=failed_count,
        output_roles=frozenset(["file"]),
        output_types={"file": "file_ref"},
        duration_seconds=1.5,
    )


class TestCheckCache:
    """Tests for StepTracker.check_cache()."""

    def test_check_cache_empty(self, tmp_path):
        """No table returns None."""
        tracker = StepTracker(str(tmp_path), "run_1")
        assert tracker.check_cache("nonexistent", CachePolicy.ALL_SUCCEEDED) is None

    def test_check_cache_no_completed(self, tmp_path):
        """Running/failed rows return None."""
        tracker = StepTracker(str(tmp_path), "run_1")
        record = _make_start_record(step_spec_id="spec_a")
        tracker.record_step_start(record)
        # Only a 'running' row exists
        assert tracker.check_cache("spec_a", CachePolicy.ALL_SUCCEEDED) is None

    def test_check_cache_hit(self, tmp_path):
        """Completed row returns correct StepResult."""
        tracker = StepTracker(str(tmp_path), "run_1")
        record = _make_start_record(step_spec_id="spec_b")
        result = _make_step_result()
        tracker.record_step_start(record)
        tracker.record_step_completed(record, result)

        cached = tracker.check_cache("spec_b", CachePolicy.ALL_SUCCEEDED)
        assert cached is not None
        assert cached.step_name == "Ingest"
        assert cached.step_number == 0
        assert cached.succeeded_count == 5
        assert cached.failed_count == 0
        assert cached.success is True
        assert cached.output_roles == frozenset(["file"])
        assert cached.output_types == {"file": "file_ref"}

    def test_check_cache_most_recent(self, tmp_path):
        """Multiple completed rows for same spec_id returns latest."""
        tracker = StepTracker(str(tmp_path), "run_1")
        record = _make_start_record(step_spec_id="spec_c")

        # First completion with 3 succeeded
        result1 = _make_step_result(succeeded_count=3)
        tracker.record_step_start(record)
        tracker.record_step_completed(record, result1)

        # Second completion with 10 succeeded (more recent)
        result2 = _make_step_result(succeeded_count=10)
        tracker.record_step_completed(record, result2)

        cached = tracker.check_cache("spec_c", CachePolicy.ALL_SUCCEEDED)
        assert cached is not None
        assert cached.succeeded_count == 10


class TestRecordOperations:
    """Tests for StepTracker.record_*() methods."""

    def test_record_start_creates_table(self, tmp_path):
        """First write creates delta table."""
        tracker = StepTracker(str(tmp_path), "run_1")
        assert not (tmp_path / "orchestration/steps").exists()

        record = _make_start_record()
        tracker.record_step_start(record)
        assert (tmp_path / "orchestration/steps").exists()

    def test_record_start_appends(self, tmp_path):
        """Second write appends to existing table."""
        tracker = StepTracker(str(tmp_path), "run_1")
        record1 = _make_start_record(step_number=0, step_run_id="run_a")
        record2 = _make_start_record(step_number=1, step_run_id="run_b")
        tracker.record_step_start(record1)
        tracker.record_step_start(record2)

        import polars as pl

        df = pl.read_delta(str(tmp_path / "orchestration/steps"))
        assert len(df) == 2

    def test_record_completed(self, tmp_path):
        """Completed row has correct status, counts, duration."""
        tracker = StepTracker(str(tmp_path), "run_1")
        record = _make_start_record()
        result = _make_step_result(succeeded_count=8, failed_count=2)
        tracker.record_step_start(record)
        tracker.record_step_completed(record, result)

        import polars as pl

        df = pl.read_delta(str(tmp_path / "orchestration/steps"))
        completed = df.filter(pl.col("status") == "completed")
        assert len(completed) == 1
        row = completed.row(0, named=True)
        assert row["total_count"] == 10
        assert row["succeeded_count"] == 8
        assert row["failed_count"] == 2
        assert row["duration_seconds"] == 1.5

    def test_record_failed(self, tmp_path):
        """Failed row has correct status and error message."""
        tracker = StepTracker(str(tmp_path), "run_1")
        record = _make_start_record()
        tracker.record_step_start(record)
        tracker.record_step_failed(record, "Something went wrong")

        import polars as pl

        df = pl.read_delta(str(tmp_path / "orchestration/steps"))
        failed = df.filter(pl.col("status") == "failed")
        assert len(failed) == 1
        row = failed.row(0, named=True)
        assert row["error"] == "Something went wrong"


class TestLoadCompletedSteps:
    """Tests for StepTracker.load_completed_steps()."""

    def test_load_completed_ordered(self, tmp_path):
        """Returns steps in step_number order."""
        tracker = StepTracker(str(tmp_path), "run_1")

        # Write steps out of order
        for step_num in [2, 0, 1]:
            record = _make_start_record(
                step_number=step_num,
                step_name=f"Step{step_num}",
                step_run_id=f"run_{step_num}",
                step_spec_id=f"spec_{step_num}",
            )
            result = _make_step_result(
                step_number=step_num, step_name=f"Step{step_num}"
            )
            tracker.record_step_start(record)
            tracker.record_step_completed(record, result)

        steps = tracker.load_completed_steps("run_1")
        assert len(steps) == 3
        assert [s.step_number for s in steps] == [0, 1, 2]

    def test_load_completed_most_recent_run(self, tmp_path):
        """None run_id returns latest run."""
        # Run 1
        tracker1 = StepTracker(str(tmp_path), "run_old")
        record1 = _make_start_record(step_run_id="old_r", step_spec_id="old_s")
        result1 = _make_step_result()
        tracker1.record_step_start(record1)
        tracker1.record_step_completed(record1, result1)

        # Run 2 (more recent)
        tracker2 = StepTracker(str(tmp_path), "run_new")
        record2 = _make_start_record(
            step_run_id="new_r", step_spec_id="new_s", step_name="ToolC"
        )
        result2 = _make_step_result(step_name="ToolC")
        tracker2.record_step_start(record2)
        tracker2.record_step_completed(record2, result2)

        # Load without specifying run_id
        tracker = StepTracker(str(tmp_path))
        steps = tracker.load_completed_steps()
        assert len(steps) == 1
        assert steps[0].step_name == "ToolC"
        assert steps[0].pipeline_run_id == "run_new"

    def test_load_completed_empty(self, tmp_path):
        """No rows returns empty list."""
        tracker = StepTracker(str(tmp_path), "run_1")
        assert tracker.load_completed_steps() == []


class TestSkippedStatus:
    """Tests for skipped step handling."""

    def test_check_cache_skipped_not_cached(self, tmp_path):
        """Skipped rows are excluded from cache lookups."""
        tracker = StepTracker(str(tmp_path), "run_1")
        record = _make_start_record(step_spec_id="spec_skip")
        result = _make_step_result(succeeded_count=0)
        tracker.record_step_start(record)
        tracker.record_step_skipped(record, result)

        # check_cache filters status=="completed", so skipped should return None
        assert tracker.check_cache("spec_skip", CachePolicy.ALL_SUCCEEDED) is None

    def test_load_completed_includes_skipped(self, tmp_path):
        """load_completed_steps() returns both completed and skipped steps."""
        tracker = StepTracker(str(tmp_path), "run_1")

        # Step 0: completed
        record0 = _make_start_record(
            step_number=0,
            step_name="Step0",
            step_run_id="run_0",
            step_spec_id="spec_0",
        )
        result0 = _make_step_result(step_number=0, step_name="Step0")
        tracker.record_step_start(record0)
        tracker.record_step_completed(record0, result0)

        # Step 1: skipped
        record1 = _make_start_record(
            step_number=1,
            step_name="Step1",
            step_run_id="run_1s",
            step_spec_id="spec_1",
        )
        result1 = _make_step_result(
            step_number=1,
            step_name="Step1",
            succeeded_count=0,
        )
        tracker.record_step_start(record1)
        tracker.record_step_skipped(record1, result1)

        steps = tracker.load_completed_steps("run_1")
        assert len(steps) == 2
        assert steps[0].step_name == "Step0"
        assert steps[0].status == "completed"
        assert steps[1].step_name == "Step1"
        assert steps[1].status == "skipped"


class TestListRuns:
    """Tests for StepTracker.list_runs()."""

    def test_list_runs(self, tmp_path):
        """Summary DataFrame with correct columns."""
        # Write two runs
        for run_id in ["run_a", "run_b"]:
            tracker = StepTracker(str(tmp_path), run_id)
            record = _make_start_record(
                step_run_id=f"{run_id}_r", step_spec_id=f"{run_id}_s"
            )
            result = _make_step_result()
            tracker.record_step_start(record)
            tracker.record_step_completed(record, result)

        tracker = StepTracker(str(tmp_path))
        runs = tracker.list_runs()
        assert len(runs) == 2
        assert "pipeline_run_id" in runs.columns
        assert "step_count" in runs.columns
        assert "last_status" in runs.columns
        assert "started_at" in runs.columns
        assert "ended_at" in runs.columns

    def test_list_runs_empty(self, tmp_path):
        """No table returns empty DataFrame."""
        tracker = StepTracker(str(tmp_path))
        runs = tracker.list_runs()
        assert len(runs) == 0
        assert "pipeline_run_id" in runs.columns


class TestCacheCorrectness:
    """Tests for cache correctness — infrastructure errors and failure policies."""

    def _write_completed_step(
        self,
        tracker: StepTracker,
        spec_id: str = "spec_001",
        succeeded: int = 5,
        failed: int = 0,
        metadata: dict | None = None,
    ) -> None:
        """Helper: write a start + completed row with given counts/metadata."""
        record = _make_start_record(step_spec_id=spec_id)
        result = _make_step_result(succeeded_count=succeeded, failed_count=failed)
        if metadata is not None:
            result = StepResult(
                step_name=result.step_name,
                step_number=result.step_number,
                success=result.success,
                total_count=result.total_count,
                succeeded_count=result.succeeded_count,
                failed_count=result.failed_count,
                output_roles=result.output_roles,
                output_types=result.output_types,
                duration_seconds=result.duration_seconds,
                metadata=metadata,
            )
        tracker.record_step_start(record)
        tracker.record_step_completed(record, result)

    def test_cache_miss_when_dispatch_error(self, tmp_path):
        """Both policies reject steps with dispatch errors."""
        tracker = StepTracker(str(tmp_path), "run_1")
        self._write_completed_step(
            tracker,
            metadata={"dispatch_error": "ConnectionError: timeout"},
        )
        assert tracker.check_cache("spec_001", CachePolicy.ALL_SUCCEEDED) is None
        assert tracker.check_cache("spec_001", CachePolicy.STEP_COMPLETED) is None

    def test_cache_miss_when_commit_error(self, tmp_path):
        """Both policies reject steps with commit errors."""
        tracker = StepTracker(str(tmp_path), "run_1")
        self._write_completed_step(
            tracker,
            metadata={"commit_error": "DeltaError: conflict"},
        )
        assert tracker.check_cache("spec_001", CachePolicy.ALL_SUCCEEDED) is None
        assert tracker.check_cache("spec_001", CachePolicy.STEP_COMPLETED) is None

    def test_cache_miss_when_failures_all_succeeded(self, tmp_path):
        """ALL_SUCCEEDED rejects steps with execution failures."""
        tracker = StepTracker(str(tmp_path), "run_1")
        self._write_completed_step(tracker, succeeded=3, failed=2)
        assert tracker.check_cache("spec_001", CachePolicy.ALL_SUCCEEDED) is None

    def test_cache_hit_when_failures_step_completed(self, tmp_path):
        """STEP_COMPLETED accepts steps with execution failures."""
        tracker = StepTracker(str(tmp_path), "run_1")
        self._write_completed_step(tracker, succeeded=3, failed=2)
        cached = tracker.check_cache("spec_001", CachePolicy.STEP_COMPLETED)
        assert cached is not None
        assert cached.succeeded_count == 3
        assert cached.failed_count == 2

    def test_cache_hit_clean_step(self, tmp_path):
        """Both policies accept clean steps (no errors, no failures)."""
        tracker = StepTracker(str(tmp_path), "run_1")
        self._write_completed_step(tracker, succeeded=5, failed=0)
        assert tracker.check_cache("spec_001", CachePolicy.ALL_SUCCEEDED) is not None
        assert tracker.check_cache("spec_001", CachePolicy.STEP_COMPLETED) is not None
