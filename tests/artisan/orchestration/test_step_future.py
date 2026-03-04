"""Tests for StepFuture non-blocking handle."""

from __future__ import annotations

import time
from concurrent.futures import Future

import pytest

from artisan.orchestration.step_future import StepFuture
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.step_result import StepResult


def _make_result(step_number: int = 0) -> StepResult:
    return StepResult(
        step_name="TestOp",
        step_number=step_number,
        success=True,
        total_count=5,
        succeeded_count=5,
        failed_count=0,
        output_roles=frozenset(["data", "metrics"]),
        output_types={"data": "data", "metrics": "metric"},
        duration_seconds=2.0,
    )


def _make_future(
    step_number: int = 0,
    output_roles: frozenset[str] | None = None,
    output_types: dict[str, str | None] | None = None,
    cf_future: Future | None = None,
) -> StepFuture:
    if output_roles is None:
        output_roles = frozenset(["data", "metrics"])
    if output_types is None:
        output_types = {"data": "data", "metrics": "metric"}
    if cf_future is None:
        cf_future = Future()
    return StepFuture(
        step_number=step_number,
        step_name="TestOp",
        output_roles=output_roles,
        output_types=output_types,
        future=cf_future,
    )


class TestOutput:
    """Tests for StepFuture.output()."""

    def test_output_returns_reference(self):
        """Correct source_step, role, artifact_type."""
        future = _make_future(step_number=3)
        ref = future.output("data")
        assert isinstance(ref, OutputReference)
        assert ref.source_step == 3
        assert ref.role == "data"
        assert ref.artifact_type == "data"

    def test_output_invalid_role(self):
        """ValueError with available roles."""
        future = _make_future()
        with pytest.raises(ValueError, match="Output role 'missing' not available"):
            future.output("missing")
        with pytest.raises(ValueError, match="data, metrics"):
            future.output("missing")

    def test_output_never_blocks(self):
        """Returns immediately even when future is not done."""
        cf = Future()  # Not resolved
        future = _make_future(cf_future=cf)
        start = time.monotonic()
        ref = future.output("data")
        elapsed = time.monotonic() - start
        assert elapsed < 0.1
        assert isinstance(ref, OutputReference)


class TestDone:
    """Tests for StepFuture.done property."""

    def test_done_before_completion(self):
        """False when future not completed."""
        cf = Future()
        future = _make_future(cf_future=cf)
        assert future.done is False

    def test_done_after_completion(self):
        """True after set_result."""
        cf = Future()
        cf.set_result(_make_result())
        future = _make_future(cf_future=cf)
        assert future.done is True


class TestStatus:
    """Tests for StepFuture.status property."""

    def test_status_running(self):
        """'running' before completion."""
        cf = Future()
        future = _make_future(cf_future=cf)
        assert future.status == "running"

    def test_status_completed(self):
        """'completed' after set_result."""
        cf = Future()
        cf.set_result(_make_result())
        future = _make_future(cf_future=cf)
        assert future.status == "completed"

    def test_status_failed(self):
        """'failed' after set_exception."""
        cf = Future()
        cf.set_exception(RuntimeError("boom"))
        future = _make_future(cf_future=cf)
        assert future.status == "failed"


class TestResult:
    """Tests for StepFuture.result()."""

    def test_result_blocks(self):
        """Blocks and returns StepResult."""
        cf = Future()
        result = _make_result()
        cf.set_result(result)
        future = _make_future(cf_future=cf)
        assert future.result() == result

    def test_result_timeout(self):
        """TimeoutError with descriptive message."""
        cf = Future()  # Never resolved
        future = _make_future(cf_future=cf)
        with pytest.raises(TimeoutError, match="did not complete within"):
            future.result(timeout=0.01)

    def test_result_propagates_exception(self):
        """Original exception raised."""
        cf = Future()
        cf.set_exception(ValueError("bad input"))
        future = _make_future(cf_future=cf)
        with pytest.raises(ValueError, match="bad input"):
            future.result()
