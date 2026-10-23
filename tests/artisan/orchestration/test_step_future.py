"""Tests for StepFuture non-blocking handle."""

from __future__ import annotations

import time
from concurrent.futures import Future

import pytest

from artisan.orchestration.step_future import StepFuture
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_result import StepResult


def _make_result(step_number: int = 0) -> StepResult:
    return StepResult(
        step_name="TestOp",
        step_number=step_number,
        status=StepStatus.SUCCEEDED,
        disposition=StepDisposition.EXECUTED,
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
    status: StepStatus = StepStatus.PENDING,
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
        status_reader=lambda: status,
    )


class TestOutputRoles:
    """Tests for StepFuture.output_roles property."""

    def test_output_roles_returns_frozenset(self):
        """Returns the declared output roles."""
        future = _make_future()
        assert future.output_roles == frozenset(["data", "metrics"])

    def test_output_roles_empty(self):
        """Empty frozenset when no outputs declared."""
        future = _make_future(output_roles=frozenset(), output_types={})
        assert future.output_roles == frozenset()

    def test_output_types_returns_copy(self):
        """Callers cannot mutate the future's declared output types."""
        future = _make_future()

        output_types = future.output_types
        output_types["data"] = "changed"

        assert future.output_types["data"] == "data"


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
        """Durable running state is returned before closure completion."""
        cf = Future()
        future = _make_future(cf_future=cf, status=StepStatus.RUNNING)
        assert future.status == StepStatus.RUNNING

    def test_status_succeeded(self):
        """Durable succeeded state is returned after terminalization."""
        cf = Future()
        cf.set_result(_make_result())
        future = _make_future(cf_future=cf, status=StepStatus.SUCCEEDED)
        assert future.status == StepStatus.SUCCEEDED

    def test_python_exception_does_not_derive_status(self):
        """Raw closure state cannot override the durable lifecycle reader."""
        cf = Future()
        cf.set_exception(RuntimeError("boom"))
        future = _make_future(cf_future=cf, status=StepStatus.RUNNING)
        assert future.status == StepStatus.RUNNING

    def test_python_cancellation_does_not_fabricate_terminal_status(self):
        """Executor cancellation alone is not durable cancellation proof."""
        cf = Future()
        cf.cancel()
        future = _make_future(cf_future=cf, status=StepStatus.PENDING)
        assert future.status == StepStatus.PENDING


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
