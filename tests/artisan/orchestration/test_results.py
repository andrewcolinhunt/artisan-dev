"""Tests for orchestration/engine/results.py."""

from __future__ import annotations

import pytest

from artisan.orchestration.engine.results import (
    FailFastAbort,
    aggregate_results,
    extract_execution_run_ids,
    raise_if_fail_fast,
)
from artisan.schemas.enums import FailurePolicy
from artisan.schemas.execution.unit_result import UnitResult


def _result(**overrides: object) -> UnitResult:
    """Build a UnitResult with sensible defaults."""
    defaults = {
        "success": True,
        "error": None,
        "item_count": 1,
        "execution_run_ids": [],
    }
    return UnitResult(**{**defaults, **overrides})


class TestAggregateResults:
    """Tests for aggregate_results()."""

    def test_all_success(self):
        """All results successful — counts match."""
        results = [
            _result(item_count=1, execution_run_ids=["a"]),
            _result(item_count=3, execution_run_ids=["b"]),
        ]
        succeeded, failed = aggregate_results(results, FailurePolicy.CONTINUE)
        assert succeeded == 4
        assert failed == 0

    def test_mixed_success_failure(self):
        """Mixed results — counts failures correctly."""
        results = [
            _result(item_count=2, execution_run_ids=["a"]),
            _result(success=False, item_count=1, error="oops"),
            _result(item_count=1, execution_run_ids=["c"]),
        ]
        succeeded, failed = aggregate_results(results, FailurePolicy.CONTINUE)
        assert succeeded == 3
        assert failed == 1

    def test_fail_fast_does_not_raise(self):
        """fail_fast aggregation only counts — the abort moved post-commit."""
        results = [
            _result(execution_run_ids=["a"]),
            _result(success=False, error="step failed"),
        ]
        succeeded, failed = aggregate_results(results, FailurePolicy.FAIL_FAST)
        assert succeeded == 1
        assert failed == 1

    def test_empty_results(self):
        """Empty results — zero counts."""
        succeeded, failed = aggregate_results([], FailurePolicy.CONTINUE)
        assert succeeded == 0
        assert failed == 0


class TestRaiseIfFailFast:
    """Tests for raise_if_fail_fast() — the post-commit fail_fast abort."""

    def test_raises_on_failure_under_fail_fast(self):
        """FAIL_FAST + a failure raises with the first failure's error."""
        results = [
            _result(execution_run_ids=["a"]),
            _result(success=False, error="step failed"),
        ]
        with pytest.raises(FailFastAbort, match="fail_fast"):
            raise_if_fail_fast(FailurePolicy.FAIL_FAST, failed=1, results=results)

    def test_message_carries_first_failure_error(self):
        """The abort message surfaces the failing unit's error string."""
        results = [_result(success=False, error="boom detail")]
        with pytest.raises(FailFastAbort, match="boom detail"):
            raise_if_fail_fast(FailurePolicy.FAIL_FAST, failed=1, results=results)

    def test_falls_back_to_dispatch_error(self):
        """With no per-unit error, the dispatch_error is surfaced."""
        with pytest.raises(FailFastAbort, match="pool broke"):
            raise_if_fail_fast(
                FailurePolicy.FAIL_FAST,
                failed=1,
                results=[],
                dispatch_error="pool broke",
            )

    def test_no_raise_under_continue(self):
        """CONTINUE never aborts, even with failures."""
        results = [_result(success=False, error="x")]
        raise_if_fail_fast(FailurePolicy.CONTINUE, failed=1, results=results)

    def test_no_raise_when_no_failures(self):
        """FAIL_FAST with zero failures is a no-op."""
        raise_if_fail_fast(FailurePolicy.FAIL_FAST, failed=0, results=[])


class TestExtractExecutionRunIds:
    """Tests for extract_execution_run_ids()."""

    def test_extracts_ids(self):
        """Extracts all execution_run_ids from results."""
        results = [
            _result(execution_run_ids=["a", "b"]),
            _result(execution_run_ids=["c"]),
        ]
        ids = extract_execution_run_ids(results)
        assert ids == ["a", "b", "c"]
