"""Tests for orchestration/engine/results.py."""

from __future__ import annotations

import pytest

from artisan.orchestration.engine.results import (
    aggregate_results,
    extract_execution_run_ids,
)
from artisan.schemas.enums import FailurePolicy
from artisan.schemas.execution.unit_result import UnitResult


def _result(**overrides: object) -> UnitResult:
    """Build a UnitResult with sensible defaults."""
    defaults = dict(success=True, error=None, item_count=1, execution_run_ids=[])
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

    def test_fail_fast_raises(self):
        """fail_fast policy raises RuntimeError on first failure."""
        results = [
            _result(execution_run_ids=["a"]),
            _result(success=False, error="step failed"),
        ]
        with pytest.raises(RuntimeError, match="fail_fast"):
            aggregate_results(results, FailurePolicy.FAIL_FAST)

    def test_empty_results(self):
        """Empty results — zero counts."""
        succeeded, failed = aggregate_results([], FailurePolicy.CONTINUE)
        assert succeeded == 0
        assert failed == 0


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
