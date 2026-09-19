"""Tests for orchestration/engine/results.py."""

from __future__ import annotations

from artisan.orchestration.engine.results import (
    aggregate_results,
    extract_execution_run_ids,
)
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
        succeeded, failed = aggregate_results(results)
        assert succeeded == 4
        assert failed == 0

    def test_mixed_success_failure(self):
        """Mixed results — counts failures correctly."""
        results = [
            _result(item_count=2, execution_run_ids=["a"]),
            _result(success=False, item_count=1, error="oops"),
            _result(item_count=1, execution_run_ids=["c"]),
        ]
        succeeded, failed = aggregate_results(results)
        assert succeeded == 3
        assert failed == 1

    def test_empty_results(self):
        """Empty results — zero counts."""
        succeeded, failed = aggregate_results([])
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

    def test_filters_missing_ids_and_preserves_order(self):
        results = [
            _result(execution_run_ids=["id1"]),
            _result(execution_run_ids=["id2", "id3"]),
            _result(success=False, error="fail", execution_run_ids=[None]),
            _result(execution_run_ids=[]),
        ]

        assert extract_execution_run_ids(results) == ["id1", "id2", "id3"]

    def test_empty_results(self):
        assert extract_execution_run_ids([]) == []
