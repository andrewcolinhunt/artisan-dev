"""Tests for orchestration/engine/results.py."""

from __future__ import annotations

import pytest

from artisan.orchestration.engine.results import (
    aggregate_results,
    extract_execution_run_ids,
)
from artisan.schemas.enums import FailurePolicy


class TestAggregateResults:
    """Tests for aggregate_results()."""

    def test_all_success(self):
        """All results successful — counts match."""
        results = [
            {"success": True, "item_count": 1, "execution_run_ids": ["a"]},
            {"success": True, "item_count": 3, "execution_run_ids": ["b"]},
        ]
        succeeded, failed = aggregate_results(results, FailurePolicy.CONTINUE)
        assert succeeded == 4
        assert failed == 0

    def test_mixed_success_failure(self):
        """Mixed results — counts failures correctly."""
        results = [
            {"success": True, "item_count": 2, "execution_run_ids": ["a"]},
            {"success": False, "item_count": 1, "error": "oops"},
            {"success": True, "item_count": 1, "execution_run_ids": ["c"]},
        ]
        succeeded, failed = aggregate_results(results, FailurePolicy.CONTINUE)
        assert succeeded == 3
        assert failed == 1

    def test_fail_fast_raises(self):
        """fail_fast policy raises RuntimeError on first failure."""
        results = [
            {"success": True, "item_count": 1, "execution_run_ids": ["a"]},
            {"success": False, "item_count": 1, "error": "step failed"},
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
            {"execution_run_ids": ["a", "b"]},
            {"execution_run_ids": ["c"]},
        ]
        ids = extract_execution_run_ids(results)
        assert ids == ["a", "b", "c"]

    def test_old_format_raises_key_error(self):
        """Old 'execution_run_id' format raises KeyError with clear message."""
        results = [{"execution_run_id": "old_format"}]
        with pytest.raises(KeyError, match="deprecated"):
            extract_execution_run_ids(results)

    def test_f_string_in_error_message(self):
        """The error message properly formats the result dict (F13 fix)."""
        results = [{"execution_run_id": "old", "some_key": "value"}]
        with pytest.raises(KeyError) as exc_info:
            extract_execution_run_ids(results)
        # The error message should contain the actual dict repr, not '{r}'
        assert "{r}" not in str(exc_info.value)
