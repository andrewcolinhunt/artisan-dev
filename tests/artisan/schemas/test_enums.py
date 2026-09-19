"""Tests for enums.py"""

from __future__ import annotations

from artisan.schemas.enums import (
    CacheValidationReason,
    FailurePolicy,
    GroupByStrategy,
    TablePath,
)


class TestGroupByStrategy:
    """Tests for GroupByStrategy enum."""

    def test_strategies_defined(self):
        """Verify all grouping strategies are present."""
        assert GroupByStrategy.LINEAGE.value == "lineage"
        assert GroupByStrategy.CROSS_PRODUCT.value == "cross_product"
        assert GroupByStrategy.ZIP.value == "zip"
        assert GroupByStrategy.NAME.value == "name"

    def test_enum_count(self):
        """Ensure exactly 4 strategies."""
        assert len(GroupByStrategy) == 4


class TestCacheValidationReason:
    """Tests for cache miss reasons."""

    def test_reasons_defined(self):
        """Verify the persisted cache miss reason values."""
        assert (
            CacheValidationReason.NO_PREVIOUS_EXECUTION.value == "no_previous_execution"
        )
        assert CacheValidationReason.EXECUTION_FAILED.value == "execution_failed"

    def test_enum_count(self):
        """Ensure exactly 2 reasons."""
        assert len(CacheValidationReason) == 2


class TestTablePath:
    """Tests for framework table paths."""

    def test_tables_defined(self):
        """Verify all framework Delta Lake table paths."""
        assert TablePath.ARTIFACT_INDEX == "artifacts/index"
        assert TablePath.ARTIFACT_LOCATIONS == "artifacts/locations"
        assert TablePath.ARTIFACT_EDGES == "provenance/artifact_edges"
        assert TablePath.EXECUTION_EDGES == "provenance/execution_edges"
        assert TablePath.EXECUTIONS == "orchestration/executions"
        assert TablePath.CACHE_REUSE == "orchestration/cache_reuse"
        assert TablePath.LOGICAL_COMMITS == "orchestration/logical_commits"
        assert TablePath.STEPS == "orchestration/steps"

    def test_enum_count(self):
        """Ensure exactly 8 framework tables."""
        assert len(TablePath) == 8

    def test_execution_edges_value(self):
        """Test EXECUTION_EDGES enum value."""
        assert TablePath.EXECUTION_EDGES == "provenance/execution_edges"

    def test_steps_value(self):
        """Test STEPS enum value."""
        assert TablePath.STEPS == "orchestration/steps"


class TestFailurePolicy:
    """Tests for FailurePolicy enum."""

    def test_valid_values(self):
        """Test that FailurePolicy enum has expected members."""
        assert FailurePolicy.CONTINUE.value == "continue"
        assert FailurePolicy.FAIL_FAST.value == "fail_fast"
