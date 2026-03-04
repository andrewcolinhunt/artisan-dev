"""Tests for enums.py"""

from __future__ import annotations

from artisan.schemas.enums import (
    CacheValidationReason,
    GroupByStrategy,
    TablePath,
)


class TestGroupByStrategy:
    """Tests for GroupByStrategy enum."""

    def test_strategies_defined(self):
        """Verify all grouping strategies from v3 design."""
        assert GroupByStrategy.LINEAGE.value == "lineage"
        assert GroupByStrategy.CROSS_PRODUCT.value == "cross_product"
        assert GroupByStrategy.ZIP.value == "zip"

    def test_enum_count(self):
        """Ensure exactly 3 strategies."""
        assert len(GroupByStrategy) == 3


class TestCacheValidationReason:
    """Tests for CacheValidationReason enum.

    Reference: v3 design - simplified cache validation based on execution_spec_id.
    """

    def test_reasons_defined(self):
        """Verify all cache validation reasons from v3 design."""
        # Cache miss reasons
        assert (
            CacheValidationReason.NO_PREVIOUS_EXECUTION.value == "no_previous_execution"
        )
        assert CacheValidationReason.EXECUTION_FAILED.value == "execution_failed"

    def test_enum_count(self):
        """Ensure exactly 2 reasons."""
        assert len(CacheValidationReason) == 2


class TestTablePath:
    """Tests for TablePath enum.

    Reference: Phase 2 storage layer design.
    """

    def test_tables_defined(self):
        """Verify all framework Delta Lake table paths."""
        assert TablePath.ARTIFACT_INDEX == "artifacts/index"
        assert TablePath.ARTIFACT_EDGES == "provenance/artifact_edges"
        assert TablePath.EXECUTION_EDGES == "provenance/execution_edges"
        assert TablePath.EXECUTIONS == "orchestration/executions"
        assert TablePath.STEPS == "orchestration/steps"

    def test_enum_count(self):
        """Ensure exactly 5 framework tables."""
        assert len(TablePath) == 5

    def test_execution_edges_value(self):
        """Test EXECUTION_EDGES enum value."""
        assert TablePath.EXECUTION_EDGES == "provenance/execution_edges"

    def test_steps_value(self):
        """Test STEPS enum value."""
        assert TablePath.STEPS == "orchestration/steps"
