"""Tests for storage/provenance_utils.py."""

from __future__ import annotations

from artisan.storage.provenance_utils import trace_derived_artifacts


class TestTraceDerivedArtifacts:
    """Tests for the BFS provenance tracer."""

    def test_direct_metric(self):
        """Structure directly linked to metric."""
        forward = {"s": ["m"]}
        result = trace_derived_artifacts("s", forward, {"m"})
        assert result == ["m"]

    def test_multi_hop_through_group(self):
        """Structure -> group -> metric."""
        forward = {"s": ["g"], "g": ["m"]}
        result = trace_derived_artifacts("s", forward, {"m"})
        assert result == ["m"]

    def test_no_metrics(self):
        """Structure with no derived metrics."""
        forward = {"s": ["g"]}
        result = trace_derived_artifacts("s", forward, set())
        assert result == []

    def test_multiple_metrics(self):
        """Structure derives multiple metrics."""
        forward = {"s": ["g"], "g": ["m1", "m2"]}
        result = trace_derived_artifacts("s", forward, {"m1", "m2"})
        assert set(result) == {"m1", "m2"}

    def test_no_provenance_edges(self):
        """Structure with no outgoing edges."""
        result = trace_derived_artifacts("s", {}, {"m"})
        assert result == []

    def test_cycle_handling(self):
        """Handles cycles without infinite loop."""
        forward = {"s": ["g"], "g": ["s", "m"]}
        result = trace_derived_artifacts("s", forward, {"m"})
        assert result == ["m"]

    def test_empty_forward_map(self):
        """Empty forward map returns empty list."""
        result = trace_derived_artifacts("s", {}, set())
        assert result == []
