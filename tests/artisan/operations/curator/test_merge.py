"""Unit tests for the Merge operation.

Tests cover:
1. Basic merge with artifact ID lists
2. Passthrough semantics (output ID == input ID)
3. Flattened _merged_streams input format
4. Dict input format with named roles
5. Memory-only operation (no file outputs)
"""

from __future__ import annotations

from unittest.mock import Mock

import polars as pl
import pytest

from artisan.operations.curator.merge import Merge
from artisan.schemas.artifact.types import ArtifactTypes


def _df(ids: list[str]) -> pl.DataFrame:
    """Helper to create a DataFrame with artifact_id column."""
    return pl.DataFrame({"artifact_id": ids})


def _mock_store() -> Mock:
    """Create a mock ArtifactStore."""
    return Mock()


class TestMergeBasicExecution:
    """Tests for basic Merge execute_curator() execution."""

    def test_should_passthrough_all_artifact_ids(self):
        """Test should passthrough all artifact IDs from _merged_streams."""
        op = Merge()
        result = op.execute_curator(
            inputs={"_merged_streams": _df(["artifact_1", "artifact_2", "artifact_3"])},
            step_number=0,
            artifact_store=_mock_store(),
        )

        assert result.success
        assert len(result.passthrough["merged"]) == 3
        for i in range(1, 4):
            artifact_id = f"artifact_{i}"
            assert artifact_id in result.passthrough["merged"]

    def test_should_handle_single_artifact(self):
        """Test should handle single artifact input."""
        op = Merge()
        result = op.execute_curator(
            inputs={"_merged_streams": _df(["single_artifact_123"])},
            step_number=0,
            artifact_store=_mock_store(),
        )

        assert result.success
        assert len(result.passthrough["merged"]) == 1
        assert "single_artifact_123" in result.passthrough["merged"]


class TestMergeDictInputFormat:
    """Tests for dict input format with named roles."""

    def test_should_merge_multiple_roles(self):
        """Test should merge artifacts from multiple named roles."""
        op = Merge()
        result = op.execute_curator(
            inputs={
                "branch_a": _df(["artifact_a1", "artifact_a2"]),
                "branch_b": _df(["artifact_b1"]),
            },
            step_number=0,
            artifact_store=_mock_store(),
        )

        # Should have 3 total results (2 from a, 1 from b)
        assert result.success
        assert len(result.passthrough["merged"]) == 3

        assert "artifact_a1" in result.passthrough["merged"]
        assert "artifact_a2" in result.passthrough["merged"]
        assert "artifact_b1" in result.passthrough["merged"]


class TestMergePassthroughSemantics:
    """Tests for passthrough semantics (no new artifact created)."""

    def test_output_is_same_as_input_id(self):
        """Test output artifact ID matches input artifact ID exactly."""
        input_ids = [
            "original_artifact_id_12345678901234567890",
            "another_artifact_abcdef",
        ]

        op = Merge()
        result = op.execute_curator(
            inputs={"_merged_streams": _df(input_ids)},
            step_number=0,
            artifact_store=_mock_store(),
        )

        assert result.success
        for artifact_id in input_ids:
            assert artifact_id in result.passthrough["merged"]


class TestMergeEmptyInputs:
    """Tests for empty input handling."""

    def test_should_return_empty_results_for_empty_list(self):
        """Test should return empty results for empty input list."""
        op = Merge()
        result = op.execute_curator(
            inputs={"_merged_streams": _df([])},
            step_number=0,
            artifact_store=_mock_store(),
        )

        assert result.success
        assert len(result.passthrough["merged"]) == 0

    def test_should_return_empty_results_for_no_roles(self):
        """Test should return empty results when no inputs provided."""
        op = Merge()
        result = op.execute_curator(
            inputs={},
            step_number=0,
            artifact_store=_mock_store(),
        )

        assert result.success
        assert len(result.passthrough["merged"]) == 0


class TestMergeClassAttributes:
    """Tests for Merge class attributes."""

    def test_should_have_correct_name(self):
        """Test Merge has correct operation name."""
        assert Merge.name == "merge"

    def test_should_have_description(self):
        """Test Merge has a description."""
        assert Merge.description is not None
        assert "union" in Merge.description.lower()

    def test_should_have_empty_inputs_spec(self):
        """Test Merge has empty inputs spec (dynamic inputs)."""
        assert Merge.inputs == {}

    def test_should_have_merged_output_spec(self):
        """Test Merge has 'merged' output specification."""
        assert "merged" in Merge.outputs
        assert Merge.outputs["merged"].artifact_type == ArtifactTypes.ANY

    def test_should_have_runtime_defined_inputs(self):
        """Test Merge accepts runtime-defined inputs."""
        assert Merge.runtime_defined_inputs is True

    def test_should_allow_independent_input_streams(self):
        """Test Merge allows independent input streams (union semantics)."""
        assert Merge.independent_input_streams is True


class TestMergeConsistency:
    """Tests for consistent behavior across multiple calls."""

    def test_same_input_produces_same_output(self):
        """Test same input produces same output across calls."""
        artifact_ids = ["artifact_1", "artifact_2", "artifact_3"]

        op = Merge()
        result1 = op.execute_curator(
            inputs={"_merged_streams": _df(artifact_ids)},
            step_number=0,
            artifact_store=_mock_store(),
        )
        result2 = op.execute_curator(
            inputs={"_merged_streams": _df(artifact_ids)},
            step_number=0,
            artifact_store=_mock_store(),
        )

        assert result1.passthrough == result2.passthrough


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
