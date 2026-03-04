"""Tests for curator operations provenance behavior.

This module verifies that curator operations (Merge, Filter) have the
correct provenance behavior as documented in design_provenance_phase5_operations.md.

Summary of expected behavior:
- Merge: No provenance edges - passthrough semantics (artifacts unchanged)
- Filter: No provenance edges - selection semantics (artifacts unchanged)

Reference: design_provenance_phase5_operations.md "Curator Operations Provenance"
"""

from __future__ import annotations

import pytest

from artisan.operations.curator.filter import Filter
from artisan.operations.curator.merge import Merge
from artisan.schemas.artifact.types import ArtifactTypes


class TestMergeProvenance:
    """Tests for Merge operation provenance behavior.

    Merge is a routing/passthrough operation that creates NO provenance edges.
    Artifacts pass through unchanged and retain their original provenance.
    """

    def test_output_is_any_type(self):
        """Test that Merge uses ArtifactTypes.ANY (accepts any concrete type).

        ArtifactTypes.ANY indicates that the operation does not constrain
        artifact types - they pass through with their original type preserved.
        """
        spec = Merge.outputs["merged"]
        assert spec.artifact_type == ArtifactTypes.ANY

    def test_no_infer_lineage_from(self):
        """Test that Merge does not declare lineage.

        Merge is a routing operation - artifacts pass through unchanged
        and retain their original provenance. No new edges are created.
        """
        spec = Merge.outputs["merged"]
        assert spec.infer_lineage_from is None


class TestFilterProvenance:
    """Tests for Filter operation provenance behavior.

    Filter is a selection operation that creates NO provenance edges.
    Selected artifacts pass through unchanged and retain their original provenance.
    """

    def test_output_is_any_type(self):
        """Test that Filter uses ArtifactTypes.ANY (accepts any concrete type).

        ArtifactTypes.ANY indicates that the operation does not constrain
        artifact types - they pass through with their original type preserved.
        """
        spec = Filter.outputs["passthrough"]
        assert spec.artifact_type == ArtifactTypes.ANY

    def test_no_infer_lineage_from(self):
        """Test that Filter does not declare lineage.

        Filter is a selection operation - artifacts pass through unchanged
        and retain their original provenance. No new edges are created.
        """
        spec = Filter.outputs["passthrough"]
        assert spec.infer_lineage_from is None

    def test_runtime_defined_inputs(self):
        """Test that Filter uses runtime-defined inputs (direct-input pattern)."""
        assert Filter.runtime_defined_inputs is True
        assert "passthrough" in Filter.inputs


class TestCuratorOperationsSummary:
    """Summary tests for curator operations provenance patterns."""

    @pytest.mark.parametrize(
        "op_class,output_role,expected_type,expects_edges",
        [
            (Merge, "merged", ArtifactTypes.ANY, False),
            (Filter, "passthrough", ArtifactTypes.ANY, False),
        ],
    )
    def test_artifact_type_indicates_provenance_pattern(
        self, op_class, output_role, expected_type, expects_edges
    ):
        """Test that artifact_type indicates the provenance pattern.

        - ArtifactTypes.ANY (passthrough): No provenance edges created
        """
        spec = op_class.outputs[output_role]
        assert spec.artifact_type == expected_type

    @pytest.mark.parametrize(
        ("op_class", "output_role"),
        [
            (Merge, "merged"),
            (Filter, "passthrough"),
        ],
    )
    def test_passthrough_curator_outputs_use_any_type(self, op_class, output_role):
        """Test that passthrough curator operations use ArtifactTypes.ANY."""
        spec = op_class.outputs[output_role]
        assert spec.artifact_type == ArtifactTypes.ANY
