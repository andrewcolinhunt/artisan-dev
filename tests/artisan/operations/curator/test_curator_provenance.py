"""Check passthrough curators' static output and lineage declarations.

These assertions do not inspect runtime edges. Passthrough execution records
input/output edges even when no new artifact-lineage edges are declared.
"""

from __future__ import annotations

from artisan.operations.curator.filter import Filter
from artisan.operations.curator.merge import Merge
from artisan.schemas.artifact.types import ArtifactTypes


class TestMergeProvenance:
    """Check Merge's passthrough output declarations."""

    def test_output_is_any_type(self):
        """Test that Merge uses ArtifactTypes.ANY (accepts any concrete type).

        ArtifactTypes.ANY indicates that the operation does not constrain
        artifact types - they pass through with their original type preserved.
        """
        spec = Merge.outputs["merged"]
        assert spec.artifact_type == ArtifactTypes.ANY

    def test_no_infer_lineage_from(self):
        """Test that Merge does not declare lineage.

        The output spec requests no automatic artifact-lineage inference.
        """
        spec = Merge.outputs["merged"]
        assert spec.infer_lineage_from is None


class TestFilterProvenance:
    """Check Filter's passthrough input and output declarations."""

    def test_output_is_any_type(self):
        """Test that Filter uses ArtifactTypes.ANY (accepts any concrete type).

        ArtifactTypes.ANY indicates that the operation does not constrain
        artifact types - they pass through with their original type preserved.
        """
        spec = Filter.outputs["passthrough"]
        assert spec.artifact_type == ArtifactTypes.ANY

    def test_no_infer_lineage_from(self):
        """Test that Filter does not declare lineage.

        The output spec requests no automatic artifact-lineage inference.
        """
        spec = Filter.outputs["passthrough"]
        assert spec.infer_lineage_from is None

    def test_fixed_inputs(self):
        """Test that Filter declares fixed passthrough input only."""
        assert Filter.runtime_defined_inputs is False
        assert "passthrough" in Filter.inputs
        assert len(Filter.inputs) == 1
