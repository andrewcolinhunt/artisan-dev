"""Tests for output_spec.py"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.specs.output_spec import OutputSpec


class TestOutputSpec:
    """Tests for OutputSpec dataclass."""

    def test_create_data_output(self):
        """Create OutputSpec for data output with default filename matching."""
        spec = OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            description="Processed data files",
        )
        assert spec.artifact_type == ArtifactTypes.DATA
        assert spec.infer_lineage_from is None  # Default filename matching

    def test_default_values(self):
        """Test default values."""
        spec = OutputSpec()
        assert spec.artifact_type == ArtifactTypes.ANY
        assert spec.description == ""
        assert spec.required is True
        assert spec.infer_lineage_from is None

    def test_metric_output(self):
        """OutputSpec for metric output."""
        spec = OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            description="Score values",
        )
        assert spec.artifact_type == ArtifactTypes.METRIC

    def test_any_type_output(self):
        """OutputSpec with ArtifactTypes.ANY (passthrough/routing pattern)."""
        spec = OutputSpec(
            artifact_type=ArtifactTypes.ANY,
            description="Artifact that passed filter",
        )
        assert spec.artifact_type == ArtifactTypes.ANY

    def test_frozen(self):
        """OutputSpec is immutable."""
        spec = OutputSpec(artifact_type=ArtifactTypes.DATA)
        with pytest.raises(Exception):  # Pydantic raises ValidationError for frozen
            spec.artifact_type = ArtifactTypes.METRIC


class TestInferLineageFrom:
    """Tests for infer_lineage_from field validation."""

    def test_infer_lineage_from_none(self):
        """Test OutputSpec with default lineage (filename matching)."""
        spec = OutputSpec(
            artifact_type=ArtifactTypes.DATA,
        )
        assert spec.infer_lineage_from is None

    def test_infer_lineage_from_inputs(self):
        """Test OutputSpec with explicit input lineage."""
        spec = OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            infer_lineage_from={"inputs": ["data"]},
        )
        assert spec.infer_lineage_from == {"inputs": ["data"]}

    def test_infer_lineage_from_outputs(self):
        """Test OutputSpec with output->output lineage."""
        spec = OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            infer_lineage_from={"outputs": ["data"]},
        )
        assert spec.infer_lineage_from == {"outputs": ["data"]}

    def test_rejects_combined_inputs_outputs(self):
        """Test that combined inputs+outputs (old GROUP pattern) is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["data"], "outputs": ["data"]},
            )
        assert "no longer supported" in str(exc_info.value)

    def test_infer_lineage_from_generative(self):
        """Test OutputSpec with generative operation (no parents)."""
        spec = OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": []},  # Explicit: no parents
        )
        assert spec.infer_lineage_from == {"inputs": []}

    def test_rejects_empty_dict(self):
        """Test that empty dict {} is rejected as invalid."""
        with pytest.raises(ValidationError) as exc_info:
            OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={},
            )
        assert "Empty dict {}" in str(exc_info.value)

    def test_rejects_invalid_keys(self):
        """Test that only 'inputs' and 'outputs' keys are allowed."""
        with pytest.raises(ValidationError) as exc_info:
            OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"invalid_key": ["data"]},
            )
        assert "Invalid keys" in str(exc_info.value)
        assert "invalid_key" in str(exc_info.value)

    def test_rejects_mixed_invalid_keys(self):
        """Test rejection when mixing valid and invalid keys."""
        with pytest.raises(ValidationError) as exc_info:
            OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={
                    "inputs": ["a"],
                    "bad_key": ["b"],
                },
            )
        assert "Invalid keys" in str(exc_info.value)

    def test_hash_with_infer_lineage_from(self):
        """Test that OutputSpec is hashable with infer_lineage_from."""
        spec1 = OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            infer_lineage_from={"inputs": ["a", "b"]},
        )
        spec2 = OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            infer_lineage_from={"inputs": ["a", "b"]},
        )
        spec3 = OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            infer_lineage_from={"inputs": ["b", "a"]},  # Different order
        )

        # Same config should have same hash
        assert hash(spec1) == hash(spec2)

        # Different order in list should have different hash
        # (lists are order-sensitive)
        assert hash(spec1) != hash(spec3)

    def test_hash_none_lineage(self):
        """Test hash with None infer_lineage_from."""
        spec = OutputSpec(artifact_type=ArtifactTypes.METRIC)
        # Should not raise
        assert isinstance(hash(spec), int)

    def test_hash_with_outputs_lineage(self):
        """Test hash with outputs key."""
        spec1 = OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            infer_lineage_from={"outputs": ["b"]},
        )
        spec2 = OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            infer_lineage_from={"outputs": ["b"]},
        )

        assert hash(spec1) == hash(spec2)
