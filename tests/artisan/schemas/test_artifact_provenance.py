"""Tests for ArtifactProvenanceEdge model."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge


class TestArtifactProvenanceEdge:
    """Tests for ArtifactProvenanceEdge model."""

    def test_valid_provenance_edge(self):
        """Test creating a valid provenance edge."""
        edge = ArtifactProvenanceEdge(
            execution_run_id="a" * 32,
            source_artifact_id="b" * 32,
            target_artifact_id="c" * 32,
            source_artifact_type="data",
            target_artifact_type="metric",
            source_role="data",
            target_role="score",
        )

        assert edge.execution_run_id == "a" * 32
        assert edge.source_artifact_id == "b" * 32
        assert edge.target_artifact_id == "c" * 32
        assert edge.source_artifact_type == "data"
        assert edge.target_artifact_type == "metric"
        assert edge.source_role == "data"
        assert edge.target_role == "score"
        assert edge.group_id is None

    def test_valid_provenance_edge_with_group_id(self):
        """Test creating a provenance edge with group_id set."""
        edge = ArtifactProvenanceEdge(
            execution_run_id="a" * 32,
            source_artifact_id="b" * 32,
            target_artifact_id="c" * 32,
            source_artifact_type="data",
            target_artifact_type="metric",
            source_role="data",
            target_role="score",
            group_id="g" * 32,
        )

        assert edge.group_id == "g" * 32

    def test_invalid_execution_run_id_too_short(self):
        """Test that execution_run_id must be at least 32 chars."""
        with pytest.raises(ValidationError) as exc_info:
            ArtifactProvenanceEdge(
                execution_run_id="short",
                source_artifact_id="b" * 32,
                target_artifact_id="c" * 32,
                source_artifact_type="data",
                target_artifact_type="metric",
                source_role="data",
                target_role="score",
            )
        assert (
            "min_length" in str(exc_info.value).lower()
            or "at least 32" in str(exc_info.value).lower()
        )

    def test_invalid_execution_run_id_too_long(self):
        """Test that execution_run_id must be at most 32 chars."""
        with pytest.raises(ValidationError):
            ArtifactProvenanceEdge(
                execution_run_id="a" * 33,
                source_artifact_id="b" * 32,
                target_artifact_id="c" * 32,
                source_artifact_type="data",
                target_artifact_type="metric",
                source_role="data",
                target_role="score",
            )

    def test_invalid_source_artifact_id_length(self):
        """Test that source_artifact_id must be exactly 32 chars."""
        with pytest.raises(ValidationError):
            ArtifactProvenanceEdge(
                execution_run_id="a" * 32,
                source_artifact_id="short",
                target_artifact_id="c" * 32,
                source_artifact_type="data",
                target_artifact_type="metric",
                source_role="data",
                target_role="score",
            )

    def test_invalid_target_artifact_id_length(self):
        """Test that target_artifact_id must be exactly 32 chars."""
        with pytest.raises(ValidationError):
            ArtifactProvenanceEdge(
                execution_run_id="a" * 32,
                source_artifact_id="b" * 32,
                target_artifact_id="short",
                source_artifact_type="data",
                target_artifact_type="metric",
                source_role="data",
                target_role="score",
            )

    def test_frozen_model(self):
        """Test that ArtifactProvenanceEdge is immutable."""
        edge = ArtifactProvenanceEdge(
            execution_run_id="a" * 32,
            source_artifact_id="b" * 32,
            target_artifact_id="c" * 32,
            source_artifact_type="data",
            target_artifact_type="metric",
            source_role="data",
            target_role="score",
        )

        with pytest.raises(ValidationError):
            edge.source_role = "other"

    def test_extra_fields_forbidden(self):
        """Test that extra fields are not allowed."""
        with pytest.raises(ValidationError) as exc_info:
            ArtifactProvenanceEdge(
                execution_run_id="a" * 32,
                source_artifact_id="b" * 32,
                target_artifact_id="c" * 32,
                source_artifact_type="data",
                target_artifact_type="metric",
                source_role="data",
                target_role="score",
                extra_field="not allowed",
            )
        assert "extra" in str(exc_info.value).lower()

    def test_hashable(self):
        """Test that ArtifactProvenanceEdge is hashable (8 fields)."""
        edge1 = ArtifactProvenanceEdge(
            execution_run_id="a" * 32,
            source_artifact_id="b" * 32,
            target_artifact_id="c" * 32,
            source_artifact_type="data",
            target_artifact_type="metric",
            source_role="data",
            target_role="score",
        )
        edge2 = ArtifactProvenanceEdge(
            execution_run_id="a" * 32,
            source_artifact_id="b" * 32,
            target_artifact_id="c" * 32,
            source_artifact_type="data",
            target_artifact_type="metric",
            source_role="data",
            target_role="score",
        )

        assert hash(edge1) == hash(edge2)
        assert edge1 == edge2

        # Can be used in sets
        edge_set = {edge1, edge2}
        assert len(edge_set) == 1

    def test_hashable_with_group_id(self):
        """Test that edges with same group_id are equal and hashable."""
        edge1 = ArtifactProvenanceEdge(
            execution_run_id="a" * 32,
            source_artifact_id="b" * 32,
            target_artifact_id="c" * 32,
            source_artifact_type="data",
            target_artifact_type="metric",
            source_role="data",
            target_role="score",
            group_id="g" * 32,
        )
        edge2 = ArtifactProvenanceEdge(
            execution_run_id="a" * 32,
            source_artifact_id="b" * 32,
            target_artifact_id="c" * 32,
            source_artifact_type="data",
            target_artifact_type="metric",
            source_role="data",
            target_role="score",
            group_id="g" * 32,
        )

        assert hash(edge1) == hash(edge2)
        assert edge1 == edge2

    def test_different_fields_produce_different_hash(self):
        """Test that different field values produce different hashes."""
        edge1 = ArtifactProvenanceEdge(
            execution_run_id="a" * 32,
            source_artifact_id="b" * 32,
            target_artifact_id="c" * 32,
            source_artifact_type="data",
            target_artifact_type="metric",
            source_role="data",
            target_role="score",
        )
        edge2 = ArtifactProvenanceEdge(
            execution_run_id="a" * 32,
            source_artifact_id="b" * 32,
            target_artifact_id="c" * 32,
            source_artifact_type="data",
            target_artifact_type="metric",
            source_role="different_role",  # Different role
            target_role="score",
        )

        assert hash(edge1) != hash(edge2)
        assert edge1 != edge2

    def test_different_group_id_produces_different_hash(self):
        """Test that different group_id values produce different hashes."""
        edge1 = ArtifactProvenanceEdge(
            execution_run_id="a" * 32,
            source_artifact_id="b" * 32,
            target_artifact_id="c" * 32,
            source_artifact_type="data",
            target_artifact_type="metric",
            source_role="data",
            target_role="score",
            group_id="g" * 32,
        )
        edge2 = ArtifactProvenanceEdge(
            execution_run_id="a" * 32,
            source_artifact_id="b" * 32,
            target_artifact_id="c" * 32,
            source_artifact_type="data",
            target_artifact_type="metric",
            source_role="data",
            target_role="score",
            group_id="h" * 32,
        )

        assert hash(edge1) != hash(edge2)
        assert edge1 != edge2

    def test_none_vs_set_group_id_produces_different_hash(self):
        """Test that None group_id differs from a set group_id."""
        edge_none = ArtifactProvenanceEdge(
            execution_run_id="a" * 32,
            source_artifact_id="b" * 32,
            target_artifact_id="c" * 32,
            source_artifact_type="data",
            target_artifact_type="metric",
            source_role="data",
            target_role="score",
            group_id=None,
        )
        edge_set = ArtifactProvenanceEdge(
            execution_run_id="a" * 32,
            source_artifact_id="b" * 32,
            target_artifact_id="c" * 32,
            source_artifact_type="data",
            target_artifact_type="metric",
            source_role="data",
            target_role="score",
            group_id="g" * 32,
        )

        assert hash(edge_none) != hash(edge_set)
        assert edge_none != edge_set


class TestEdgeTypeDetection:
    """Tests for edge type detection via schema fields."""

    def test_co_input_edges_share_group_id(self):
        """Test co-input edges linked by shared group_id."""
        group_id = "g" * 32
        target = "c" * 32
        edge_data = ArtifactProvenanceEdge(
            execution_run_id="a" * 32,
            source_artifact_id="b" * 32,
            target_artifact_id=target,
            source_artifact_type="data",
            target_artifact_type="data",
            source_role="data",
            target_role="processed",
            group_id=group_id,
        )
        edge_config = ArtifactProvenanceEdge(
            execution_run_id="a" * 32,
            source_artifact_id="d" * 32,
            target_artifact_id=target,
            source_artifact_type="file_ref",
            target_artifact_type="data",
            source_role="config",
            target_role="processed",
            group_id=group_id,
        )
        assert edge_data.group_id == edge_config.group_id
        assert edge_data.target_artifact_id == edge_config.target_artifact_id

    def test_input_to_output_edge(self):
        """Test regular input->output edge (single-input, no group_id)."""
        edge = ArtifactProvenanceEdge(
            execution_run_id="a" * 32,
            source_artifact_id="b" * 32,
            target_artifact_id="c" * 32,
            source_artifact_type="data",
            target_artifact_type="data",
            source_role="input",
            target_role="processed",
        )
        assert edge.group_id is None

    def test_output_to_output_edge(self):
        """Test output->output edge (e.g., metric computed from output data)."""
        edge = ArtifactProvenanceEdge(
            execution_run_id="a" * 32,
            source_artifact_id="b" * 32,  # Output data
            target_artifact_id="c" * 32,  # Metric
            source_artifact_type="data",
            target_artifact_type="metric",
            source_role="data",
            target_role="score",
        )
        # This is an output->output edge, detected by checking if source
        # is in execution outputs (via join), not from schema alone
        assert edge.source_artifact_type == "data"
        assert edge.target_artifact_type == "metric"


class TestArtifactTypeValues:
    """Tests for artifact type field values."""

    def test_artifact_type_allows_unknown(self):
        """Test that UNKNOWN is acceptable as artifact type fallback."""
        edge = ArtifactProvenanceEdge(
            execution_run_id="a" * 32,
            source_artifact_id="b" * 32,
            target_artifact_id="c" * 32,
            source_artifact_type="UNKNOWN",
            target_artifact_type="UNKNOWN",
            source_role="data",
            target_role="result",
        )
        assert edge.source_artifact_type == "UNKNOWN"
        assert edge.target_artifact_type == "UNKNOWN"

    def test_standard_artifact_types(self):
        """Test that standard artifact type values work."""
        for artifact_type in ["data", "metric", "file_ref"]:
            edge = ArtifactProvenanceEdge(
                execution_run_id="a" * 32,
                source_artifact_id="b" * 32,
                target_artifact_id="c" * 32,
                source_artifact_type=artifact_type,
                target_artifact_type=artifact_type,
                source_role="source",
                target_role="target",
            )
            assert edge.source_artifact_type == artifact_type
            assert edge.target_artifact_type == artifact_type


class TestImportFromSchemas:
    """Tests for importing ArtifactProvenanceEdge from schemas module."""

    def test_import_from_schemas(self):
        """Test that ArtifactProvenanceEdge can be imported from schemas."""
        from artisan.schemas import ArtifactProvenanceEdge as ImportedClass

        assert ImportedClass is ArtifactProvenanceEdge

    def test_in_schemas_all(self):
        """Test that ArtifactProvenanceEdge is in __all__."""
        from artisan.schemas import __all__

        assert "ArtifactProvenanceEdge" in __all__
