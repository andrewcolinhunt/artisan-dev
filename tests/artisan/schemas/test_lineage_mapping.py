"""Tests for lineage_mapping.py"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.schemas.provenance.lineage_mapping import LineageMapping


class TestLineageMapping:
    """Tests for LineageMapping model."""

    def test_create_valid_mapping(self):
        """Create a valid LineageMapping."""
        mapping = LineageMapping(
            draft_original_name="sample_001_processed.dat",
            source_artifact_id="abc123def456ghijklmnopqrstuvwx01",  # 32 chars
            source_role="data",
        )
        assert mapping.draft_original_name == "sample_001_processed.dat"
        assert mapping.source_artifact_id == "abc123def456ghijklmnopqrstuvwx01"
        assert mapping.source_role == "data"

    def test_frozen_model(self):
        """LineageMapping is immutable."""
        mapping = LineageMapping(
            draft_original_name="test.dat",
            source_artifact_id="abc123def456ghijklmnopqrstuvwx01",
            source_role="input",
        )
        with pytest.raises(Exception):
            mapping.draft_original_name = "new.dat"

    def test_draft_original_name_required(self):
        """draft_original_name is required."""
        with pytest.raises(ValidationError) as exc_info:
            LineageMapping(
                source_artifact_id="abc123def456ghijklmnopqrstuvwx01",
                source_role="input",
            )
        assert "draft_original_name" in str(exc_info.value)

    def test_neither_source_field_set_raises(self):
        """One of source_artifact_id or source_original_name must be set."""
        with pytest.raises(ValidationError) as exc_info:
            LineageMapping(
                draft_original_name="test.dat",
                source_role="input",
            )
        assert (
            "Provide exactly one of source_artifact_id or source_original_name"
            in str(exc_info.value)
        )

    def test_both_source_fields_set_raises(self):
        """Setting both source_artifact_id and source_original_name is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            LineageMapping(
                draft_original_name="test.dat",
                source_artifact_id="abc123def456ghijklmnopqrstuvwx01",
                source_original_name="sister_output",
                source_role="input",
            )
        assert (
            "Provide exactly one of source_artifact_id or source_original_name"
            in str(exc_info.value)
        )

    def test_source_role_required(self):
        """source_role is required."""
        with pytest.raises(ValidationError) as exc_info:
            LineageMapping(
                draft_original_name="test.dat",
                source_artifact_id="abc123def456ghijklmnopqrstuvwx01",
            )
        assert "source_role" in str(exc_info.value)

    def test_draft_original_name_min_length(self):
        """draft_original_name must be at least 1 character."""
        with pytest.raises(ValidationError) as exc_info:
            LineageMapping(
                draft_original_name="",
                source_artifact_id="abc123def456ghijklmnopqrstuvwx01",
                source_role="input",
            )
        assert "draft_original_name" in str(exc_info.value)

    def test_source_role_min_length(self):
        """source_role must be at least 1 character."""
        with pytest.raises(ValidationError) as exc_info:
            LineageMapping(
                draft_original_name="test.dat",
                source_artifact_id="abc123def456ghijklmnopqrstuvwx01",
                source_role="",
            )
        assert "source_role" in str(exc_info.value)

    def test_source_artifact_id_exact_32_chars_when_set(self):
        """source_artifact_id must be exactly 32 characters when provided."""
        # Too short
        with pytest.raises(ValidationError) as exc_info:
            LineageMapping(
                draft_original_name="test.dat",
                source_artifact_id="abc123",  # Only 6 chars
                source_role="input",
            )
        assert "source_artifact_id" in str(exc_info.value)

        # Too long
        with pytest.raises(ValidationError) as exc_info:
            LineageMapping(
                draft_original_name="test.dat",
                source_artifact_id="abc123def456ghijklmnopqrstuvwx01yz123",  # 35 chars
                source_role="input",
            )
        assert "source_artifact_id" in str(exc_info.value)

    def test_accepts_source_original_name_alone(self):
        """source_original_name without source_artifact_id is valid."""
        mapping = LineageMapping(
            draft_original_name="1abc_out_energy",
            source_original_name="1abc_out",
            source_role="structures",
        )
        assert mapping.source_artifact_id is None
        assert mapping.source_original_name == "1abc_out"
        assert mapping.source_role == "structures"

    def test_source_original_name_min_length(self):
        """source_original_name must be at least 1 character when provided."""
        with pytest.raises(ValidationError) as exc_info:
            LineageMapping(
                draft_original_name="1abc_out_energy",
                source_original_name="",
                source_role="structures",
            )
        assert "source_original_name" in str(exc_info.value)

    def test_equality_with_source_original_name(self):
        """Two mappings with same source_original_name values are equal."""
        mapping1 = LineageMapping(
            draft_original_name="1abc_out_energy",
            source_original_name="1abc_out",
            source_role="structures",
        )
        mapping2 = LineageMapping(
            draft_original_name="1abc_out_energy",
            source_original_name="1abc_out",
            source_role="structures",
        )
        assert mapping1 == mapping2
        assert hash(mapping1) == hash(mapping2)

    def test_inequality_id_vs_name_source(self):
        """Mappings keyed by id vs. name are not equal even with matching role."""
        mapping_by_id = LineageMapping(
            draft_original_name="1abc_out_energy",
            source_artifact_id="abc123def456ghijklmnopqrstuvwx01",
            source_role="structures",
        )
        mapping_by_name = LineageMapping(
            draft_original_name="1abc_out_energy",
            source_original_name="1abc_out",
            source_role="structures",
        )
        assert mapping_by_id != mapping_by_name

    def test_hashable(self):
        """LineageMapping is hashable (frozen model)."""
        mapping = LineageMapping(
            draft_original_name="test.dat",
            source_artifact_id="abc123def456ghijklmnopqrstuvwx01",
            source_role="input",
        )
        # Should not raise
        assert isinstance(hash(mapping), int)

    def test_equality(self):
        """Two LineageMappings with same values are equal."""
        mapping1 = LineageMapping(
            draft_original_name="test.dat",
            source_artifact_id="abc123def456ghijklmnopqrstuvwx01",
            source_role="input",
        )
        mapping2 = LineageMapping(
            draft_original_name="test.dat",
            source_artifact_id="abc123def456ghijklmnopqrstuvwx01",
            source_role="input",
        )
        assert mapping1 == mapping2

    def test_inequality(self):
        """Two LineageMappings with different values are not equal."""
        mapping1 = LineageMapping(
            draft_original_name="test1.dat",
            source_artifact_id="abc123def456ghijklmnopqrstuvwx01",
            source_role="input",
        )
        mapping2 = LineageMapping(
            draft_original_name="test2.dat",
            source_artifact_id="abc123def456ghijklmnopqrstuvwx01",
            source_role="input",
        )
        assert mapping1 != mapping2

    def test_inequality_different_source_role(self):
        """Two LineageMappings with different source_role are not equal."""
        mapping1 = LineageMapping(
            draft_original_name="test.dat",
            source_artifact_id="abc123def456ghijklmnopqrstuvwx01",
            source_role="data",
        )
        mapping2 = LineageMapping(
            draft_original_name="test.dat",
            source_artifact_id="abc123def456ghijklmnopqrstuvwx01",
            source_role="reference",
        )
        assert mapping1 != mapping2
