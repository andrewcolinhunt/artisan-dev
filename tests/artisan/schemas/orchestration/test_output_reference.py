"""Tests for immutable output references."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.orchestration.output_reference import OutputReference


class TestOutputReference:
    """Tests for OutputReference schema model."""

    def test_create_basic(self):
        """Test basic OutputReference creation."""
        ref = OutputReference(source_step=0, role="data")
        assert ref.source_step == 0
        assert ref.role == "data"
        assert ref.artifact_type == ArtifactTypes.ANY

    def test_create_with_artifact_type(self):
        """Test OutputReference with artifact_type."""
        ref = OutputReference(source_step=1, role="scores", artifact_type="metric")
        assert ref.source_step == 1
        assert ref.role == "scores"
        assert ref.artifact_type == "metric"

    def test_immutable(self):
        """Test that OutputReference is immutable (frozen schema)."""
        ref = OutputReference(source_step=0, role="data")
        with pytest.raises(ValidationError):
            ref.source_step = 1

    def test_hashable(self):
        """Test that OutputReference is hashable."""
        ref1 = OutputReference(source_step=0, role="data")
        ref2 = OutputReference(source_step=0, role="data")
        ref3 = OutputReference(source_step=1, role="data")

        refs = {ref1, ref2, ref3}
        assert len(refs) == 2  # ref1 and ref2 are equal

        d = {ref1: "first", ref3: "second"}
        assert d[ref2] == "first"  # ref2 == ref1

    def test_equality(self):
        """Test OutputReference equality."""
        ref1 = OutputReference(source_step=0, role="data")
        ref2 = OutputReference(source_step=0, role="data")
        ref3 = OutputReference(source_step=0, role="metrics")

        assert ref1 == ref2
        assert ref1 != ref3
