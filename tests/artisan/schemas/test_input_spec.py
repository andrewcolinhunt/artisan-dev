"""Tests for input_spec.py"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.specs.input_spec import InputSpec


class TestInputSpec:
    """Tests for InputSpec dataclass."""

    def test_create_with_artifact_type(self):
        """Create InputSpec with explicit artifact type."""
        spec = InputSpec(artifact_type=ArtifactTypes.DATA, required=True)
        assert spec.artifact_type == ArtifactTypes.DATA
        assert spec.required is True

    def test_default_values(self):
        """Test default values."""
        spec = InputSpec()
        assert spec.artifact_type == ArtifactTypes.ANY
        assert spec.required is True
        assert spec.description == ""

    def test_accepts_type_exact_match(self):
        """accepts_type returns True for exact match."""
        spec = InputSpec(artifact_type=ArtifactTypes.DATA)
        assert spec.accepts_type(ArtifactTypes.DATA) is True
        assert spec.accepts_type(ArtifactTypes.METRIC) is False

    def test_accepts_type_any(self):
        """accepts_type returns True for any type when artifact_type is ANY."""
        spec = InputSpec(artifact_type=ArtifactTypes.ANY)
        assert spec.accepts_type(ArtifactTypes.DATA) is True
        assert spec.accepts_type(ArtifactTypes.METRIC) is True
        assert spec.accepts_type(ArtifactTypes.FILE_REF) is True

    def test_frozen(self):
        """InputSpec is immutable."""
        spec = InputSpec(artifact_type=ArtifactTypes.DATA)
        with pytest.raises(Exception):  # Pydantic raises ValidationError for frozen
            spec.artifact_type = ArtifactTypes.METRIC

    def test_with_associated_default_empty(self):
        """with_associated defaults to empty tuple."""
        spec = InputSpec()
        assert spec.with_associated == ()

    def test_with_associated_set(self):
        """with_associated can be set to a tuple of type strings."""
        spec = InputSpec(with_associated=("data_annotation",))
        assert spec.with_associated == ("data_annotation",)

    def test_hash_includes_with_associated(self):
        """Hash function includes with_associated field."""
        spec1 = InputSpec(artifact_type=ArtifactTypes.DATA)
        spec2 = InputSpec(
            artifact_type=ArtifactTypes.DATA,
            with_associated=("data_annotation",),
        )
        assert hash(spec1) != hash(spec2)


class TestMaterializeAs:
    """Tests for materialize_as field."""

    def test_materialize_as_default_none(self):
        """materialize_as defaults to None."""
        spec = InputSpec()
        assert spec.materialize_as is None

    def test_materialize_as_with_materialize_true(self):
        """materialize_as can be set when materialize=True."""
        spec = InputSpec(materialize=True, materialize_as=".dat")
        assert spec.materialize_as == ".dat"

    def test_materialize_as_requires_materialize_true(self):
        """materialize_as raises when materialize=False."""
        with pytest.raises(
            ValidationError, match="materialize_as requires materialize=True"
        ):
            InputSpec(materialize=False, materialize_as=".dat")

    def test_hash_includes_materialize_as(self):
        """Hash function includes materialize_as field."""
        spec1 = InputSpec(artifact_type=ArtifactTypes.DATA)
        spec2 = InputSpec(
            artifact_type=ArtifactTypes.DATA,
            materialize_as=".dat",
        )
        assert hash(spec1) != hash(spec2)
