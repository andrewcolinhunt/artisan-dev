"""Tests for SourceTargetPair dataclass."""

from __future__ import annotations

import pytest

from artisan.schemas.provenance.source_target_pair import SourceTargetPair


class TestSourceTargetPair:
    """Tests for SourceTargetPair dataclass."""

    def test_valid_pair(self):
        """Test creating a valid source-target pair (5 fields)."""
        pair = SourceTargetPair(
            source="abc123" + "0" * 26,
            target="def456" + "0" * 26,
            source_role="data",
            target_role="processed_data",
        )
        assert pair.source == "abc123" + "0" * 26
        assert pair.target == "def456" + "0" * 26
        assert pair.source_role == "data"
        assert pair.target_role == "processed_data"
        assert pair.group_id is None

    def test_valid_pair_with_group_id(self):
        """Test creating a source-target pair with group_id set."""
        pair = SourceTargetPair(
            source="abc123" + "0" * 26,
            target="def456" + "0" * 26,
            source_role="data",
            target_role="processed_data",
            group_id="g" * 32,
        )
        assert pair.group_id == "g" * 32

    def test_frozen_dataclass(self):
        """Test that SourceTargetPair is immutable (frozen=True)."""
        pair = SourceTargetPair(
            source="abc123",
            target="def456",
            source_role="input",
            target_role="output",
        )
        with pytest.raises(Exception):  # FrozenInstanceError
            pair.source = "new_value"

    def test_hashable(self):
        """Test that SourceTargetPair is hashable (5 fields)."""
        pair1 = SourceTargetPair(
            source="abc",
            target="def",
            source_role="input",
            target_role="output",
        )
        pair2 = SourceTargetPair(
            source="abc",
            target="def",
            source_role="input",
            target_role="output",
        )

        assert hash(pair1) == hash(pair2)
        assert pair1 == pair2

        # Can be used in sets
        pair_set = {pair1, pair2}
        assert len(pair_set) == 1

    def test_different_group_id_produces_different_pair(self):
        """Test that different group_id values produce different pairs."""
        pair1 = SourceTargetPair(
            source="abc",
            target="def",
            source_role="input",
            target_role="output",
            group_id="g" * 32,
        )
        pair2 = SourceTargetPair(
            source="abc",
            target="def",
            source_role="input",
            target_role="output",
            group_id="h" * 32,
        )

        assert pair1 != pair2
        assert hash(pair1) != hash(pair2)

    def test_different_roles_produce_different_pairs(self):
        """Test that different roles produce different pairs."""
        pair1 = SourceTargetPair(
            source="abc",
            target="def",
            source_role="input",
            target_role="output",
        )
        pair2 = SourceTargetPair(
            source="abc",
            target="def",
            source_role="data",
            target_role="output",
        )

        assert pair1 != pair2
        assert hash(pair1) != hash(pair2)

    def test_different_artifacts_produce_different_pairs(self):
        """Test that different artifact IDs produce different pairs."""
        pair1 = SourceTargetPair(
            source="abc",
            target="def",
            source_role="input",
            target_role="output",
        )
        pair2 = SourceTargetPair(
            source="xyz",
            target="def",
            source_role="input",
            target_role="output",
        )

        assert pair1 != pair2
        assert hash(pair1) != hash(pair2)

    def test_equality(self):
        """Test equality comparison."""
        pair1 = SourceTargetPair(
            source="abc",
            target="def",
            source_role="input",
            target_role="output",
        )
        pair2 = SourceTargetPair(
            source="abc",
            target="def",
            source_role="input",
            target_role="output",
        )
        pair3 = SourceTargetPair(
            source="abc",
            target="ghi",  # Different target
            source_role="input",
            target_role="output",
        )

        assert pair1 == pair2
        assert pair1 != pair3

    def test_can_be_used_in_dict(self):
        """Test that SourceTargetPair can be used as dict key."""
        pair = SourceTargetPair(
            source="abc",
            target="def",
            source_role="input",
            target_role="output",
        )
        d = {pair: "value"}
        assert d[pair] == "value"

    def test_repr(self):
        """Test that repr works and contains field values."""
        pair = SourceTargetPair(
            source="abc123",
            target="def456",
            source_role="data",
            target_role="processed",
        )
        repr_str = repr(pair)
        assert "abc123" in repr_str
        assert "def456" in repr_str
        assert "data" in repr_str
        assert "processed" in repr_str


class TestImportFromSchemas:
    """Tests for importing SourceTargetPair from schemas module."""

    def test_import_from_schemas(self):
        """Test that SourceTargetPair can be imported from schemas."""
        from artisan.schemas import SourceTargetPair as ImportedClass

        assert ImportedClass is SourceTargetPair

    def test_in_schemas_all(self):
        """Test that SourceTargetPair is in __all__."""
        from artisan.schemas import __all__

        assert "SourceTargetPair" in __all__


class TestTwoDataclassPattern:
    """Tests verifying the two-dataclass pattern for provenance."""

    def test_source_target_pair_has_5_fields(self):
        """Test that SourceTargetPair has exactly 5 fields."""
        from dataclasses import fields

        field_names = [f.name for f in fields(SourceTargetPair)]
        assert len(field_names) == 5
        assert set(field_names) == {
            "source",
            "target",
            "source_role",
            "target_role",
            "group_id",
        }

    def test_artifact_edge_has_8_fields(self):
        """Test that ArtifactProvenanceEdge has exactly 8 fields."""
        from artisan.schemas import ArtifactProvenanceEdge

        field_names = list(ArtifactProvenanceEdge.model_fields.keys())
        assert len(field_names) == 8
        assert set(field_names) == {
            "execution_run_id",
            "source_artifact_id",
            "target_artifact_id",
            "source_artifact_type",
            "target_artifact_type",
            "source_role",
            "target_role",
            "group_id",
        }

    def test_source_target_pair_is_lightweight(self):
        """Test that SourceTargetPair doesn't have execution context."""
        from dataclasses import fields

        field_names = [f.name for f in fields(SourceTargetPair)]

        # SourceTargetPair should NOT have these (they're added in ArtifactProvenanceEdge)
        assert "execution_run_id" not in field_names
        assert "source_artifact_type" not in field_names
        assert "target_artifact_type" not in field_names
