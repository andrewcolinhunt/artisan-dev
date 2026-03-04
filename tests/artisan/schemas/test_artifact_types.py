"""Tests for ArtifactTypes facade."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.types import ArtifactTypes


class TestBuiltinTypes:
    """Built-in types are plain strings with correct values."""

    def test_data_is_string(self) -> None:
        assert ArtifactTypes.DATA == "data"

    def test_metric_is_string(self) -> None:
        assert ArtifactTypes.METRIC == "metric"

    def test_file_ref_is_string(self) -> None:
        assert ArtifactTypes.FILE_REF == "file_ref"

    def test_config_is_string(self) -> None:
        assert ArtifactTypes.CONFIG == "config"

    def test_string_equality_both_directions(self) -> None:
        assert ArtifactTypes.DATA == "data"
        assert ArtifactTypes.DATA == "data"


class TestGet:
    """String lookup via get()."""

    def test_get_existing(self) -> None:
        assert ArtifactTypes.get("data") == "data"

    def test_get_unknown_raises(self) -> None:
        try:
            ArtifactTypes.get("nonexistent")
            assert False, "Should have raised KeyError"
        except KeyError:
            pass


class TestMembership:
    """Membership checks via ``in``."""

    def test_contains_builtin(self) -> None:
        assert "data" in ArtifactTypes
        assert "metric" in ArtifactTypes
        assert "file_ref" in ArtifactTypes
        assert "config" in ArtifactTypes

    def test_not_contains_unknown(self) -> None:
        assert "nonexistent" not in ArtifactTypes

    def test_non_string_not_contained(self) -> None:
        assert 42 not in ArtifactTypes


class TestIteration:
    """Iteration via ``for t in ArtifactTypes``."""

    def test_iter_contains_all_builtins(self) -> None:
        keys = list(ArtifactTypes)
        assert "data" in keys
        assert "metric" in keys
        assert "file_ref" in keys
        assert "config" in keys


class TestAll:
    """all() returns all registered keys."""

    def test_all_contains_builtins(self) -> None:
        result = ArtifactTypes.all()
        assert set(result) >= {"data", "metric", "file_ref", "config"}


class TestIsRegistered:
    """is_registered() check."""

    def test_builtin_is_registered(self) -> None:
        assert ArtifactTypes.is_registered("data") is True

    def test_unknown_is_not_registered(self) -> None:
        assert ArtifactTypes.is_registered("nonexistent") is False


class TestAnySentinel:
    """Tests for ArtifactTypes.ANY wildcard sentinel."""

    def test_any_equals_string(self) -> None:
        assert ArtifactTypes.ANY == "any"

    def test_any_not_in_registry(self) -> None:
        assert "any" not in ArtifactTypes

    def test_any_not_in_all(self) -> None:
        assert "any" not in ArtifactTypes.all()

    def test_any_not_in_iteration(self) -> None:
        assert "any" not in list(ArtifactTypes)

    def test_any_not_registered(self) -> None:
        assert ArtifactTypes.is_registered("any") is False


class TestIsConcrete:
    """Tests for is_concrete() helper."""

    def test_builtin_is_concrete(self) -> None:
        assert ArtifactTypes.is_concrete("data") is True
        assert ArtifactTypes.is_concrete("metric") is True

    def test_any_is_not_concrete(self) -> None:
        assert ArtifactTypes.is_concrete("any") is False

    def test_unknown_is_not_concrete(self) -> None:
        assert ArtifactTypes.is_concrete("nonexistent") is False


class TestMatches:
    """Tests for matches() spec-type matching."""

    def test_any_matches_all(self) -> None:
        assert ArtifactTypes.matches(ArtifactTypes.ANY, "data") is True
        assert ArtifactTypes.matches(ArtifactTypes.ANY, "metric") is True
        assert ArtifactTypes.matches(ArtifactTypes.ANY, "custom") is True

    def test_exact_match(self) -> None:
        assert ArtifactTypes.matches("data", "data") is True

    def test_mismatch(self) -> None:
        assert ArtifactTypes.matches("data", "metric") is False


class TestArtifactRejectsAny:
    """Concrete Artifact instances must not use ArtifactTypes.ANY."""

    def test_artifact_with_any_raises(self) -> None:
        with pytest.raises(ValidationError, match="spec-only sentinel"):
            Artifact(artifact_type="any")

    def test_artifact_with_concrete_type_ok(self) -> None:
        artifact = Artifact(artifact_type="data")
        assert artifact.artifact_type == "data"
