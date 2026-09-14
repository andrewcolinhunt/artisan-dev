"""Tests for ArtifactTypeDef registry."""

from __future__ import annotations

import contextlib
from typing import ClassVar

import polars as pl
import pytest
from pydantic import Field

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes

# --- Mock model for testing ---


def _model_for(key: str) -> type[Artifact]:
    """Build a minimal inline Artifact model for a registry key."""

    class _TestArtifact(Artifact):
        POLARS_SCHEMA: ClassVar[dict[str, type[pl.DataType]]] = {
            "artifact_id": pl.String,
            "value": pl.Int32,
        }
        artifact_type: str = Field(default=key, frozen=True)
        value: int = 0

        def _identity_payload(self) -> bytes:
            return str(self.value).encode()

    return _TestArtifact


class _BadModel:
    """Model missing required interface."""


# --- Fixtures ---


@pytest.fixture(autouse=True)
def _clean_registry():
    """Remove test-registered types after each test."""
    original_registry = dict(ArtifactTypeDef._registry)
    original_types_registry = dict(ArtifactTypes._registry)
    original_attrs = set(dir(ArtifactTypes))
    yield
    ArtifactTypeDef._registry.clear()
    ArtifactTypeDef._registry.update(original_registry)
    ArtifactTypes._registry.clear()
    ArtifactTypes._registry.update(original_types_registry)
    # Clean up dynamic attributes
    for attr in set(dir(ArtifactTypes)) - original_attrs:
        with contextlib.suppress(AttributeError):
            delattr(ArtifactTypes, attr)


# --- Tests ---


class TestRegistration:
    """Auto-registration via __init_subclass__."""

    def test_register_concrete_type(self) -> None:
        test_model = _model_for("_test_register")

        class TestTypeDef(ArtifactTypeDef):
            key = "_test_register"
            table_path = "artifacts/_test"
            model = test_model

        assert ArtifactTypeDef.get("_test_register") is TestTypeDef

    def test_abstract_subclass_not_registered(self) -> None:
        class AbstractDef(ArtifactTypeDef):
            pass  # No key set

        assert "_abstract" not in ArtifactTypeDef._registry

    def test_duplicate_key_raises(self) -> None:
        test_model = _model_for("_test_dup")

        class FirstDef(ArtifactTypeDef):
            key = "_test_dup"
            table_path = "artifacts/_dup1"
            model = test_model

        with pytest.raises(ValueError, match="Duplicate artifact type key"):

            class SecondDef(ArtifactTypeDef):
                key = "_test_dup"
                table_path = "artifacts/_dup2"
                model = test_model

    def test_missing_table_path_raises(self) -> None:
        with pytest.raises(TypeError, match="must set 'table_path'"):

            class BadDef(ArtifactTypeDef):
                key = "_test_no_table"
                model = _model_for("_test_no_table")

    def test_missing_model_raises(self) -> None:
        with pytest.raises(TypeError, match="must set 'model'"):

            class BadDef(ArtifactTypeDef):
                key = "_test_no_model"
                table_path = "artifacts/_test"

    def test_bad_model_raises(self) -> None:
        with pytest.raises(TypeError, match="must subclass Artifact"):

            class BadDef(ArtifactTypeDef):
                key = "_test_bad_model"
                table_path = "artifacts/_test"
                model = _BadModel

    def test_registers_in_artifact_types(self) -> None:
        test_model = _model_for("_test_facade")

        class TestTypeDef(ArtifactTypeDef):
            key = "_test_facade"
            table_path = "artifacts/_facade"
            model = test_model

        assert ArtifactTypes.is_registered("_test_facade")


class TestLookup:
    """Public lookup API."""

    def test_get_unknown_raises(self) -> None:
        with pytest.raises(KeyError, match="Unknown artifact type"):
            ArtifactTypeDef.get("nonexistent")

    def test_get_all_returns_dict(self) -> None:
        result = ArtifactTypeDef.get_all()
        assert isinstance(result, dict)

    def test_get_model(self) -> None:
        test_model = _model_for("_test_model_lookup")

        class TestTypeDef(ArtifactTypeDef):
            key = "_test_model_lookup"
            table_path = "artifacts/_model"
            model = test_model

        assert ArtifactTypeDef.get_model("_test_model_lookup") is test_model

    def test_get_table_path(self) -> None:
        test_model = _model_for("_test_path_lookup")

        class TestTypeDef(ArtifactTypeDef):
            key = "_test_path_lookup"
            table_path = "artifacts/_path"
            model = test_model

        assert ArtifactTypeDef.get_table_path("_test_path_lookup") == "artifacts/_path"

    def test_get_schema(self) -> None:
        test_model = _model_for("_test_schema_lookup")

        class TestTypeDef(ArtifactTypeDef):
            key = "_test_schema_lookup"
            table_path = "artifacts/_schema"
            model = test_model

        schema = ArtifactTypeDef.get_schema("_test_schema_lookup")
        assert schema == test_model.POLARS_SCHEMA


class TestDerivedProperties:
    """parquet_filename and polars_schema derived from type def."""

    def test_parquet_filename(self) -> None:
        test_model = _model_for("_test_parquet")

        class TestTypeDef(ArtifactTypeDef):
            key = "_test_parquet"
            table_path = "artifacts/test_things"
            model = test_model

        assert TestTypeDef.parquet_filename() == "test_things.parquet"

    def test_polars_schema(self) -> None:
        test_model = _model_for("_test_polars")

        class TestTypeDef(ArtifactTypeDef):
            key = "_test_polars"
            table_path = "artifacts/_polars"
            model = test_model

        assert TestTypeDef.polars_schema() == test_model.POLARS_SCHEMA
