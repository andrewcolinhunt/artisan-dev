"""Tests for ArtifactTypeDef registry."""

from __future__ import annotations

from typing import Any, ClassVar

import polars as pl
import pytest
from pydantic import BaseModel

from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes

# --- Mock model for testing ---


class _MockModel(BaseModel):
    """Minimal model satisfying the ArtifactTypeDef interface."""

    POLARS_SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "artifact_id": pl.String,
        "value": pl.Int32,
    }

    artifact_id: str | None = None
    value: int = 0

    def to_row(self) -> dict[str, Any]:
        return {"artifact_id": self.artifact_id, "value": self.value}

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> _MockModel:
        return cls(**row)


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
        try:
            delattr(ArtifactTypes, attr)
        except AttributeError:
            pass


# --- Tests ---


class TestRegistration:
    """Auto-registration via __init_subclass__."""

    def test_register_concrete_type(self) -> None:
        class TestTypeDef(ArtifactTypeDef):
            key = "_test_register"
            table_path = "artifacts/_test"
            model = _MockModel

        assert ArtifactTypeDef.get("_test_register") is TestTypeDef

    def test_abstract_subclass_not_registered(self) -> None:
        class AbstractDef(ArtifactTypeDef):
            pass  # No key set

        assert "_abstract" not in ArtifactTypeDef._registry

    def test_duplicate_key_raises(self) -> None:
        class FirstDef(ArtifactTypeDef):
            key = "_test_dup"
            table_path = "artifacts/_dup1"
            model = _MockModel

        with pytest.raises(ValueError, match="Duplicate artifact type key"):

            class SecondDef(ArtifactTypeDef):
                key = "_test_dup"
                table_path = "artifacts/_dup2"
                model = _MockModel

    def test_missing_table_path_raises(self) -> None:
        with pytest.raises(TypeError, match="must set 'table_path'"):

            class BadDef(ArtifactTypeDef):
                key = "_test_no_table"
                model = _MockModel

    def test_missing_model_raises(self) -> None:
        with pytest.raises(TypeError, match="must set 'model'"):

            class BadDef(ArtifactTypeDef):
                key = "_test_no_model"
                table_path = "artifacts/_test"

    def test_bad_model_raises(self) -> None:
        with pytest.raises(TypeError, match="must have 'POLARS_SCHEMA'"):

            class BadDef(ArtifactTypeDef):
                key = "_test_bad_model"
                table_path = "artifacts/_test"
                model = _BadModel

    def test_registers_in_artifact_types(self) -> None:
        class TestTypeDef(ArtifactTypeDef):
            key = "_test_facade"
            table_path = "artifacts/_facade"
            model = _MockModel

        assert ArtifactTypes.is_registered("_test_facade")
        assert ArtifactTypes.get("_test_facade") == "_test_facade"


class TestLookup:
    """Public lookup API."""

    def test_get_unknown_raises(self) -> None:
        with pytest.raises(KeyError, match="Unknown artifact type"):
            ArtifactTypeDef.get("nonexistent")

    def test_get_all_returns_dict(self) -> None:
        result = ArtifactTypeDef.get_all()
        assert isinstance(result, dict)

    def test_get_model(self) -> None:
        class TestTypeDef(ArtifactTypeDef):
            key = "_test_model_lookup"
            table_path = "artifacts/_model"
            model = _MockModel

        assert ArtifactTypeDef.get_model("_test_model_lookup") is _MockModel

    def test_get_table_path(self) -> None:
        class TestTypeDef(ArtifactTypeDef):
            key = "_test_path_lookup"
            table_path = "artifacts/_path"
            model = _MockModel

        assert ArtifactTypeDef.get_table_path("_test_path_lookup") == "artifacts/_path"

    def test_get_schema(self) -> None:
        class TestTypeDef(ArtifactTypeDef):
            key = "_test_schema_lookup"
            table_path = "artifacts/_schema"
            model = _MockModel

        schema = ArtifactTypeDef.get_schema("_test_schema_lookup")
        assert schema == _MockModel.POLARS_SCHEMA


class TestDerivedProperties:
    """parquet_filename and polars_schema derived from type def."""

    def test_parquet_filename(self) -> None:
        class TestTypeDef(ArtifactTypeDef):
            key = "_test_parquet"
            table_path = "artifacts/test_things"
            model = _MockModel

        assert TestTypeDef.parquet_filename() == "test_things.parquet"

    def test_polars_schema(self) -> None:
        class TestTypeDef(ArtifactTypeDef):
            key = "_test_polars"
            table_path = "artifacts/_polars"
            model = _MockModel

        assert TestTypeDef.polars_schema() == _MockModel.POLARS_SCHEMA
