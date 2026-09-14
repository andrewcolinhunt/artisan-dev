"""Tests for ArtifactTypeDef registry."""

from __future__ import annotations

import contextlib
from typing import Any, ClassVar

import polars as pl
import pytest
from pydantic import Field

from artisan.errors import ArtifactIntegrityError
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.utils.hashing import canonical_json_bytes

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

    def test_artifact_model_requires_polars_schema(self) -> None:
        class MissingSchema(Artifact):
            artifact_type: str = Field(default="_test_missing_schema", frozen=True)

        with pytest.raises(TypeError, match="POLARS_SCHEMA"):

            class BadDef(ArtifactTypeDef):
                key = "_test_missing_schema"
                table_path = "artifacts/_missing_schema"
                model = MissingSchema

    def test_key_must_match_model_default(self) -> None:
        with pytest.raises(ValueError, match="must match"):

            class BadDef(ArtifactTypeDef):
                key = "_test_wrong_key"
                table_path = "artifacts/_wrong_key"
                model = _model_for("_test_model_key")

    def test_any_key_is_reserved(self) -> None:
        with pytest.raises(ValueError, match="reserved"):

            class BadDef(ArtifactTypeDef):
                key = ArtifactTypes.ANY
                table_path = "artifacts/_any"
                model = _model_for(ArtifactTypes.ANY)

    def test_framework_table_path_is_reserved(self) -> None:
        with pytest.raises(ValueError, match="framework-reserved"):

            class BadDef(ArtifactTypeDef):
                key = "_test_reserved_path"
                table_path = "artifacts/locations"
                model = _model_for("_test_reserved_path")

    def test_duplicate_table_path_raises(self) -> None:
        first_model = _model_for("_test_path_first")

        class FirstDef(ArtifactTypeDef):
            key = "_test_path_first"
            table_path = "artifacts/_same_path"
            model = first_model

        with pytest.raises(ValueError, match="Duplicate artifact table path"):

            class SecondDef(ArtifactTypeDef):
                key = "_test_path_second"
                table_path = "artifacts/_same_path"
                model = _model_for("_test_path_second")

    def test_registers_in_artifact_types(self) -> None:
        test_model = _model_for("_test_facade")

        class TestTypeDef(ArtifactTypeDef):
            key = "_test_facade"
            table_path = "artifacts/_facade"
            model = test_model

        assert ArtifactTypes.is_registered("_test_facade")

    def test_external_locator_must_be_declared_model_field(self) -> None:
        class BadExternal(Artifact):
            POLARS_SCHEMA: ClassVar[dict[str, type[pl.DataType]]] = {
                "artifact_id": pl.String
            }
            EXTERNALLY_BACKED: ClassVar[bool] = True
            LOCATOR_FIELDS: ClassVar[frozenset[str]] = frozenset({"missing"})
            artifact_type: str = Field(default="_test_missing_locator", frozen=True)

            def verify_external_content(self, *, fs: Any = None) -> None:
                pass

            def _materialize_content(self, directory: str, *, fs: Any = None) -> str:
                return directory

        with pytest.raises(TypeError, match="unknown locator field"):

            class BadDef(ArtifactTypeDef):
                key = "_test_missing_locator"
                table_path = "artifacts/_missing_locator"
                model = BadExternal

    def test_external_locator_must_not_be_in_content_table(self) -> None:
        class BadExternal(Artifact):
            POLARS_SCHEMA: ClassVar[dict[str, type[pl.DataType]]] = {
                "artifact_id": pl.String,
                "external_path": pl.String,
            }
            EXTERNALLY_BACKED: ClassVar[bool] = True
            LOCATOR_FIELDS: ClassVar[frozenset[str]] = frozenset({"external_path"})
            artifact_type: str = Field(default="_test_stored_locator", frozen=True)

            def verify_external_content(self, *, fs: Any = None) -> None:
                pass

            def _materialize_content(self, directory: str, *, fs: Any = None) -> str:
                return directory

        with pytest.raises(TypeError, match="must not be stored"):

            class BadDef(ArtifactTypeDef):
                key = "_test_stored_locator"
                table_path = "artifacts/_stored_locator"
                model = BadExternal

    def test_external_type_must_implement_materialization(self) -> None:
        class BadExternal(Artifact):
            POLARS_SCHEMA: ClassVar[dict[str, type[pl.DataType]]] = {
                "artifact_id": pl.String
            }
            EXTERNALLY_BACKED: ClassVar[bool] = True
            LOCATOR_FIELDS: ClassVar[frozenset[str]] = frozenset({"external_path"})
            artifact_type: str = Field(default="_test_no_materializer", frozen=True)

            def verify_external_content(self, *, fs: Any = None) -> None:
                pass

        with pytest.raises(TypeError, match="_materialize_content"):

            class BadDef(ArtifactTypeDef):
                key = "_test_no_materializer"
                table_path = "artifacts/_no_materializer"
                model = BadExternal

    def test_external_type_must_implement_verification(self) -> None:
        class BadExternal(Artifact):
            POLARS_SCHEMA: ClassVar[dict[str, type[pl.DataType]]] = {
                "artifact_id": pl.String
            }
            EXTERNALLY_BACKED: ClassVar[bool] = True
            LOCATOR_FIELDS: ClassVar[frozenset[str]] = frozenset({"external_path"})
            artifact_type: str = Field(default="_test_no_verifier", frozen=True)

            def _materialize_content(self, directory: str, *, fs: Any = None) -> str:
                return directory

        with pytest.raises(TypeError, match="verify_external_content"):

            class BadDef(ArtifactTypeDef):
                key = "_test_no_verifier"
                table_path = "artifacts/_no_verifier"
                model = BadExternal

    def test_identical_custom_payloads_are_separated_by_type(self) -> None:
        first_model = _model_for("_test_domain_first")
        second_model = _model_for("_test_domain_second")

        class FirstDef(ArtifactTypeDef):
            key = "_test_domain_first"
            table_path = "artifacts/_domain_first"
            model = first_model

        class SecondDef(ArtifactTypeDef):
            key = "_test_domain_second"
            table_path = "artifacts/_domain_second"
            model = second_model

        first = first_model(origin_step_number=1, value=7).finalize()
        second = second_model(origin_step_number=1, value=7).finalize()

        assert first.artifact_id != second.artifact_id

    def test_custom_nested_payload_is_protected_after_finalization(self) -> None:
        class NestedArtifact(Artifact):
            POLARS_SCHEMA: ClassVar[dict[str, type[pl.DataType]]] = {
                "artifact_id": pl.String,
                "payload": pl.String,
            }
            artifact_type: str = Field(default="_test_nested", frozen=True)
            payload: dict[str, Any]

            def _identity_payload(self) -> bytes:
                return canonical_json_bytes(self.payload)

        class NestedDef(ArtifactTypeDef):
            key = "_test_nested"
            table_path = "artifacts/_nested"
            model = NestedArtifact

        artifact = NestedArtifact(
            origin_step_number=1,
            payload={"outer": {"value": 1}},
        ).finalize()
        artifact.payload["outer"]["value"] = 2

        with pytest.raises(ArtifactIntegrityError, match="was mutated"):
            artifact.to_row()


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
