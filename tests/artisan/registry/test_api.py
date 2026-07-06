"""Tests for ``artisan.registry.api`` — list_operations, describe, examples."""

from __future__ import annotations

import pytest

from artisan.errors import ArtisanError
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.registry import describe, examples, list_operations


@pytest.fixture(autouse=True)
def _populated_registry() -> None:
    """Force built-in op imports so the registry has content."""
    import artisan.operations.curator
    import artisan.operations.examples  # noqa: F401


class TestListOperations:
    def test_returns_sorted_summaries(self) -> None:
        summaries = list_operations()
        names = [s.name for s in summaries]
        assert names == sorted(names)
        registered = set(OperationDefinition._registry.keys())
        assert {s.name for s in summaries} == registered

    def test_filter_by_kind_curator_excludes_creators(self) -> None:
        curators = list_operations(kind="curator")
        assert all(s.kind == "curator" for s in curators)
        names = {s.name for s in curators}
        assert "filter" in names
        assert "merge" in names
        assert "data_transformer" not in names

    def test_filter_by_kind_creator_excludes_curators(self) -> None:
        creators = list_operations(kind="creator")
        assert all(s.kind == "creator" for s in creators)
        names = {s.name for s in creators}
        assert "data_transformer" in names
        assert "filter" not in names

    def test_query_filters_by_name_substring(self) -> None:
        results = list_operations(query="transformer")
        names = [s.name for s in results]
        assert "data_transformer" in names
        assert all("transformer" in s.name.lower() for s in results)

    def test_filter_by_tag(self) -> None:
        results = list_operations(tag="fixture")
        names = {s.name for s in results}
        assert "_test_docstring_only_op" in names  # declares tags=["test", "fixture"]
        assert all("fixture" in s.tags for s in results)

    def test_unknown_tag_returns_empty(self) -> None:
        assert list_operations(tag="no_such_tag_anywhere") == []

    def test_filters_and_together(self) -> None:
        # The fixture op is a creator tagged "fixture"; the kind AND tag
        # both match, so it survives; a mismatched kind excludes it.
        assert {s.name for s in list_operations(kind="creator", tag="fixture")} == {
            "_test_docstring_only_op"
        }
        assert list_operations(kind="curator", tag="fixture") == []


class TestDescribe:
    def test_returns_full_metadata_for_known_op(self) -> None:
        meta = describe("data_transformer")
        assert meta.name == "data_transformer"
        assert meta.kind == "creator"
        assert meta.input_roles == ["dataset"]
        assert meta.output_roles == ["dataset"]
        assert meta.source_module.startswith("artisan.operations.examples")
        assert "scale_factor" in meta.params_schema["properties"]

    def test_unknown_operation_raises_artisan_error(self) -> None:
        with pytest.raises(ArtisanError) as exc_info:
            describe("does_not_exist")
        assert exc_info.value.code == "unknown_operation"
        assert exc_info.value.envelope.field == "name"
        assert exc_info.value.envelope.recovery_hint == "CHECK_INPUT"

    def test_unknown_operation_suggests_close_match(self) -> None:
        with pytest.raises(ArtisanError) as exc_info:
            describe("data_transformr")  # typo
        assert "data_transformer" in exc_info.value.envelope.suggestions

    def test_input_spec_metadata_populated(self) -> None:
        meta = describe("data_transformer")
        spec = meta.inputs["dataset"]
        assert spec.artifact_type == "data"
        assert spec.required is True
        assert spec.materialize is True

    def test_output_spec_metadata_populated(self) -> None:
        meta = describe("data_transformer")
        spec = meta.outputs["dataset"]
        assert spec.artifact_type == "data"


class TestExamples:
    def test_returns_declared_examples(self) -> None:
        result = examples("_test_docstring_only_op")
        assert len(result) == 1
        assert result[0].description == "Minimal example."
        assert result[0].params == {"alpha": 2}

    def test_empty_for_op_without_examples(self) -> None:
        result = examples("data_transformer")
        assert result == []

    def test_unknown_operation_raises_artisan_error(self) -> None:
        with pytest.raises(ArtisanError) as exc_info:
            examples("does_not_exist")
        assert exc_info.value.code == "unknown_operation"


class TestFixtureRegistration:
    """Sanity: the conftest fixture op registered and is described correctly."""

    def test_fixture_op_appears_in_list_operations(self) -> None:
        names = {s.name for s in list_operations()}
        assert "_test_docstring_only_op" in names

    def test_fixture_op_tags_round_trip(self) -> None:
        meta = describe("_test_docstring_only_op")
        assert meta.tags == ["test", "fixture"]
