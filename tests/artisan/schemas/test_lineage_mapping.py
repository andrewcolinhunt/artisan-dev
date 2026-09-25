"""Strict, filename-independent lineage reference schema."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.schemas import LineageMapping


def test_input_and_output_references_roundtrip() -> None:
    for source in ({"source_artifact_id": "a" * 32}, {"source_output_index": 0}):
        mapping = LineageMapping(draft_index=2, source_role="data", **source)
        assert LineageMapping.model_validate_json(mapping.model_dump_json()) == mapping
        with pytest.raises(ValidationError, match="frozen"):
            mapping.draft_index = 1


@pytest.mark.parametrize(
    "source", [{}, {"source_artifact_id": "a" * 32, "source_output_index": 0}]
)
def test_exactly_one_source_required(source: dict) -> None:
    with pytest.raises(ValidationError, match="exactly one"):
        LineageMapping(draft_index=0, source_role="data", **source)


@pytest.mark.parametrize("field", ["draft_index", "source_output_index"])
@pytest.mark.parametrize("value", [-1, True, False, 1.0, "0", None])
def test_strict_nonnegative_indices(field: str, value: object) -> None:
    values = {"draft_index": 0, "source_role": "data", "source_output_index": 0}
    values[field] = value
    with pytest.raises(ValidationError):
        LineageMapping(**values)


@pytest.mark.parametrize("source_id", ["a" * 31, "a" * 33, "g" * 32, "", True, 123])
def test_malformed_ids_rejected(source_id: object) -> None:
    with pytest.raises(ValidationError):
        LineageMapping(draft_index=0, source_role="data", source_artifact_id=source_id)


@pytest.mark.parametrize(
    "extra", ["draft_original_name", "source_original_name", "group_id", "anything"]
)
def test_removed_fields_rejected(extra: str) -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        LineageMapping(
            draft_index=0, source_role="data", source_output_index=0, **{extra: "old"}
        )


def test_source_role_must_be_nonempty() -> None:
    with pytest.raises(ValidationError):
        LineageMapping(draft_index=0, source_role="", source_output_index=0)
