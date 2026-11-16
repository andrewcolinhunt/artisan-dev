"""Immutable output references retain their source across serialization and use."""

from __future__ import annotations

import pytest
from fixtures.output_references import SYNTHETIC_STORE, output_reference
from pydantic import ValidationError

from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.orchestration.output_reference import OutputReference


def test_reference_serialization_preserves_exact_source() -> None:
    reference = output_reference("a" * 32, role="scores", artifact_type="metric")
    restored = OutputReference.model_validate_json(reference.model_dump_json())
    assert restored == reference
    assert restored.source.store == SYNTHETIC_STORE
    assert restored.source.step_run_id == "a" * 32
    assert restored.role == "scores"
    assert restored.artifact_type == "metric"


def test_reference_defaults_to_any_artifact_type() -> None:
    reference = output_reference("a" * 32)
    assert reference.artifact_type == ArtifactTypes.ANY


def test_reference_and_nested_source_are_immutable() -> None:
    reference = output_reference("a" * 32)
    with pytest.raises(ValidationError, match="frozen"):
        reference.role = "changed"
    with pytest.raises(ValidationError, match="frozen"):
        reference.source.step_run_id = "b" * 32
    with pytest.raises(ValidationError, match="frozen"):
        reference.source.store.files_root = "/another/files"


def test_reference_hash_and_equality_include_store_attempt_and_role() -> None:
    reference = output_reference("a" * 32)
    equal = output_reference("a" * 32)
    another_attempt = output_reference("b" * 32)
    another_role = output_reference("a" * 32, role="metrics")
    another_store = output_reference(
        "a" * 32,
        store=SYNTHETIC_STORE.model_copy(update={"steps_table_id": "another-table"}),
    )
    assert len({reference, equal, another_attempt, another_role, another_store}) == 4
    assert {reference: "selected"}[equal] == "selected"


def test_position_only_reference_is_rejected() -> None:
    with pytest.raises(ValidationError, match="source"):
        OutputReference.model_validate({"source_step": 0, "role": "data"})
