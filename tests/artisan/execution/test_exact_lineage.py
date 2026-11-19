"""Exact ordered occurrence validation without content-ID branch merging."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.execution.exceptions import LineageIntegrityError
from artisan.execution.lineage.validation import validate_lineage_integrity
from artisan.schemas import LineageMapping, MetricArtifact, OutputSpec

SOURCE = "a" * 32


def validate(mappings: list[LineageMapping], ids: list[str]) -> None:
    validate_lineage_integrity(
        {"metrics": mappings},
        {"data": ids},
        {"metrics": [MetricArtifact.draft({"value": 1}, "value.json", 1)]},
        {
            "metrics": OutputSpec(
                artifact_type="metric", derives_from={"inputs": ["data"]}
            )
        },
        invocation_id="call",
    )


def exact(index: int, **changes: object) -> LineageMapping:
    return LineageMapping(
        **(
            {
                "draft_index": 0,
                "source_role": "data",
                "source_artifact_id": SOURCE,
                "source_input_index": index,
                "source_invocation": "call",
            }
            | changes
        )
    )


def test_repeated_content_can_name_both_distinct_input_positions() -> None:
    validate([exact(0), exact(1)], [SOURCE, SOURCE])


def test_mixed_parents_keep_equal_input_and_output_positions_distinct() -> None:
    artifact = MetricArtifact.draft({"value": 1}, "value.json", 1)
    sibling = LineageMapping(draft_index=0, source_role="data", source_output_index=0)
    mappings = [exact(0), exact(1), sibling]
    specs = {
        "data": OutputSpec(artifact_type="metric", derives_from={"inputs": []}),
        "metrics": OutputSpec(
            artifact_type="metric",
            derives_from={"inputs": ["data"], "outputs": ["data"]},
        ),
    }
    validate_lineage_integrity(
        {"data": [], "metrics": mappings},
        {"data": [SOURCE, SOURCE]},
        {"data": [artifact], "metrics": [artifact]},
        specs,
        invocation_id="call",
    )
    for duplicate in (exact(0), sibling):
        with pytest.raises(LineageIntegrityError, match="Duplicate"):
            validate_lineage_integrity(
                {"data": [], "metrics": [*mappings, duplicate]},
                {"data": [SOURCE, SOURCE]},
                {"data": [artifact], "metrics": [artifact]},
                specs,
                invocation_id="call",
            )


def test_id_only_reference_requires_unique_occurrence() -> None:
    mapping = LineageMapping(
        draft_index=0, source_role="data", source_artifact_id=SOURCE
    )
    validate([mapping], [SOURCE])
    with pytest.raises(LineageIntegrityError, match="ambiguous"):
        validate([mapping], [SOURCE, SOURCE])
    with pytest.raises(LineageIntegrityError, match="Duplicate"):
        validate([mapping, exact(0)], [SOURCE])


@pytest.mark.parametrize(
    ("mapping", "message"),
    [
        (exact(0, source_invocation="foreign"), "another invocation"),
        (exact(1), "outside"),
        (exact(0, source_artifact_id="b" * 32), "identity does not match"),
    ],
)
def test_exact_reference_rejects_wrong_owner_index_or_identity(
    mapping: LineageMapping, message: str
) -> None:
    with pytest.raises(LineageIntegrityError, match=message):
        validate([mapping], [SOURCE])


@pytest.mark.parametrize(
    "changes",
    [
        {"source_invocation": None},
        {"source_input_index": None},
        {"source_input_index": True},
        {"source_input_index": -1},
        {"source_artifact_id": None, "source_output_index": 0},
    ],
)
def test_precise_mapping_requires_valid_position_owner_and_id(changes: dict) -> None:
    with pytest.raises(ValidationError):
        exact(0, **changes)


def test_exact_inputs_do_not_use_a_precomputed_id_set() -> None:
    with pytest.raises(LineageIntegrityError, match="identity does not match"):
        validate_lineage_integrity(
            {"metrics": [exact(0)]},
            {"data": ["b" * 32]},
            {"metrics": [MetricArtifact.draft({"v": 1}, "v.json", 1)]},
            {
                "metrics": OutputSpec(
                    artifact_type="metric", derives_from={"inputs": ["data"]}
                )
            },
            prepared_input_ids={"data": {SOURCE}},
            invocation_id="call",
        )


def test_precise_reference_requires_consumer_even_on_existing_path() -> None:
    with pytest.raises(LineageIntegrityError, match="consuming invocation"):
        validate_lineage_integrity(
            {"metrics": [exact(0)]},
            {"data": [SOURCE]},
            {"metrics": [MetricArtifact.draft({"v": 1}, "v.json", 1)]},
            {
                "metrics": OutputSpec(
                    artifact_type="metric", derives_from={"inputs": ["data"]}
                )
            },
        )


def test_sibling_indices_remain_separate_from_input_positions() -> None:
    artifact = MetricArtifact.draft({"v": 1}, "v.json", 1)
    validate_lineage_integrity(
        {
            "data": [],
            "metrics": [
                LineageMapping(draft_index=0, source_role="data", source_output_index=i)
                for i in (0, 1)
            ],
        },
        {},
        {"data": [artifact, artifact], "metrics": [artifact]},
        {
            "data": OutputSpec(artifact_type="metric", derives_from={"inputs": []}),
            "metrics": OutputSpec(
                artifact_type="metric", derives_from={"outputs": ["data"]}
            ),
        },
        invocation_id="call",
    )
