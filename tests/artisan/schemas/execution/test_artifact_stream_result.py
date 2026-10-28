"""Strict, worker-local result contracts for incremental curators."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.schemas import ArtifactEmission, ArtifactStreamResult, CSVArtifact


def _draft():
    return CSVArtifact.draft(b"value\n1\n", "sample.csv", step_number=1)


@pytest.mark.parametrize(
    "sources",
    [{"file": [1]}, {"file": []}, {"": ["a" * 32]}, {"file": [""]}],
)
def test_emission_rejects_invalid_parent_references(sources):
    with pytest.raises(ValidationError):
        ArtifactEmission(role="csv", artifact=_draft(), sources=sources)


@pytest.mark.parametrize("role", ["", 1])
def test_emission_requires_nonempty_string_role(role):
    with pytest.raises(ValidationError):
        ArtifactEmission(role=role, artifact=_draft(), sources={})


def test_stream_requires_iterator_without_consuming_it():
    seen = []

    def items():
        seen.append(True)
        yield ArtifactEmission(role="csv", artifact=_draft(), sources={})

    result = ArtifactStreamResult(items=items())
    assert seen == []
    assert next(result.items).role == "csv"
    assert seen == [True]
    with pytest.raises(ValidationError):
        ArtifactStreamResult(items=[])


def test_emission_revalidates_mutated_parent_collection():
    item = ArtifactEmission(role="csv", artifact=_draft(), sources={"file": ["a" * 32]})
    item.sources["file"].append(2)
    with pytest.raises(ValidationError):
        ArtifactEmission.model_validate(item)
