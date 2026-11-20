"""Fixed artifact declarations detach data without acquiring external payloads."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from artisan.errors import ArtifactIntegrityError
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.specs.input_spec import InputSpec


def _metric() -> MetricArtifact:
    return MetricArtifact.draft(
        {"value": 7}, "value.json", metadata={"labels": ["original"]}
    ).finalize()


def test_default_declaration_and_capture_detach_nested_artifact_state() -> None:
    supplied = _metric()
    members = [supplied, supplied]
    spec = InputSpec(artifact_type="metric", default=members)
    members.clear()
    supplied.metadata["labels"].append("changed")
    selected = spec.snapshot_default()
    assert selected is not None
    assert spec.default is not None
    assert len(selected) == 2
    assert selected[0].artifact_id == selected[1].artifact_id
    assert selected[0] is not selected[1]
    assert selected[0] is not spec.default[0]
    assert selected[0].metadata == {"labels": ["original"]}
    selected[0].metadata["labels"].append("execution")
    assert spec.default[0].metadata == {"labels": ["original"]}


def test_selected_default_rejects_corrupted_finalized_metadata() -> None:
    spec = InputSpec(artifact_type="metric", default=(_metric(),))
    assert spec.default is not None
    spec.default[0].metadata["labels"].append("corrupted")
    with pytest.raises(ArtifactIntegrityError, match="mutated"):
        spec.snapshot_default()


@pytest.mark.parametrize(
    "value",
    ["a" * 32, ["a" * 32], lambda: (), set(), {"artifact_id": "a" * 32}],
)
def test_default_rejects_callbacks_references_and_unordered_data(value: object) -> None:
    with pytest.raises(ValidationError, match="Input defaults require"):
        InputSpec.model_validate({"default": value})


def test_default_rejects_draft_and_wrong_type() -> None:
    draft = MetricArtifact.draft({"value": 7}, "value.json")
    with pytest.raises(ValidationError, match="finalized artifact"):
        InputSpec(default=(draft,))
    with pytest.raises(ValidationError, match="incompatible artifact type"):
        InputSpec(artifact_type="csv", default=(_metric(),))


def test_empty_default_is_distinct_from_absent_default() -> None:
    assert InputSpec(default=()).snapshot_default() == ()
    assert InputSpec().snapshot_default() is None


@pytest.mark.usefixtures("external_artifact_types")
def test_external_default_drops_runtime_placement_without_payload_io(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from artisan.schemas.artifact.external_descriptor import (
        ExternalFile,
        ExternalFilesDescriptor,
    )
    from artisan.schemas.external_artifacts import ModelWeightsArtifact

    descriptor = ExternalFilesDescriptor(
        files=(ExternalFile(path="weights.bin", content_hash="a" * 32, size_bytes=3),)
    )
    artifact = ModelWeightsArtifact.draft_weights(
        descriptor, model_family="synthetic"
    ).finalize()
    artifact.bind_external_root(tmp_path)
    artifact.materialized_path = str(tmp_path / "old-runtime")

    def unexpected_io(*args: object, **kwargs: object) -> None:
        pytest.fail("Default capture accessed an external payload")

    monkeypatch.setattr(ModelWeightsArtifact, "read_content", unexpected_io)
    monkeypatch.setattr(ModelWeightsArtifact, "materialize_to", unexpected_io)
    monkeypatch.setattr(ModelWeightsArtifact, "verify_external_content", unexpected_io)
    spec = InputSpec(artifact_type="model_weights", default=(artifact,))
    selected = spec.snapshot_default()
    assert selected is not None
    assert spec.default is not None
    assert selected[0].artifact_id == artifact.artifact_id
    assert artifact.external_root == tmp_path
    for captured in (spec.default[0], selected[0]):
        assert captured.materialized_path is None
        with pytest.raises(ValueError, match="no bound external installation"):
            _ = captured.external_root
