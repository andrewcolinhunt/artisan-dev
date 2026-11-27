"""Tests for output_spec.py"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.specs.output_spec import OutputSpec


class TestOutputSpec:
    """Tests for the OutputSpec model."""

    def test_create_data_output(self):
        """Create a data output without an explicit lineage declaration."""
        spec = OutputSpec(
            artifact_type=ArtifactTypes.CSV,
            description="Processed data files",
        )
        assert spec.artifact_type == ArtifactTypes.CSV
        assert spec.derives_from is None

    def test_default_values(self):
        """Test default values."""
        spec = OutputSpec()
        assert spec.artifact_type == ArtifactTypes.ANY
        assert spec.description == ""
        assert spec.required is True
        assert spec.derives_from is None

    def test_metric_output(self):
        """OutputSpec for metric output."""
        spec = OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            description="Score values",
        )
        assert spec.artifact_type == ArtifactTypes.METRIC

    def test_any_type_output(self):
        """OutputSpec with ArtifactTypes.ANY (passthrough/routing pattern)."""
        spec = OutputSpec(
            artifact_type=ArtifactTypes.ANY,
            description="Artifact that passed filter",
        )
        assert spec.artifact_type == ArtifactTypes.ANY

    def test_frozen(self):
        """OutputSpec is immutable."""
        spec = OutputSpec(artifact_type=ArtifactTypes.CSV)
        with pytest.raises(Exception):
            spec.artifact_type = ArtifactTypes.METRIC


class TestDerivesFrom:
    """Tests for derives_from field validation."""

    def test_derives_from_none(self):
        """Allow an absent lineage declaration at the schema level."""
        spec = OutputSpec(
            artifact_type=ArtifactTypes.CSV,
        )
        assert spec.derives_from is None

    def test_derives_from_inputs(self):
        """Test OutputSpec with explicit input lineage."""
        spec = OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            derives_from={"inputs": ["data"]},
        )
        assert spec.derives_from == {"inputs": ["data"]}

    def test_derives_from_outputs(self):
        """Test OutputSpec with output->output lineage."""
        spec = OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            derives_from={"outputs": ["data"]},
        )
        assert spec.derives_from == {"outputs": ["data"]}

    def test_derives_from_combined_namespaces(self) -> None:
        spec = OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            derives_from={"inputs": ["data"], "outputs": ["data"]},
        )
        assert spec.derives_from == {"inputs": ["data"], "outputs": ["data"]}

    @pytest.mark.parametrize("kind", ["inputs", "outputs"])
    def test_combined_namespaces_reject_duplicate_roles(self, kind: str) -> None:
        parents = {"inputs": ["data"], "outputs": ["data"]}
        parents[kind].append("data")
        with pytest.raises(ValidationError, match="Duplicate roles"):
            OutputSpec(derives_from=parents)

    def test_combined_namespaces_reject_empty_outputs(self) -> None:
        with pytest.raises(ValidationError, match="at least one output role"):
            OutputSpec(derives_from={"inputs": ["data"], "outputs": []})

    def test_derives_from_generative(self):
        """Test OutputSpec with generative operation (no parents)."""
        spec = OutputSpec(
            artifact_type=ArtifactTypes.CSV,
            derives_from={"inputs": []},
        )
        assert spec.derives_from == {"inputs": []}

    def test_rejects_empty_dict(self):
        """Test that empty dict {} is rejected as invalid."""
        with pytest.raises(ValidationError) as exc_info:
            OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                derives_from={},
            )
        assert "Empty dict {}" in str(exc_info.value)

    def test_rejects_invalid_keys(self):
        """Test that only 'inputs' and 'outputs' keys are allowed."""
        with pytest.raises(ValidationError) as exc_info:
            OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                derives_from={"invalid_key": ["data"]},
            )
        assert "Invalid keys" in str(exc_info.value)
        assert "invalid_key" in str(exc_info.value)

    def test_rejects_mixed_invalid_keys(self):
        """Test rejection when mixing valid and invalid keys."""
        with pytest.raises(ValidationError) as exc_info:
            OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                derives_from={
                    "inputs": ["a"],
                    "bad_key": ["b"],
                },
            )
        assert "Invalid keys" in str(exc_info.value)

    def test_rejects_empty_output_references(self) -> None:
        with pytest.raises(ValidationError, match="at least one output role"):
            OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                derives_from={"outputs": []},
            )

    @pytest.mark.parametrize("reference_kind", ["inputs", "outputs"])
    def test_rejects_duplicate_lineage_roles(self, reference_kind: str) -> None:
        with pytest.raises(ValidationError, match="Duplicate roles"):
            OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                derives_from={reference_kind: ["data", "data"]},
            )

    def test_hash_with_derives_from(self):
        """Test that OutputSpec is hashable with derives_from."""
        spec1 = OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            derives_from={"inputs": ["a", "b"]},
        )
        spec2 = OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            derives_from={"inputs": ["a", "b"]},
        )
        spec3 = OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            derives_from={"inputs": ["b", "a"]},
        )

        assert hash(spec1) == hash(spec2)

        assert hash(spec1) != hash(spec3)

    def test_hash_none_lineage(self):
        """Test hash with None derives_from."""
        spec = OutputSpec(artifact_type=ArtifactTypes.METRIC)
        assert isinstance(hash(spec), int)

    def test_hash_with_outputs_lineage(self):
        """Test hash with outputs key."""
        spec1 = OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            derives_from={"outputs": ["b"]},
        )
        spec2 = OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            derives_from={"outputs": ["b"]},
        )

        assert hash(spec1) == hash(spec2)


class TestArtifactTypeValidator:
    """Tests for the artifact_type field validator."""

    def test_default_any_accepted(self):
        """OutputSpec() constructs and defaults artifact_type to ANY."""
        spec = OutputSpec()
        assert spec.artifact_type == ArtifactTypes.ANY

    @pytest.mark.parametrize(
        "artifact_type",
        [
            ArtifactTypes.METRIC,
            ArtifactTypes.FILE_REF,
            ArtifactTypes.CONFIG,
            ArtifactTypes.CSV,
        ],
    )
    def test_registered_key_accepted(self, artifact_type: str):
        """Each built-in registered key is accepted."""
        spec = OutputSpec(artifact_type=artifact_type)
        assert spec.artifact_type == artifact_type

    def test_unregistered_string_rejected(self):
        """Unregistered strings raise with a helpful message."""
        with pytest.raises(ValidationError) as exc_info:
            OutputSpec(artifact_type="totally_made_up")
        msg = str(exc_info.value)
        assert "totally_made_up" in msg
        assert "metric" in msg
        assert "ArtifactTypeDef" in msg


def test_removed_lineage_keyword_is_rejected() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        OutputSpec(infer_lineage_from={"inputs": []})


@pytest.mark.parametrize("kind", ["inputs", "outputs"])
def test_optional_parents_are_an_explicit_subset(kind: str) -> None:
    spec = OutputSpec(
        derives_from={kind: ["source", "context"]}, optional_parent_roles=("context",)
    )
    assert OutputSpec.model_validate_json(spec.model_dump_json()) == spec
    assert spec != spec.model_copy(update={"optional_parent_roles": ()})
    assert hash(spec) != hash(spec.model_copy(update={"optional_parent_roles": ()}))


@pytest.mark.parametrize(
    ("derives_from", "optional"),
    [
        (None, ("source",)),
        ({"inputs": []}, ("source",)),
        ({"inputs": ["source"]}, ("other",)),
        ({"inputs": ["source"]}, ("source", "source")),
        ({"inputs": ["source"]}, ("",)),
        ({"inputs": ["source"], "outputs": ["candidate"]}, ("other",)),
        ({"inputs": ["source"], "outputs": ["candidate"]}, ("source", "source")),
    ],
)
def test_invalid_optional_parent_contract_fails(
    derives_from: dict[str, list[str]] | None, optional: tuple[str, ...]
) -> None:
    with pytest.raises(ValidationError, match="Optional parent"):
        OutputSpec(derives_from=derives_from, optional_parent_roles=optional)
