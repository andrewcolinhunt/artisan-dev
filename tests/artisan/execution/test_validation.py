"""Tests for artifact, lineage and structured validation errors."""

from __future__ import annotations

import pytest

from artisan.errors import ArtisanError
from artisan.execution.exceptions import (
    ArtifactValidationError,
    LineageCompletenessError,
    LineageIntegrityError,
    PassthroughValidationError,
)
from artisan.execution.lineage.validation import (
    validate_artifacts_match_specs,
    validate_lineage_completeness,
    validate_lineage_integrity,
)
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.provenance.lineage_mapping import LineageMapping
from artisan.schemas.specs.output_spec import OutputSpec


@pytest.fixture
def draft_artifact():
    """Create a draft MetricArtifact for testing."""
    return MetricArtifact.draft(
        content={"score": 0.95, "confidence": 0.87},
        original_name="sample_001.json",
        step_number=1,
    )


@pytest.fixture
def draft_artifact_2():
    """Create a second draft MetricArtifact for testing."""
    return MetricArtifact.draft(
        content={"score": 0.72, "accuracy": 1.23},
        original_name="sample_002.json",
        step_number=1,
    )


@pytest.fixture
def finalized_artifact():
    """Create a finalized MetricArtifact for testing."""
    artifact = MetricArtifact.draft(
        content={"score": 0.95, "confidence": 0.87},
        original_name="input_001.json",
        step_number=0,
    )
    return artifact.finalize()


@pytest.fixture
def finalized_artifact_2():
    """Create a second finalized MetricArtifact for testing."""
    artifact = MetricArtifact.draft(
        content={"score": 0.72, "accuracy": 1.23},
        original_name="input_002.json",
        step_number=0,
    )
    return artifact.finalize()


@pytest.fixture
def draft_config_artifact():
    """Create a draft ExecutionConfigArtifact for type-mismatch tests."""
    from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

    return ExecutionConfigArtifact.draft(
        content={"key": "value"},
        original_name="config.json",
        step_number=1,
    )


class TestValidateArtifactsMatchSpecs:
    """Tests for validate_artifacts_match_specs function."""

    def test_valid_artifacts_match_specs(self, draft_artifact):
        """Artifacts matching specs should pass validation."""
        artifacts = {"outputs": [draft_artifact]}
        specs = {
            "outputs": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                required=True,
            )
        }

        validate_artifacts_match_specs(artifacts, specs)

    def test_missing_required_role_raises_error(self):
        """Missing required role should raise ArtifactValidationError."""
        artifacts = {}
        specs = {
            "outputs": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                required=True,
            )
        }

        with pytest.raises(
            ArtifactValidationError, match="Missing required output role"
        ):
            validate_artifacts_match_specs(artifacts, specs)

    def test_empty_artifact_list_for_required_role_raises_error(self, draft_artifact):
        """Empty artifact list for required role should raise error."""
        artifacts = {"outputs": []}
        specs = {
            "outputs": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                required=True,
            )
        }

        with pytest.raises(
            ArtifactValidationError, match="Empty artifact list for required role"
        ):
            validate_artifacts_match_specs(artifacts, specs)

    def test_empty_artifact_list_for_optional_role_passes(self):
        """Empty artifact list for optional role should pass."""
        artifacts = {"outputs": []}
        specs = {
            "outputs": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                required=False,
            )
        }

        validate_artifacts_match_specs(artifacts, specs)

    def test_missing_optional_role_passes(self):
        """Missing optional role should pass validation."""
        artifacts = {}
        specs = {
            "outputs": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                required=False,
            )
        }

        validate_artifacts_match_specs(artifacts, specs)

    def test_artifact_type_mismatch_raises_error(self, draft_artifact):
        """Wrong artifact type for spec should raise error."""
        artifacts = {"outputs": [draft_artifact]}
        specs = {
            "outputs": OutputSpec(
                artifact_type=ArtifactTypes.FILE_REF,
                required=True,
            )
        }

        with pytest.raises(ArtifactValidationError, match="Artifact type mismatch"):
            validate_artifacts_match_specs(artifacts, specs)

    def test_extra_roles_raise_error(self, draft_artifact):
        """Extra roles not in specs should raise error."""
        artifacts = {
            "outputs": [draft_artifact],
            "extra_role": [draft_artifact],
        }
        specs = {
            "outputs": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                required=True,
            )
        }

        with pytest.raises(ArtifactValidationError, match="Unexpected output roles"):
            validate_artifacts_match_specs(artifacts, specs)

    def test_any_spec_skips_type_check(self, draft_artifact):
        """ArtifactTypes.ANY specs skip type validation."""
        artifacts = {"passthrough": [draft_artifact]}
        specs = {
            "passthrough": OutputSpec(
                artifact_type=ArtifactTypes.ANY,
                required=True,
            )
        }

        validate_artifacts_match_specs(artifacts, specs)

    def test_multiple_artifacts_per_role(self, draft_artifact, draft_artifact_2):
        """Multiple artifacts per role should all be validated."""
        artifacts = {"outputs": [draft_artifact, draft_artifact_2]}
        specs = {
            "outputs": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                required=True,
            )
        }

        validate_artifacts_match_specs(artifacts, specs)

    def test_multiple_artifacts_one_wrong_type_raises_error(
        self, draft_artifact, draft_config_artifact
    ):
        """If one artifact has wrong type, should raise error."""
        artifacts = {"outputs": [draft_artifact, draft_config_artifact]}
        specs = {
            "outputs": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                required=True,
            )
        }

        with pytest.raises(ArtifactValidationError, match="Artifact type mismatch"):
            validate_artifacts_match_specs(artifacts, specs)


def test_dynamic_output_roles_require_explicit_curator_permission(draft_artifact):
    outputs = {"runtime_role": [draft_artifact]}
    with pytest.raises(ArtifactValidationError, match="Unexpected output roles"):
        validate_artifacts_match_specs(outputs, {})
    validate_artifacts_match_specs(outputs, {}, allow_dynamic_outputs=True)
    with pytest.raises(ArtifactValidationError, match="Unexpected output roles"):
        validate_artifacts_match_specs(
            outputs,
            {"declared": OutputSpec(required=False)},
            allow_dynamic_outputs=True,
        )


class TestExplicitLineageValidation:
    @staticmethod
    def mapping(
        index: int = 0, role: str = "input", source: str = "a" * 32
    ) -> LineageMapping:
        return LineageMapping(
            draft_index=index, source_role=role, source_artifact_id=source
        )

    @staticmethod
    def specs(*roles: str) -> dict[str, OutputSpec]:
        return {"out": OutputSpec(derives_from={"inputs": list(roles)})}

    def test_same_role_fan_in_and_repeated_inputs_are_valid(self, draft_artifact):
        outputs = {"out": [draft_artifact]}
        lineage = {"out": [self.mapping(), self.mapping(source="b" * 32)]}
        specs = self.specs("input")
        validate_lineage_integrity(
            lineage, {"input": ["a" * 32, "a" * 32, "b" * 32]}, outputs, specs
        )
        validate_lineage_completeness(outputs, specs, lineage)

    @pytest.mark.parametrize("lineage", [{}, {"out": [], "extra": []}])
    def test_role_coverage_is_exact_even_for_roots(self, draft_artifact, lineage):
        with pytest.raises(LineageIntegrityError, match="exactly match artifact roles"):
            validate_lineage_integrity(
                lineage, {}, {"out": [draft_artifact]}, self.specs()
            )

    @pytest.mark.parametrize(
        "specs", [{}, {"out": OutputSpec(derives_from={"inputs": []})}]
    )
    def test_roots_require_empty_mapping_lists(self, draft_artifact, specs):
        outputs = {"out": [draft_artifact]}
        validate_lineage_integrity({"out": []}, {}, outputs, specs)
        validate_lineage_completeness(outputs, specs, {"out": []})
        with pytest.raises(LineageIntegrityError, match="Root output role"):
            validate_lineage_integrity(
                {"out": [self.mapping()]}, {"input": ["a" * 32]}, outputs, specs
            )

    def test_artifact_result_requires_contract_even_for_empty_optional_role(self):
        with pytest.raises(LineageIntegrityError, match="must declare derives_from"):
            validate_lineage_integrity(
                {"out": []}, {}, {"out": []}, {"out": OutputSpec(required=False)}
            )

    @pytest.mark.parametrize("present", [True, False])
    def test_optional_derived_role_can_be_omitted_or_empty(self, present):
        outputs = {"out": []} if present else {}
        lineage = {"out": []} if present else {}
        specs = {
            "out": OutputSpec(required=False, derives_from={"inputs": ["optional"]})
        }
        validate_artifacts_match_specs(outputs, specs)
        validate_lineage_integrity(lineage, {}, outputs, specs)
        validate_lineage_completeness(outputs, specs, lineage)

    def test_same_human_name_does_not_cover_another_occurrence(self, draft_artifact):
        outputs = {"out": [draft_artifact, draft_artifact]}
        lineage = {"out": [self.mapping()]}
        specs = self.specs("input")
        validate_lineage_integrity(lineage, {"input": ["a" * 32]}, outputs, specs)
        with pytest.raises(LineageCompletenessError, match=r"'out'\[1\].*input"):
            validate_lineage_completeness(outputs, specs, lineage)

    def test_each_required_parent_role_is_needed(self, draft_artifact):
        with pytest.raises(LineageCompletenessError, match="second"):
            validate_lineage_completeness(
                {"out": [draft_artifact]},
                self.specs("input", "second"),
                {"out": [self.mapping()]},
            )

    def test_missing_lineage_role_is_not_a_root(self, draft_artifact):
        with pytest.raises(LineageCompletenessError, match="Missing lineage"):
            validate_lineage_completeness({"out": [draft_artifact]}, self.specs(), {})

    def test_reference_only_input_cannot_become_parent(self, draft_artifact):
        with pytest.raises(LineageIntegrityError, match="forbidden source role"):
            validate_lineage_integrity(
                {"out": [self.mapping(role="reference")]},
                {"reference": ["a" * 32]},
                {"out": [draft_artifact]},
                self.specs("input"),
            )

    def test_input_id_must_exist_in_exact_source_role(self, draft_artifact):
        with pytest.raises(LineageIntegrityError, match="non-existent input source"):
            validate_lineage_integrity(
                {"out": [self.mapping()]},
                {"other": ["a" * 32]},
                {"out": [draft_artifact]},
                self.specs("input"),
            )

    def test_target_index_out_of_range(self, draft_artifact):
        with pytest.raises(
            LineageIntegrityError, match=r"non-existent output 'out'\[1\]"
        ):
            validate_lineage_integrity(
                {"out": [self.mapping(1)]},
                {"input": ["a" * 32]},
                {"out": [draft_artifact]},
                self.specs("input"),
            )

    def test_identical_reference_is_a_duplicate(self, draft_artifact):
        with pytest.raises(LineageIntegrityError, match="Duplicate lineage mapping"):
            validate_lineage_integrity(
                {"out": [self.mapping(), self.mapping()]},
                {"input": ["a" * 32]},
                {"out": [draft_artifact]},
                self.specs("input"),
            )

    @pytest.mark.parametrize("kind", ["inputs", "outputs"])
    def test_same_role_name_cannot_cross_input_output_namespaces(
        self, draft_artifact, kind
    ):
        mapping = LineageMapping(
            draft_index=0,
            source_role="data",
            **(
                {"source_output_index": 0}
                if kind == "inputs"
                else {"source_artifact_id": "a" * 32}
            ),
        )
        specs = {
            "data": OutputSpec(derives_from={"inputs": []}),
            "out": OutputSpec(derives_from={kind: ["data"]}),
        }
        with pytest.raises(LineageIntegrityError, match="lineage requires source_"):
            validate_lineage_integrity(
                {"data": [], "out": [mapping]},
                {"data": ["a" * 32]},
                {"data": [draft_artifact], "out": [draft_artifact]},
                specs,
            )

    def test_id_of_output_is_not_an_input_source(self, finalized_artifact):
        lineage = {
            "data": [],
            "out": [self.mapping(role="data", source=finalized_artifact.artifact_id)],
        }
        specs = {"data": OutputSpec(derives_from={"inputs": []}), **self.specs("data")}
        with pytest.raises(LineageIntegrityError, match="non-existent input source"):
            validate_lineage_integrity(
                lineage,
                {},
                {"data": [finalized_artifact], "out": [finalized_artifact]},
                specs,
            )

    @pytest.mark.parametrize("index", [1, 2])
    def test_invalid_sibling_indices_fail(self, draft_artifact, index):
        lineage = {
            "data": [],
            "out": [
                LineageMapping(
                    draft_index=0, source_role="data", source_output_index=index
                )
            ],
        }
        specs = {
            "data": OutputSpec(derives_from={"inputs": []}),
            "out": OutputSpec(derives_from={"outputs": ["data"]}),
        }
        with pytest.raises(LineageIntegrityError, match="non-existent output source"):
            validate_lineage_integrity(
                lineage, {}, {"data": [draft_artifact], "out": [draft_artifact]}, specs
            )

    def test_distinct_sibling_occurrences_are_valid_even_when_equal(
        self, draft_artifact
    ):
        lineage = {
            "data": [],
            "out": [
                LineageMapping(draft_index=0, source_role="data", source_output_index=i)
                for i in range(2)
            ],
        }
        specs = {
            "data": OutputSpec(derives_from={"inputs": []}),
            "out": OutputSpec(derives_from={"outputs": ["data"]}),
        }
        outputs = {"data": [draft_artifact, draft_artifact], "out": [draft_artifact]}
        validate_lineage_integrity(lineage, {}, outputs, specs)
        validate_lineage_completeness(outputs, specs, lineage)
        lineage["out"].append(lineage["out"][0])
        with pytest.raises(LineageIntegrityError, match="Duplicate"):
            validate_lineage_integrity(lineage, {}, outputs, specs)

    def test_artifacts_without_original_name_are_supported(self):
        from artisan.schemas.artifact.base import Artifact

        class NamelessArtifact(Artifact):
            artifact_type: str = "custom"

        outputs = {"out": [NamelessArtifact()]}
        lineage = {"out": [self.mapping()]}
        specs = self.specs("input")
        validate_artifacts_match_specs(outputs, specs)
        validate_lineage_integrity(lineage, {"input": ["a" * 32]}, outputs, specs)
        validate_lineage_completeness(outputs, specs, lineage)


class TestExceptions:
    """Tests for the re-parented domain exception classes.

    Each keeps ``Exception`` identity (so by-type ``except``/
    ``pytest.raises`` still catch it) and ``str(err) == message`` (so
    log-matching holds), while gaining an ``ArtisanError`` envelope with a
    validation code and ``REPORT_TO_USER`` recovery hint.
    """

    def test_artifact_validation_error_envelope(self):
        error = ArtifactValidationError("test message")
        assert isinstance(error, ArtisanError)
        assert isinstance(error, Exception)
        assert str(error) == "test message"
        assert error.envelope.code == "artifact_validation_failed"
        assert error.envelope.error_type == "validation"
        assert error.envelope.recovery_hint == "REPORT_TO_USER"

    def test_lineage_completeness_error_envelope(self):
        error = LineageCompletenessError("test message")
        assert isinstance(error, ArtisanError)
        assert isinstance(error, Exception)
        assert str(error) == "test message"
        assert error.envelope.code == "lineage_incomplete"
        assert error.envelope.error_type == "validation"
        assert error.envelope.recovery_hint == "REPORT_TO_USER"

    def test_lineage_integrity_error_envelope(self):
        error = LineageIntegrityError("test message")
        assert isinstance(error, ArtisanError)
        assert isinstance(error, Exception)
        assert str(error) == "test message"
        assert error.envelope.code == "lineage_integrity_failed"
        assert error.envelope.error_type == "validation"
        assert error.envelope.recovery_hint == "REPORT_TO_USER"

    def test_passthrough_validation_error_envelope(self):
        error = PassthroughValidationError("test message")
        assert isinstance(error, ArtisanError)
        assert isinstance(error, Exception)
        assert str(error) == "test message"
        assert error.envelope.code == "passthrough_validation_failed"
        assert error.envelope.error_type == "validation"
        assert error.envelope.recovery_hint == "REPORT_TO_USER"
