"""Tests for validation functions in artifact-centric execution.

Reference: v4 design - validation framework for artifacts and lineage.
"""

from __future__ import annotations

import pytest

from artisan.execution.exceptions import (
    ArtifactValidationError,
    LineageCompletenessError,
    LineageIntegrityError,
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

# =============================================================================
# Test Fixtures
# =============================================================================


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


# =============================================================================
# Test validate_artifacts_match_specs
# =============================================================================


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

        # Should not raise
        validate_artifacts_match_specs(artifacts, specs)

    def test_missing_required_role_raises_error(self):
        """Missing required role should raise ArtifactValidationError."""
        artifacts = {}  # No artifacts
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
        artifacts = {"outputs": []}  # Empty list
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
        artifacts = {"outputs": []}  # Empty list
        specs = {
            "outputs": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                required=False,  # Optional
            )
        }

        # Should not raise
        validate_artifacts_match_specs(artifacts, specs)

    def test_missing_optional_role_passes(self):
        """Missing optional role should pass validation."""
        artifacts = {}  # No artifacts
        specs = {
            "outputs": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                required=False,  # Optional
            )
        }

        # Should not raise
        validate_artifacts_match_specs(artifacts, specs)

    def test_artifact_type_mismatch_raises_error(self, draft_artifact):
        """Wrong artifact type for spec should raise error."""
        artifacts = {"outputs": [draft_artifact]}  # Metric
        specs = {
            "outputs": OutputSpec(
                artifact_type=ArtifactTypes.FILE_REF,  # Expects FileRef
                required=True,
            )
        }

        with pytest.raises(ArtifactValidationError, match="Artifact type mismatch"):
            validate_artifacts_match_specs(artifacts, specs)

    def test_extra_roles_raise_error(self, draft_artifact):
        """Extra roles not in specs should raise error."""
        artifacts = {
            "outputs": [draft_artifact],
            "extra_role": [draft_artifact],  # Not in specs
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

        # Should not raise - no type checking for ANY
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

        # Should not raise
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


# =============================================================================
# Test validate_lineage_completeness
# =============================================================================


class TestValidateLineageCompleteness:
    """Tests for validate_lineage_completeness function."""

    def test_all_artifacts_have_lineage(self, draft_artifact, finalized_artifact):
        """All non-orphan artifacts with lineage should pass."""
        artifacts = {"outputs": [draft_artifact]}
        specs = {
            "outputs": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
            )
        }
        lineage = {
            "outputs": [
                LineageMapping(
                    draft_original_name="sample_001",
                    source_artifact_id=finalized_artifact.artifact_id,
                    source_role="input_metrics",
                )
            ]
        }

        # Should not raise
        validate_lineage_completeness(artifacts, specs, lineage)

    def test_missing_lineage_for_non_orphan_raises_error(self, draft_artifact):
        """Non-orphan artifact without lineage should raise error."""
        artifacts = {"outputs": [draft_artifact]}
        specs = {
            "outputs": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
            )
        }
        lineage = {}  # No lineage mappings

        with pytest.raises(LineageCompletenessError, match="has no lineage mapping"):
            validate_lineage_completeness(artifacts, specs, lineage)

    def test_orphan_outputs_skip_lineage_check(self, draft_artifact):
        """Orphan outputs (generative ops) don't need lineage."""
        artifacts = {"outputs": [draft_artifact]}
        specs = {
            "outputs": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": []},  # Orphan - no parents
            )
        }
        lineage = {}  # No lineage mappings

        # Should not raise - orphans don't need lineage
        validate_lineage_completeness(artifacts, specs, lineage)

    def test_empty_artifact_list_passes(self):
        """Empty artifact list should pass (no artifacts to validate)."""
        artifacts = {"outputs": []}
        specs = {
            "outputs": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
            )
        }
        lineage = {}

        # Should not raise - no artifacts means nothing to validate
        validate_lineage_completeness(artifacts, specs, lineage)

    def test_role_not_in_specs_skipped(self, draft_artifact):
        """Roles not in specs should be skipped."""
        artifacts = {"unknown_role": [draft_artifact]}
        specs = {}  # No specs
        lineage = {}

        # Should not raise - role not in specs, skip validation
        validate_lineage_completeness(artifacts, specs, lineage)

    def test_multiple_artifacts_all_need_lineage(
        self,
        draft_artifact,
        draft_artifact_2,
        finalized_artifact,
    ):
        """Multiple artifacts all need lineage mappings."""
        artifacts = {"outputs": [draft_artifact, draft_artifact_2]}
        specs = {
            "outputs": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
            )
        }
        # Only first artifact has lineage
        lineage = {
            "outputs": [
                LineageMapping(
                    draft_original_name="sample_001",
                    source_artifact_id=finalized_artifact.artifact_id,
                    source_role="input_metrics",
                )
            ]
        }

        with pytest.raises(LineageCompletenessError, match="sample_002"):
            validate_lineage_completeness(artifacts, specs, lineage)

    def test_explicit_input_lineage_requires_mapping(
        self, draft_artifact, finalized_artifact
    ):
        """Explicit infer_lineage_from with inputs requires mapping."""
        draft_for_test = MetricArtifact.draft(
            content={"derived": 1.0},
            original_name="sample_001_derived.json",
            step_number=1,
        )
        artifacts = {"derived": [draft_for_test]}
        specs = {
            "derived": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["metrics"]},  # Explicit
            )
        }
        lineage = {
            "derived": [
                LineageMapping(
                    draft_original_name="sample_001_derived",
                    source_artifact_id=finalized_artifact.artifact_id,
                    source_role="metrics",
                )
            ]
        }

        # Should not raise
        validate_lineage_completeness(artifacts, specs, lineage)

    def test_output_to_output_lineage(self, draft_artifact, finalized_artifact):
        """Output->output lineage (outputs key) still requires mapping."""
        draft_for_test = MetricArtifact.draft(
            content={"derived": 1.0},
            original_name="derived_metrics.json",
            step_number=1,
        )
        artifacts = {"derived": [draft_for_test]}
        specs = {
            "derived": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"outputs": ["metrics"]},
            )
        }
        lineage = {}  # No lineage

        # Should raise - outputs lineage still needs mapping
        with pytest.raises(LineageCompletenessError):
            validate_lineage_completeness(artifacts, specs, lineage)


# =============================================================================
# Test validate_lineage_integrity
# =============================================================================


class TestValidateLineageIntegrity:
    """Tests for validate_lineage_integrity function."""

    def test_valid_lineage_references(self, draft_artifact, finalized_artifact):
        """Valid lineage references should pass."""
        lineage = {
            "outputs": [
                LineageMapping(
                    draft_original_name="sample_001",
                    source_artifact_id=finalized_artifact.artifact_id,
                    source_role="input_metrics",
                )
            ]
        }
        input_artifacts = {"input_metrics": [finalized_artifact]}
        output_artifacts = {"outputs": [draft_artifact]}

        # Should not raise
        validate_lineage_integrity(lineage, input_artifacts, output_artifacts)

    def test_nonexistent_source_raises_error(self, draft_artifact):
        """Reference to non-existent source should raise error."""
        lineage = {
            "outputs": [
                LineageMapping(
                    draft_original_name="sample_001",
                    source_artifact_id="deadbeefdeadbeefdeadbeefdeadbeef",
                    source_role="input_metrics",
                )
            ]
        }
        input_artifacts = {}
        output_artifacts = {"outputs": [draft_artifact]}

        with pytest.raises(LineageIntegrityError, match="non-existent source"):
            validate_lineage_integrity(lineage, input_artifacts, output_artifacts)

    def test_nonexistent_draft_raises_error(self, finalized_artifact):
        """Reference to non-existent draft should raise error."""
        lineage = {
            "outputs": [
                LineageMapping(
                    draft_original_name="nonexistent",
                    source_artifact_id=finalized_artifact.artifact_id,
                    source_role="input_metrics",
                )
            ]
        }
        input_artifacts = {"input_metrics": [finalized_artifact]}
        output_artifacts = {"outputs": []}  # No outputs

        with pytest.raises(LineageIntegrityError, match="non-existent output"):
            validate_lineage_integrity(lineage, input_artifacts, output_artifacts)

    def test_duplicate_mapping_raises_error(
        self,
        draft_artifact,
        finalized_artifact,
        finalized_artifact_2,
    ):
        """Duplicate mapping for same draft should raise error."""
        lineage = {
            "outputs": [
                LineageMapping(
                    draft_original_name="sample_001",
                    source_artifact_id=finalized_artifact.artifact_id,
                    source_role="input_metrics",
                ),
                LineageMapping(
                    draft_original_name="sample_001",
                    source_artifact_id=finalized_artifact_2.artifact_id,
                    source_role="input_metrics",
                ),
            ]
        }
        input_artifacts = {
            "input_metrics": [
                finalized_artifact,
                finalized_artifact_2,
            ]
        }
        output_artifacts = {"outputs": [draft_artifact]}

        with pytest.raises(LineageIntegrityError, match="Duplicate lineage mapping"):
            validate_lineage_integrity(lineage, input_artifacts, output_artifacts)

    def test_output_to_output_lineage_valid(self):
        """Output->output lineage should be valid when source is an output."""
        draft_metric = MetricArtifact.draft(
            content={"score": 0.95},
            original_name="sample_001_metrics.json",
            step_number=1,
        )
        finalized_output = draft_metric.finalize()

        dependent_metric = MetricArtifact.draft(
            content={"derived_score": 1.0},
            original_name="derived_metrics.json",
            step_number=1,
        )

        lineage = {
            "derived": [
                LineageMapping(
                    draft_original_name="derived_metrics",
                    source_artifact_id=finalized_output.artifact_id,
                    source_role="metrics",
                )
            ]
        }
        input_artifacts = {}
        output_artifacts = {
            "metrics": [finalized_output],
            "derived": [dependent_metric],
        }

        # Should not raise - output->output is valid
        validate_lineage_integrity(lineage, input_artifacts, output_artifacts)

    def test_empty_lineage_passes(self):
        """Empty lineage should pass validation."""
        lineage = {}
        input_artifacts = {}
        output_artifacts = {}

        # Should not raise
        validate_lineage_integrity(lineage, input_artifacts, output_artifacts)

    def test_multiple_roles_all_validated(
        self,
        draft_artifact,
        finalized_artifact,
    ):
        """Lineage from multiple roles should all be validated."""
        draft_derived = MetricArtifact.draft(
            content={"derived": 1.0},
            original_name="sample_001_derived.json",
            step_number=1,
        )
        lineage = {
            "outputs": [
                LineageMapping(
                    draft_original_name="sample_001",
                    source_artifact_id=finalized_artifact.artifact_id,
                    source_role="input_metrics",
                )
            ],
            "derived": [
                LineageMapping(
                    draft_original_name="sample_001_derived",
                    source_artifact_id=finalized_artifact.artifact_id,
                    source_role="input_metrics",
                )
            ],
        }
        input_artifacts = {"input_metrics": [finalized_artifact]}
        output_artifacts = {
            "outputs": [draft_artifact],
            "derived": [draft_derived],
        }

        # Should not raise
        validate_lineage_integrity(lineage, input_artifacts, output_artifacts)

    def test_draft_without_artifact_id_valid_as_output(
        self, draft_artifact, finalized_artifact
    ):
        """Draft artifacts (no artifact_id) are valid as output targets."""
        lineage = {
            "outputs": [
                LineageMapping(
                    draft_original_name="sample_001",
                    source_artifact_id=finalized_artifact.artifact_id,
                    source_role="inputs",
                )
            ]
        }
        input_artifacts = {"inputs": [finalized_artifact]}
        output_artifacts = {"outputs": [draft_artifact]}  # Draft

        # Should not raise - drafts without artifact_id are valid outputs
        validate_lineage_integrity(lineage, input_artifacts, output_artifacts)


# =============================================================================
# Test Exception Classes
# =============================================================================


class TestExceptions:
    """Tests for exception classes."""

    def test_artifact_validation_error_is_exception(self):
        """ArtifactValidationError should be an Exception."""
        error = ArtifactValidationError("test message")
        assert isinstance(error, Exception)
        assert str(error) == "test message"

    def test_lineage_completeness_error_is_exception(self):
        """LineageCompletenessError should be an Exception."""
        error = LineageCompletenessError("test message")
        assert isinstance(error, Exception)
        assert str(error) == "test message"

    def test_lineage_integrity_error_is_exception(self):
        """LineageIntegrityError should be an Exception."""
        error = LineageIntegrityError("test message")
        assert isinstance(error, Exception)
        assert str(error) == "test message"
