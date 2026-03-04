"""Tests for lineage_utils module.

Reference: v4 design - Phase 4 lineage utilities.

Tests cover:
- capture_lineage_metadata() with various configurations
- Stem matching algorithm matches current behavior exactly
- Output->Output lineage capture
- build_edges() conversion
- Edge cases (no matches, multiple matches, orphans)
- Multi-role co-input edges with group_by / group_id
"""

from __future__ import annotations

import pytest

from artisan.execution.lineage.builder import build_edges
from artisan.execution.lineage.capture import (
    _build_candidates_from_inputs,
    _build_candidates_from_outputs,
    _build_stem_index,
    _match_by_stem_indexed,
    capture_lineage_metadata,
)
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.provenance.lineage_mapping import LineageMapping
from artisan.schemas.specs.output_spec import OutputSpec

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def finalized_input_artifact():
    """Create a finalized MetricArtifact as input."""
    artifact = MetricArtifact.draft(
        content={"score": 0.95, "sample": "001"},
        original_name="sample_001.json",
        step_number=0,
    )
    return artifact.finalize()


@pytest.fixture
def finalized_input_artifact_2():
    """Create a second finalized MetricArtifact as input."""
    artifact = MetricArtifact.draft(
        content={"score": 0.72, "sample": "002"},
        original_name="sample_002.json",
        step_number=0,
    )
    return artifact.finalize()


@pytest.fixture
def finalized_input_design_1():
    """Create an input artifact named design_1."""
    artifact = MetricArtifact.draft(
        content={"design": 1},
        original_name="design_1.json",
        step_number=0,
    )
    return artifact.finalize()


@pytest.fixture
def finalized_input_design_10():
    """Create an input artifact named design_10."""
    artifact = MetricArtifact.draft(
        content={"design": 10},
        original_name="design_10.json",
        step_number=0,
    )
    return artifact.finalize()


@pytest.fixture
def draft_output_artifact():
    """Create a draft MetricArtifact as output."""
    return MetricArtifact.draft(
        content={"score": 0.95, "processed": True, "sample": "001"},
        original_name="sample_001_processed.json",
        step_number=1,
    )


@pytest.fixture
def draft_output_artifact_2():
    """Create a second draft MetricArtifact as output."""
    return MetricArtifact.draft(
        content={"score": 0.72, "processed": True, "sample": "002"},
        original_name="sample_002_processed.json",
        step_number=1,
    )


@pytest.fixture
def draft_metric_artifact():
    """Create a draft MetricArtifact as output."""
    return MetricArtifact.draft(
        content={"score": 0.95},
        original_name="sample_001_metrics.json",
        step_number=1,
    )


@pytest.fixture
def draft_output_design_1():
    """Create an output artifact named design_1_processed."""
    return MetricArtifact.draft(
        content={"design": 1, "processed": True},
        original_name="design_1_processed.json",
        step_number=1,
    )


@pytest.fixture
def draft_output_design_10():
    """Create an output artifact named design_10_processed."""
    return MetricArtifact.draft(
        content={"design": 10, "processed": True},
        original_name="design_10_processed.json",
        step_number=1,
    )


# =============================================================================
# Test _build_stem_index + _match_by_stem_indexed - Stem Matching Algorithm
# =============================================================================


def _match(
    output_name: str, candidates: list[tuple[str, str, str]]
) -> tuple[str, str] | None:
    """Helper: build index from candidates and match output_name."""
    return _match_by_stem_indexed(output_name, _build_stem_index(candidates))


class TestBuildStemIndex:
    """Tests for _build_stem_index structure."""

    def test_single_candidate(self):
        """Single candidate produces single-entry index."""
        candidates = [("design_1.json", "id_1", "data")]
        index = _build_stem_index(candidates)
        assert index == {"design_1": [("id_1", "data")]}

    def test_multiple_candidates_same_stem(self):
        """Candidates with the same stem are grouped."""
        candidates = [
            ("sample.json", "id_1", "data"),
            ("sample.csv", "id_2", "reference"),
        ]
        index = _build_stem_index(candidates)
        assert index == {"sample": [("id_1", "data"), ("id_2", "reference")]}

    def test_multiple_candidates_different_stems(self):
        """Candidates with different stems get separate entries."""
        candidates = [
            ("alpha.json", "id_a", "data"),
            ("beta.json", "id_b", "data"),
        ]
        index = _build_stem_index(candidates)
        assert index == {
            "alpha": [("id_a", "data")],
            "beta": [("id_b", "data")],
        }

    def test_empty_candidates(self):
        """Empty candidate list produces empty index."""
        assert _build_stem_index([]) == {}


class TestMatchByStemIndexed:
    """Tests for _match_by_stem_indexed - the core matching algorithm."""

    def test_exact_stem_match(self):
        """Output stem starting with input stem should match."""
        candidates = [("input_001.json", "artifact_id_001", "data")]
        result = _match("input_001_processed.json", candidates)
        assert result == ("artifact_id_001", "data")

    def test_no_match_when_stem_not_prefix(self):
        """Output stem not starting with input stem should not match."""
        candidates = [("input_001.json", "artifact_id_001", "data")]
        result = _match("different_file.json", candidates)
        assert result is None

    def test_digit_boundary_protection_design_1_not_matches_design_10(self):
        """design_1 should NOT match design_10 (digit boundary protection).

        This is the CRITICAL test - verifies the digit boundary protection.
        design_10 starts with 'design_1' but we should NOT match because
        a digit follows immediately after 'design_1'.
        """
        candidates = [("design_1.json", "artifact_id_1", "data")]
        result = _match("design_10_processed.json", candidates)
        # Should NOT match because '0' digit follows 'design_1'
        assert result is None

    def test_digit_boundary_protection_design_10_matches_design_10(self):
        """design_10 should match design_10 (exact match)."""
        candidates = [("design_10.json", "artifact_id_10", "data")]
        result = _match("design_10_processed.json", candidates)
        assert result == ("artifact_id_10", "data")

    def test_underscore_suffix_allowed(self):
        """Underscore suffix after stem is allowed (non-digit)."""
        candidates = [("design_1.json", "artifact_id_1", "data")]
        result = _match("design_1_processed.json", candidates)
        assert result == ("artifact_id_1", "data")

    def test_dot_suffix_allowed(self):
        """Dot suffix after stem is allowed (exact match to stem)."""
        candidates = [("sample.json", "artifact_id", "data")]
        result = _match("sample.processed.json", candidates)
        assert result == ("artifact_id", "data")

    def test_ambiguous_match_returns_none(self):
        """Multiple matches should return None (ambiguous)."""
        candidates = [
            ("sample.json", "artifact_id_1", "data"),
            ("sample.csv", "artifact_id_2", "reference"),
        ]
        # Both 'sample' stems match 'sample_processed'
        result = _match("sample_processed.json", candidates)
        assert result is None

    def test_empty_candidates_returns_none(self):
        """Empty candidate list should return None."""
        candidates = []
        result = _match("sample.json", candidates)
        assert result is None

    def test_compound_extensions_stripped(self):
        """Compound extensions like .json.gz should be fully stripped."""
        candidates = [("sample.json.gz", "artifact_id", "data")]
        result = _match("sample_processed.json", candidates)
        assert result == ("artifact_id", "data")

    def test_case_sensitive_matching(self):
        """Matching should be case-sensitive."""
        candidates = [("Sample.json", "artifact_id", "data")]
        result = _match("sample_processed.json", candidates)
        # 'Sample' != 'sample', should not match
        assert result is None

    def test_returns_correct_role(self):
        """Should return the correct source role in the match."""
        candidates = [("sample.json", "artifact_id", "reference")]
        result = _match("sample_accuracy.json", candidates)
        assert result == ("artifact_id", "reference")

    def test_prefix_related_stems_longest_wins(self):
        """When stems are prefix-related, the longest matching stem wins.

        Stems 'dataset_00000_0' and 'dataset_00000_0_0' are both in the
        index. Output 'dataset_00000_0_0_metrics.json' should match the
        longer stem 'dataset_00000_0_0', not accumulate both as ambiguous.
        """
        candidates = [
            ("dataset_00000_0.json", "id_short", "data"),
            ("dataset_00000_0_0.json", "id_long", "data"),
        ]
        result = _match("dataset_00000_0_0_metrics.json", candidates)
        assert result == ("id_long", "data")

    def test_prefix_related_stems_shorter_match(self):
        """When the output only matches the shorter stem, it resolves.

        Output 'dataset_00000_0_metrics.json' matches 'dataset_00000_0'
        but NOT 'dataset_00000_0_0' (which is not a prefix of the output
        stem 'dataset_00000_0_metrics').
        """
        candidates = [
            ("dataset_00000_0.json", "id_short", "data"),
            ("dataset_00000_0_0.json", "id_long", "data"),
        ]
        result = _match("dataset_00000_0_metrics.json", candidates)
        assert result == ("id_short", "data")


class TestDigitBoundaryProtectionComprehensive:
    """Comprehensive tests for digit boundary protection.

    These tests verify that the algorithm correctly handles the subtle
    cases around digit boundaries to prevent design_1 from matching design_10.
    """

    def test_design_1_matches_design_1_underscore(self):
        """design_1 matches design_1_ (underscore is not a digit)."""
        candidates = [("design_1.json", "id_1", "data")]
        result = _match("design_1_output.json", candidates)
        assert result == ("id_1", "data")

    def test_design_1_not_matches_design_10(self):
        """design_1 does NOT match design_10 (digit follows)."""
        candidates = [("design_1.json", "id_1", "data")]
        result = _match("design_10.json", candidates)
        assert result is None

    def test_design_1_not_matches_design_100(self):
        """design_1 does NOT match design_100 (digit follows)."""
        candidates = [("design_1.json", "id_1", "data")]
        result = _match("design_100.json", candidates)
        assert result is None

    def test_design_10_matches_design_10_underscore(self):
        """design_10 matches design_10_ (underscore is not a digit)."""
        candidates = [("design_10.json", "id_10", "data")]
        result = _match("design_10_output.json", candidates)
        assert result == ("id_10", "data")

    def test_design_10_not_matches_design_100(self):
        """design_10 does NOT match design_100 (digit follows)."""
        candidates = [("design_10.json", "id_10", "data")]
        result = _match("design_100.json", candidates)
        assert result is None

    def test_sample_001_matches_sample_001_processed(self):
        """sample_001 matches sample_001_processed."""
        candidates = [("sample_001.json", "id_001", "data")]
        result = _match("sample_001_processed.json", candidates)
        assert result == ("id_001", "data")

    def test_sample_00_not_matches_sample_001(self):
        """sample_00 does NOT match sample_001 (digit follows)."""
        candidates = [("sample_00.json", "id_00", "data")]
        result = _match("sample_001.json", candidates)
        assert result is None

    def test_exact_stem_match_at_boundary(self):
        """Exact stem match at boundary (no suffix) should match."""
        candidates = [("design_1.json", "id_1", "data")]
        result = _match("design_1.json", candidates)
        assert result == ("id_1", "data")


# =============================================================================
# Test _build_candidates_from_inputs
# =============================================================================


class TestBuildCandidatesFromInputs:
    """Tests for _build_candidates_from_inputs helper."""

    def test_builds_candidates_from_single_role(self, finalized_input_artifact):
        """Should build candidates from specified role."""
        input_artifacts = {"data": [finalized_input_artifact]}
        candidates = _build_candidates_from_inputs(input_artifacts, ["data"])

        assert len(candidates) == 1
        assert candidates[0][0] == "sample_001"  # Stem only (no extension)
        assert candidates[0][1] == finalized_input_artifact.artifact_id
        assert candidates[0][2] == "data"

    def test_builds_candidates_from_multiple_roles(
        self, finalized_input_artifact, finalized_input_artifact_2
    ):
        """Should build candidates from multiple roles."""
        input_artifacts = {
            "role_a": [finalized_input_artifact],
            "role_b": [finalized_input_artifact_2],
        }
        candidates = _build_candidates_from_inputs(
            input_artifacts, ["role_a", "role_b"]
        )

        assert len(candidates) == 2
        names = [c[0] for c in candidates]
        roles = [c[2] for c in candidates]
        assert "sample_001" in names  # Stem only
        assert "sample_002" in names  # Stem only
        assert "role_a" in roles
        assert "role_b" in roles

    def test_skips_missing_roles(self, finalized_input_artifact):
        """Should skip roles not in input_artifacts."""
        input_artifacts = {"data": [finalized_input_artifact]}
        candidates = _build_candidates_from_inputs(input_artifacts, ["data", "missing"])

        assert len(candidates) == 1

    def test_empty_role_list(self, finalized_input_artifact):
        """Empty role list should return no candidates."""
        input_artifacts = {"data": [finalized_input_artifact]}
        candidates = _build_candidates_from_inputs(input_artifacts, [])

        assert len(candidates) == 0


# =============================================================================
# Test _build_candidates_from_outputs
# =============================================================================


class TestBuildCandidatesFromOutputs:
    """Tests for _build_candidates_from_outputs helper."""

    def test_builds_candidates_from_finalized_outputs(self, finalized_input_artifact):
        """Should build candidates from finalized output artifacts."""
        output_artifacts = {"data": [finalized_input_artifact]}
        candidates = _build_candidates_from_outputs(output_artifacts, ["data"])

        assert len(candidates) == 1
        assert candidates[0][0] == "sample_001"  # Stem only
        assert candidates[0][1] == finalized_input_artifact.artifact_id
        assert candidates[0][2] == "data"

    def test_builds_candidates_from_draft_outputs(self, draft_output_artifact):
        """Should build candidates from draft outputs with __draft__ prefix."""
        output_artifacts = {"data": [draft_output_artifact]}
        candidates = _build_candidates_from_outputs(output_artifacts, ["data"])

        assert len(candidates) == 1
        assert candidates[0][0] == "sample_001_processed"  # Stem only
        # Draft artifacts get __draft__ prefix using stem
        assert candidates[0][1] == "__draft__sample_001_processed"
        assert candidates[0][2] == "data"


# =============================================================================
# Test capture_lineage_metadata - Explicit Input Matching
# =============================================================================


class TestCaptureLineageMetadataExplicitMatching:
    """Tests for capture_lineage_metadata with explicit input matching."""

    def test_matches_output_to_input_by_stem(
        self, finalized_input_artifact, draft_output_artifact
    ):
        """Should match output to input based on filename stem."""
        input_artifacts = {"data": [finalized_input_artifact]}
        output_artifacts = {"processed": [draft_output_artifact]}
        output_specs = {
            "processed": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["data"]},
            )
        }

        lineage = capture_lineage_metadata(
            output_artifacts, input_artifacts, output_specs
        )

        assert "processed" in lineage
        assert len(lineage["processed"]) == 1
        assert (
            lineage["processed"][0].draft_original_name == "sample_001_processed"
        )  # Stem
        assert (
            lineage["processed"][0].source_artifact_id
            == finalized_input_artifact.artifact_id
        )
        assert lineage["processed"][0].source_role == "data"

    def test_matches_multiple_outputs_to_multiple_inputs(
        self,
        finalized_input_artifact,
        finalized_input_artifact_2,
        draft_output_artifact,
        draft_output_artifact_2,
    ):
        """Should match each output to its corresponding input."""
        input_artifacts = {
            "data": [finalized_input_artifact, finalized_input_artifact_2]
        }
        output_artifacts = {
            "processed": [draft_output_artifact, draft_output_artifact_2]
        }
        output_specs = {
            "processed": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["data"]},
            )
        }

        lineage = capture_lineage_metadata(
            output_artifacts, input_artifacts, output_specs
        )

        assert len(lineage["processed"]) == 2
        mapped_names = {m.draft_original_name for m in lineage["processed"]}
        assert "sample_001_processed" in mapped_names  # Stem only
        assert "sample_002_processed" in mapped_names  # Stem only

    def test_no_match_for_unrelated_names(self, finalized_input_artifact):
        """Output with unrelated name should not have lineage."""
        unrelated_output = MetricArtifact.draft(
            content={"value": 1.0},
            original_name="totally_different.json",
            step_number=1,
        )
        input_artifacts = {"data": [finalized_input_artifact]}
        output_artifacts = {"processed": [unrelated_output]}
        output_specs = {
            "processed": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["data"]},
            )
        }

        lineage = capture_lineage_metadata(
            output_artifacts, input_artifacts, output_specs
        )

        # No lineage mapped for unrelated output
        assert len(lineage["processed"]) == 0

    def test_role_not_in_specs_skipped(
        self, finalized_input_artifact, draft_output_artifact
    ):
        """Roles not in output_specs should be skipped."""
        input_artifacts = {"data": [finalized_input_artifact]}
        output_artifacts = {"unknown_role": [draft_output_artifact]}
        output_specs = {}  # No specs

        lineage = capture_lineage_metadata(
            output_artifacts, input_artifacts, output_specs
        )

        assert "unknown_role" not in lineage

    def test_none_config_skips_role(
        self, finalized_input_artifact, draft_output_artifact
    ):
        """None config (curator ops) should skip the role entirely."""
        input_artifacts = {"data": [finalized_input_artifact]}
        output_artifacts = {"processed": [draft_output_artifact]}
        output_specs = {
            "processed": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                # infer_lineage_from=None — curator op
            )
        }

        lineage = capture_lineage_metadata(
            output_artifacts, input_artifacts, output_specs
        )

        # None config now skips the role
        assert "processed" not in lineage


# =============================================================================
# Test capture_lineage_metadata - Explicit Input Roles
# =============================================================================


class TestCaptureLineageMetadataExplicitInputs:
    """Tests for capture_lineage_metadata with explicit input roles."""

    def test_matches_only_from_specified_inputs(
        self,
        finalized_input_artifact,
        finalized_input_artifact_2,
        draft_output_artifact,
    ):
        """Should only match from specified input roles."""
        input_artifacts = {
            "data": [finalized_input_artifact],
            "reference": [finalized_input_artifact_2],
        }
        output_artifacts = {"processed": [draft_output_artifact]}
        output_specs = {
            "processed": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["data"]},  # Only data inputs
            )
        }

        lineage = capture_lineage_metadata(
            output_artifacts, input_artifacts, output_specs
        )

        assert len(lineage["processed"]) == 1
        assert (
            lineage["processed"][0].source_artifact_id
            == finalized_input_artifact.artifact_id
        )
        assert lineage["processed"][0].source_role == "data"


# =============================================================================
# Test capture_lineage_metadata - Orphan Outputs
# =============================================================================


class TestCaptureLineageMetadataOrphanOutputs:
    """Tests for capture_lineage_metadata with orphan (generative) outputs."""

    def test_orphan_outputs_get_empty_lineage(self, draft_output_artifact):
        """Orphan outputs (generative ops) should get empty lineage list."""
        output_artifacts = {"generated": [draft_output_artifact]}
        output_specs = {
            "generated": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": []},  # Orphan - no parents
            )
        }

        lineage = capture_lineage_metadata(output_artifacts, {}, output_specs)

        assert "generated" in lineage
        assert lineage["generated"] == []  # Empty list, not None


# =============================================================================
# Test capture_lineage_metadata - Output->Output Edges
# =============================================================================


class TestCaptureLineageMetadataOutputToOutput:
    """Tests for capture_lineage_metadata with Output->Output edges."""

    def test_matches_output_to_output_by_stem(self, finalized_input_artifact):
        """Should match output to another output role by stem."""
        # Create output artifact (finalized)
        output_artifact = MetricArtifact.draft(
            content={"processed": True},
            original_name="sample_001_processed.json",
            step_number=1,
        ).finalize()

        # Create metric that derives from the output artifact
        output_metric = MetricArtifact.draft(
            content={"energy": -100.0},
            original_name="sample_001_processed_energy.json",
            step_number=1,
        )

        output_artifacts = {
            "processed": [output_artifact],
            "energy": [output_metric],
        }
        output_specs = {
            "processed": OutputSpec(artifact_type=ArtifactTypes.METRIC),
            "energy": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"outputs": ["processed"]},  # Output->Output
            ),
        }

        lineage = capture_lineage_metadata(
            output_artifacts, {"data": [finalized_input_artifact]}, output_specs
        )

        assert "energy" in lineage
        assert len(lineage["energy"]) == 1
        # Source is the output artifact
        assert lineage["energy"][0].source_artifact_id == output_artifact.artifact_id
        # Source role should be 'processed' (the output role), not 'input'
        assert lineage["energy"][0].source_role == "processed"


# =============================================================================
# Test capture_lineage_metadata - Digit Boundary in Full Flow
# =============================================================================


class TestCaptureLineageMetadataDigitBoundary:
    """Test digit boundary protection in full capture flow."""

    def test_design_1_not_matches_design_10_output(
        self,
        finalized_input_design_1,
        finalized_input_design_10,
        draft_output_design_10,
    ):
        """design_1 input should NOT match design_10 output."""
        input_artifacts = {
            "data": [finalized_input_design_1, finalized_input_design_10]
        }
        output_artifacts = {"processed": [draft_output_design_10]}
        output_specs = {
            "processed": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["data"]},
            )
        }

        lineage = capture_lineage_metadata(
            output_artifacts, input_artifacts, output_specs
        )

        # Should match design_10 input, NOT design_1
        assert len(lineage["processed"]) == 1
        assert (
            lineage["processed"][0].source_artifact_id
            == finalized_input_design_10.artifact_id
        )

    def test_design_1_matches_design_1_output(
        self, finalized_input_design_1, finalized_input_design_10, draft_output_design_1
    ):
        """design_1 input should match design_1 output."""
        input_artifacts = {
            "data": [finalized_input_design_1, finalized_input_design_10]
        }
        output_artifacts = {"processed": [draft_output_design_1]}
        output_specs = {
            "processed": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["data"]},
            )
        }

        lineage = capture_lineage_metadata(
            output_artifacts, input_artifacts, output_specs
        )

        # Should match design_1 input
        assert len(lineage["processed"]) == 1
        assert (
            lineage["processed"][0].source_artifact_id
            == finalized_input_design_1.artifact_id
        )


# =============================================================================
# Test build_edges - Basic Conversion
# =============================================================================


class TestBuildEdgesBasicConversion:
    """Tests for build_edges basic conversion from LineageMapping to edges."""

    def test_converts_lineage_mapping_to_edge(
        self, finalized_input_artifact, draft_output_artifact
    ):
        """Should convert LineageMapping to SourceTargetPair."""
        # Finalize the output
        finalized_output = draft_output_artifact.finalize()

        lineage = {
            "processed": [
                LineageMapping(
                    draft_original_name="sample_001_processed",  # Stem only
                    source_artifact_id=finalized_input_artifact.artifact_id,
                    source_role="data",
                )
            ]
        }
        finalized_artifacts = {"processed": [finalized_output]}
        output_specs = {"processed": OutputSpec(artifact_type=ArtifactTypes.METRIC)}

        edges = build_edges(lineage, finalized_artifacts, {}, output_specs)

        assert len(edges) == 1
        assert edges[0].source == finalized_input_artifact.artifact_id
        assert edges[0].target == finalized_output.artifact_id
        assert edges[0].source_role == "data"
        assert edges[0].target_role == "processed"

    def test_handles_multiple_edges(
        self,
        finalized_input_artifact,
        finalized_input_artifact_2,
        draft_output_artifact,
        draft_output_artifact_2,
    ):
        """Should handle multiple edges from multiple mappings."""
        finalized_output_1 = draft_output_artifact.finalize()
        finalized_output_2 = draft_output_artifact_2.finalize()

        lineage = {
            "processed": [
                LineageMapping(
                    draft_original_name="sample_001_processed",  # Stem only
                    source_artifact_id=finalized_input_artifact.artifact_id,
                    source_role="data",
                ),
                LineageMapping(
                    draft_original_name="sample_002_processed",  # Stem only
                    source_artifact_id=finalized_input_artifact_2.artifact_id,
                    source_role="data",
                ),
            ]
        }
        finalized_artifacts = {"processed": [finalized_output_1, finalized_output_2]}
        output_specs = {"processed": OutputSpec(artifact_type=ArtifactTypes.METRIC)}

        edges = build_edges(lineage, finalized_artifacts, {}, output_specs)

        assert len(edges) == 2
        # All edges should have correct source_role
        for edge in edges:
            assert edge.source_role == "data"

    def test_skips_unmapped_original_names(self, finalized_input_artifact):
        """Should skip mappings where draft_original_name not found in artifacts."""
        lineage = {
            "processed": [
                LineageMapping(
                    draft_original_name="nonexistent",  # Not in finalized_artifacts
                    source_artifact_id=finalized_input_artifact.artifact_id,
                    source_role="data",
                )
            ]
        }
        finalized_artifacts = {"processed": []}  # Empty
        output_specs = {"processed": OutputSpec(artifact_type=ArtifactTypes.METRIC)}

        edges = build_edges(lineage, finalized_artifacts, {}, output_specs)

        assert len(edges) == 0


# =============================================================================
# Test build_edges - Draft Reference Resolution
# =============================================================================


class TestBuildEdgesDraftReferenceResolution:
    """Tests for build_edges resolving __draft__ references.

    Note: The __draft__ prefix is an internal format used by
    capture_lineage_metadata for temporary tracking before finalization.
    It should NOT be used in actual LineageMapping models.

    The build_edges() function handles the case where lineage was captured
    BEFORE finalization and the draft references need resolution. This
    works via the name_to_id lookup from finalized_artifacts.
    """

    def test_output_to_output_resolved_after_finalization(
        self, finalized_input_artifact
    ):
        """Output->Output lineage should work when captured after finalization.

        This tests the common case where lineage is captured on finalized
        artifacts, so source_artifact_id is already valid.
        """
        # Create and finalize both outputs
        finalized_structure = MetricArtifact.draft(
            content={"processed": True},
            original_name="sample_001_processed.json",
            step_number=1,
        ).finalize()

        finalized_metric = MetricArtifact.draft(
            content={"energy": -100.0},
            original_name="sample_001_processed_energy.json",
            step_number=1,
        ).finalize()

        # Capture lineage AFTER finalization (normal case)
        # source_artifact_id is the actual finalized artifact ID
        lineage = {
            "processed": [],
            "energy": [
                LineageMapping(
                    draft_original_name="sample_001_processed_energy",  # Stem only
                    source_artifact_id=finalized_structure.artifact_id,
                    source_role="processed",
                )
            ],
        }
        finalized_artifacts = {
            "processed": [finalized_structure],
            "energy": [finalized_metric],
        }
        output_specs = {
            "processed": OutputSpec(artifact_type=ArtifactTypes.METRIC),
            "energy": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"outputs": ["processed"]},
            ),
        }

        edges = build_edges(lineage, finalized_artifacts, {}, output_specs)

        energy_edges = [e for e in edges if e.target_role == "energy"]
        assert len(energy_edges) == 1
        assert energy_edges[0].source == finalized_structure.artifact_id
        assert energy_edges[0].target == finalized_metric.artifact_id
        assert energy_edges[0].source_role == "processed"


# =============================================================================
# Test source_role Tracking (Design Doc Verification)
# =============================================================================


class TestSourceRoleTracking:
    """Tests verifying correct source_role tracking per design doc.

    These tests ensure that source_role correctly captures the actual
    role of the source artifact rather than hardcoding "input".
    """

    def test_source_role_from_explicit_input_matching(self, finalized_input_artifact):
        """Explicit input->output matching should capture actual input role."""
        output_artifact = MetricArtifact.draft(
            content={"processed": True},
            original_name="sample_001_processed.json",
            step_number=1,
        ).finalize()

        input_artifacts = {"data": [finalized_input_artifact]}
        output_artifacts = {"processed": [output_artifact]}
        output_specs = {
            "processed": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["data"]},
            )
        }

        lineage = capture_lineage_metadata(
            output_artifacts, input_artifacts, output_specs
        )
        edges = build_edges(lineage, output_artifacts, input_artifacts, output_specs)

        assert len(edges) == 1
        assert edges[0].source_role == "data"  # Not "input"!
        assert edges[0].target_role == "processed"

    def test_source_role_from_explicit_input_spec(self, finalized_input_artifact):
        """Explicit input role specification should preserve source role."""
        # Create reference artifact with different name
        reference_artifact = MetricArtifact.draft(
            content={"reference": True},
            original_name="ref.json",
            step_number=0,
        ).finalize()

        output_metric = MetricArtifact.draft(
            content={"accuracy": 0.5},
            original_name="ref_accuracy.json",
            step_number=1,
        ).finalize()

        input_artifacts = {
            "data": [finalized_input_artifact],
            "reference": [reference_artifact],
        }
        output_artifacts = {"accuracy": [output_metric]}
        output_specs = {
            "accuracy": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["reference"]},
            )
        }

        lineage = capture_lineage_metadata(
            output_artifacts, input_artifacts, output_specs
        )
        edges = build_edges(lineage, output_artifacts, input_artifacts, output_specs)

        assert len(edges) == 1
        assert edges[0].source_role == "reference"  # Explicit role preserved
        assert edges[0].target_role == "accuracy"

    def test_source_role_from_output_to_output(self):
        """Output->Output edges should have output role as source_role."""
        # The processed data is the source output
        processed_artifact = MetricArtifact.draft(
            content={"processed": True},
            original_name="sample_001_processed.json",
            step_number=1,
        ).finalize()

        # The energy metric derives from the processed data (stem matches)
        energy_artifact = MetricArtifact.draft(
            content={"energy": -100.0},
            original_name="sample_001_processed_energy.json",
            step_number=1,
        ).finalize()

        output_artifacts = {
            "processed": [processed_artifact],
            "energy": [energy_artifact],
        }
        output_specs = {
            "processed": OutputSpec(artifact_type=ArtifactTypes.METRIC),
            "energy": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"outputs": ["processed"]},
            ),
        }

        lineage = capture_lineage_metadata(output_artifacts, {}, output_specs)
        edges = build_edges(lineage, output_artifacts, {}, output_specs)

        assert len(edges) == 1
        assert edges[0].source_role == "processed"  # Output role, not "input"
        assert edges[0].target_role == "energy"


# =============================================================================
# Test build_edges - Per-Role Edge Resolution
# =============================================================================


class TestBuildEdgesPerRoleResolution:
    """Tests for per-role edge resolution in build_edges.

    Verifies that the role-scoped lookup (role -> original_name -> artifact_id)
    correctly handles cases where different roles produce artifacts with the
    same original_name (which happens because Artifact.draft() strips extensions).
    """

    def test_two_roles_same_original_name_resolve_correctly(self):
        """Two roles with same original_name should resolve to correct artifacts.

        This is the core bug scenario: sample_001.csv and sample_001.json both
        become original_name="sample_001" after extension stripping. Without
        per-role scoping, the second role's artifact_id overwrites the first's.
        """
        # Role A: sample_001.csv -> original_name="sample_001"
        artifact_a = MetricArtifact.draft(
            content={"role": "a"},
            original_name="sample_001.csv",
            step_number=1,
        ).finalize()

        # Role B: sample_001.json -> original_name="sample_001"
        artifact_b = MetricArtifact.draft(
            content={"role": "b"},
            original_name="sample_001.json",
            step_number=1,
        ).finalize()

        # Both have original_name="sample_001" after extension stripping
        assert artifact_a.original_name == "sample_001"
        assert artifact_b.original_name == "sample_001"
        # But different artifact IDs
        assert artifact_a.artifact_id != artifact_b.artifact_id

        # Input artifact that both derive from
        input_artifact = MetricArtifact.draft(
            content={"input": True},
            original_name="sample_001.json",
            step_number=0,
        ).finalize()

        finalized_artifacts = {
            "role_a": [artifact_a],
            "role_b": [artifact_b],
        }

        # Lineage: each role's artifact derives from the input
        lineage = {
            "role_a": [
                LineageMapping(
                    draft_original_name="sample_001",
                    source_artifact_id=input_artifact.artifact_id,
                    source_role="inputs",
                )
            ],
            "role_b": [
                LineageMapping(
                    draft_original_name="sample_001",
                    source_artifact_id=input_artifact.artifact_id,
                    source_role="inputs",
                )
            ],
        }
        output_specs = {
            "role_a": OutputSpec(artifact_type=ArtifactTypes.METRIC),
            "role_b": OutputSpec(artifact_type=ArtifactTypes.METRIC),
        }

        edges = build_edges(lineage, finalized_artifacts, {}, output_specs)

        assert len(edges) == 2
        role_a_edges = [e for e in edges if e.target_role == "role_a"]
        role_b_edges = [e for e in edges if e.target_role == "role_b"]

        assert len(role_a_edges) == 1
        assert len(role_b_edges) == 1

        # Each edge should point to the correct artifact in its own role
        assert role_a_edges[0].target == artifact_a.artifact_id
        assert role_b_edges[0].target == artifact_b.artifact_id

    def test_output_to_output_draft_uses_source_role(self):
        """Draft source resolution should use mapping.source_role, not current role.

        When an Output->Output edge has a __draft__ source, the source should be
        resolved from the role specified in mapping.source_role, not the role
        being iterated.
        """
        # Source output: processed data
        processed_artifact = MetricArtifact.draft(
            content={"processed": True},
            original_name="sample_001_processed.json",
            step_number=1,
        ).finalize()

        # Target output: energy metric derived from processed data
        energy_artifact = MetricArtifact.draft(
            content={"energy": -100.0},
            original_name="sample_001_processed_energy.json",
            step_number=1,
        ).finalize()

        finalized_artifacts = {
            "processed": [processed_artifact],
            "energy": [energy_artifact],
        }

        # Lineage with __draft__ reference pointing to source in "processed" role
        lineage = {
            "energy": [
                LineageMapping(
                    draft_original_name="sample_001_processed_energy",
                    source_artifact_id=processed_artifact.artifact_id,
                    source_role="processed",
                )
            ],
        }
        output_specs = {
            "processed": OutputSpec(artifact_type=ArtifactTypes.METRIC),
            "energy": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"outputs": ["processed"]},
            ),
        }

        edges = build_edges(lineage, finalized_artifacts, {}, output_specs)

        assert len(edges) == 1
        assert edges[0].source == processed_artifact.artifact_id
        assert edges[0].target == energy_artifact.artifact_id
        assert edges[0].source_role == "processed"
        assert edges[0].target_role == "energy"

    def test_single_role_regression(
        self, finalized_input_artifact, draft_output_artifact
    ):
        """Single role should work identically to before (regression test)."""
        finalized_output = draft_output_artifact.finalize()

        lineage = {
            "processed": [
                LineageMapping(
                    draft_original_name="sample_001_processed",
                    source_artifact_id=finalized_input_artifact.artifact_id,
                    source_role="data",
                )
            ]
        }
        finalized_artifacts = {"processed": [finalized_output]}
        output_specs = {"processed": OutputSpec(artifact_type=ArtifactTypes.METRIC)}

        edges = build_edges(lineage, finalized_artifacts, {}, output_specs)

        assert len(edges) == 1
        assert edges[0].source == finalized_input_artifact.artifact_id
        assert edges[0].target == finalized_output.artifact_id
        assert edges[0].source_role == "data"
        assert edges[0].target_role == "processed"

    def test_target_not_found_in_wrong_role(self):
        """Target lookup should only find artifacts in the current role."""
        artifact = MetricArtifact.draft(
            content={"value": 1.0},
            original_name="sample_001.json",
            step_number=1,
        ).finalize()

        input_artifact = MetricArtifact.draft(
            content={"input_data": True},
            original_name="sample_input.json",
            step_number=0,
        ).finalize()

        # Artifact is in "data" role, but lineage references "other" role
        finalized_artifacts = {"data": [artifact]}
        lineage = {
            "other": [
                LineageMapping(
                    draft_original_name="sample_001",
                    source_artifact_id=input_artifact.artifact_id,
                    source_role="inputs",
                )
            ]
        }
        output_specs = {"other": OutputSpec(artifact_type=ArtifactTypes.METRIC)}

        edges = build_edges(lineage, finalized_artifacts, {}, output_specs)

        # No edges: "sample_001" is in "data" role, not "other" role
        assert len(edges) == 0


# =============================================================================
# Test capture_lineage_metadata - Multi-Role Co-Input Edges (group_by)
# =============================================================================


def _make_config(name: str, step: int = 0) -> ExecutionConfigArtifact:
    """Helper: create a finalized ExecutionConfigArtifact."""
    return ExecutionConfigArtifact.draft(
        content={"param": name},
        original_name=f"{name}.json",
        step_number=step,
    ).finalize()


def _make_metric_from_name(name: str, step: int = 0) -> MetricArtifact:
    """Helper: create a finalized MetricArtifact with unique content."""
    return MetricArtifact.draft(
        content={"name": name},
        original_name=f"{name}.json",
        step_number=step,
    ).finalize()


class TestCaptureLineageMultiRoleCoInputs:
    """Tests for capture_lineage_metadata with group_by and co-input edges."""

    def test_co_input_edges_created_when_group_by_set(self):
        """When group_by is set, co-input edges from non-primary roles are created."""
        config_a = _make_config("design_a")
        config_b = _make_config("design_b")
        struct_a = _make_metric_from_name("struct_a")
        struct_b = _make_metric_from_name("struct_b")

        # Outputs named after configs (stem matching against config role)
        out_a = MetricArtifact.draft(
            content={"output": "a"},
            original_name="design_a_sample_0.json",
            step_number=1,
        ).finalize()
        out_b = MetricArtifact.draft(
            content={"output": "b"},
            original_name="design_b_sample_0.json",
            step_number=1,
        ).finalize()

        input_artifacts = {
            "config": [config_a, config_b],
            "data": [struct_a, struct_b],
        }
        output_artifacts = {"samples": [out_a, out_b]}
        output_specs = {
            "samples": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["config"]},
            )
        }
        group_ids = ["gid_0", "gid_1"]

        lineage = capture_lineage_metadata(
            output_artifacts,
            input_artifacts,
            output_specs,
            group_by=GroupByStrategy.LINEAGE,
            group_ids=group_ids,
        )

        # Each output should have 2 edges: one from config, one from data
        assert len(lineage["samples"]) == 4
        edges_for_a = [
            m
            for m in lineage["samples"]
            if m.draft_original_name == "design_a_sample_0"
        ]
        edges_for_b = [
            m
            for m in lineage["samples"]
            if m.draft_original_name == "design_b_sample_0"
        ]

        assert len(edges_for_a) == 2
        assert len(edges_for_b) == 2

        # Verify roles
        roles_a = {m.source_role for m in edges_for_a}
        assert roles_a == {"config", "data"}

        roles_b = {m.source_role for m in edges_for_b}
        assert roles_b == {"config", "data"}

    def test_co_input_edges_at_correct_index(self):
        """data[2] must be paired with config[2], not config[0]."""
        configs = [_make_config(f"cfg_{i}") for i in range(3)]
        items = [_make_metric_from_name(f"item_{i}") for i in range(3)]

        # Output matches config at index 2
        output = MetricArtifact.draft(
            content={"output": 2},
            original_name="cfg_2_sample.json",
            step_number=1,
        ).finalize()

        input_artifacts = {
            "config": configs,
            "data": items,
        }
        output_artifacts = {"samples": [output]}
        output_specs = {
            "samples": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["config"]},
            )
        }
        group_ids = ["gid_0", "gid_1", "gid_2"]

        lineage = capture_lineage_metadata(
            output_artifacts,
            input_artifacts,
            output_specs,
            group_by=GroupByStrategy.ZIP,
            group_ids=group_ids,
        )

        assert len(lineage["samples"]) == 2  # config edge + data edge

        config_edge = next(m for m in lineage["samples"] if m.source_role == "config")
        data_edge = next(m for m in lineage["samples"] if m.source_role == "data")

        # Verify correct index pairing
        assert config_edge.source_artifact_id == configs[2].artifact_id
        assert data_edge.source_artifact_id == items[2].artifact_id

    def test_group_id_stamped_on_all_edges_for_same_output(self):
        """All co-input edges for the same output share the same group_id."""
        config = _make_config("design_x")
        item = _make_metric_from_name("struct_x")

        output = MetricArtifact.draft(
            content={"output": "x"},
            original_name="design_x_sample.json",
            step_number=1,
        ).finalize()

        input_artifacts = {
            "config": [config],
            "data": [item],
        }
        output_artifacts = {"samples": [output]}
        output_specs = {
            "samples": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["config"]},
            )
        }
        group_ids = ["group_abc"]

        lineage = capture_lineage_metadata(
            output_artifacts,
            input_artifacts,
            output_specs,
            group_by=GroupByStrategy.ZIP,
            group_ids=group_ids,
        )

        # Both edges should carry the same group_id
        assert len(lineage["samples"]) == 2
        for mapping in lineage["samples"]:
            assert mapping.group_id == "group_abc"

    def test_single_input_no_group_id(self):
        """Single-input operations produce group_id=None (no group_by)."""
        item = _make_metric_from_name("sample_001")
        output = MetricArtifact.draft(
            content={"processed_val": True},
            original_name="sample_001_processed.json",
            step_number=1,
        ).finalize()

        input_artifacts = {"data": [item]}
        output_artifacts = {"processed": [output]}
        output_specs = {
            "processed": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["data"]},
            )
        }

        lineage = capture_lineage_metadata(
            output_artifacts,
            input_artifacts,
            output_specs,
            # group_by=None (default)
        )

        assert len(lineage["processed"]) == 1
        assert lineage["processed"][0].group_id is None

    def test_group_id_correct_value_from_group_ids_list(self):
        """group_id on each edge matches the correct value from group_ids list."""
        configs = [_make_config(f"cfg_{i}") for i in range(3)]
        items = [_make_metric_from_name(f"struct_{i}") for i in range(3)]

        outputs = [
            MetricArtifact.draft(
                content={"output": f"OUT_{i}"},
                original_name=f"cfg_{i}_sample.json",
                step_number=1,
            ).finalize()
            for i in range(3)
        ]

        input_artifacts = {
            "config": configs,
            "data": items,
        }
        output_artifacts = {"samples": outputs}
        output_specs = {
            "samples": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["config"]},
            )
        }
        group_ids = ["gid_AAA", "gid_BBB", "gid_CCC"]

        lineage = capture_lineage_metadata(
            output_artifacts,
            input_artifacts,
            output_specs,
            group_by=GroupByStrategy.ZIP,
            group_ids=group_ids,
        )

        # 3 outputs x 2 edges each = 6 total
        assert len(lineage["samples"]) == 6

        for i in range(3):
            name = f"cfg_{i}_sample"
            edges_for_i = [
                m for m in lineage["samples"] if m.draft_original_name == name
            ]
            assert len(edges_for_i) == 2
            for edge in edges_for_i:
                assert edge.group_id == group_ids[i]

    def test_group_by_without_group_ids_gives_none_group_id(self):
        """When group_by is set but group_ids is None, group_id is None."""
        config = _make_config("cfg_0")
        item = _make_metric_from_name("struct_0")

        output = MetricArtifact.draft(
            content={"out": True},
            original_name="cfg_0_sample.json",
            step_number=1,
        ).finalize()

        input_artifacts = {"config": [config], "data": [item]}
        output_artifacts = {"samples": [output]}
        output_specs = {
            "samples": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["config"]},
            )
        }

        lineage = capture_lineage_metadata(
            output_artifacts,
            input_artifacts,
            output_specs,
            group_by=GroupByStrategy.ZIP,
            group_ids=None,
        )

        # Co-input edges are still created (group_by is set), but group_id is None
        assert len(lineage["samples"]) == 2
        for mapping in lineage["samples"]:
            assert mapping.group_id is None

    def test_three_input_roles_all_get_co_input_edges(self):
        """Three input roles: primary (config) + 2 co-input roles."""
        config = _make_config("design_1")
        item = _make_metric_from_name("struct_1")
        ref = _make_metric_from_name("ref_1")

        output = MetricArtifact.draft(
            content={"output_val": True},
            original_name="design_1_out.json",
            step_number=1,
        ).finalize()

        input_artifacts = {
            "config": [config],
            "data": [item],
            "reference": [ref],
        }
        output_artifacts = {"samples": [output]}
        output_specs = {
            "samples": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["config"]},
            )
        }
        group_ids = ["gid_triple"]

        lineage = capture_lineage_metadata(
            output_artifacts,
            input_artifacts,
            output_specs,
            group_by=GroupByStrategy.ZIP,
            group_ids=group_ids,
        )

        # 3 edges: config, data, reference
        assert len(lineage["samples"]) == 3
        roles = {m.source_role for m in lineage["samples"]}
        assert roles == {"config", "data", "reference"}
        # All share same group_id
        for m in lineage["samples"]:
            assert m.group_id == "gid_triple"


class TestGroupIdFlowThroughBuildEdges:
    """Tests that group_id flows from LineageMapping through build_edges to SourceTargetPair."""

    def test_group_id_propagated_to_source_target_pair(self):
        """group_id on LineageMapping should appear on SourceTargetPair."""
        config = _make_config("cfg_0")
        item = _make_metric_from_name("struct_0")

        output = MetricArtifact.draft(
            content={"out": True},
            original_name="cfg_0_sample.json",
            step_number=1,
        ).finalize()

        input_artifacts = {
            "config": [config],
            "data": [item],
        }
        output_artifacts = {"samples": [output]}
        output_specs = {
            "samples": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["config"]},
            )
        }
        group_ids = ["gid_flow_test"]

        lineage = capture_lineage_metadata(
            output_artifacts,
            input_artifacts,
            output_specs,
            group_by=GroupByStrategy.ZIP,
            group_ids=group_ids,
        )

        edges = build_edges(lineage, output_artifacts, input_artifacts, output_specs)

        assert len(edges) == 2
        for edge in edges:
            assert edge.group_id == "gid_flow_test"

    def test_none_group_id_propagated_when_no_group_by(self):
        """Without group_by, group_id on edges is None."""
        item = _make_metric_from_name("sample_001")
        output = MetricArtifact.draft(
            content={"processed_val": True},
            original_name="sample_001_processed.json",
            step_number=1,
        ).finalize()

        input_artifacts = {"data": [item]}
        output_artifacts = {"processed": [output]}
        output_specs = {
            "processed": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"inputs": ["data"]},
            )
        }

        lineage = capture_lineage_metadata(
            output_artifacts,
            input_artifacts,
            output_specs,
        )

        edges = build_edges(lineage, output_artifacts, input_artifacts, output_specs)

        assert len(edges) == 1
        assert edges[0].group_id is None


class TestGroupIdFlowThroughEnrichment:
    """Tests that group_id flows through enrichment to ArtifactProvenanceEdge."""

    def test_group_id_on_artifact_edge_from_store(self):
        """group_id from SourceTargetPair appears on ArtifactProvenanceEdge (store path)."""
        from unittest.mock import Mock

        from artisan.execution.lineage.enrich import build_artifact_edges_from_store
        from artisan.schemas.provenance.source_target_pair import SourceTargetPair

        mock_store = Mock()
        mock_store.load_artifact_type_map.return_value = {
            "a" * 32: ArtifactTypes.METRIC,
            "b" * 32: ArtifactTypes.METRIC,
            "c" * 32: ArtifactTypes.METRIC,
        }

        pairs = [
            SourceTargetPair(
                source="a" * 32,
                target="b" * 32,
                source_role="config",
                target_role="samples",
                group_id="gid_enrich_test",
            ),
            SourceTargetPair(
                source="c" * 32,
                target="b" * 32,
                source_role="data",
                target_role="samples",
                group_id="gid_enrich_test",
            ),
        ]

        edges = build_artifact_edges_from_store(pairs, "r" * 32, mock_store)

        assert len(edges) == 2
        for edge in edges:
            assert edge.group_id == "gid_enrich_test"

    def test_group_id_on_artifact_edge_from_dict(self):
        """group_id from SourceTargetPair appears on ArtifactProvenanceEdge (dict path)."""
        from artisan.execution.lineage.enrich import build_artifact_edges_from_dict
        from artisan.schemas.provenance.source_target_pair import SourceTargetPair

        metric = MetricArtifact(
            artifact_id="s" * 32,
            origin_step_number=0,
            content=b'{"value": 1.0}',
        )

        pairs = [
            SourceTargetPair(
                source="s" * 32,
                target="t" * 32,
                source_role="data",
                target_role="samples",
                group_id="gid_dict_test",
            )
        ]

        edges = build_artifact_edges_from_dict(pairs, "r" * 32, {"s" * 32: metric})

        assert len(edges) == 1
        assert edges[0].group_id == "gid_dict_test"

    def test_none_group_id_preserved_through_enrichment(self):
        """None group_id is preserved through enrichment."""
        from artisan.execution.lineage.enrich import build_artifact_edges_from_dict
        from artisan.schemas.provenance.source_target_pair import SourceTargetPair

        pairs = [
            SourceTargetPair(
                source="a" * 32,
                target="b" * 32,
                source_role="data",
                target_role="processed",
                group_id=None,
            )
        ]

        edges = build_artifact_edges_from_dict(pairs, "r" * 32, {})

        assert len(edges) == 1
        assert edges[0].group_id is None
