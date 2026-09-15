"""Tests for derive_human_names()."""

from __future__ import annotations

from artisan.execution.lineage.name_derivation import derive_human_names
from artisan.schemas.artifact.data import DataArtifact


def _csv_bytes(text: str) -> bytes:
    return text.encode("utf-8")


def _make_input(original_name: str) -> DataArtifact:
    """Create a finalized input artifact with a human-readable name."""
    return DataArtifact.draft(_csv_bytes("a\n1\n"), original_name, 0).finalize()


def _make_output(original_name: str) -> DataArtifact:
    """Create a draft output artifact with a transient name."""
    return DataArtifact.draft(_csv_bytes("b\n2\n"), original_name, 1)


class TestSuffixExtraction:
    """Suffix is extracted from output name by removing the input artifact_id prefix."""

    def test_suffix_appended_to_input_name(self):
        inp = _make_input("sample_001")
        input_id = inp.artifact_id
        assert input_id is not None
        out = _make_output(f"{input_id}_scored")

        match_map = {f"{input_id}_scored": input_id}
        derive_human_names({"data": [out]}, {"data": [inp]}, match_map)

        assert out.original_name == "sample_001_scored"

    def test_empty_suffix(self):
        """When output stem exactly equals input artifact_id, suffix is empty."""
        inp = _make_input("sample_42")
        input_id = inp.artifact_id
        assert input_id is not None
        out = _make_output(input_id)

        match_map = {input_id: input_id}
        derive_human_names({"data": [out]}, {"data": [inp]}, match_map)

        assert out.original_name == "sample_42"


class TestHumanNameDerivation:
    """Full derivation with multiple artifacts."""

    def test_multiple_outputs_derived(self):
        inp_a = _make_input("sample_001")
        inp_b = _make_input("sample_002")
        id_a = inp_a.artifact_id
        id_b = inp_b.artifact_id
        assert id_a is not None
        assert id_b is not None

        out_a = _make_output(f"{id_a}_scored")
        out_b = _make_output(f"{id_b}_scored")

        match_map = {
            f"{id_a}_scored": id_a,
            f"{id_b}_scored": id_b,
        }
        derive_human_names(
            {"data": [out_a, out_b]},
            {"data": [inp_a, inp_b]},
            match_map,
        )

        assert out_a.original_name == "sample_001_scored"
        assert out_b.original_name == "sample_002_scored"


class TestUnmatchedOutputsPreserved:
    """Outputs not in the match map keep their original_name unchanged."""

    def test_unmatched_output_unchanged(self):
        inp = _make_input("sample_001")
        out = _make_output("summary_report")

        # Empty match map - no filesystem matches
        derive_human_names({"data": [out]}, {"data": [inp]}, {})

        assert out.original_name == "summary_report"

    def test_output_with_none_name_skipped(self):
        """Artifacts with original_name=None are skipped without error."""
        inp = _make_input("sample_001")
        out = _make_output("temp")
        out.original_name = None

        derive_human_names({"data": [out]}, {"data": [inp]}, {"temp": inp.artifact_id})

        assert out.original_name is None

    def test_mixed_matched_and_unmatched(self):
        """Only matched outputs get renamed; unmatched are preserved."""
        inp = _make_input("sample_001")
        input_id = inp.artifact_id
        assert input_id is not None

        matched_out = _make_output(f"{input_id}_scored")
        unmatched_out = _make_output("custom_report")

        match_map = {f"{input_id}_scored": input_id}
        derive_human_names(
            {"data": [matched_out, unmatched_out]},
            {"data": [inp]},
            match_map,
        )

        assert matched_out.original_name == "sample_001_scored"
        assert unmatched_out.original_name == "custom_report"
