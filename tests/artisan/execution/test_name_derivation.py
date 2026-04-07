"""Tests for derive_human_names()."""

from __future__ import annotations

from artisan.execution.lineage.name_derivation import derive_human_names
from artisan.schemas.artifact.data import DataArtifact


def _csv_bytes(text: str) -> bytes:
    return text.encode("utf-8")


def _make_input(artifact_id: str, original_name: str) -> DataArtifact:
    """Create a finalized input artifact with a known ID and name."""
    art = DataArtifact(
        artifact_type="data",
        artifact_id=artifact_id,
        origin_step_number=0,
        content=b"a\n1\n",
        original_name=original_name,
        extension=".csv",
        size_bytes=4,
    )
    return art


def _make_output(artifact_id: str, original_name: str) -> DataArtifact:
    """Create a finalized output artifact with a transient name."""
    art = DataArtifact(
        artifact_type="data",
        artifact_id=artifact_id,
        origin_step_number=1,
        content=b"b\n2\n",
        original_name=original_name,
        extension=".csv",
        size_bytes=4,
    )
    return art


class TestSuffixExtraction:
    """Suffix is extracted from output name by removing the input artifact_id prefix."""

    def test_suffix_appended_to_input_name(self):
        input_id = "a" * 32
        output_id = "x" * 32
        inp = _make_input(input_id, "protein_001")
        out = _make_output(output_id, f"{input_id}_scored")

        match_map = {f"{input_id}_scored": input_id}
        derive_human_names({"data": [out]}, [], {"data": [inp]}, match_map)

        assert out.original_name == "protein_001_scored"

    def test_empty_suffix(self):
        """When output stem exactly equals input artifact_id, suffix is empty."""
        input_id = "b" * 32
        output_id = "y" * 32
        inp = _make_input(input_id, "sample_42")
        out = _make_output(output_id, input_id)

        match_map = {input_id: input_id}
        derive_human_names({"data": [out]}, [], {"data": [inp]}, match_map)

        assert out.original_name == "sample_42"


class TestHumanNameDerivation:
    """Full derivation with multiple artifacts."""

    def test_multiple_outputs_derived(self):
        id_a = "a" * 32
        id_b = "b" * 32
        inp_a = _make_input(id_a, "protein_001")
        inp_b = _make_input(id_b, "protein_002")

        out_a = _make_output("x" * 32, f"{id_a}_scored")
        out_b = _make_output("y" * 32, f"{id_b}_scored")

        match_map = {
            f"{id_a}_scored": id_a,
            f"{id_b}_scored": id_b,
        }
        derive_human_names(
            {"data": [out_a, out_b]},
            [],
            {"data": [inp_a, inp_b]},
            match_map,
        )

        assert out_a.original_name == "protein_001_scored"
        assert out_b.original_name == "protein_002_scored"


class TestUnmatchedOutputsPreserved:
    """Outputs not in the match map keep their original_name unchanged."""

    def test_unmatched_output_unchanged(self):
        inp = _make_input("a" * 32, "protein_001")
        out = _make_output("z" * 32, "summary_report")

        # Empty match map - no filesystem matches
        derive_human_names({"data": [out]}, [], {"data": [inp]}, {})

        assert out.original_name == "summary_report"

    def test_output_with_none_name_skipped(self):
        """Artifacts with original_name=None are skipped without error."""
        inp = _make_input("a" * 32, "protein_001")
        out = _make_output("z" * 32, "temp")
        out.original_name = None

        derive_human_names({"data": [out]}, [], {"data": [inp]}, {"temp": "a" * 32})

        assert out.original_name is None

    def test_mixed_matched_and_unmatched(self):
        """Only matched outputs get renamed; unmatched are preserved."""
        input_id = "a" * 32
        inp = _make_input(input_id, "protein_001")

        matched_out = _make_output("x" * 32, f"{input_id}_scored")
        unmatched_out = _make_output("y" * 32, "custom_report")

        match_map = {f"{input_id}_scored": input_id}
        derive_human_names(
            {"data": [matched_out, unmatched_out]},
            [],
            {"data": [inp]},
            match_map,
        )

        assert matched_out.original_name == "protein_001_scored"
        assert unmatched_out.original_name == "custom_report"
