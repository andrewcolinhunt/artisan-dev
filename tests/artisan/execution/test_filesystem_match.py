"""Tests for build_filesystem_match_map()."""

from __future__ import annotations

from pathlib import Path

from artisan.execution.lineage.filesystem_match import build_filesystem_match_map


class TestPrefixMatching:
    """Output file stems are matched to input artifact_ids by prefix."""

    def test_exact_match(self):
        """Output stem that equals an input artifact_id matches."""
        ids = {"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"}
        outputs = [Path("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4.csv")]

        result = build_filesystem_match_map(ids, outputs)

        assert result == {
            "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"
        }

    def test_prefix_with_suffix(self):
        """Output stem that starts with artifact_id + suffix matches."""
        input_id = "a" * 32
        ids = {input_id}
        outputs = [Path(f"{input_id}_scored.csv")]

        result = build_filesystem_match_map(ids, outputs)

        assert result == {f"{input_id}_scored": input_id}

    def test_multiple_inputs(self):
        """Each output matches its correct input."""
        id_a = "a" * 32
        id_b = "b" * 32
        ids = {id_a, id_b}
        outputs = [
            Path(f"{id_a}_processed.csv"),
            Path(f"{id_b}_processed.csv"),
        ]

        result = build_filesystem_match_map(ids, outputs)

        assert result == {
            f"{id_a}_processed": id_a,
            f"{id_b}_processed": id_b,
        }


class TestNoMatchFallback:
    """Outputs that don't match any input are absent from the map."""

    def test_unrelated_output(self):
        """Output with no matching prefix is not in the map."""
        ids = {"a" * 32}
        outputs = [Path("summary_report.csv")]

        result = build_filesystem_match_map(ids, outputs)

        assert result == {}

    def test_empty_inputs(self):
        """No materialized inputs means empty match map."""
        result = build_filesystem_match_map(set(), [Path("output.csv")])

        assert result == {}

    def test_empty_outputs(self):
        """No output files means empty match map."""
        result = build_filesystem_match_map({"a" * 32}, [])

        assert result == {}


class TestEdgeCases:
    """Edge cases for filesystem match map construction."""

    def test_no_suffix(self):
        """Output stem exactly equals input artifact_id (no suffix)."""
        input_id = "c" * 32
        ids = {input_id}
        outputs = [Path(f"{input_id}.json")]

        result = build_filesystem_match_map(ids, outputs)

        assert result == {input_id: input_id}

    def test_extension_stripped(self):
        """Compound extensions are stripped before matching."""
        input_id = "d" * 32
        ids = {input_id}
        outputs = [Path(f"{input_id}_result.tar.gz")]

        result = build_filesystem_match_map(ids, outputs)

        assert result == {f"{input_id}_result": input_id}

    def test_nested_output_path(self):
        """Output files in subdirectories still match on filename."""
        input_id = "e" * 32
        ids = {input_id}
        outputs = [Path("subdir") / f"{input_id}_out.csv"]

        result = build_filesystem_match_map(ids, outputs)

        assert result == {f"{input_id}_out": input_id}
