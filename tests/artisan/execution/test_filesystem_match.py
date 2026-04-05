"""Tests for build_filesystem_match_map() and augment_match_map_from_artifacts()."""

from __future__ import annotations

from pathlib import Path

from artisan.execution.lineage.filesystem_match import (
    augment_match_map_from_artifacts,
    build_filesystem_match_map,
)
from artisan.schemas.artifact.data import DataArtifact


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


def _make_artifact(artifact_id: str, original_name: str) -> DataArtifact:
    """Create a finalized DataArtifact for testing."""
    return DataArtifact(
        artifact_type="data",
        artifact_id=artifact_id,
        origin_step_number=1,
        content=b"a\n1\n",
        original_name=original_name,
        extension=".csv",
        size_bytes=4,
    )


class TestAugmentMatchMapFromArtifacts:
    """augment_match_map_from_artifacts adds entries from artifact names."""

    def test_adds_memory_based_output(self):
        """Artifacts with artifact_id-prefixed names get added to match map."""
        input_id = "a" * 32
        output_art = _make_artifact("x" * 32, f"{input_id}_metrics")

        match_map: dict[str, str] = {}
        augment_match_map_from_artifacts(
            match_map, {input_id}, {"metrics": [output_art]}
        )

        assert match_map == {f"{input_id}_metrics": input_id}

    def test_skips_already_matched(self):
        """Names already in the match map are not overwritten."""
        input_id = "a" * 32
        output_art = _make_artifact("x" * 32, f"{input_id}_scored")

        match_map = {f"{input_id}_scored": input_id}
        augment_match_map_from_artifacts(
            match_map, {input_id}, {"data": [output_art]}
        )

        assert match_map == {f"{input_id}_scored": input_id}

    def test_skips_unrelated_names(self):
        """Artifact names that don't start with any input ID are skipped."""
        output_art = _make_artifact("x" * 32, "summary_report")

        match_map: dict[str, str] = {}
        augment_match_map_from_artifacts(
            match_map, {"a" * 32}, {"data": [output_art]}
        )

        assert match_map == {}

    def test_handles_none_original_name(self):
        """Artifacts with None original_name are skipped."""
        output_art = _make_artifact("x" * 32, "temp")
        output_art.original_name = None

        match_map: dict[str, str] = {}
        augment_match_map_from_artifacts(
            match_map, {"a" * 32}, {"data": [output_art]}
        )

        assert match_map == {}
