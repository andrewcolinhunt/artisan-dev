"""Tests for AppendableGenerator operation."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from artisan.operations.examples.appendable_generator import AppendableGenerator
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.specs.input_models import ExecuteInput, PostprocessInput
from artisan.utils.hashing import compute_content_hash


def _run(
    tmp_path: Path,
    count: int = 3,
    num_files: int = 1,
    fields_per_record: int = 2,
    seed: int | None = 42,
) -> tuple[dict, ArtifactResult]:
    """Run execute + postprocess and return both results."""
    files_dir = str(tmp_path / "files")
    os.makedirs(files_dir, exist_ok=True)
    execute_dir = str(tmp_path / "execute")
    os.makedirs(execute_dir, exist_ok=True)

    op = AppendableGenerator(
        params=AppendableGenerator.Params(
            count=count,
            num_files=num_files,
            fields_per_record=fields_per_record,
            seed=seed,
        ),
    )

    execute_input = ExecuteInput(
        execute_dir=execute_dir,
        files_dir=files_dir,
    )
    raw = op.execute(execute_input)

    post_input = PostprocessInput(
        step_number=0,
        postprocess_dir=str(tmp_path / "post"),
        memory_outputs=raw,
    )
    result = op.postprocess(post_input)
    return raw, result


class TestAppendableGenerator:
    """Tests for the AppendableGenerator operation."""

    def test_generates_correct_record_count(self, tmp_path: Path) -> None:
        raw, result = _run(tmp_path, count=5)
        assert len(raw["records"]) == 5
        assert len(result.artifacts["records"]) == 5

    def test_jsonl_format_valid(self, tmp_path: Path) -> None:
        raw, _ = _run(tmp_path, count=3)
        jsonl_path = raw["records"][0]["output_path"]
        with open(jsonl_path) as fh:
            lines = fh.read().strip().split("\n")
        assert len(lines) == 3
        for line in lines:
            record = json.loads(line)
            assert "record_id" in record
            assert "values" in record

    def test_records_have_unique_ids(self, tmp_path: Path) -> None:
        raw, _ = _run(tmp_path, count=10)
        ids = [r["record_id"] for r in raw["records"]]
        assert len(set(ids)) == 10

    def test_reproducible_with_seed(self, tmp_path: Path) -> None:
        _, r1 = _run(tmp_path / "a", seed=99)
        _, r2 = _run(tmp_path / "b", seed=99)
        hashes_1 = [a.content_hash for a in r1.artifacts["records"]]
        hashes_2 = [a.content_hash for a in r2.artifacts["records"]]
        assert hashes_1 == hashes_2

    def test_different_seeds_different_output(self, tmp_path: Path) -> None:
        _, r1 = _run(tmp_path / "a", seed=1)
        _, r2 = _run(tmp_path / "b", seed=2)
        hashes_1 = [a.content_hash for a in r1.artifacts["records"]]
        hashes_2 = [a.content_hash for a in r2.artifacts["records"]]
        assert hashes_1 != hashes_2

    def test_artifacts_share_external_path(self, tmp_path: Path) -> None:
        _, result = _run(tmp_path, count=5)
        paths = {a.external_path for a in result.artifacts["records"]}
        assert len(paths) == 1

    def test_requires_files_dir(self, tmp_path: Path) -> None:
        op = AppendableGenerator()
        ei = ExecuteInput(execute_dir=str(tmp_path), files_dir=None)
        with pytest.raises(ValueError, match="files_dir required"):
            op.execute(ei)

    def test_artifact_type_is_appendable(self, tmp_path: Path) -> None:
        _, result = _run(tmp_path)
        for art in result.artifacts["records"]:
            assert art.artifact_type == "appendable"

    def test_content_hash_correct(self, tmp_path: Path) -> None:
        raw, _ = _run(tmp_path, count=1)
        jsonl_path = raw["records"][0]["output_path"]
        with open(jsonl_path) as fh:
            line = fh.read().strip()
        expected = compute_content_hash(line.encode())
        assert raw["records"][0]["content_hash"] == expected


class TestNumFiles:
    """Tests for the num_files parameter."""

    def test_splits_records_evenly(self, tmp_path: Path) -> None:
        raw, result = _run(tmp_path, count=6, num_files=3)
        paths = {a.external_path for a in result.artifacts["records"]}
        assert len(paths) == 3
        for path in paths:
            with open(path) as fh:
                lines = fh.read().strip().split("\n")
            assert len(lines) == 2

    def test_uneven_split(self, tmp_path: Path) -> None:
        raw, result = _run(tmp_path, count=7, num_files=3)
        paths = sorted({a.external_path for a in result.artifacts["records"]})
        assert len(paths) == 3
        line_counts = []
        for p in paths:
            with open(p) as fh:
                line_counts.append(len(fh.read().strip().split("\n")))
        assert line_counts == [3, 2, 2]

    def test_default_single_file(self, tmp_path: Path) -> None:
        _, result = _run(tmp_path, count=5)
        paths = {a.external_path for a in result.artifacts["records"]}
        assert len(paths) == 1

    def test_all_records_present_across_files(self, tmp_path: Path) -> None:
        raw, _ = _run(tmp_path, count=10, num_files=3)
        all_paths = {rec["output_path"] for rec in raw["records"]}
        all_lines: list[str] = []
        for path in sorted(all_paths):
            with open(path) as fh:
                all_lines.extend(fh.read().strip().split("\n"))
        assert len(all_lines) == 10
        ids = {json.loads(line)["record_id"] for line in all_lines}
        assert len(ids) == 10
