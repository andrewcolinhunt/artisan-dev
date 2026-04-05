"""Tests for RecordBundleGenerator operation."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from artisan.operations.examples.record_bundle_generator import RecordBundleGenerator
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.specs.input_models import ExecuteInput, PostprocessInput
from artisan.utils.hashing import compute_content_hash


def _run(
    tmp_path: Path,
    count: int = 3,
    fields_per_record: int = 2,
    seed: int | None = 42,
) -> tuple[dict, ArtifactResult]:
    """Run execute + postprocess and return both results."""
    files_dir = tmp_path / "files"
    files_dir.mkdir(parents=True)
    execute_dir = tmp_path / "execute"
    execute_dir.mkdir(parents=True)

    op = RecordBundleGenerator(
        params=RecordBundleGenerator.Params(
            count=count,
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
        postprocess_dir=tmp_path / "post",
        memory_outputs=raw,
    )
    result = op.postprocess(post_input)
    return raw, result


class TestRecordBundleGenerator:
    """Tests for the RecordBundleGenerator operation."""

    def test_generates_correct_record_count(self, tmp_path: Path) -> None:
        raw, result = _run(tmp_path, count=5)
        assert len(raw["records"]) == 5
        assert len(result.artifacts["records"]) == 5

    def test_jsonl_format_valid(self, tmp_path: Path) -> None:
        raw, _ = _run(tmp_path, count=3)
        jsonl_path = Path(raw["output_path"])
        lines = jsonl_path.read_text().strip().split("\n")
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
        op = RecordBundleGenerator()
        ei = ExecuteInput(execute_dir=tmp_path, files_dir=None)
        with pytest.raises(ValueError, match="files_dir required"):
            op.execute(ei)

    def test_artifact_type_is_record_bundle(self, tmp_path: Path) -> None:
        _, result = _run(tmp_path)
        for art in result.artifacts["records"]:
            assert art.artifact_type == "record_bundle"

    def test_content_hash_correct(self, tmp_path: Path) -> None:
        raw, _ = _run(tmp_path, count=1)
        jsonl_path = Path(raw["output_path"])
        line = jsonl_path.read_text().strip()
        expected = compute_content_hash(line.encode())
        assert raw["records"][0]["content_hash"] == expected
