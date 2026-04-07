"""Tests for LargeFileGenerator operation."""

from __future__ import annotations

from pathlib import Path

import pytest

from artisan.operations.examples.large_file_generator import LargeFileGenerator
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.specs.input_models import ExecuteInput, PostprocessInput
from artisan.utils.hashing import compute_content_hash


def _run(
    tmp_path: Path,
    count: int = 2,
    file_size_bytes: int = 1000,
    seed: int | None = 42,
) -> tuple[dict, ArtifactResult]:
    """Run execute + postprocess and return both results."""
    files_dir = tmp_path / "files"
    files_dir.mkdir(parents=True)
    execute_dir = tmp_path / "execute"
    execute_dir.mkdir(parents=True)

    op = LargeFileGenerator(
        params=LargeFileGenerator.Params(
            count=count,
            file_size_bytes=file_size_bytes,
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


class TestLargeFileGenerator:
    """Tests for the LargeFileGenerator operation."""

    def test_generates_correct_file_count(self, tmp_path: Path) -> None:
        raw, result = _run(tmp_path, count=3)
        assert len(raw["files"]) == 3
        assert len(result.artifacts["files"]) == 3

    def test_file_size_matches_param(self, tmp_path: Path) -> None:
        raw, _ = _run(tmp_path, count=1, file_size_bytes=500)
        file_path = Path(raw["files"][0]["path"])
        assert file_path.stat().st_size == 500

    def test_reproducible_with_seed(self, tmp_path: Path) -> None:
        _, r1 = _run(tmp_path / "a", seed=99)
        _, r2 = _run(tmp_path / "b", seed=99)
        hashes_1 = [a.content_hash for a in r1.artifacts["files"]]
        hashes_2 = [a.content_hash for a in r2.artifacts["files"]]
        assert hashes_1 == hashes_2

    def test_different_seeds_different_output(self, tmp_path: Path) -> None:
        _, r1 = _run(tmp_path / "a", seed=1)
        _, r2 = _run(tmp_path / "b", seed=2)
        hashes_1 = [a.content_hash for a in r1.artifacts["files"]]
        hashes_2 = [a.content_hash for a in r2.artifacts["files"]]
        assert hashes_1 != hashes_2

    def test_one_artifact_per_file(self, tmp_path: Path) -> None:
        raw, result = _run(tmp_path, count=3)
        paths = {a.external_path for a in result.artifacts["files"]}
        assert len(paths) == 3

    def test_requires_files_dir(self, tmp_path: Path) -> None:
        op = LargeFileGenerator()
        ei = ExecuteInput(execute_dir=tmp_path, files_dir=None)
        with pytest.raises(ValueError, match="files_dir required"):
            op.execute(ei)

    def test_artifact_type_is_large_file(self, tmp_path: Path) -> None:
        _, result = _run(tmp_path)
        for art in result.artifacts["files"]:
            assert art.artifact_type == "large_file"

    def test_content_hash_correct(self, tmp_path: Path) -> None:
        raw, _ = _run(tmp_path, count=1, file_size_bytes=100)
        file_path = Path(raw["files"][0]["path"])
        data = file_path.read_bytes()
        expected = compute_content_hash(data)
        assert raw["files"][0]["content_hash"] == expected
