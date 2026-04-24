"""Tests for LargeFileGenerator operation."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.execution.executors.creator import run_creator_lifecycle
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.examples.large_file_generator import LargeFileGenerator
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.specs.input_models import ExecuteInput, PostprocessInput
from artisan.utils.hashing import compute_content_hash


def _run(
    tmp_path: Path,
    count: int = 2,
    file_size_bytes: int = 1000,
    seed: int | None = 42,
) -> tuple[dict, ArtifactResult]:
    """Run execute + postprocess and return both results."""
    files_dir = str(tmp_path / "files")
    os.makedirs(files_dir, exist_ok=True)
    execute_dir = str(tmp_path / "execute")
    os.makedirs(execute_dir, exist_ok=True)

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
        postprocess_dir=str(tmp_path / "post"),
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
        file_path = raw["files"][0]["path"]
        assert os.path.getsize(file_path) == 500

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
        ei = ExecuteInput(execute_dir=str(tmp_path), files_dir=None)
        with pytest.raises(ValueError, match="files_dir required"):
            op.execute(ei)

    def test_artifact_type_is_large_file(self, tmp_path: Path) -> None:
        _, result = _run(tmp_path)
        for art in result.artifacts["files"]:
            assert art.artifact_type == "large_file"

    def test_content_hash_correct(self, tmp_path: Path) -> None:
        raw, _ = _run(tmp_path, count=1, file_size_bytes=100)
        file_path = raw["files"][0]["path"]
        with open(file_path, "rb") as fh:
            data = fh.read()
        expected = compute_content_hash(data)
        assert raw["files"][0]["content_hash"] == expected


# ---------------------------------------------------------------------------
# Parametrized [local, s3] lifecycle smoke — exercises the framework
# upload step from PR 7 (files-root-cloud-uploads).
# ---------------------------------------------------------------------------


@pytest.fixture(params=["local", "s3"])
def backend_env(request, tmp_path, s3_fs):
    """Yield ``(fs, storage, files_root, working_root)`` for both backends.

    Inline here because ``tests/artisan/operations/examples/`` does not
    share the storage-layer ``backend_fs`` fixture. The s3 param skips
    cleanly when MinIO is unavailable via ``s3_fs``.
    """
    working = tmp_path / "working"
    working.mkdir()
    if request.param == "local":
        files_root = tmp_path / "files_root"
        files_root.mkdir()
        return (
            LocalFileSystem(),
            StorageConfig(protocol="file"),
            str(files_root),
            str(working),
        )
    fs, storage, uri_prefix = s3_fs
    return fs, storage, f"{uri_prefix}/files", str(working)


class TestLargeFileGeneratorLifecycle:
    """End-to-end creator lifecycle for LargeFileGenerator on both backends.

    Regression guard for the files_root upload path: without
    ``_upload_files_to_root`` the cloud run would leak a literal-colon
    directory under the executor and store a local path in
    ``external_path``.
    """

    def test_external_path_resolves_on_parametrized_backend(
        self, backend_env, tmp_path: Path
    ) -> None:
        fs, storage, files_root, working_root = backend_env

        env = RuntimeEnvironment(
            delta_root=str(tmp_path / "delta"),
            working_root=working_root,
            staging_root=str(tmp_path / "staging"),
            files_root=files_root,
            storage=storage,
        )
        unit = ExecutionUnit(
            operation=LargeFileGenerator(
                params=LargeFileGenerator.Params(
                    count=2, file_size_bytes=256, seed=0
                )
            ),
            inputs={},
            execution_spec_id="lfg_smoke_" + "0" * 22,
            step_number=3,
        )

        result = run_creator_lifecycle(unit, env)

        artifacts = result.artifacts["files"]
        assert len(artifacts) == 2
        for art in artifacts:
            assert art.external_path is not None
            assert art.external_path.startswith(files_root), (
                f"external_path {art.external_path!r} not under {files_root!r}"
            )
            assert fs.exists(art.external_path), (
                f"external_path {art.external_path!r} does not resolve on {fs}"
            )
