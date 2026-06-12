"""Tests for Dockerfile resolution and image builds (subprocess mocked)."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
from fixtures.endpoint_ops import GpuTool

from artisan.execution.tool_endpoint.docker import build_image, dockerfile_for
from artisan.operations.examples import DataGenerator
from artisan.schemas.operation_config.compute import ARTISAN_WORKER_IMAGE


def _make_dockerfile(root: Path, name: str) -> Path:
    path = root / "docker" / name / "Dockerfile"
    path.parent.mkdir(parents=True)
    path.write_text("FROM scratch\n")
    return path


class TestDockerfileFor:
    @pytest.mark.parametrize(
        ("image", "name"),
        [
            ("artisan-worker", "artisan-worker"),
            ("artisan-worker:latest", "artisan-worker"),
            ("ghcr.io/dexterity-systems/artisan-worker:0.3.0", "artisan-worker"),
            ("ghcr.io/org/tool@sha256:deadbeef", "tool"),
            ("registry.example.com:5000/foo/bar", "bar"),
            ("registry.example.com:5000/foo/bar:1.2", "bar"),
            ("registry.example.com:5000/foo/bar@sha256:deadbeef", "bar"),
        ],
    )
    def test_resolves_conventional_path(self, tmp_path: Path, image: str, name: str):
        expected = _make_dockerfile(tmp_path, name)
        assert dockerfile_for(image, tmp_path) == expected

    def test_missing_dockerfile_names_expected_path(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError, match="docker/artisan-worker/Dockerfile"):
            dockerfile_for("ghcr.io/org/artisan-worker:latest", tmp_path)


class TestBuildImage:
    def test_builds_tagged_with_config_ref(self, tmp_path: Path):
        """The -t tag is exactly the config ref — no second place a tag is typed."""
        dockerfile = _make_dockerfile(tmp_path, "artisan-worker")
        with patch("artisan.execution.tool_endpoint.docker.subprocess.run") as run:
            ref = build_image(GpuTool, tmp_path)
        assert ref == ARTISAN_WORKER_IMAGE
        run.assert_called_once_with(
            [
                "docker",
                "build",
                "-f",
                str(dockerfile),
                "-t",
                ARTISAN_WORKER_IMAGE,
                str(tmp_path),
            ],
            check=True,
        )

    def test_build_failure_propagates(self, tmp_path: Path):
        _make_dockerfile(tmp_path, "artisan-worker")
        with (
            patch(
                "artisan.execution.tool_endpoint.docker.subprocess.run",
                side_effect=subprocess.CalledProcessError(1, ["docker", "build"]),
            ),
            pytest.raises(subprocess.CalledProcessError),
        ):
            build_image(GpuTool, tmp_path)

    def test_non_tool_op_raises_before_any_build(self, tmp_path: Path):
        with (
            patch("artisan.execution.tool_endpoint.docker.subprocess.run") as run,
            pytest.raises(ValueError, match="not a command op"),
        ):
            build_image(DataGenerator, tmp_path)
        run.assert_not_called()
