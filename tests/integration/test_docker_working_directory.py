"""Docker respects the runner cwd even when the image declares another one."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from artisan.schemas.operation_config.environment_spec import DockerEnvironmentSpec
from artisan.utils.external_tools import run_command

pytestmark = pytest.mark.integration


def test_docker_working_directory(docker_image: str, tmp_path: Path) -> None:
    """Override the prepared image WORKDIR with the runner working directory."""
    inspected = subprocess.run(
        ["docker", "image", "inspect", docker_image],
        capture_output=True,
        text=True,
        check=True,
    )
    declaration = json.loads(inspected.stdout)[0]
    assert declaration["Config"]["WorkingDir"], "Use an image with a declared WORKDIR"
    cwd = tmp_path / "execution with spaces"
    cwd.mkdir()
    assert declaration["Config"]["WorkingDir"] != str(cwd)
    # Use the inspected immutable local ID; Docker cannot pull this test image.
    environment = DockerEnvironmentSpec(image=declaration["Id"])
    run_command(environment, ["sh", "-c", "pwd > relative-result.txt"], cwd=str(cwd))
    assert (cwd / "relative-result.txt").read_text().strip() == str(cwd)
