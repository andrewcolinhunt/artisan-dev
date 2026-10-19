"""Docker respects the runner cwd even when the image declares another one."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from artisan.schemas.operation_config.environment_spec import DockerEnvironmentSpec
from artisan.utils.external_tools import run_command

pytestmark = pytest.mark.integration


def test_docker_working_directory(tmp_path: Path) -> None:
    """Run an explicitly selected local image without pulling or building it."""
    image = os.environ.get("ARTISAN_DOCKER_CWD_TEST_IMAGE")
    if not image or shutil.which("docker") is None:
        pytest.skip("Set ARTISAN_DOCKER_CWD_TEST_IMAGE to an existing image with sh")
    inspected = subprocess.run(
        ["docker", "image", "inspect", image], capture_output=True, text=True, check=True
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
