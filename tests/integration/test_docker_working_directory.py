"""Exact managed Docker commands override the test image's declared WORKDIR."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
from fixtures.docker_commands import DockerCommandProbe

from artisan.utils import external_tools
from artisan.utils.external_tools import run_command

pytestmark = pytest.mark.integration


def test_docker_working_directory(docker_image: str, tmp_path: Path) -> None:
    inspected = subprocess.run(
        ["docker", "image", "inspect", docker_image],
        capture_output=True,
        text=True,
        check=True,
    )
    declaration = json.loads(inspected.stdout)[0]
    assert declaration["Config"]["WorkingDir"]
    cwd = tmp_path / "execution with spaces"
    cwd.mkdir()
    assert declaration["Config"]["WorkingDir"] != str(cwd)
    environment = DockerCommandProbe(image=declaration["Id"])
    with patch.object(
        external_tools, "_run_managed", wraps=external_tools._run_managed
    ) as launch:
        run_command(
            environment, ["sh", "-c", "pwd > relative-result.txt"], cwd=str(cwd)
        )
    admission = launch.call_args.kwargs["docker_create"]
    assert admission[:2] == ["docker", "create"]
    assert "--pull=never" in admission
    assert (cwd / "relative-result.txt").read_text().strip() == str(cwd)
