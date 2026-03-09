"""Execution environment specifications.

Hierarchy of environment types that wrap commands for different runtimes.
Each subclass implements ``wrap_command()``, ``prepare_env()``, and
``validate_environment()`` — no match statement needed.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from pydantic import BaseModel, Field


class EnvironmentSpec(BaseModel):
    """Base class for execution environment configuration.

    Attributes:
        env: Extra environment variables merged into the subprocess env.
    """

    env: dict[str, str] = Field(default_factory=dict)

    def wrap_command(self, cmd: list[str], cwd: Path | None = None) -> list[str]:
        """Wrap a command for this environment. Base returns unchanged."""
        return cmd

    def prepare_env(self) -> dict[str, str] | None:
        """Return env dict for subprocess, or None to inherit."""
        if self.env:
            env = os.environ.copy()
            env.update(self.env)
            return env
        return None

    def validate_environment(self) -> None:
        """Check that the environment is available. Override in subclasses."""


class LocalEnvironmentSpec(EnvironmentSpec):
    """Run directly on the host, optionally inside a virtualenv.

    Attributes:
        venv_path: Path to a virtualenv root. When set, the venv's bin/
            directory is prepended to PATH.
    """

    venv_path: Path | None = None

    def prepare_env(self) -> dict[str, str] | None:
        if self.venv_path:
            env = os.environ.copy()
            env["PATH"] = f"{self.venv_path / 'bin'}:{env.get('PATH', '')}"
            env["VIRTUAL_ENV"] = str(self.venv_path)
            env.update(self.env)
            return env
        return super().prepare_env()


class DockerEnvironmentSpec(EnvironmentSpec):
    """Run inside a Docker container.

    Attributes:
        image: Docker image reference (e.g. "biocontainers/samtools:1.17").
        gpu: Whether to pass --gpus all.
        binds: List of (host_path, container_path) volume mounts.
    """

    image: str
    gpu: bool = False
    binds: list[tuple[Path, Path]] = Field(default_factory=list)

    def wrap_command(self, cmd: list[str], cwd: Path | None = None) -> list[str]:
        parts = ["docker", "run", "--rm"]
        if self.gpu:
            parts.extend(["--gpus", "all"])
        if cwd is not None:
            parts.extend(["--volume", f"{cwd.parent}:{cwd.parent}"])
        for host, container in self.binds:
            parts.extend(["--volume", f"{host}:{container}"])
        for k, v in self.env.items():
            parts.extend(["--env", f"{k}={v}"])
        parts.append(self.image)
        parts.extend(cmd)
        return parts

    def validate_environment(self) -> None:
        if not shutil.which("docker"):
            raise FileNotFoundError("Docker is not installed or not on PATH")


class ApptainerEnvironmentSpec(EnvironmentSpec):
    """Run inside an Apptainer (Singularity) container.

    Attributes:
        image: Filesystem path to .sif container image.
        gpu: Whether to pass --nv for GPU access.
        binds: List of (host_path, container_path) bind mounts.
    """

    image: Path
    gpu: bool = False
    binds: list[tuple[Path, Path]] = Field(default_factory=list)

    def wrap_command(self, cmd: list[str], cwd: Path | None = None) -> list[str]:
        parts = ["apptainer", "exec"]
        if self.gpu:
            parts.append("--nv")
        if cwd is not None:
            parts.extend(["--bind", f"{cwd.parent}:{cwd.parent}"])
        for host, container in self.binds:
            parts.extend(["--bind", f"{host}:{container}"])
        for k, v in self.env.items():
            parts.extend(["--env", f"{k}={v}"])
        parts.append(str(self.image))
        parts.extend(cmd)
        return parts

    def validate_environment(self) -> None:
        if not shutil.which("apptainer"):
            raise FileNotFoundError("Apptainer is not installed or not on PATH")
        if not self.image.exists():
            raise FileNotFoundError(f"Container image not found: {self.image}")


class PixiEnvironmentSpec(EnvironmentSpec):
    """Run inside a Pixi-managed environment.

    Attributes:
        pixi_environment: Pixi environment name (default: "default").
        manifest_path: Path to pixi.toml manifest file.
    """

    pixi_environment: str = "default"
    manifest_path: Path | None = None

    def wrap_command(self, cmd: list[str], cwd: Path | None = None) -> list[str]:
        parts = ["pixi", "run", "-e", self.pixi_environment]
        if self.manifest_path:
            parts.extend(["--manifest-path", str(self.manifest_path)])
        parts.extend(cmd)
        return parts

    def validate_environment(self) -> None:
        if not shutil.which("pixi"):
            raise FileNotFoundError("Pixi is not installed or not on PATH")
