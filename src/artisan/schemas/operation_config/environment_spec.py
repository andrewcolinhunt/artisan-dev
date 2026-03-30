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

    def wrap_command(self, cmd: list[str], _cwd: Path | None = None) -> list[str]:
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


def _container_wrap(
    prefix: list[str],
    gpu_args: list[str],
    bind_flag: str,
    binds: list[tuple[Path, Path]],
    env: dict[str, str],
    image: str,
    cmd: list[str],
    cwd: Path | None,
    *,
    gpu: bool,
) -> list[str]:
    """Build a container-wrapped command line (shared by Docker and Apptainer)."""
    parts = list(prefix)
    if gpu:
        parts.extend(gpu_args)
    if cwd is not None:
        parts.extend([bind_flag, f"{cwd.parent}:{cwd.parent}"])
    for host, container in binds:
        parts.extend([bind_flag, f"{host}:{container}"])
    for k, v in env.items():
        parts.extend(["--env", f"{k}={v}"])
    parts.append(image)
    parts.extend(cmd)
    return parts


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
        return _container_wrap(
            prefix=["docker", "run", "--rm"],
            gpu_args=["--gpus", "all"],
            bind_flag="--volume",
            binds=self.binds,
            env=self.env,
            image=self.image,
            cmd=cmd,
            cwd=cwd,
            gpu=self.gpu,
        )

    def validate_environment(self) -> None:
        if not shutil.which("docker"):
            msg = "Docker is not installed or not on PATH"
            raise FileNotFoundError(msg)


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
        return _container_wrap(
            prefix=["apptainer", "exec"],
            gpu_args=["--nv"],
            bind_flag="--bind",
            binds=self.binds,
            env=self.env,
            image=str(self.image),
            cmd=cmd,
            cwd=cwd,
            gpu=self.gpu,
        )

    def validate_environment(self) -> None:
        if not shutil.which("apptainer"):
            msg = "Apptainer is not installed or not on PATH"
            raise FileNotFoundError(msg)
        if not self.image.exists():
            msg = f"Container image not found: {self.image}"
            raise FileNotFoundError(msg)


class PixiEnvironmentSpec(EnvironmentSpec):
    """Run inside a Pixi-managed environment.

    Attributes:
        pixi_environment: Pixi environment name (default: "default").
        manifest_path: Path to pixi.toml manifest file.
    """

    pixi_environment: str = "default"
    manifest_path: Path | None = None

    def wrap_command(self, cmd: list[str], _cwd: Path | None = None) -> list[str]:
        parts = ["pixi", "run", "-e", self.pixi_environment]
        if self.manifest_path:
            parts.extend(["--manifest-path", str(self.manifest_path)])
        parts.extend(cmd)
        return parts

    def validate_environment(self) -> None:
        if not shutil.which("pixi"):
            msg = "Pixi is not installed or not on PATH"
            raise FileNotFoundError(msg)
