"""Measure explicit local container launchers without installing or pulling."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from artisan.execution.environments.apptainer_gpu import capture_apptainer_payload
from artisan.schemas.execution.container_environment import (
    ContainerLauncherIdentity,
)
from artisan.schemas.execution.gpu import GPUDriverIdentity


def file_digest(path: Path) -> str:
    """Hash actual complete file bytes with bounded memory."""
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _validate_executable(path: Path) -> None:
    if not path.is_absolute() or not path.is_file() or not os.access(path, os.X_OK):
        msg = "Container launcher must be an explicit installed executable"
        raise ValueError(msg)


def validate_gpu_devices(devices: tuple[str, ...] | None) -> None:
    """Require explicit individual allocation identifiers; never expand to all GPUs."""
    if devices is not None and (
        len(set(devices)) != len(devices)
        or any(
            not item
            or not (item.isdecimal() or item.startswith("GPU-"))
            or any(character in item for character in ',"\0\n ')
            for item in devices
        )
    ):
        msg = "GPU allocation requires unique exact indices or GPU UUIDs"
        raise ValueError(msg)


def capture_gpu_driver(devices: tuple[str, ...] | None) -> GPUDriverIdentity | None:
    """Measure selected device models and driver bytes without creating a context."""
    validate_gpu_devices(devices)
    if not devices:
        return None
    executable = Path("/usr/bin/nvidia-smi")
    output = subprocess.check_output(
        [
            str(executable),
            "--query-gpu=index,uuid,name,driver_version",
            "--format=csv,noheader",
        ],
        text=True,
        timeout=10,
    )
    available = [
        tuple(part.strip() for part in line.split(",")) for line in output.splitlines()
    ]
    selected = []
    for device in devices:
        matches = [
            record for record in available if len(record) == 4 and device in record[:2]
        ]
        if len(matches) != 1 or not all(matches[0]):
            msg = "GPU allocation requires available exact device identifiers"
            raise ValueError(msg)
        selected.append(matches[0])
    if len({record[1] for record in selected}) != len(selected):
        msg = "GPU allocation names the same physical device more than once"
        raise ValueError(msg)
    versions = {record[3] for record in selected}
    if len(versions) != 1:
        msg = "GPU allocation has inconsistent driver versions"
        raise ValueError(msg)
    return GPUDriverIdentity(
        driver_version=versions.pop(),
        device_models=tuple(sorted({record[2] for record in selected})),
        libcuda_sha256=file_digest(Path("/lib/x86_64-linux-gnu/libcuda.so.1")),
        nvidia_smi_sha256=file_digest(executable),
    )


@dataclass(frozen=True)
class DockerRuntime:
    """A local daemon and measured CLI; remote engine fallback is unsupported."""

    executable: Path
    endpoint: str = "unix:///var/run/docker.sock"
    gpu_devices: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        _validate_executable(self.executable)
        validate_gpu_devices(self.gpu_devices)
        if self.executable.name != "docker" or not self.endpoint.startswith("unix:///"):
            msg = "Prepared Docker execution requires the local Unix-socket engine"
            raise ValueError(msg)

    @property
    def command(self) -> list[str]:
        return [str(self.executable), "--host", self.endpoint]

    def output(self, arguments: list[str]) -> str:
        """Run a bounded read-only engine query through the explicit connection."""
        return subprocess.check_output(
            [*self.command, *arguments], text=True, timeout=30
        )

    def identity(self) -> ContainerLauncherIdentity:
        """Require the qualified engine profile and bind its actual daemon identity."""
        version = self.output(
            ["version", "--format", "{{.Client.Version}}/{{.Server.Version}}"]
        ).strip()
        if version != "28.5.1/28.5.1":
            msg = "Prepared Docker execution requires qualified client/server 28.5.1"
            raise ValueError(msg)
        return ContainerLauncherIdentity(
            kind="docker",
            version=version,
            executable_sha256=file_digest(self.executable),
            engine_id=self.output(["info", "--format", "{{.ID}}"]).strip(),
        )

    def inspect_image(self, image: str) -> dict[str, Any]:
        """Resolve an already present image; this operation never pulls."""
        result = json.loads(self.output(["image", "inspect", image]))
        if not isinstance(result, list) or len(result) != 1:
            msg = "Prepared OCI image is unavailable; prepare it explicitly"
            raise ValueError(msg)
        item = result[0]
        if not isinstance(item, dict):
            msg = "Container engine returned malformed image metadata"
            raise ValueError(msg)
        return item


@dataclass(frozen=True)
class ApptainerRuntime:
    """One explicitly installed non-root Apptainer launcher."""

    executable: Path
    temporary_root: Path | None = None
    gpu_devices: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        _validate_executable(self.executable)
        validate_gpu_devices(self.gpu_devices)
        if self.temporary_root is not None and (
            not self.temporary_root.is_absolute()
            or self.temporary_root.resolve() != self.temporary_root
        ):
            msg = "Apptainer extraction scratch must be an exact absolute directory"
            raise ValueError(msg)

    def identity(self) -> ContainerLauncherIdentity:
        """Measure the qualified launcher before image conversion or execution."""
        version = subprocess.check_output(
            [str(self.executable), "--version"], text=True, timeout=10
        ).strip()
        if version != "apptainer version 1.5.4":
            msg = "Prepared SIF execution requires qualified Apptainer 1.5.4"
            raise ValueError(msg)
        return ContainerLauncherIdentity(
            kind="apptainer",
            version=version,
            executable_sha256=file_digest(self.executable),
            helpers=capture_apptainer_payload(
                self.executable, nvidia=bool(self.gpu_devices)
            ),
        )
