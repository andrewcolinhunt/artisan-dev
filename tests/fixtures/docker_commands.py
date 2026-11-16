"""Literal Docker argv for supervision tests, without scientific qualification."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import ClassVar, Literal


@dataclass
class DockerCommandProbe:
    """Exercise exact external ownership against the explicitly selected test image."""

    image: str
    binds: list[tuple[str, str]] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    cancellation_scope: ClassVar[Literal["external"]] = "external"
    public_env: tuple[str, ...] = ()

    def prepare_env(self) -> dict[str, str]:
        return {
            key: value
            for key, value in os.environ.items()
            if key in {"PATH", "HOME"} or key.startswith("DOCKER_")
        }

    def wrap_command(self, command: list[str], cwd: str | None = None) -> list[str]:
        argv = ["docker", "run", "--rm", "--pull=never", "--network=none"]
        mounts = list(self.binds)
        if cwd is not None:
            argv.extend(["--workdir", cwd])
            mounts.append((cwd, cwd))
        for source, target in mounts:
            argv.extend(["--mount", f"type=bind,source={source},target={target}"])
        for key, value in self.env.items():
            argv.extend(["--env", f"{key}={value}"])
        return [*argv, self.image, *command]

    def _managed_container_command(self, wrapped: list[str]) -> list[str]:
        return ["docker", "create", *wrapped[3:]]
