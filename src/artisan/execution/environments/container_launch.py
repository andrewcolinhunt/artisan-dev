"""Launch prepared tool containers through the shared command ownership scope."""

from __future__ import annotations

import os
import posixpath
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar, Literal

from artisan.execution.compute.prepared import freeze_runtime_injections
from artisan.execution.environments.command_witness import WitnessInvocation
from artisan.execution.environments.container_runtime import (
    ApptainerRuntime,
    DockerRuntime,
)
from artisan.execution.environments.container_source import CONTAINER_PIXI
from artisan.schemas.execution.container_environment import (
    PreparedOCIEnvironment,
    PreparedSIFEnvironment,
)
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment


def validate_container_bind_path(
    path: Path, runtime: DockerRuntime | ApptainerRuntime
) -> None:
    """Reject paths the qualified SIF mount profile cannot preserve literally."""
    if isinstance(runtime, ApptainerRuntime) and any(
        path.is_relative_to(root) for root in ("/dev", "/proc", "/sys")
    ):
        msg = "Prepared Apptainer binds require ordinary filesystem paths; use a work directory outside /dev, /proc and /sys"
        raise ValueError(msg)


def container_executable(target: PreparedNativeEnvironment, name: str) -> str:
    """Select a prefix path; the command witness resolves it inside the container."""
    path = Path(name)
    if not path.is_absolute():
        path = target.prefix / "bin" / path
    path = Path(posixpath.normpath(str(path)))
    if not path.is_relative_to(target.prefix) or path == target.prefix:
        msg = "Command must be inside the prepared container prefix"
        raise ValueError(msg)
    return str(path)


@dataclass(frozen=True)
class PreparedContainerCommandEnvironment:
    """One admitted container tool, exact binds and literal post-activation values."""

    target: PreparedOCIEnvironment | PreparedSIFEnvironment
    runtime: DockerRuntime | ApptainerRuntime
    sandbox: Path
    env: Mapping[str, str] = field(default_factory=dict)
    input_files: tuple[Path, ...] = ()
    witness: WitnessInvocation | None = None
    public_env: ClassVar[tuple[str, ...]] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "env", freeze_runtime_injections(self.env))
        if isinstance(self.target, PreparedOCIEnvironment) != isinstance(
            self.runtime, DockerRuntime
        ):
            msg = "Prepared target and container launcher disagree"
            raise ValueError(msg)

    @property
    def cancellation_scope(self) -> Literal["process_group", "external"]:
        return (
            "external" if isinstance(self.runtime, DockerRuntime) else "process_group"
        )

    @property
    def native(self) -> PreparedNativeEnvironment:
        return (
            self.target.native
            if isinstance(self.target, PreparedOCIEnvironment)
            else self.target.oci.native
        )

    @property
    def scratch(self) -> Path:
        return self.sandbox / "command-runtime"

    def wrap_command(self, command: list[str], cwd: str | None = None) -> list[str]:
        """Use immutable image identity with no pull, install or host executable fallback."""
        if not command or cwd is None:
            msg = "Prepared container commands require an executable and owned working directory"
            raise ValueError(msg)
        working = Path(cwd)
        if not working.is_relative_to(self.sandbox / "execute"):
            msg = "Container working directory must be inside this invocation's execute directory"
            raise ValueError(msg)
        if self.witness is None:
            msg = "Prepared container commands require a command witness"
            raise ValueError(msg)
        executable = container_executable(self.native, command[0])
        resolved = self.witness.wrap(
            [executable, *command[1:]], prefix=self.native.prefix
        )
        values = {
            "HOME": str(self.scratch / "home"),
            "TMPDIR": str(self.scratch / "tmp"),
            **self.env,
        }
        devices = self.runtime.gpu_devices
        if devices is not None:
            values["CUDA_VISIBLE_DEVICES"] = (
                ",".join(str(index) for index in range(len(devices)))
                if isinstance(self.runtime, DockerRuntime)
                else ",".join(devices)
            )
            if devices and isinstance(self.runtime, ApptainerRuntime):
                values["LD_LIBRARY_PATH"] = "/.singularity.d/libs"
        activated = [
            "-i",
            "PATH=/usr/bin:/bin",
            "LANG=C.UTF-8",
            f"HOME={self.scratch / 'home'}",
            f"TMPDIR={self.scratch / 'tmp'}",
            str(CONTAINER_PIXI),
            "run",
            "--no-config",
            "--offline",
            "--as-is",
            "--executable",
            "--manifest-path",
            str(self.native.workspace / self.native.source.manifest_name),
            "--environment",
            self.native.source.name,
            "/usr/bin/env",
            *[f"{key}={value}" for key, value in values.items()],
            *resolved,
        ]
        mounts = self._mounts(working)
        if isinstance(self.runtime, DockerRuntime):
            assert isinstance(self.target, PreparedOCIEnvironment)
            prefix = [
                str(self.runtime.executable),
                "run",
                "--rm",
                "--pull=never",
                "--read-only",
                "--network=none",
                "--init",
                "--user",
                f"{os.getuid()}:{os.getgid()}",
                "--workdir",
                str(working),
                "--entrypoint",
                "/usr/bin/env",
            ]
            for path, readonly in mounts:
                prefix.extend(
                    [
                        "--mount",
                        f"type=bind,source={path},target={path}"
                        + (",readonly" if readonly else ""),
                    ]
                )
            if devices:
                prefix.extend(["--gpus", '"device=' + ",".join(devices) + '"'])
            return [*prefix, self.target.image_id, *activated]
        assert isinstance(self.target, PreparedSIFEnvironment)
        prefix = [
            str(self.runtime.executable),
            "exec",
            "--unsquash",
            "--containall",
            "--cleanenv",
            "--no-eval",
            "--no-home",
            "--no-mount",
            "hostfs,cwd",
            "--pwd",
            str(working),
        ]
        for path, readonly in mounts:
            prefix.extend(["--bind", f"{path}:{path}:" + ("ro" if readonly else "rw")])
        if devices:
            prefix.append("--nv")
        return [*prefix, str(self.target.sif), "/usr/bin/env", *activated]

    def _mounts(self, working: Path) -> tuple[tuple[Path, bool], ...]:
        mounts = {
            self.sandbox.parent: True,
            **dict.fromkeys(self.input_files, True),
            working: False,
            self.scratch: False,
        }
        if self.witness is not None:
            mounts[self.witness.target.executable] = True
        for path in mounts:
            validate_container_bind_path(path, self.runtime)
            if (
                not path.is_absolute()
                or path.resolve() != path
                or any(character in str(path) for character in ":,\n")
                or path.is_relative_to(self.native.workspace)
                or self.native.workspace.is_relative_to(path)
            ):
                msg = "Container binds require exact absolute paths outside packaged software"
                raise ValueError(msg)
        return tuple(
            sorted(mounts.items(), key=lambda item: (len(item[0].parts), str(item[0])))
        )

    def _managed_container_command(self, wrapped: list[str]) -> list[str] | None:
        """Let the existing native owner create and retain the exact Docker container."""
        return (
            ["docker", "create", *wrapped[3:]]
            if isinstance(self.runtime, DockerRuntime)
            else None
        )

    def prepare_env(self) -> dict[str, str]:
        """Construct a clean host launcher environment and owned scratch directories."""
        for name in ("home", "tmp", "cache", "config"):
            (self.scratch / name).mkdir(parents=True, exist_ok=True)
        environment = {
            "PATH": f"{self.runtime.executable.parent}{os.pathsep}/usr/bin{os.pathsep}/bin"
            if isinstance(self.runtime, DockerRuntime)
            else os.defpath,
            "HOME": str(self.scratch / "home"),
            "TMPDIR": str(self.scratch / "tmp"),
            "LANG": "C.UTF-8",
        }
        if isinstance(self.runtime, DockerRuntime):
            environment["DOCKER_HOST"] = self.runtime.endpoint
        else:
            temporary = self.runtime.temporary_root or self.scratch / "tmp"
            temporary.mkdir(parents=True, exist_ok=True)
            environment.update(
                APPTAINER_CACHEDIR=str(self.scratch / "cache"),
                APPTAINER_TMPDIR=str(temporary),
                APPTAINER_CONFIGDIR=str(self.scratch / "config"),
            )
        return environment
