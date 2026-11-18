"""Qualify an existing local interpreter without claiming a managed environment."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import ClassVar, Literal

from artisan.execution.compute.prepared import freeze_runtime_injections
from artisan.execution.environments.command_witness import WitnessInvocation
from artisan.execution.environments.native_gpu import (
    native_gpu_command,
    native_gpu_environment,
)
from artisan.execution.local_software import LOCAL_SOFTWARE_ENV, LocalSoftware
from artisan.execution.runtime_compatibility import (
    capture_runtime_compatibility,
    verify_runtime_compatibility,
)
from artisan.schemas.execution.gpu import NativeGPUAllocation
from artisan.schemas.execution.worker_transport import RuntimeCompatibility
from artisan.utils.command_supervisor import strip_internal_attachment
from artisan.utils.configuration_secrets import credential_name, credential_url
from artisan.utils.hashing import canonical_json_bytes


def _file_digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


@dataclass(frozen=True)
class ObservedWorker:
    """Observed Python and package metadata; ambient software is not cacheable."""

    executable: Path
    prefix: Path
    base_prefix: Path
    executable_sha256: str
    compatibility: RuntimeCompatibility
    environment: Mapping[str, str] = field(repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "environment", MappingProxyType(dict(self.environment))
        )

    @property
    def digest(self) -> str:
        """Bind observed execution integrity without claiming a canonical build."""
        return hashlib.sha256(
            b"artisan.observed-worker-v1\0"
            + canonical_json_bytes(
                {
                    "executable": str(self.executable),
                    "prefix": str(self.prefix),
                    "base_prefix": str(self.base_prefix),
                    "executable_sha256": self.executable_sha256,
                    "compatibility": self.compatibility.model_dump(mode="json"),
                    "environment_sha256": hashlib.sha256(
                        canonical_json_bytes(dict(self.environment))
                    ).hexdigest(),
                }
            )
        ).hexdigest()

    def verify(self) -> None:
        """Check the selected interpreter and package compatibility before execution."""
        if (
            platform.system() != "Linux"
            or Path(sys.executable).absolute() != self.executable
            or Path(sys.prefix) != self.prefix
            or Path(sys.base_prefix) != self.base_prefix
            or not self.executable.is_file()
            or _file_digest(self.executable) != self.executable_sha256
        ):
            msg = "Observed local interpreter changed"
            raise ValueError(msg)
        verify_runtime_compatibility(self.compatibility)

    @property
    def sensitive_values(self) -> tuple[str, ...]:
        """Keep observed credential values out of command and worker diagnostics."""
        return tuple(
            value
            for name, value in self.environment.items()
            if value and (credential_name(name) or credential_url(value))
        )


def observe_current_worker(names: Sequence[str]) -> ObservedWorker:
    """Measure this interpreter and selected packages without qualifying a child."""
    if platform.system() != "Linux":
        msg = "Observed local workers require Linux"
        raise ValueError(msg)
    executable = Path(sys.executable).absolute()
    if not executable.is_file() or not os.access(executable, os.X_OK):
        msg = "The current Python executable is unavailable"
        raise ValueError(msg)
    worker = ObservedWorker(
        executable,
        Path(sys.prefix),
        Path(sys.base_prefix),
        _file_digest(executable),
        capture_runtime_compatibility(names),
        {
            key: value
            for key, value in strip_internal_attachment(os.environ).items()
            if key != LOCAL_SOFTWARE_ENV
        },
    )
    worker.verify()
    return worker


def qualify_observed_worker(names: Sequence[str]) -> ObservedWorker:
    """Measure current and child interpreters without installing or building."""
    worker = observe_current_worker(names)
    executable = worker.executable
    result = subprocess.run(
        [str(executable), "-I", "-m", __name__, json.dumps(list(names))],
        env=dict(worker.environment),
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    measured = json.loads(result.stdout)
    expected = {
        "executable": str(worker.executable),
        "prefix": str(worker.prefix),
        "base_prefix": str(worker.base_prefix),
        "executable_sha256": worker.executable_sha256,
        "compatibility": worker.compatibility.model_dump(mode="json"),
    }
    if measured != expected:
        msg = "The current interpreter cannot reproduce qualified worker imports"
        raise ValueError(msg)
    worker.verify()
    return worker


@dataclass(frozen=True)
class ObservedCommandEnvironment:
    """Use existing worker software directly, with owned scratch and literal values."""

    target: ObservedWorker
    scratch: Path
    env: Mapping[str, str] = field(default_factory=dict)
    witness: WitnessInvocation | None = None
    gpu: NativeGPUAllocation | None = None
    local_software: LocalSoftware | None = None
    cancellation_scope: ClassVar[Literal["process_group"]] = "process_group"
    public_env: ClassVar[tuple[str, ...]] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "env", freeze_runtime_injections(self.env))
        native_gpu_environment(self.gpu, self.env)

    def wrap_command(self, command: list[str], cwd: str | None = None) -> list[str]:
        """Freeze an absolute executable from the captured worker search path."""
        if not command:
            msg = "Observed commands require an executable"
            raise ValueError(msg)
        directory = Path(cwd or Path.cwd()).absolute()
        executable = (
            self.target.executable if command[0] == "python" else Path(command[0])
        )
        if command[0] != "python" and "/" not in command[0]:
            search_path = os.pathsep.join(
                str(Path(part) if Path(part).is_absolute() else directory / part)
                for part in self.target.environment.get("PATH", os.defpath).split(
                    os.pathsep
                )
            )
            selected = shutil.which(command[0], path=search_path)
            if selected is None:
                msg = "Command is not available in the captured worker PATH"
                raise ValueError(msg)
            executable = Path(selected)
        elif not executable.is_absolute():
            executable = directory / executable
        if not executable.is_file() or not os.access(executable, os.X_OK):
            msg = "Observed command executable is unavailable"
            raise ValueError(msg)
        resolved = [str(executable), *command[1:]]
        if self.witness is not None:
            resolved = self.witness.wrap(resolved)
        return native_gpu_command(self.gpu, resolved, self.scratch)

    def prepare_env(self) -> dict[str, str]:
        """Preserve observed activation while replacing owned scratch and injections."""
        home, temporary = self.scratch / "home", self.scratch / "tmp"
        home.mkdir(parents=True, exist_ok=True)
        temporary.mkdir(parents=True, exist_ok=True)
        return {
            **self.target.environment,
            "HOME": str(home),
            "TMPDIR": str(temporary),
            **self.env,
            **native_gpu_environment(self.gpu, self.env),
            **(self.local_software.environment() if self.local_software else {}),
        }


def main() -> None:
    """Report this process's installed observation for read-only qualification."""
    names = json.loads(sys.argv[1])
    executable = Path(sys.executable).absolute()
    compatibility = capture_runtime_compatibility(names)
    sys.stdout.write(
        json.dumps(
            {
                "executable": str(executable),
                "prefix": sys.prefix,
                "base_prefix": sys.base_prefix,
                "executable_sha256": _file_digest(executable),
                "compatibility": compatibility.model_dump(mode="json"),
            }
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
