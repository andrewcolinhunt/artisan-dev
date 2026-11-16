"""Measured local worker launch capability, separate from scientific software."""

from __future__ import annotations

import hashlib
import os
import platform
import subprocess
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from artisan.execution.compute.prepared import (
    CommandEnvironment,
    PreparedCommandEnvironment,
)
from artisan.execution.environments.native_gpu import verify_native_gpu
from artisan.execution.environments.native_target import verify_native_environment
from artisan.execution.environments.observed_worker import (
    ObservedCommandEnvironment,
    ObservedWorker,
)
from artisan.schemas.execution.gpu import NativeGPUAllocation
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.execution.worker_transport import RuntimeCompatibility


@dataclass(frozen=True)
class LocalLauncherIdentity:
    """Qualified native activation binaries and host runtime observations."""

    pixi_version: str
    pixi_sha256: str
    system_env_sha256: str
    libc: tuple[str, str]

    @classmethod
    def capture(cls, pixi: Path, system_env: Path) -> LocalLauncherIdentity:
        """Measure explicit local launchers without provisioning any software."""
        if platform.system() != "Linux":
            msg = "Prepared local assignments require Linux"
            raise ValueError(msg)
        for path in (pixi, system_env):
            if (
                not path.is_absolute()
                or not path.is_file()
                or not os.access(path, os.X_OK)
            ):
                msg = "Local launchers must be explicit executable paths"
                raise ValueError(msg)
        version = subprocess.check_output(
            [str(pixi), "--version"], text=True, timeout=5
        ).strip()
        if version != "pixi 0.83.0":
            msg = "Prepared local launch requires qualified Pixi 0.83.0"
            raise ValueError(msg)
        return cls(
            version, _file_digest(pixi), _file_digest(system_env), platform.libc_ver()
        )

    def descriptor(self) -> dict[str, object]:
        """Describe only measured launcher identity, without physical locators."""
        return {
            "kind": "local-native",
            "pixi_version": self.pixi_version,
            "pixi_sha256": self.pixi_sha256,
            "system_env_sha256": self.system_env_sha256,
            "libc": self.libc,
        }


@dataclass(frozen=True)
class LocalWorkerProfile:
    """Launch a qualified interpreter without carrying a scientific plan or tools."""

    worker: PreparedNativeEnvironment | ObservedWorker
    pixi: Path
    system_env: Path
    launcher: LocalLauncherIdentity | None
    native_gpu: NativeGPUAllocation | None = None

    def to_json(self) -> bytes:
        """Encode inert launch evidence; callers must protect captured environment values."""
        worker = self.worker
        return (
            _ProfilePayload(
                domain="artisan.local-worker-profile",
                schema_version=1,
                worker_kind="observed" if self.observed else "prepared",
                worker=_ObservedPayload(
                    executable=worker.executable,
                    prefix=worker.prefix,
                    base_prefix=worker.base_prefix,
                    executable_sha256=worker.executable_sha256,
                    compatibility=worker.compatibility,
                    environment=dict(worker.environment),
                )
                if isinstance(worker, ObservedWorker)
                else worker,
                pixi=self.pixi,
                system_env=self.system_env,
                launcher=_LauncherPayload(**asdict(self.launcher))
                if self.launcher is not None
                else None,
                native_gpu=self.native_gpu,
            )
            .model_dump_json()
            .encode()
        )

    @classmethod
    def from_json(cls, encoded: bytes | str) -> LocalWorkerProfile:
        """Decode only fixed framework models without importing or loading live code.

        Parsing does not qualify software or authenticate a sender. The receiver
        must verify this profile and the call's compatibility before unpickling.
        """
        payload = _ProfilePayload.model_validate_json(encoded, strict=True)
        worker = payload.worker
        return cls(
            ObservedWorker(
                worker.executable,
                worker.prefix,
                worker.base_prefix,
                worker.executable_sha256,
                worker.compatibility,
                worker.environment,
            )
            if isinstance(worker, _ObservedPayload)
            else worker,
            payload.pixi,
            payload.system_env,
            LocalLauncherIdentity(**payload.launcher.model_dump())
            if payload.launcher is not None
            else None,
            payload.native_gpu,
        )

    def __post_init__(self) -> None:
        if isinstance(self.worker, PreparedNativeEnvironment) != (
            self.launcher is not None
        ):
            msg = "Prepared worker activation requires its exact local launcher"
            raise ValueError(msg)

    @property
    def observed(self) -> bool:
        return isinstance(self.worker, ObservedWorker)

    @property
    def sensitive_values(self) -> tuple[str, ...]:
        return (
            self.worker.sensitive_values
            if isinstance(self.worker, ObservedWorker)
            else ()
        )

    def verify(self) -> None:
        """Verify local launch bytes; the trusted call verifies full scientific software."""
        if self.native_gpu is not None:
            verify_native_gpu(self.native_gpu)
        if isinstance(self.worker, ObservedWorker):
            self.worker.verify()
        else:
            if (
                LocalLauncherIdentity.capture(self.pixi, self.system_env)
                != self.launcher
            ):
                msg = "Qualified local launcher identity changed"
                raise ValueError(msg)
            verify_native_environment(self.worker)

    def environment(
        self, scratch: Path, runtime_env: Mapping[str, str]
    ) -> CommandEnvironment:
        """Use the admitted activation and GPU visibility without provisioning."""
        if isinstance(self.worker, ObservedWorker):
            return ObservedCommandEnvironment(
                self.worker, scratch, runtime_env, gpu=self.native_gpu
            )
        return PreparedCommandEnvironment(
            self.worker,
            self.pixi,
            self.system_env,
            scratch,
            runtime_env,
            gpu=self.native_gpu,
        )


def _file_digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


class _ProfileModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _LauncherPayload(_ProfileModel):
    pixi_version: Literal["pixi 0.83.0"]
    pixi_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    system_env_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    libc: tuple[str, str]


class _ObservedPayload(_ProfileModel):
    executable: Path
    prefix: Path
    base_prefix: Path
    executable_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    compatibility: RuntimeCompatibility
    environment: dict[str, str] = Field(repr=False)

    @model_validator(mode="after")
    def _absolute_paths(self) -> _ObservedPayload:
        if any(
            not path.is_absolute()
            for path in (self.executable, self.prefix, self.base_prefix)
        ):
            msg = "Observed worker locators must be absolute"
            raise ValueError(msg)
        return self


class _ProfilePayload(_ProfileModel):
    domain: Literal["artisan.local-worker-profile"]
    schema_version: Literal[1]
    worker_kind: Literal["prepared", "observed"]
    worker: PreparedNativeEnvironment | _ObservedPayload
    pixi: Path
    system_env: Path
    launcher: _LauncherPayload | None
    native_gpu: NativeGPUAllocation | None

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        if type(value) is not int:
            msg = "Local worker profile version must be an integer"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _validate_profile(self) -> _ProfilePayload:
        prepared = isinstance(self.worker, PreparedNativeEnvironment)
        if (
            prepared != (self.worker_kind == "prepared")
            or prepared != (self.launcher is not None)
            or not self.pixi.is_absolute()
            or not self.system_env.is_absolute()
            or self.worker.compatibility is None
        ):
            msg = "Local worker profile requires exact qualified activation and absolute locators"
            raise ValueError(msg)
        return self
