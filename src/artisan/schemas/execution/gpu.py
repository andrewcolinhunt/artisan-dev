"""Measured GPU software and concrete local placement kept separately."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

type GPUDriverLibraryName = Literal["libcuda.so.1", "libnvidia-ptxjitcompiler.so.1"]


class GPUDriverIdentity(BaseModel):
    """Host driver identity without physical device placement."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    driver_version: str = Field(min_length=1, strict=True)
    device_models: tuple[str, ...]
    libcuda_sha256: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)
    nvidia_smi_sha256: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)


class GPUDriverLibrary(BaseModel):
    """One driver library exposed by name, without exposing its parent directory."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    name: GPUDriverLibraryName
    path: Path
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)


class GPUDriverPayload(BaseModel):
    """Measured driver bytes without a node-local filesystem locator."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )
    name: GPUDriverLibraryName
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class NativeGPUSoftware(BaseModel):
    """Current GPU software evidence, independent of concrete allocation."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )
    device_count: int = Field(gt=0)
    driver: GPUDriverIdentity
    libraries: tuple[GPUDriverPayload, ...]
    shell_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _validate_inventory(self) -> Self:
        if (
            not self.driver.device_models
            or len(self.driver.device_models) > self.device_count
            or tuple(item.name for item in self.libraries)
            != ("libcuda.so.1", "libnvidia-ptxjitcompiler.so.1")
            or self.libraries[0].sha256 != self.driver.libcuda_sha256
        ):
            msg = "GPU software requires device models and exact measured driver libraries"
            raise ValueError(msg)
        return self


class NativeGPUAllocation(BaseModel):
    """Frozen allowed devices and measured native launch capability."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    devices: tuple[
        Annotated[
            str, Field(pattern=r"^GPU-[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$")
        ],
        ...,
    ]
    driver: GPUDriverIdentity
    libraries: tuple[GPUDriverLibrary, ...]
    shell_sha256: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)

    @model_validator(mode="after")
    def _validate_inventory(self) -> Self:
        if (
            not self.devices
            or len(set(self.devices)) != len(self.devices)
            or any(not device.startswith("GPU-") for device in self.devices)
            or tuple(item.name for item in self.libraries)
            != ("libcuda.so.1", "libnvidia-ptxjitcompiler.so.1")
            or any(not item.path.is_absolute() for item in self.libraries)
            or self.libraries[0].sha256 != self.driver.libcuda_sha256
        ):
            msg = "Native GPU allocation requires unique UUIDs and exact measured driver libraries"
            raise ValueError(msg)
        return self

    def software_descriptor(self) -> dict[str, object]:
        """Exclude concrete UUIDs and host locators from scientific software identity."""
        return self.software_profile().model_dump(mode="json")

    def software_profile(self) -> NativeGPUSoftware:
        """Return the closed software observation without UUIDs or host paths."""
        return NativeGPUSoftware(
            device_count=len(self.devices),
            driver=self.driver,
            libraries=tuple(
                GPUDriverPayload(name=item.name, sha256=item.sha256)
                for item in self.libraries
            ),
            shell_sha256=self.shell_sha256,
        )
