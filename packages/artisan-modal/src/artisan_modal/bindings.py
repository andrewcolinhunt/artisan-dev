"""Closed preparation evidence for independent prefixes in one Modal image."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from artisan.orchestration.runner_api import (
    DerivedWheel,
    EnvironmentSource,
    LocalLauncherIdentity,
    PreparedCommandWitness,
    PreparedNativeEnvironment,
)

PIXI = Path("/usr/local/bin/artisan-pixi")
BUILDER = Path("/opt/artisan/builder")
LAYOUT_FILE = Path("/opt/artisan/modal-layout.json")


class _ClosedModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ImageEnvironment(_ClosedModel):
    """Canonical source and wheel evidence installed at one fixed image location."""

    source: EnvironmentSource
    wheels: tuple[DerivedWheel, ...] = ()

    @property
    def workspace(self) -> Path:
        return Path("/opt/artisan/environments-v2") / self.source.digest

    @property
    def prefix(self) -> Path:
        return self.workspace / ".pixi/envs" / self.source.name


class ImageLayout(_ClosedModel):
    """Build-time layout, with no scientific parameters or call-path authority."""

    schema_version: Literal[2] = 2
    worker_source: str = Field(pattern=r"^[0-9a-f]{64}$")
    environments: tuple[ImageEnvironment, ...] = Field(min_length=1)
    pixi_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    worker_entrypoint_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    witness: PreparedCommandWitness | None = None

    @model_validator(mode="after")
    def _validate_selection(self) -> ImageLayout:
        sources = [item.source.digest for item in self.environments]
        if sources != sorted(set(sources)) or self.worker_source not in sources:
            message = "Image layout requires unique sorted sources and its exact worker"
            raise ValueError(message)
        return self

    @property
    def worker(self) -> ImageEnvironment:
        return next(
            item
            for item in self.environments
            if item.source.digest == self.worker_source
        )

    def activation_config(self) -> bytes:
        """Encode derived launch inputs without another authored recipe."""
        worker = self.worker
        payload = {
            "schema_version": 2,
            "workspace": str(worker.workspace),
            "environment": worker.source.name,
            "manifest_name": worker.source.manifest_name,
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


class ModalRuntimePayload(_ClosedModel):
    """Supported SDK metadata at the platform-owned import location."""

    schema_version: Literal[2] = 2
    sdk_version: Literal["1.4.1"]
    mount: Literal["/pkg"] = "/pkg"


class ImageStartup(_ClosedModel):
    """Completed targets and compatibility observed during image preparation."""

    layout: ImageLayout
    targets: tuple[PreparedNativeEnvironment, ...]
    launcher: LocalLauncherIdentity
    runtime: ModalRuntimePayload
    activation_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _validate_targets(self) -> ImageStartup:
        if (
            self.activation_sha256
            != hashlib.sha256(self.layout.activation_config()).hexdigest()
        ):
            msg = "Startup activation differs from its canonical layout"
            raise ValueError(msg)
        if len(self.targets) != len(self.layout.environments):
            message = "Startup must measure every selected image environment"
            raise ValueError(message)
        for target, declared in zip(
            self.targets, self.layout.environments, strict=True
        ):
            if (target.source, target.wheels, target.workspace, target.prefix) != (
                declared.source,
                declared.wheels,
                declared.workspace,
                declared.prefix,
            ):
                message = "Startup target differs from its exact image layout"
                raise ValueError(message)
        if self.launcher.pixi_sha256 != self.layout.pixi_sha256:
            message = "Startup Pixi binary differs from its captured build input"
            raise ValueError(message)
        return self

    @property
    def worker(self) -> PreparedNativeEnvironment:
        return next(
            item
            for item in self.targets
            if item.source.digest == self.layout.worker_source
        )


class PreparedModalImage(_ClosedModel):
    """Internal image binding; deployment and scientific call selection are separate."""

    schema_version: Literal[2] = 2
    image_id: str = Field(pattern=r"^im-[A-Za-z0-9]+$")
    base_image: str = Field(pattern=r"^.+@sha256:[0-9a-f]{64}$")
    builder_version: Literal["2025.06"] = "2025.06"
    recipe_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    startup: ImageStartup

    @property
    def digest(self) -> str:
        encoded = json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()
        return hashlib.sha256(b"artisan.modal-image-v2\0" + encoded).hexdigest()


class ModalFunctionRuntime(_ClosedModel):
    """Actual SDK-assigned function image and allocation profile at worker entry."""

    image_id: str = Field(pattern=r"^im-[A-Za-z0-9]+$")
    milli_cpu: int = Field(gt=0, strict=True)
    memory_mb: int = Field(gt=0, strict=True)
    gpu_count: int = Field(ge=0, strict=True)
    gpu_type: str
    max_inputs: int = Field(ge=1, le=1, strict=True)
    timeout_seconds: int = Field(gt=0, strict=True)
