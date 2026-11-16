"""Private bindings for canonical environments packaged as OCI and derived SIF."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.execution.worker_transport import PayloadFile
from artisan.utils.hashing import canonical_json_bytes


class _ContainerModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )


class ContainerLauncherIdentity(_ContainerModel):
    """Measured executable and local engine identity, separate from image contents."""

    kind: Literal["docker", "apptainer"]
    version: str = Field(min_length=1, strict=True)
    executable_sha256: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)
    engine_id: str | None = None
    helpers: tuple[PayloadFile, ...] = ()


class PreparedOCIEnvironment(_ContainerModel):
    """An immutable image containing software installed at its final paths."""

    schema_version: Literal[2] = 2
    native: PreparedNativeEnvironment
    image_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$", strict=True)
    base_image_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$", strict=True)
    recipe_sha256: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)
    pixi_sha256: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)
    build_runtime: ContainerLauncherIdentity

    @model_validator(mode="after")
    def _validate_target(self) -> PreparedOCIEnvironment:
        if self.build_runtime.kind != "docker" or self.native.workspace != Path(
            "/opt/artisan/environment"
        ):
            msg = (
                "OCI preparation requires the Docker builder and fixed final workspace"
            )
            raise ValueError(msg)
        return self

    @property
    def digest(self) -> str:
        """Bind canonical source, immutable image and packaging inputs."""
        return _digest("artisan.prepared-oci-v2", self.model_dump(mode="json"))

    @property
    def payload_digest(self) -> str:
        """Bind measured preparation evidence into immutable OCI/SIF metadata."""
        return _digest(
            "artisan.oci-payload-v2", self.model_dump(mode="json", exclude={"image_id"})
        )


class OCIDerivation(_ContainerModel):
    """A separately identified flattened archive derived from the original OCI."""

    source_image_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$", strict=True)
    source_config_sha256: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)
    rootfs_sha256: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)
    flattened_image_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$", strict=True)
    archive_sha256: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)


class PreparedSIFEnvironment(_ContainerModel):
    """Exact SIF bytes and their complete relationship to the prepared OCI image."""

    schema_version: Literal[2] = 2
    oci: PreparedOCIEnvironment
    derivation: OCIDerivation
    sif: Path
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)
    build_runtime: ContainerLauncherIdentity

    @model_validator(mode="after")
    def _validate_derivation(self) -> PreparedSIFEnvironment:
        if (
            not self.sif.is_absolute()
            or self.derivation.source_image_id != self.oci.image_id
            or self.build_runtime.kind != "apptainer"
        ):
            msg = (
                "SIF binding must retain its absolute locator, original OCI and builder"
            )
            raise ValueError(msg)
        return self

    @property
    def digest(self) -> str:
        """Identify the actual SIF and derivation independently of its local locator."""
        return _digest(
            "artisan.prepared-sif-v2", self.model_dump(mode="json", exclude={"sif"})
        )


def _digest(domain: str, value: object) -> str:
    return hashlib.sha256(
        domain.encode() + b"\0" + canonical_json_bytes(value)
    ).hexdigest()
