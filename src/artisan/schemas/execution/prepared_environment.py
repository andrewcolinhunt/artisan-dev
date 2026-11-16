"""Private content bindings for explicitly prepared native environments."""

from __future__ import annotations

import hashlib
from pathlib import Path, PurePosixPath
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from artisan.schemas.execution.worker_transport import PayloadFile, RuntimeCompatibility
from artisan.utils.hashing import canonical_json_bytes

NATIVE_WHEEL_DIRECTORY = ".artisan-wheels"
NATIVE_BINDING_FILENAME = ".artisan-native.json"
NATIVE_PENDING_FILENAME = ".artisan-native.pending.json"


class _PreparedModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EnvironmentFile(PayloadFile):
    """Captured source bytes and executable permissions."""

    executable: int = Field(default=0, ge=0, le=0o111, strict=True)

    @model_validator(mode="after")
    def _validate_executable(self) -> EnvironmentFile:
        if self.executable & ~0o111:
            msg = "Environment executable permissions contain non-executable bits"
            raise ValueError(msg)
        return self


class SourcePayload(_PreparedModel):
    """One static source file's logical installed wheel path."""

    path: str
    source: str


class LocalPackageSource(_PreparedModel):
    """Qualified selected files and a conservative bounded membership check."""

    root: str
    name: str = Field(min_length=1, strict=True)
    version: str = Field(min_length=1, strict=True)
    files: tuple[str, ...] = Field(min_length=1)
    payloads: tuple[SourcePayload, ...] = Field(min_length=1)
    scan_roots: tuple[str, ...] = Field(min_length=1)
    membership_sha256: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)

    @model_validator(mode="after")
    def _validate_paths(self) -> LocalPackageSource:
        paths = [*self.files, *self.scan_roots]
        paths.extend(
            path for payload in self.payloads for path in (payload.path, payload.source)
        )
        if self.root != ".":
            paths.append(self.root)
        for value in paths:
            path = PurePosixPath(value)
            if (
                path.is_absolute()
                or ".." in path.parts
                or str(path) != value
                or value == "."
                or "\\" in value
                or ":" in value
            ):
                msg = "Local package paths must remain canonical and relative"
                raise ValueError(msg)
        if self.files != tuple(sorted(set(self.files))) or self.scan_roots != tuple(
            sorted(set(self.scan_roots))
        ):
            msg = (
                "Local source file and scan-root inventories must be sorted and unique"
            )
            raise ValueError(msg)
        targets = [payload.path for payload in self.payloads]
        if targets != sorted(set(targets)) or any(
            payload.source not in self.files for payload in self.payloads
        ):
            msg = "Local source payloads must uniquely map captured files"
            raise ValueError(msg)
        return self


class EnvironmentSource(_PreparedModel):
    """Canonical authored bytes and their bounded referenced file closure."""

    manifest_name: Literal["pixi.toml", "pyproject.toml"]
    name: str = Field(pattern=r"^[a-zA-Z0-9_-]+$", strict=True)
    files: tuple[EnvironmentFile, ...] = Field(min_length=2)
    packages: tuple[LocalPackageSource, ...] = ()

    @model_validator(mode="after")
    def _validate_files(self) -> EnvironmentSource:
        paths = [item.path for item in self.files]
        if any("__pycache__" in PurePosixPath(path).parts for path in paths):
            msg = "Authored environment inputs cannot include interpreter caches"
            raise ValueError(msg)
        if paths != sorted(set(paths)):
            msg = "Source files must be unique and sorted"
            raise ValueError(msg)
        if self.manifest_name not in paths or "pixi.lock" not in paths:
            msg = "Environment source must include its canonical manifest and lock"
            raise ValueError(msg)
        roots = [package.root for package in self.packages]
        if roots != sorted(set(roots)):
            msg = "Local package roots must be unique and sorted"
            raise ValueError(msg)
        for package in self.packages:
            if any(
                str(PurePosixPath(package.root) / name) not in paths
                for name in package.files
            ):
                msg = (
                    "Local package files must belong to the captured environment source"
                )
                raise ValueError(msg)
        return self

    @property
    def digest(self) -> str:
        """Identify the selected source without physical source paths."""
        return _digest("artisan.environment-source-v2", self.model_dump(mode="json"))


class DerivedWheel(_PreparedModel):
    """A local wheel bound to captured source, declared builder recipe and bytes."""

    name: str = Field(min_length=1, strict=True)
    version: str = Field(min_length=1, strict=True)
    source_root: str
    source_digest: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)
    builder_digest: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)
    filename: str = Field(min_length=1, strict=True)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)

    @field_validator("filename")
    @classmethod
    def _validate_filename(cls, value: str) -> str:
        if (
            PurePosixPath(value).name != value
            or "\\" in value
            or not value.endswith(".whl")
        ):
            msg = "Derived wheel filename must name one ordinary wheel file"
            raise ValueError(msg)
        return value


class PreparedNativeEnvironment(_PreparedModel):
    """Completed recipe and build identity with managed installation locations."""

    schema_version: Literal[2] = 2
    source: EnvironmentSource
    platform: str = Field(min_length=1, strict=True)
    pixi_version: Literal["0.83.0"] = "0.83.0"
    wheels: tuple[DerivedWheel, ...] = ()
    compatibility: RuntimeCompatibility | None = None
    workspace: Path
    prefix: Path

    @model_validator(mode="after")
    def _validate_locations(self) -> PreparedNativeEnvironment:
        if not self.workspace.is_absolute() or not self.prefix.is_absolute():
            msg = "Prepared native paths must be absolute"
            raise ValueError(msg)
        if self.prefix != self.workspace / ".pixi" / "envs" / self.source.name:
            msg = "Prepared native prefix must be installed at its final workspace path"
            raise ValueError(msg)
        packages = {package.root: package for package in self.source.packages}
        roots = [wheel.source_root for wheel in self.wheels]
        if len(roots) != len(set(roots)) or set(roots) != packages.keys():
            msg = "Derived wheels must uniquely bind captured local package roots"
            raise ValueError(msg)
        for wheel in self.wheels:
            package = packages[wheel.source_root]
            if wheel.name != package.name or wheel.version != package.version:
                msg = "Derived wheel identity differs from captured package metadata"
                raise ValueError(msg)
        return self

    @property
    def digest(self) -> str:
        """Bind preparation inputs independently of runtime observations.

        Assignment software identity also includes its selected transport
        compatibility and launcher/provider identity. This digest alone is not
        a cache equivalence claim across execution targets.
        """
        return _digest(
            "artisan.prepared-native-v2",
            self.model_dump(
                mode="json", exclude={"workspace", "prefix", "compatibility"}
            ),
        )


def _digest(domain: str, descriptor: object) -> str:
    return hashlib.sha256(
        domain.encode() + b"\0" + canonical_json_bytes(descriptor)
    ).hexdigest()
