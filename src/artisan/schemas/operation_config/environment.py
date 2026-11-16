"""One canonical Pixi source for worker or command software."""

from __future__ import annotations

from importlib.resources.abc import Traversable
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Environment(BaseModel):
    """Declare software without reading files or provisioning an environment.

    The manifest and its sibling lock are authoritative. Activation scripts,
    local package inputs and extra files declared in the manifest are captured
    during explicit preparation. A package resource must expose its parent.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    manifest: Path | Traversable = Field(exclude=True, repr=False)
    name: str = Field(default="default", pattern=r"^[a-zA-Z0-9_-]+$", strict=True)

    @field_validator("manifest")
    @classmethod
    def _validate_manifest(cls, value: Path | Traversable) -> Path | Traversable:
        if value.name not in {"pixi.toml", "pyproject.toml"}:
            msg = "Environment requires pixi.toml or pyproject.toml"
            raise ValueError(msg)
        if isinstance(value, Path) and not value.is_absolute():
            msg = "Environment manifest paths must be explicitly absolute"
            raise ValueError(msg)
        if not isinstance(getattr(value, "parent", None), (Path, Traversable)):
            msg = "Environment package resources must expose their parent directory"
            raise ValueError(msg)
        return value

    @classmethod
    def from_pixi(
        cls, manifest: str | Path | Traversable, *, name: str = "default"
    ) -> Environment:
        """Record an explicit manifest path or package resource and selection."""
        return cls(
            manifest=Path(manifest) if isinstance(manifest, str) else manifest,
            name=name,
        )
