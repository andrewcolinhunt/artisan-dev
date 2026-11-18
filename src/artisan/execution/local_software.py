"""Local command locations inherited by calls inside managed Python processes."""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path

from pydantic import BaseModel, ConfigDict, field_validator

LOCAL_SOFTWARE_ENV = "_ARTISAN_LOCAL_SOFTWARE"
_current: ContextVar[LocalSoftware | None] = ContextVar(
    "artisan_local_software", default=None
)


@dataclass(frozen=True)
class LocalSoftware:
    """Locations and literal runtime values for fresh local command binding."""

    cache_root: Path
    pixi: Path
    system_env: Path
    runtime_env: Mapping[str, str] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        from artisan.execution.compute.prepared import freeze_runtime_injections

        if any(
            not path.is_absolute()
            for path in (self.cache_root, self.pixi, self.system_env)
        ):
            msg = "Local software locations must be absolute paths"
            raise ValueError(msg)
        object.__setattr__(
            self, "runtime_env", freeze_runtime_injections(self.runtime_env)
        )

    def environment(self) -> dict[str, str]:
        """Encode locations and key names; values use the normal injection path."""
        payload = _LocalSoftwarePayload(
            cache_root=self.cache_root,
            pixi=self.pixi,
            system_env=self.system_env,
            runtime_keys=tuple(sorted(self.runtime_env)),
        )
        return {LOCAL_SOFTWARE_ENV: payload.model_dump_json()}


def local_software() -> LocalSoftware:
    """Read scoped or inherited locations before consulting standalone defaults."""
    current = _current.get()
    if current is not None:
        return current
    encoded = os.environ.get(LOCAL_SOFTWARE_ENV)
    if encoded is not None:
        payload = _LocalSoftwarePayload.model_validate_json(encoded)
        missing = set(payload.runtime_keys) - os.environ.keys()
        if missing:
            msg = "Local software handoff is missing declared runtime values"
            raise ValueError(msg)
        return LocalSoftware(
            payload.cache_root,
            payload.pixi,
            payload.system_env,
            {key: os.environ[key] for key in payload.runtime_keys},
        )
    return LocalSoftware(
        Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
        / "artisan/environments",
        Path.home() / ".pixi/bin/pixi",
        Path("/usr/bin/env"),
    )


@contextmanager
def local_software_scope(software: LocalSoftware) -> Iterator[None]:
    """Make selected local locations available through all operation phases."""
    token = _current.set(software)
    try:
        yield
    finally:
        _current.reset(token)


class _LocalSoftwarePayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    cache_root: Path
    pixi: Path
    system_env: Path
    runtime_keys: tuple[str, ...] = ()

    @field_validator("cache_root", "pixi", "system_env")
    @classmethod
    def _absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            msg = "Local software handoff requires absolute paths"
            raise ValueError(msg)
        return value
