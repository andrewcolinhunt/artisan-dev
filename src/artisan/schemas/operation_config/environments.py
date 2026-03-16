"""Multi-environment configuration model.

Holds all environment configs for an operation and selects which one
is active at runtime.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from artisan.schemas.operation_config.environment_spec import (
    ApptainerEnvironmentSpec,
    DockerEnvironmentSpec,
    EnvironmentSpec,
    LocalEnvironmentSpec,
    PixiEnvironmentSpec,
)


class Environments(BaseModel):
    """Multi-environment configuration for an operation.

    Each field holds the config for one environment type. The ``active``
    field selects which one is used at runtime. Unconfigured types are None.

    Attributes:
        active: Name of the currently selected environment.
        local: Local execution config (always available by default).
        docker: Docker execution config, or None if not configured.
        apptainer: Apptainer execution config, or None if not configured.
        pixi: Pixi execution config, or None if not configured.
    """

    active: str = "local"
    local: LocalEnvironmentSpec = Field(default_factory=LocalEnvironmentSpec)
    docker: DockerEnvironmentSpec | None = None
    apptainer: ApptainerEnvironmentSpec | None = None
    pixi: PixiEnvironmentSpec | None = None

    def current(self) -> EnvironmentSpec:
        """Return the active environment spec.

        Raises:
            ValueError: If the active environment is not configured.
        """
        env = getattr(self, self.active, None)
        if env is None:
            raise ValueError(
                f"Environment '{self.active}' is not configured. "
                f"Available: {self.available()}"
            )
        return env

    def available(self) -> list[str]:
        """Return names of configured environments."""
        return [
            name
            for name in ("local", "docker", "apptainer", "pixi")
            if getattr(self, name) is not None
        ]
