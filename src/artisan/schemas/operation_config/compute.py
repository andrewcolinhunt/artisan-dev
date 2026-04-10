"""Compute routing configuration models.

Mirrors the ``Environments`` pattern: named providers with an active
selector. Pipeline-level overrides change ``active`` via ``model_copy()``.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class ComputeConfig(BaseModel):
    """Base class for compute provider configs.

    Mirrors the ``EnvironmentSpec`` hierarchy — each provider
    extends this base and ``create_router()`` dispatches by type.
    """


class LocalComputeConfig(ComputeConfig):
    """Local compute (default, today's behavior)."""


class ModalComputeConfig(ComputeConfig):
    """Configuration for routing execute() to a Modal container.

    The container image must have artisan installed (transport
    functions run inside the container).

    Attributes:
        image: Container image for the Modal function.
        gpu: GPU type (e.g. "A10G", "A100", "H100").
        memory_gb: Container memory in GB.
        timeout: Per-call timeout in seconds.
        retries: Number of retries on preemption.
    """

    image: str
    gpu: str | None = None
    memory_gb: int = 8
    timeout: int = 3600
    retries: int = 3


class Compute(BaseModel):
    """Multi-provider compute routing configuration.

    Follows the ``Environments`` pattern: named providers with an
    active selector. Pipeline-level overrides change ``active``
    via ``model_copy()``.

    Attributes:
        active: Name of the currently selected provider.
        local: Local compute config (always available).
    """

    active: str = "local"
    local: LocalComputeConfig = Field(
        default_factory=LocalComputeConfig,
    )
    modal: ModalComputeConfig | None = None

    def current(self) -> ComputeConfig:
        """Return the active provider config.

        Raises:
            ValueError: If the active provider is not configured.
        """
        config = getattr(self, self.active, None)
        if config is None:
            raise ValueError(
                f"Compute provider '{self.active}' is not configured. "
                f"Available: {self.available()}"
            )
        return config

    def available(self) -> list[str]:
        """Return names of configured providers."""
        return [name for name in ("local", "modal") if getattr(self, name) is not None]
