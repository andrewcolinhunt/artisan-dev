"""Compute routing configuration models.

Mirrors the ``Environments`` pattern: named providers with an active
selector. Pipeline-level overrides change ``active`` via ``model_copy()``.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

ARTISAN_WORKER_IMAGE = "ghcr.io/dexterity-systems/artisan-worker:latest"


class ComputeConfig(BaseModel):
    """Base class for compute_provider provider configs.

    Mirrors the ``EnvironmentSpec`` hierarchy — each provider
    extends this base and ``create_router()`` dispatches by type.
    """


class LocalComputeConfig(ComputeConfig):
    """Local compute_provider (default, today's behavior)."""


class ModalComputeConfig(ComputeConfig):
    """Provider-specific configuration for routing execute() to Modal.

    Hardware fields (gpu / memory_gb / timeout) live on
    ``ComputeResources`` so the same hardware spec can apply to any
    compute provider; this class carries Modal-specific non-hardware
    concerns only.

    Attributes:
        image: Container image for the Modal function.
        retries: Number of retries on preemption.
        min_containers: Containers kept warm even at zero traffic.
            Set to match expected batch parallelism to eliminate
            cold starts. 0 means scale-to-zero (Modal default).
        scaledown_window: Seconds a container idles before shutdown.
            None uses Modal's default (60s). Max 1200s.
        image_registry_secret: Name of a Modal Secret (created via
            ``modal secret create ...``) carrying ``REGISTRY_USERNAME``
            and ``REGISTRY_PASSWORD`` for pulling private images. None
            (default) pulls without authentication; set when ``image``
            points at a private registry.
        local_python_sources: Top-level Python package names to overlay
            onto the Modal image via
            ``modal.Image.add_local_python_source``. The default
            ``["artisan"]`` preserves the existing behavior of shipping
            the dev-host artisan source live, shadowing whatever version
            the image pip-installed. Add your own package name(s) to
            also overlay project sources live
            (e.g. ``["artisan", "pipelines"]``). Remove ``"artisan"`` to
            use the image's pinned version instead; pass ``[]`` to
            overlay nothing. Mounted at cold-start rather than baked
            into the image — upload bandwidth scales with total source
            size.
    """

    image: str = ARTISAN_WORKER_IMAGE
    retries: int = 3
    min_containers: int = 0
    scaledown_window: int | None = None
    image_registry_secret: str | None = None
    local_python_sources: list[str] = Field(default_factory=lambda: ["artisan"])


class ComputeProvider(BaseModel):
    """Multi-provider compute_provider routing configuration.

    Follows the ``Environments`` pattern: named providers with an
    active selector. Pipeline-level overrides change ``active``
    via ``model_copy()``.

    Attributes:
        active: Name of the currently selected provider.
        local: Local compute_provider config (always available).
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
        config: ComputeConfig | None = getattr(self, self.active, None)
        if config is None:
            msg = (
                f"Compute provider '{self.active}' is not configured. "
                f"Available: {self.available()}"
            )
            raise ValueError(msg)
        return config

    def available(self) -> list[str]:
        """Return names of configured providers."""
        return [name for name in ("local", "modal") if getattr(self, name) is not None]
