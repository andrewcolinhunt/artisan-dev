"""Factory for creating compute provider routers from provider configs."""

from __future__ import annotations

from artisan.execution.compute.base import ComputeRouter
from artisan.execution.compute.local import LocalComputeRouter
from artisan.schemas.operation_config.compute import (
    ComputeConfig,
    LocalComputeConfig,
    ModalComputeConfig,
)
from artisan.schemas.operation_config.compute_resources import ComputeResources


def create_router(
    config: ComputeConfig,
    compute_resources: ComputeResources | None = None,
) -> ComputeRouter:
    """Create a compute provider router from a provider config.

    Args:
        config: Provider config from ``ComputeProvider.current()``.
        compute_resources: Hardware spec (gpu/memory_gb/timeout) for
            providers that consume one. Local routing ignores it.

    Returns:
        Router instance for the provider.

    Raises:
        ValueError: If the config type is not recognized.
    """
    if isinstance(config, LocalComputeConfig):
        return LocalComputeRouter()
    if isinstance(config, ModalComputeConfig):
        from artisan.execution.compute.modal import ModalComputeRouter

        return ModalComputeRouter(config, compute_resources=compute_resources)
    msg = f"Unknown compute provider config: {type(config).__name__}"
    raise ValueError(msg)
