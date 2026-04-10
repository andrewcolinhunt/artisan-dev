"""Factory for creating compute routers from provider configs."""

from __future__ import annotations

from artisan.execution.compute.base import ComputeRouter
from artisan.execution.compute.local import LocalComputeRouter
from artisan.schemas.operation_config.compute import (
    ComputeConfig,
    LocalComputeConfig,
    ModalComputeConfig,
)


def create_router(config: ComputeConfig) -> ComputeRouter:
    """Create a compute router from a provider config.

    Args:
        config: Provider config from ``Compute.current()``.

    Returns:
        Router instance for the provider.

    Raises:
        ValueError: If the config type is not recognized.
    """
    if isinstance(config, LocalComputeConfig):
        return LocalComputeRouter()
    if isinstance(config, ModalComputeConfig):
        from artisan.execution.compute.modal import ModalComputeRouter

        return ModalComputeRouter(config)
    msg = f"Unknown compute config: {type(config).__name__}"
    raise ValueError(msg)
