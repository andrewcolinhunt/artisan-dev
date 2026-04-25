"""Operation runtime configuration schemas."""

from __future__ import annotations

from artisan.schemas.operation_config.compute import (
    ARTISAN_WORKER_IMAGE,
    ComputeConfig,
    ComputeProvider,
    LocalComputeConfig,
    ModalComputeConfig,
)
from artisan.schemas.operation_config.environment_spec import (
    ApptainerEnvironmentSpec,
    DockerEnvironmentSpec,
    EnvironmentSpec,
    LocalEnvironmentSpec,
    PixiEnvironmentSpec,
)
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.operation_config.tool_spec import ToolSpec

__all__ = [
    # ComputeConfig hierarchy
    "ARTISAN_WORKER_IMAGE",
    "ApptainerEnvironmentSpec",
    "ComputeConfig",
    # Compute provider model
    "ComputeProvider",
    "DockerEnvironmentSpec",
    # EnvironmentSpec hierarchy
    "EnvironmentSpec",
    # Environments model
    "Environments",
    "LocalComputeConfig",
    "LocalEnvironmentSpec",
    "ModalComputeConfig",
    "PixiEnvironmentSpec",
    # ResourceConfig
    "ResourceConfig",
    # ToolSpec
    "ToolSpec",
]
