"""Operation runtime configuration schemas."""

from __future__ import annotations

from artisan.schemas.operation_config.compute import (
    Compute,
    ComputeConfig,
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
    # ToolSpec
    "ToolSpec",
    # EnvironmentSpec hierarchy
    "EnvironmentSpec",
    "LocalEnvironmentSpec",
    "DockerEnvironmentSpec",
    "ApptainerEnvironmentSpec",
    "PixiEnvironmentSpec",
    # Environments model
    "Environments",
    # ComputeConfig hierarchy
    "ComputeConfig",
    "LocalComputeConfig",
    "ModalComputeConfig",
    # Compute model
    "Compute",
    # ResourceConfig
    "ResourceConfig",
]
