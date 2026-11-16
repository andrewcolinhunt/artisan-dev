"""Operation runtime configuration schemas."""

from __future__ import annotations

from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.operation_config.tool_spec import ToolSpec

__all__ = [
    "RunnerResources",
    "ToolSpec",
]
