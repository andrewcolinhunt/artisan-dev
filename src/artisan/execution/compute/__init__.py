"""Execute routing: route the execute phase to its compute target."""

from __future__ import annotations

from artisan.execution.compute.base import ExecuteRouter
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.compute.routing import create_execute_router

__all__ = [
    "ExecuteRouter",
    "LocalExecuteRouter",
    "create_execute_router",
]
