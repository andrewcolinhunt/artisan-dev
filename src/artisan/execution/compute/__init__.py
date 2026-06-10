"""Compute routing: route execute() to the local target."""

from __future__ import annotations

from artisan.execution.compute.base import ComputeRouter
from artisan.execution.compute.local import LocalComputeRouter
from artisan.execution.compute.routing import create_router

__all__ = [
    "ComputeRouter",
    "LocalComputeRouter",
    "create_router",
]
