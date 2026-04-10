"""Compute routing: route execute() to local or remote targets."""

from __future__ import annotations

from artisan.execution.compute.base import ComputeRouter
from artisan.execution.compute.local import LocalComputeRouter
from artisan.execution.compute.modal import ModalComputeRouter
from artisan.execution.compute.routing import create_router
from artisan.execution.compute.validation import validate_remote_execute

__all__ = [
    "ComputeRouter",
    "LocalComputeRouter",
    "ModalComputeRouter",
    "create_router",
    "validate_remote_execute",
]
