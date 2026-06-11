"""Factory for creating execute routers from provider configs."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from artisan.errors import ArtisanError, ErrorCode
from artisan.execution.compute.base import ExecuteRouter
from artisan.execution.compute.endpoint import EndpointExecuteRouter
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.schemas.operation_config.compute import (
    ComputeConfig,
    LocalComputeConfig,
    ModalComputeConfig,
)
from artisan.schemas.operation_config.compute_resources import ComputeResources


def create_execute_router(
    config: ComputeConfig,
    operation: Any,
    compute_resources: ComputeResources | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> ExecuteRouter:
    """Create an execute router from a provider config.

    The single axis-2 decision site: ``compute_provider`` is consulted
    here and nowhere else.

    Args:
        config: Provider config from ``ComputeProvider.current()``.
        operation: The operation instance (validated against the provider).
        compute_resources: Hardware spec (gpu/memory_gb/timeout) for
            providers that consume one. Local routing ignores it.
        cancel_check: Orchestrator-cancellation probe for routers whose
            calls outlive the orchestrator's threads. None disables soft
            cancel.

    Returns:
        Router instance for the provider.

    Raises:
        ArtisanError: TOOL_ENDPOINT_MISCONFIGURED when modal is selected
            for an op that is not a command op.
        ValueError: If the config type is not recognized.
    """
    del compute_resources  # accepted for signature stability; unused so far
    if isinstance(config, LocalComputeConfig):
        return LocalExecuteRouter()
    if isinstance(config, ModalComputeConfig):
        if not operation.is_command_op():
            raise ArtisanError(
                code=ErrorCode.TOOL_ENDPOINT_MISCONFIGURED,
                message=(
                    "compute_provider='modal' requires a command op "
                    f"(ToolSpec + execute_command()); {operation.name} "
                    "declares neither"
                ),
                error_type="config",
                operation_name=operation.name,
                recovery_hint="CHECK_INPUT",
            )
        return EndpointExecuteRouter(
            cancel_check=cancel_check,
            max_concurrent_calls=config.max_concurrent_calls,
        )
    msg = f"Unknown compute provider config: {type(config).__name__}"
    raise ValueError(msg)
