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


def routes_to_endpoint(operation: Any) -> bool:
    """True when this operation's execute phase ships to a Modal endpoint.

    The same axis-2 signal ``create_execute_router`` switches on, exposed
    for the input-materialization pass, which must know before the router
    is constructed whether cloud inputs travel by reference. The two reads
    cannot drift: both consult ``operation.compute_provider`` on the same
    instance, and this module owns the provider→behavior mapping —
    ``create_execute_router`` owns router *construction*, this predicate
    owns the input-delivery read.

    Args:
        operation: The operation instance.

    Returns:
        True iff ``compute_provider.active == "modal"``.
    """
    return bool(operation.compute_provider.active == "modal")


def create_execute_router(
    config: ComputeConfig,
    operation: Any,
    cancel_check: Callable[[], bool] | None = None,
) -> ExecuteRouter:
    """Create an execute router from a provider config.

    The axis-2 router-construction site: ``compute_provider`` is consulted
    here for building the router and nowhere else. The read-only sibling
    ``routes_to_endpoint`` (same module) exposes the same signal to the
    input-materialization pass.

    Args:
        config: Provider config from ``ComputeProvider.current()``.
        operation: The operation instance (validated against the provider).
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
    if isinstance(config, LocalComputeConfig):
        return LocalExecuteRouter()
    if isinstance(config, ModalComputeConfig):
        if not operation.is_command_op():
            raise ArtisanError(
                code=ErrorCode.TOOL_ENDPOINT_MISCONFIGURED,
                message=(
                    "compute_provider='modal' requires a command op — a "
                    "ToolSpec + execute_command(), or execute_as_tool=True; "
                    f"{operation.name} declares neither"
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
