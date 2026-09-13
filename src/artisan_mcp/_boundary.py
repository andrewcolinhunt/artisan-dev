"""Shared validation and sanitized error handling for MCP calls."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from artisan_mcp.config import ArtisanMCPConfig


logger = logging.getLogger(__name__)


def boundary(payload_fn: Callable[[], Any]) -> Any:
    """Run a tool body, returning its payload or an error envelope.

    Args:
        payload_fn: Thunk that computes the tool's success payload. Any
            ``ArtisanError`` it raises (including ``delta_root_unset`` from
            ``require_delta_root``) or ``FileNotFoundError`` from a store
            reader is caught and serialized without internal cause details.

    Returns:
        The thunk's return value on success, or ``ArtisanError.to_dict()``
        on a handled failure.
    """
    from artisan.errors import ArtisanError, ErrorCode

    try:
        return payload_fn()
    except FileNotFoundError:
        logger.exception("MCP store read failed")
        err = ArtisanError(
            ErrorCode.STORE_NOT_FOUND,
            "Artisan store not found",
            error_type="io",
            hint="no Delta tables at this root — check ARTISAN_DELTA_ROOT",
            recovery_hint="CHECK_INPUT",
        )
        return err.to_dict(include_cause=False)
    except ArtisanError as exc:
        if exc.__cause__ is not None:
            logger.exception("MCP operation failed")
        return exc.to_dict(include_cause=False)


def invalid_input(field: str, message: str) -> None:
    """Raise a structured error for an invalid MCP argument.

    Args:
        field: Name of the invalid argument.
        message: Human-readable validation failure.

    Raises:
        ArtisanError: Always, with a machine-readable validation envelope.
    """
    from artisan.errors import ArtisanError, ErrorCode

    raise ArtisanError(
        ErrorCode.PARAM_TYPE_MISMATCH,
        message,
        error_type="validation",
        field=field,
        hint=f"provide a valid {field}",
        recovery_hint="CHECK_INPUT",
    )


def validate_int_range(value: int, *, field: str, minimum: int, maximum: int) -> None:
    """Require an integer argument to fall within an inclusive range.

    Args:
        value: Value to validate.
        field: Argument name for the error envelope.
        minimum: Smallest accepted value.
        maximum: Largest accepted value.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        invalid_input(field, f"{field} must be an integer")
    if not minimum <= value <= maximum:
        invalid_input(field, f"{field} must be between {minimum} and {maximum}")


def require_delta_root(config: ArtisanMCPConfig) -> str:
    """Resolve the configured Delta root or raise the ``delta_root_unset`` envelope.

    Args:
        config: The server configuration carrying ``delta_root``.

    Returns:
        The resolved Delta root path.

    Raises:
        ArtisanError: With ``code=delta_root_unset`` when no root is set.
    """
    from artisan.errors import ArtisanError, ErrorCode

    if not config.delta_root:
        raise ArtisanError(
            ErrorCode.DELTA_ROOT_UNSET,
            "no Delta root configured",
            error_type="config",
            hint="set ARTISAN_DELTA_ROOT or pass --delta-root",
            recovery_hint="CHECK_INPUT",
        )
    return config.delta_root
