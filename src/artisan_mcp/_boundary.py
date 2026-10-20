"""Capture domain failures and render them truthfully at each MCP boundary."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Never

from fastmcp.tools import ToolResult
from mcp.types import CallToolResult
from pydantic import TypeAdapter, ValidationError

from artisan.errors import ArtisanError, CommitError, ErrorCode
from artisan_mcp._evidence import (
    MAX_PAYLOAD_BYTES,
    MAX_TOOL_RESULT_BYTES,
    encode_json,
    json_ready,
    response_too_large,
)
from artisan_mcp.models import CommitErrorPayload, ErrorPayload

if TYPE_CHECKING:
    from collections.abc import Callable

    from artisan_mcp.config import ArtisanMCPConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Captured[T]:
    """The actual outcome, without guessing from arbitrary payload keys."""

    value: T | None = None
    error: ArtisanError | None = None


def capture[T](payload_fn: Callable[[], T]) -> Captured[T]:
    """Capture known query failures; internal validation remains a masked defect."""
    try:
        return Captured(value=payload_fn())
    except FileNotFoundError:
        logger.exception("MCP store read failed")
        return Captured(
            error=ArtisanError(
                ErrorCode.STORE_NOT_FOUND,
                "Artisan store not found",
                error_type="io",
                hint="Check ARTISAN_DELTA_ROOT for the intended store.",
                recovery_hint="CHECK_INPUT",
            )
        )
    except ArtisanError as exc:
        if exc.__cause__ is not None:
            logger.exception("MCP operation failed")
        return Captured(error=exc)
    except ValidationError as exc:
        logger.exception("Internal MCP reader validation failed")
        message = "Internal evidence validation failed"
        raise RuntimeError(message) from exc


def _payload(outcome: Captured[Any], success_type: Any) -> tuple[dict[str, Any], bool]:
    """Validate the selected outcome branch, then enforce its domain byte budget."""
    try:
        if outcome.error is None:
            model = TypeAdapter(success_type).validate_json(encode_json(outcome.value))
        else:
            error_type = (
                CommitErrorPayload
                if isinstance(outcome.error, CommitError)
                else ErrorPayload
            )
            model = error_type.model_validate(
                outcome.error.to_dict(include_cause=False)
            )
        payload = json_ready(model)
        if not isinstance(payload, dict):
            message = "MCP evidence must be an object"
            raise TypeError(message)
        if len(encode_json(payload)) > MAX_PAYLOAD_BYTES:
            return response_too_large(), True
        return payload, outcome.error is not None
    except (ValidationError, ValueError, TypeError) as exc:
        logger.exception("Internal MCP result validation failed")
        message = "Internal evidence validation failed"
        raise RuntimeError(message) from exc


def evidence_payload(
    payload_fn: Callable[[], Any], success_type: Any
) -> dict[str, Any]:
    """Render JSON evidence for a resource or prompt, without a tool wrapper."""
    return _payload(capture(payload_fn), success_type)[0]


def render_tool(payload_fn: Callable[[], Any], success_type: Any) -> ToolResult:
    """Build matching structured/text content with native error signaling."""
    payload, failed = _payload(capture(payload_fn), success_type)
    result = _tool_result(payload, failed)
    wire = CallToolResult(
        content=result.content,
        structuredContent=result.structured_content,
        isError=failed,
        _meta=result.meta,
    )
    if len(wire.model_dump_json(by_alias=True).encode("utf-8")) > MAX_TOOL_RESULT_BYTES:
        result = _tool_result(response_too_large(), True)
    return result


def _tool_result(payload: dict[str, Any], failed: bool) -> ToolResult:
    """Serialize one validated object exactly once for the text fallback."""
    return ToolResult(
        structured_content=payload,
        content=encode_json(payload).decode("utf-8"),
        is_error=failed,
    )


def invalid_input(field: str, message: str) -> Never:
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
