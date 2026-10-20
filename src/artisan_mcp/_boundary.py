"""Render validated core evidence with native MCP errors and wire limits."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from fastmcp import Context
from fastmcp.tools import ToolResult
from mcp.types import CallToolResult
from pydantic import ValidationError

from artisan.errors import ArtisanError
from artisan.inspection._boundary import (
    error_payload,
    store_not_found,
    validate_response,
)
from artisan.inspection._evidence import encode_json, json_ready, response_too_large

MAX_TOOL_RESULT_BYTES = 128 * 1024
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Captured[T]:
    """Keep the actual outcome separate from arbitrary payload fields."""

    value: T | None = None
    error: ArtisanError | None = None


def capture[T](payload_fn: Callable[[], T]) -> Captured[T]:
    """Capture known read failures and preserve internal defects as native errors."""
    try:
        return Captured(value=payload_fn())
    except FileNotFoundError as exc:
        logger.exception("MCP store read failed")
        error = store_not_found()
        error.__cause__ = exc
        return Captured(error=error)
    except ArtisanError as exc:
        if exc.__cause__ is not None:
            logger.exception("MCP inspection failed")
        return Captured(error=exc)
    except ValidationError as exc:
        logger.exception("Internal MCP reader validation failed")
        message = "Internal evidence validation failed"
        raise RuntimeError(message) from exc


def _payload(outcome: Captured[Any], success_type: Any) -> tuple[dict[str, Any], bool]:
    try:
        if outcome.error is not None:
            return error_payload(outcome.error), True
        try:
            return json_ready(validate_response(outcome.value, success_type)), False
        except ArtisanError as exc:
            return error_payload(exc), True
    except (ValidationError, ValueError, TypeError) as exc:
        logger.exception("Internal MCP result validation failed")
        message = "Internal evidence validation failed"
        raise RuntimeError(message) from exc


def evidence_payload(
    payload_fn: Callable[[], Any], success_type: Any
) -> dict[str, Any]:
    """Render bounded JSON for transport resources and prompts."""
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
    return ToolResult(
        structured_content=payload,
        content=encode_json(payload).decode("utf-8"),
        is_error=failed,
    )


def require_delta_root(state: dict[str, Any]) -> str:
    """Strictly bootstrap the snapshotted trusted applications before store reads."""
    from artisan.inspection._boundary import require_delta_root as require_root
    from artisan.registry import bootstrap_applications

    bootstrap_applications(state["trusted_modules"], purpose="MCP inspection")
    return require_root(state["config"].delta_root)


def inspection_tool(
    capability: str, arguments: dict[str, Any], *, ctx: Context | None = None
) -> ToolResult:
    """Delegate named MCP tools to the same core request and response definitions."""
    from artisan.inspection.api import inspect_evidence, validate_request
    from artisan.inspection.catalog import definition

    item = definition(capability)

    def payload() -> Any:
        values = json_ready(arguments)
        validate_request(capability, values)
        state = ctx.lifespan_context if ctx is not None else {}
        root = require_delta_root(state) if item.requires_store else None
        return inspect_evidence(
            capability, values, delta_root=root, discovery=state.get("discovery")
        )

    return render_tool(payload, item.response_type)
