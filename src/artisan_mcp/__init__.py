"""Artisan MCP server — a read-only Model Context Protocol adapter.

Ships in the core wheel; its fastmcp dependencies install only via the
``artisan[mcp]`` extra. Every tool delegates to an artisan core reader and
serializes plain data — no server-side domain logic.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from artisan import __version__

if TYPE_CHECKING:
    from fastmcp import FastMCP

    from artisan_mcp.config import ArtisanMCPConfig

_MCP_EXTRA_MESSAGE = (
    "MCP support requires the 'mcp' extra: pip install 'dexterity-artisan[mcp]'"
)


def build_mcp_app(config: ArtisanMCPConfig | None = None) -> FastMCP:
    """Build the MCP app, loading optional dependencies on demand."""
    try:
        from artisan_mcp.server import build_mcp_app as _build_mcp_app
    except ModuleNotFoundError as exc:
        if exc.name in {"fastmcp", "pydantic_settings"}:
            raise ImportError(_MCP_EXTRA_MESSAGE) from exc
        raise
    return _build_mcp_app(config)


__all__ = ["__version__", "build_mcp_app"]
