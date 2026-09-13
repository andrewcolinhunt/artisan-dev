"""Artisan MCP server — a read-only Model Context Protocol adapter.

Ships in the core wheel; its fastmcp dependencies install only via the
``artisan[mcp]`` extra. Every tool delegates to an artisan core reader and
serializes plain data — no server-side domain logic.
"""

from __future__ import annotations

from artisan import __version__
from artisan_mcp.server import build_mcp_app

__all__ = ["__version__", "build_mcp_app"]
