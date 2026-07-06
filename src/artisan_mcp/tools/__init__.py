"""MCP tool registrations, grouped by surface area.

Each module exposes ``register(mcp)`` which attaches its tools to the
``FastMCP`` app. ``build_mcp_app`` calls them in order.
"""

from __future__ import annotations

from artisan_mcp.tools import artifacts, catalog, logs, runs

__all__ = ["artifacts", "catalog", "logs", "runs"]
