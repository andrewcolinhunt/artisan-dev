"""MCP resource registrations.

Each module exposes ``register(mcp)`` attaching its resources to the
``FastMCP`` app. All resources are one-shot (no subscriptions); agents
re-fetch to refresh.
"""

from __future__ import annotations

from artisan_mcp.resources import catalog

__all__ = ["catalog"]
