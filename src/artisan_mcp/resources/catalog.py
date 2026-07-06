"""Catalog resource: one operation's full metadata by name."""

from __future__ import annotations

from fastmcp import FastMCP


def register(mcp: FastMCP) -> None:
    """Attach the operations resource to ``mcp``."""

    @mcp.resource("artisan://operations/{name}", mime_type="application/json")
    async def operation_metadata(name: str) -> dict:
        """Full ``OperationMetadata`` for the named operation.

        Args:
            name: Registered operation name.

        Returns:
            The operation's metadata dump.
        """
        from artisan.registry import describe

        return describe(name).model_dump()
