"""Catalog resource: one operation's full metadata by name."""

from __future__ import annotations

from typing import Any

from fastmcp import FastMCP

from artisan_mcp._boundary import boundary
from artisan_mcp._common import resource_fits


def register(mcp: FastMCP) -> None:
    """Attach the operations resource to ``mcp``."""

    @mcp.resource("artisan://operations/{name}", mime_type="application/json")
    async def operation_metadata(name: str) -> dict[str, Any]:
        """Full ``OperationMetadata`` for the named operation.

        Args:
            name: Registered operation name.

        Returns:
            The operation's metadata dump.
        """
        from artisan.registry import describe

        def payload() -> dict[str, Any]:
            metadata = describe(name).model_dump()
            if resource_fits(metadata):
                return metadata
            return {
                "name": name,
                "truncated": True,
                "message": (
                    "Operation metadata exceeds the resource limit; use "
                    "artisan_describe_operation when full metadata is required."
                ),
            }

        return boundary(payload)
