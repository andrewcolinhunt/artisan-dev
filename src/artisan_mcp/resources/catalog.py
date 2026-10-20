"""Operation metadata resource using the same bounded tool projection."""

from __future__ import annotations

import asyncio

from fastmcp import FastMCP

from artisan.inspection._evidence import encode_json
from artisan.inspection.models import OperationDescription
from artisan.inspection.operations import operation_payload
from artisan_mcp._boundary import evidence_payload


def register(mcp: FastMCP) -> None:
    """Register one whole-or-explicitly-unavailable operation resource."""

    @mcp.resource("artisan://operations/{name}", mime_type="application/json")
    async def operation_metadata(name: str) -> str:
        """Read complete operation metadata or bounded section guidance."""
        payload = await asyncio.to_thread(
            evidence_payload, lambda: operation_payload(name), OperationDescription
        )
        return encode_json(payload).decode("utf-8")
