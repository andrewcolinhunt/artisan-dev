"""Run resources share tool pages and actual tool continuations."""

from __future__ import annotations

import asyncio

from fastmcp import Context, FastMCP

from artisan_mcp._boundary import evidence_payload, require_delta_root
from artisan_mcp._evidence import encode_json
from artisan_mcp.models import PageResponse, RunRow, StatusResponse
from artisan_mcp.tools.runs import runs_payload, status_payload


def register(mcp: FastMCP) -> None:
    """Register one-shot JSON run resources without subscriptions."""

    @mcp.resource("artisan://runs", mime_type="application/json")
    async def runs_list(ctx: Context) -> str:
        """Read the default run page; continue through its returned tool call."""
        config = ctx.lifespan_context["config"]
        payload = await asyncio.to_thread(
            evidence_payload,
            lambda: runs_payload(require_delta_root(config)),
            PageResponse[RunRow],
        )
        return encode_json(payload).decode("utf-8")

    @mcp.resource("artisan://runs/{pipeline_run_id}", mime_type="application/json")
    async def run_detail(pipeline_run_id: str, ctx: Context) -> str:
        """Read a run rollup and default step page with tool continuation."""
        config = ctx.lifespan_context["config"]
        payload = await asyncio.to_thread(
            evidence_payload,
            lambda: status_payload(require_delta_root(config), pipeline_run_id),
            StatusResponse,
        )
        return encode_json(payload).decode("utf-8")
