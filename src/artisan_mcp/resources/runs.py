"""Run resources share tool pages and actual capability continuations."""

from __future__ import annotations

import asyncio

from fastmcp import Context, FastMCP

from artisan.inspection._evidence import encode_json
from artisan.inspection.models import PageResponse, RunRow, StatusResponse
from artisan.inspection.runs import runs_payload, status_payload
from artisan_mcp._boundary import evidence_payload, require_delta_root


def register(mcp: FastMCP) -> None:
    """Register one-shot JSON run resources without subscriptions."""

    @mcp.resource("artisan://runs", mime_type="application/json")
    async def runs_list(ctx: Context) -> str:
        """Read the default run page; continue through its returned capability call."""
        payload = await asyncio.to_thread(
            evidence_payload,
            lambda: runs_payload(require_delta_root(ctx.lifespan_context)),
            PageResponse[RunRow],
        )
        return encode_json(payload).decode("utf-8")

    @mcp.resource("artisan://runs/{pipeline_run_id}", mime_type="application/json")
    async def run_detail(pipeline_run_id: str, ctx: Context) -> str:
        """Read a run rollup and default step page with capability continuation."""
        payload = await asyncio.to_thread(
            evidence_payload,
            lambda: status_payload(
                require_delta_root(ctx.lifespan_context), pipeline_run_id
            ),
            StatusResponse,
        )
        return encode_json(payload).decode("utf-8")
