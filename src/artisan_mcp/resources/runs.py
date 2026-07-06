"""Run resources: the run list and one run's status. One-shot, no subscriptions."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastmcp import Context, FastMCP

from artisan_mcp._boundary import boundary, require_delta_root


def register(mcp: FastMCP) -> None:
    """Attach the run resources to ``mcp``."""

    @mcp.resource("artisan://runs", mime_type="application/json")
    async def runs_list(ctx: Context) -> dict:
        """All persisted runs as ``{"items": [...]}`` (rollup rows)."""
        config = ctx.lifespan_context["config"]

        def payload() -> dict:
            from artisan.orchestration.run_history import list_runs

            root = require_delta_root(config)
            return {"items": _jsonable(list_runs(root).to_dicts())}

        return boundary(payload)

    @mcp.resource("artisan://runs/{pipeline_run_id}", mime_type="application/json")
    async def run_detail(pipeline_run_id: str, ctx: Context) -> dict:
        """One run's ``RunStatus`` — rollup plus per-step terminal statuses."""
        config = ctx.lifespan_context["config"]

        def payload() -> dict:
            from artisan.orchestration.run_status import run_status

            root = require_delta_root(config)
            return run_status(root, pipeline_run_id).model_dump()

        return boundary(payload)


def _jsonable(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert datetime values to ISO strings so the rows serialize cleanly."""
    return [
        {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in row.items()}
        for row in rows
    ]
