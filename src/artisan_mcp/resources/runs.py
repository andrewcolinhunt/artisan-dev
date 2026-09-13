"""Run resources: the run list and one run's status. One-shot, no subscriptions."""

from __future__ import annotations

from fastmcp import Context, FastMCP

from artisan_mcp._boundary import boundary, require_delta_root
from artisan_mcp._common import MAX_RESOURCE_ITEMS, jsonable_rows, resource_fits
from artisan_mcp._pagination import MAX_PAGE_SIZE, paginate


def register(mcp: FastMCP) -> None:
    """Attach the run resources to ``mcp``."""

    @mcp.resource("artisan://runs", mime_type="application/json")
    async def runs_list(ctx: Context) -> dict:
        """The 100 most recent persisted run rollups with page metadata."""
        config = ctx.lifespan_context["config"]

        def payload() -> dict:
            from artisan.orchestration.run_history import list_runs

            root = require_delta_root(config)
            rows = jsonable_rows(list_runs(root).to_dicts())
            return paginate(rows, MAX_PAGE_SIZE, None)

        return boundary(payload)

    @mcp.resource("artisan://runs/{pipeline_run_id}", mime_type="application/json")
    async def run_detail(pipeline_run_id: str, ctx: Context) -> dict:
        """One run's ``RunStatus`` — rollup plus per-step terminal statuses."""
        config = ctx.lifespan_context["config"]

        def payload() -> dict:
            from artisan.orchestration.run_status import run_status

            root = require_delta_root(config)
            result = run_status(root, pipeline_run_id).model_dump()
            steps = result["steps"]
            result["steps"] = steps[:MAX_RESOURCE_ITEMS]
            result["steps_truncated"] = len(steps) > MAX_RESOURCE_ITEMS
            if resource_fits(result):
                return result
            return {
                "pipeline_run_id": pipeline_run_id,
                "truncated": True,
                "message": (
                    "Run status exceeds the resource limit; use "
                    "artisan_get_run_status for full detail."
                ),
            }

        return boundary(payload)
