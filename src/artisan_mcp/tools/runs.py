"""Run-inspection tools: list_runs, get_run_status.

Delegate to ``run_history.list_runs`` and the ``run_status`` composite;
serialize and paginate. All store reads resolve the configured Delta root
through the shared boundary, so an unset root or empty store surfaces the
shipped envelope.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastmcp import Context, FastMCP

from artisan_mcp._boundary import boundary, require_delta_root
from artisan_mcp._pagination import paginate

_READ_ONLY = {"readOnlyHint": True, "idempotentHint": True}


def register(mcp: FastMCP) -> None:
    """Attach the run-inspection tools to ``mcp``."""

    @mcp.tool(annotations=_READ_ONLY)
    async def artisan_list_runs(
        ctx: Context, limit: int = 20, cursor: str | None = None
    ) -> dict:
        """List persisted pipeline runs, most recent first, to pick one to inspect.

        Returns a page of run rollups — pipeline_run_id, step_count,
        last_status, started_at, ended_at — with has_more and next_cursor
        for paging. Use this to find a run to feed to
        artisan_get_run_status, artisan_get_step_result, or
        artisan_diagnose_run. Reads the steps Delta table; an empty or
        unconfigured store yields an empty page (or the delta_root_unset
        envelope when no root is set). Rollups only, never step detail.
        """
        config = ctx.lifespan_context["config"]

        def payload() -> dict:
            from artisan.orchestration.run_history import list_runs

            root = require_delta_root(config)
            rows = _jsonable(list_runs(root).to_dicts())
            return paginate(rows, limit, cursor)

        return boundary(payload)

    @mcp.tool(annotations=_READ_ONLY)
    async def artisan_get_run_status(ctx: Context, pipeline_run_id: str) -> dict:
        """Get one run's terminal step statuses to see how far it got and what failed.

        Returns the run rollup (last_status, step_count, started_at,
        ended_at) plus a per-step list of terminal statuses (ok, partial,
        failed, skipped, cancelled) with a produced summary and duration. A
        step that completed with some units failing under CONTINUE is
        "partial" ("failed" if every unit failed), so a failed step is not
        mislabeled "ok". "Running" is never persisted, so an in-flight step
        shows only its last recorded terminal event. Use this to watch
        progress or locate a failed step, then artisan_get_step_logs or
        artisan_diagnose_run to dig in. An unknown run yields empty steps,
        not an error.
        """
        config = ctx.lifespan_context["config"]

        def payload() -> dict:
            from artisan.orchestration.run_status import run_status

            root = require_delta_root(config)
            return run_status(root, pipeline_run_id).model_dump()

        return boundary(payload)

    @mcp.tool(annotations=_READ_ONLY)
    async def artisan_diagnose_run(ctx: Context, pipeline_run_id: str) -> dict:
        """Diagnose a run's failures in one call: what failed, why, and what to try.

        Composes the failed executions (with their error envelopes and log
        pointers), recent runs that also failed, backward provenance from the
        failed steps' artifacts, and a deterministic list of suggested next
        actions derived from the failures' recovery hints. Use this as the
        first stop when a run failed and you want the whole picture; reach
        for artisan_get_step_logs or artisan_get_provenance_graph to drill
        into a specific step or artifact afterward.
        """
        config = ctx.lifespan_context["config"]

        def payload() -> dict:
            from artisan.visualization.inspect import diagnose_run

            root = require_delta_root(config)
            return diagnose_run(root, pipeline_run_id).model_dump()

        return boundary(payload)


def _jsonable(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert datetime values to ISO strings so the rows serialize cleanly."""
    return [
        {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in row.items()}
        for row in rows
    ]
