"""Bounded run status and diagnosis, preserving authoritative reader semantics."""

from __future__ import annotations

from typing import Any, Literal

from fastmcp import Context, FastMCP
from fastmcp.tools import ToolResult
from pydantic import StrictInt

from artisan_mcp._boundary import (
    invalid_input,
    render_tool,
    require_delta_root,
    validate_int_range,
)
from artisan_mcp._common import READ_ONLY
from artisan_mcp._evidence import MAX_PAYLOAD_BYTES, ensure_fits, json_ready
from artisan_mcp._pagination import paginate, reject_cursor
from artisan_mcp.models import (
    DiagnosisResult,
    PageResponse,
    RunRow,
    StatusResponse,
    output_schema,
)

DiagnosisView = Literal["summary", "failures", "similar_runs", "upstream_edges"]


def runs_payload(
    root: str,
    limit: StrictInt = 20,
    cursor: str | None = None,
    *,
    budget: int = MAX_PAYLOAD_BYTES,
) -> dict[str, Any]:
    """Return deterministically ordered run rollups."""
    from artisan.orchestration.run_history import list_runs

    validate_int_range(limit, field="limit", minimum=1, maximum=100)
    rows = list_runs(root).to_dicts()
    rows.sort(key=lambda row: row["pipeline_run_id"])
    rows.sort(
        key=lambda row: (row["started_at"] is not None, row["started_at"]), reverse=True
    )
    return paginate(
        rows,
        limit,
        cursor,
        tool="artisan_list_runs",
        arguments={},
        scope=root,
        budget=budget,
    )


def status_payload(
    root: str,
    pipeline_run_id: str,
    limit: StrictInt = 50,
    cursor: str | None = None,
    *,
    budget: int = MAX_PAYLOAD_BYTES,
) -> dict[str, Any]:
    """Page current steps while keeping the full run rollup in every response."""
    from artisan.orchestration.run_status import run_status

    validate_int_range(limit, field="limit", minimum=1, maximum=100)
    header = json_ready(run_status(root, pipeline_run_id))
    steps = sorted(header.pop("steps"), key=lambda step: step["step_number"])
    return paginate(
        steps,
        limit,
        cursor,
        tool="artisan_get_run_status",
        arguments={"pipeline_run_id": pipeline_run_id},
        scope=root,
        header=header,
        page_key="steps",
        budget=budget,
    )


def diagnosis_payload(
    root: str,
    pipeline_run_id: str,
    view: DiagnosisView = "summary",
    detail: Literal["full", "identity"] = "full",
    limit: StrictInt = 50,
    cursor: str | None = None,
    *,
    budget: int = MAX_PAYLOAD_BYTES,
) -> dict[str, Any]:
    """Select one diagnosis collection without concealing its source window."""
    from artisan.visualization.inspect import diagnose_run

    validate_int_range(limit, field="limit", minimum=1, maximum=100)
    if detail != "full" and view != "failures":
        invalid_input(
            "detail", "Identity detail is available only in the failures view"
        )
    diagnosis = json_ready(diagnose_run(root, pipeline_run_id))
    failures = diagnosis["failed_steps"]
    header = {
        "pipeline_run_id": pipeline_run_id,
        "last_status": diagnosis["last_status"],
        "failed_execution_count": len(failures),
        "affected_step_count": len({row["step"] for row in failures}),
        "similar_run_count": len(diagnosis["similar_runs"]),
        "matching_failed_run_count": diagnosis["matching_failed_run_count"],
        "similar_run_limit": diagnosis["similar_run_limit"],
        "upstream_edge_count": len(diagnosis["upstream_edges"]),
        "upstream_scope": diagnosis["upstream_scope"],
        "view": view,
    }
    if view == "summary":
        reject_cursor(cursor)
        return ensure_fits(
            {
                "projection_version": "1",
                "follow_up": None,
                **header,
                "suggested_actions": diagnosis["suggested_actions"],
                "detail_views": ["failures", "similar_runs", "upstream_edges"],
                "evidence_note": "Failed-execution rows do not cover every failed step. Status and empty evidence do not prove workload termination or safe retry.",
            },
            budget=budget,
        )
    rows = failures if view == "failures" else diagnosis[view]
    omitted = []
    if view == "failures":
        header["detail"] = detail
        if detail == "identity":
            rows = [
                {
                    key: value
                    for key, value in row.items()
                    if key not in {"error", "suggestions"}
                }
                for row in rows
            ]
            omitted = ["items.error", "items.suggestions"]
    return paginate(
        rows,
        limit,
        cursor,
        tool="artisan_diagnose_run",
        arguments={"pipeline_run_id": pipeline_run_id, "view": view, "detail": detail},
        scope=root,
        header=header,
        omitted_fields=omitted,
        budget=budget,
        hint="Use view='failures', detail='identity' for large diagnostics; otherwise inspect the source/Python reader.",
    )


def register(mcp: FastMCP) -> None:
    """Register status/diagnosis tools using native synchronous worker dispatch."""

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(PageResponse[RunRow]))
    def artisan_list_runs(
        ctx: Context, limit: StrictInt = 20, cursor: str | None = None
    ) -> ToolResult:
        """List persisted pipeline runs in deterministic recency order to select a run.

        Each bounded page contains rollups and continuation metadata. The latest
        updated step determines last_status, which is not a whole-run quality
        verdict. Equal start times use run ID order. Changing selected evidence
        invalidates the cursor; restart rather than combining different views.
        """
        config = ctx.lifespan_context["config"]
        return render_tool(
            lambda: runs_payload(require_delta_root(config), limit, cursor),
            PageResponse[RunRow],
        )

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(StatusResponse))
    def artisan_get_run_status(
        ctx: Context,
        pipeline_run_id: str,
        limit: StrictInt = 50,
        cursor: str | None = None,
    ) -> ToolResult:
        """Get the run rollup and a page of current per-step lifecycle statuses.

        Pending and running attempts remain visible; an unknown run has an empty
        steps page. last_status is the latest updated step, not overall success.
        The current reader omits separate cancellation evidence: failed status
        does not prove external work stopped. Use diagnosis to inspect failures.
        """
        config = ctx.lifespan_context["config"]
        return render_tool(
            lambda: status_payload(
                require_delta_root(config), pipeline_run_id, limit, cursor
            ),
            StatusResponse,
        )

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(DiagnosisResult))
    def artisan_diagnose_run(
        ctx: Context,
        pipeline_run_id: str,
        view: DiagnosisView = "summary",
        detail: Literal["full", "identity"] = "full",
        limit: StrictInt = 50,
        cursor: str | None = None,
    ) -> ToolResult:
        """Diagnose selected execution failures with explicit counts and graph coverage.

        Summary returns advisory actions and coverage, not hidden samples. Page
        failures, similar_runs or upstream_edges for evidence; identity failure
        detail omits large diagnostics. Recent-run windows and seed/depth limits
        remain separate from page exhaustion. Empty failures do not prove cleanup.
        """
        config = ctx.lifespan_context["config"]
        return render_tool(
            lambda: diagnosis_payload(
                require_delta_root(config), pipeline_run_id, view, detail, limit, cursor
            ),
            DiagnosisResult,
        )
