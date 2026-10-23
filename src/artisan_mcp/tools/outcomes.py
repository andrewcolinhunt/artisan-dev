"""Thin named MCP adapters for shared bounded inspections."""

from __future__ import annotations

from typing import Annotated, Literal

from fastmcp import Context, FastMCP
from fastmcp.tools import ToolResult
from pydantic import Field

from artisan.inspection.models import (
    AssessmentResult,
    ComparisonResult,
    output_schema,
)
from artisan.inspection.requests import (
    PageLimit,
)
from artisan.visualization.comparison import ComparisonSpec
from artisan.visualization.outcomes import (
    ArtifactId,
    AssessmentSpec,
    Label,
    OutcomeSelection,
)
from artisan_mcp._boundary import inspection_tool
from artisan_mcp._common import READ_ONLY


def register(mcp: FastMCP) -> None:
    """Register individual read-only tools using the core request definitions."""

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(AssessmentResult))
    def artisan_assess_outcomes(
        ctx: Context,
        pipeline_run_id: Label,
        step_name: Label,
        role: Label,
        spec: AssessmentSpec,
        step_number: Annotated[int, Field(strict=True, ge=0)] | None = None,
        step_run_id: ArtifactId | None = None,
        view: Literal["summary", "observations"] = "summary",
        limit: PageLimit = 50,
        cursor: str | None = None,
    ) -> ToolResult:
        """Assess one accepted metric role against explicit numeric criteria.

        Execution status and criterion results remain separate. The summary
        covers every selected observation; observations pages preserve exact
        values and artifact references. Missing measurements are incomplete,
        and empty or unavailable populations are unassessed. Follow returned
        arguments with the exact step_run_id when paging. A failed criterion
        is successful inspection, while invalid selectors are query errors.
        """
        return inspection_tool(
            "assess_outcomes",
            {
                "pipeline_run_id": pipeline_run_id,
                "step_name": step_name,
                "role": role,
                "spec": spec,
                "step_number": step_number,
                "step_run_id": step_run_id,
                "view": view,
                "limit": limit,
                "cursor": cursor,
            },
            ctx=ctx,
        )

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(ComparisonResult))
    def artisan_compare_runs(
        ctx: Context,
        baseline: OutcomeSelection,
        candidate: OutcomeSelection,
        spec: ComparisonSpec,
        view: Literal["summary", "pairs", "unmatched", "invalid"] = "summary",
        limit: PageLimit = 50,
        cursor: str | None = None,
    ) -> ToolResult:
        """Compare explicit metric populations using declared correspondence.

        Supply typed metadata-key matching or explicit artifact pairs, with
        measurement meaning, version, unit, direction and tolerance. Summaries
        retain regressions, missing values and unmatched population coverage.
        Duplicate keys suppress numeric comparison. Optional assessment applies
        to each complete role. This does not infer a winner or establish
        causality. Detail continuations require both exact step_run_id values.
        """
        return inspection_tool(
            "compare_runs",
            {
                "baseline": baseline,
                "candidate": candidate,
                "spec": spec,
                "view": view,
                "limit": limit,
                "cursor": cursor,
            },
            ctx=ctx,
        )
