"""Bounded assessment and comparison views over core read-only reports."""

from __future__ import annotations

from typing import Any, Literal

from fastmcp import Context, FastMCP
from fastmcp.tools import ToolResult
from pydantic import StrictInt

from artisan.errors import ErrorCode, OutcomeSelectionError
from artisan.visualization.comparison import ComparisonSpec, RunComparison, compare_runs
from artisan.visualization.outcomes import (
    AssessmentSpec,
    OutcomeAssessment,
    OutcomeSelection,
    ResolvedOutcomeSelection,
    assess_outcomes,
)
from artisan_mcp._boundary import render_tool, require_delta_root, validate_int_range
from artisan_mcp._common import READ_ONLY
from artisan_mcp._evidence import ensure_fits, evidence_error
from artisan_mcp._pagination import MAX_PAGE_SIZE, paginate, reject_cursor
from artisan_mcp.models import AssessmentResult, ComparisonResult, output_schema


def register(mcp: FastMCP) -> None:
    """Attach criteria-based inspection without execution or persistence."""

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(AssessmentResult))
    def artisan_assess_outcomes(
        ctx: Context,
        pipeline_run_id: str,
        step_name: str,
        role: str,
        spec: AssessmentSpec,
        step_number: StrictInt | None = None,
        step_run_id: str | None = None,
        view: Literal["summary", "observations"] = "summary",
        limit: StrictInt = 50,
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
        config = ctx.lifespan_context["config"]

        def payload() -> dict[str, Any]:
            selection = _selection(
                pipeline_run_id, step_name, role, step_number, step_run_id
            )
            _validate_view(view, limit, cursor, selection)
            root = require_delta_root(config)
            report = assess_outcomes(root, selection, spec)
            return assessment_payload(report, root, view, limit, cursor)

        return render_tool(payload, AssessmentResult)

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(ComparisonResult))
    def artisan_compare_runs(
        ctx: Context,
        baseline: OutcomeSelection,
        candidate: OutcomeSelection,
        spec: ComparisonSpec,
        view: Literal["summary", "pairs", "unmatched", "invalid"] = "summary",
        limit: StrictInt = 50,
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
        config = ctx.lifespan_context["config"]

        def payload() -> dict[str, Any]:
            _validate_view(view, limit, cursor, baseline, candidate)
            root = require_delta_root(config)
            report = compare_runs(root, baseline, candidate, spec)
            return comparison_payload(report, root, view, limit, cursor)

        return render_tool(payload, ComparisonResult)


def _selection(
    pipeline_run_id: str,
    step_name: str,
    role: str,
    step_number: int | None,
    step_run_id: str | None,
) -> OutcomeSelection:
    """Validate flattened tool arguments as a caller request, before core I/O."""
    from pydantic import ValidationError

    try:
        return OutcomeSelection(
            pipeline_run_id=pipeline_run_id,
            step_name=step_name,
            role=role,
            step_number=step_number,
            step_run_id=step_run_id,
        )
    except ValidationError as exc:
        message = "Invalid outcome selection"
        raise OutcomeSelectionError(message, field="selection") from exc


def _validate_view(
    view: str, limit: int, cursor: str | None, *selections: OutcomeSelection
) -> None:
    validate_int_range(limit, field="limit", minimum=1, maximum=MAX_PAGE_SIZE)
    if view == "summary":
        reject_cursor(cursor)
    elif cursor is not None and any(item.step_run_id is None for item in selections):
        raise evidence_error(
            ErrorCode.INVALID_EVIDENCE_CURSOR,
            field="cursor",
            hint="Continue with every exact step_run_id from the previous response.",
        )


def _exact_arguments(selection: ResolvedOutcomeSelection) -> dict[str, Any]:
    """Canonical selectors bind initial and follow-up calls to the same attempt."""
    return selection.model_dump(mode="json", include=set(OutcomeSelection.model_fields))


def assessment_payload(
    report: OutcomeAssessment,
    root: str,
    view: str,
    limit: int,
    cursor: str | None,
) -> dict[str, Any]:
    """Project an already computed full-population assessment."""
    header = report.model_dump(mode="json", exclude={"observations"})
    header["view"] = view
    if view == "summary":
        return ensure_fits(
            {"projection_version": "1", "follow_up": None, **header},
            hint="Inspect this complete assessment with the public Python reader.",
        )
    return paginate(
        report.observations,
        limit,
        cursor,
        tool="artisan_assess_outcomes",
        arguments={
            **_exact_arguments(report.selection),
            "spec": report.spec.model_dump(mode="json"),
            "view": "observations",
        },
        scope=root,
        header=header,
        hint="Use fewer checks or inspect the complete observation with Python.",
    )


def comparison_payload(
    report: RunComparison,
    root: str,
    view: str,
    limit: int,
    cursor: str | None,
) -> dict[str, Any]:
    """Project complete aggregates and at most one comparison detail collection."""
    header = report.model_dump(
        mode="json",
        exclude={
            "pairs": True,
            "unmatched": True,
            "invalid": True,
            "baseline_assessment": {"observations", "spec"},
            "candidate_assessment": {"observations", "spec"},
        },
    )
    header["view"] = view
    if view == "summary":
        return ensure_fits(
            {"projection_version": "1", "follow_up": None, **header},
            hint="Use fewer metric declarations or inspect this report with Python.",
        )
    return paginate(
        getattr(report, view),
        limit,
        cursor,
        tool="artisan_compare_runs",
        arguments={
            "baseline": _exact_arguments(report.baseline.selection),
            "candidate": _exact_arguments(report.candidate.selection),
            "spec": report.spec.model_dump(mode="json"),
            "view": view,
        },
        scope=root,
        header=header,
        hint="Use fewer metric declarations or inspect this complete row with Python.",
    )
