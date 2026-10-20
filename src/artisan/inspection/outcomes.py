"""Bounded assessment and comparison views over core read-only reports."""

from __future__ import annotations

from typing import Any

from artisan.inspection._evidence import ensure_fits
from artisan.inspection._pagination import paginate
from artisan.visualization.comparison import ComparisonSpec, RunComparison, compare_runs
from artisan.visualization.outcomes import (
    AssessmentSpec,
    OutcomeAssessment,
    OutcomeSelection,
    ResolvedOutcomeSelection,
    assess_outcomes,
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
            {"projection_version": "2", "follow_up": None, **header},
            hint="Inspect this complete assessment with the public Python reader.",
        )
    return paginate(
        report.observations,
        limit,
        cursor,
        capability="assess_outcomes",
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
            {"projection_version": "2", "follow_up": None, **header},
            hint="Use fewer metric declarations or inspect this report with Python.",
        )
    return paginate(
        getattr(report, view),
        limit,
        cursor,
        capability="compare_runs",
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


def assessment_inspection(
    root: str,
    *,
    spec: AssessmentSpec,
    view: str,
    limit: int,
    cursor: str | None,
    **selection: Any,
) -> dict[str, Any]:
    """Read one complete selected population and project its requested view."""
    report = assess_outcomes(root, OutcomeSelection.model_validate(selection), spec)
    return assessment_payload(report, root, view, limit, cursor)


def comparison_inspection(
    root: str,
    *,
    baseline: OutcomeSelection,
    candidate: OutcomeSelection,
    spec: ComparisonSpec,
    view: str,
    limit: int,
    cursor: str | None,
) -> dict[str, Any]:
    """Read both complete populations and project the explicit correspondence."""
    report = compare_runs(root, baseline, candidate, spec)
    return comparison_payload(report, root, view, limit, cursor)
