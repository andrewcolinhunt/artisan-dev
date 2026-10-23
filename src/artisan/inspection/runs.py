"""Bounded run status and diagnosis, preserving authoritative reader semantics."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import StrictInt

from artisan.inspection._boundary import invalid_input, validate_int_range
from artisan.inspection._evidence import MAX_PAYLOAD_BYTES, ensure_fits, json_ready
from artisan.inspection._pagination import paginate, reject_cursor
from artisan.inspection.requests import DiagnosisView


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
        capability="list_runs",
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
        capability="get_run_status",
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
                "projection_version": "2",
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
        capability="diagnose_run",
        arguments={"pipeline_run_id": pipeline_run_id, "view": view, "detail": detail},
        scope=root,
        header=header,
        omitted_fields=omitted,
        budget=budget,
        hint="Use view='failures', detail='identity' for large diagnostics; otherwise inspect the source/Python reader.",
    )
