"""MCP comparisons preserve correspondence, coverage and exact continuation."""

from __future__ import annotations

import asyncio
import json

import jsonschema
import pytest
from fastmcp import Client
from fixtures.outcome_store import append_metric_attempt

from artisan.schemas import MetricArtifact
from artisan.visualization.comparison import (
    ComparisonMetric,
    ComparisonSpec,
    MetadataKeyMatch,
)
from artisan.visualization.outcomes import AssessmentCheck, AssessmentSpec
from artisan_mcp._evidence import MAX_PAYLOAD_BYTES, encode_json


def _request(tmp_path):
    root = tmp_path / "delta"
    selections = []
    for run_id, scores in (
        ("baseline", [("a", 0.9), ("b", 0.1)]),
        ("candidate", [("a", 0.8), ("b", 0.5), ("c", 1.0)]),
    ):
        artifacts = [
            MetricArtifact.draft(
                {"score": value}, f"{run_id}-{key}.json", 0, metadata={"case": key}
            ).finalize()
            for key, value in scores
        ]
        selections.append(
            append_metric_attempt(root, artifacts, pipeline_run_id=run_id)
        )
    spec = ComparisonSpec(
        label="Paired score change",
        matching=MetadataKeyMatch(key="case"),
        metrics=[
            ComparisonMetric(
                metric_id="score",
                metric_path=["score"],
                direction="higher",
                measurement_id="score",
                measurement_version="1",
                unit="unitless",
            )
        ],
        assessment=AssessmentSpec(
            label="minimum",
            checks=[
                AssessmentCheck(
                    check_id="minimum",
                    metric_path=["score"],
                    operator="ge",
                    threshold=0.7,
                )
            ],
        ),
    )
    return root, {
        "baseline": selections[0].model_dump(),
        "candidate": selections[1].model_dump(),
        "spec": spec.model_dump(mode="json"),
    }


def test_comparison_reports_regression_hidden_by_mean_and_population_change(
    tmp_path, make_app, invoke_result
):
    root, request = _request(tmp_path)
    result = invoke_result(make_app(delta_root=root), "artisan_compare_runs", request)
    payload = result.structured_content
    assert not result.is_error
    assert payload["comparison_status"] == "compared"
    assert payload["comparability_basis"] == "caller_declared"
    assert payload["coverage"]["matched_pair_count"] == 2
    assert payload["coverage"]["candidate_unmatched_count"] == 1
    assert payload["metrics"][0]["mean_delta"] == pytest.approx(0.15)
    assert payload["metrics"][0]["regressed_count"] == 1
    assert payload["metrics"][0]["improved_count"] == 1
    assert payload["candidate_assessment"]["observation_count"] == 3
    assert payload["candidate_assessment"]["checks"][0]["pass_count"] == 2
    assert "spec" not in payload["candidate_assessment"]
    assert "observations" not in payload["candidate_assessment"]
    assert json.loads(result.content[0].text) == payload


@pytest.mark.parametrize(
    ("view", "expected_count"), [("pairs", 2), ("unmatched", 1), ("invalid", 0)]
)
def test_comparison_views_preserve_complete_summary(
    tmp_path, make_app, invoke, view, expected_count
):
    root, request = _request(tmp_path)
    app = make_app(delta_root=root)
    summary = invoke(app, "artisan_compare_runs", request)
    page = invoke(app, "artisan_compare_runs", {**request, "view": view, "limit": 1})
    seen = []
    while True:
        assert page["coverage"] == summary["coverage"]
        assert page["metrics"] == summary["metrics"]
        assert page["total_items"] == expected_count
        assert len(encode_json(page)) <= MAX_PAYLOAD_BYTES
        seen.extend(page["items"])
        if not page["has_more"]:
            break
        follow_up = page["follow_up"]
        for side in ("baseline", "candidate"):
            assert (
                follow_up["arguments"][side]["step_run_id"]
                == request[side]["step_run_id"]
            )
        page = invoke(app, follow_up["tool"], follow_up["arguments"])
    assert len(seen) == expected_count
    if view == "pairs":
        assert [item["key"] for item in seen] == ["a", "b"]


@pytest.mark.parametrize("mutation", ["baseline", "candidate", "view", "spec"])
def test_comparison_continuations_bind_both_attempts_spec_and_view(
    tmp_path, make_app, invoke, invoke_result, mutation
):
    root, request = _request(tmp_path)
    app = make_app(delta_root=root)
    page = invoke(app, "artisan_compare_runs", {**request, "view": "pairs", "limit": 1})
    follow_up = page["follow_up"]["arguments"]
    if mutation in {"baseline", "candidate"}:
        follow_up[mutation].pop("step_run_id")
    elif mutation == "view":
        follow_up["view"] = "unmatched"
    else:
        follow_up["spec"]["label"] = "changed"
    error = invoke_result(app, "artisan_compare_runs", follow_up)
    assert error.is_error
    assert error.structured_content["code"] == "invalid_evidence_cursor"


def test_comparison_schema_validates_summary_details_and_native_errors(
    tmp_path, make_app
):
    root, request = _request(tmp_path)
    app = make_app(delta_root=root)

    async def check():
        async with Client(app) as client:
            schema = next(
                tool.outputSchema
                for tool in await client.list_tools()
                if tool.name == "artisan_compare_runs"
            )
            for view in ("summary", "pairs", "unmatched", "invalid"):
                result = await client.call_tool(
                    "artisan_compare_runs", {**request, "view": view}
                )
                jsonschema.validate(result.structured_content, schema)
            request["candidate"]["step_name"] = "absent"
            error = await client.call_tool(
                "artisan_compare_runs", request, raise_on_error=False
            )
            assert error.is_error
            jsonschema.validate(error.structured_content, schema)

    asyncio.run(check())


@pytest.mark.parametrize("limit", [0, -1, 101])
def test_comparison_summary_rejects_invalid_limit_before_store_read(
    tmp_path, make_app, invoke_result, limit
):
    _, request = _request(tmp_path)
    result = invoke_result(
        make_app(), "artisan_compare_runs", {**request, "limit": limit}
    )
    assert result.is_error
    assert result.structured_content["field"] == "limit"
    assert result.structured_content["code"] == "param_type_mismatch"
