"""Exact assessment evidence survives native MCP rendering and pagination."""

from __future__ import annotations

import asyncio
import json

import jsonschema
import pytest
from fastmcp import Client
from fixtures.outcome_store import append_metric_attempt

from artisan.inspection._evidence import MAX_PAYLOAD_BYTES, encode_json
from artisan.schemas import MetricArtifact
from artisan.visualization.outcomes import AssessmentCheck, AssessmentSpec


def _criteria() -> dict:
    return AssessmentSpec(
        label="minimum score",
        checks=[
            AssessmentCheck(
                check_id="score",
                metric_path=["score"],
                operator="ge",
                threshold=0.8,
            )
        ],
    ).model_dump(mode="json")


def _population(tmp_path, values):
    root = tmp_path / "delta"
    artifacts = [
        MetricArtifact.draft(
            {"score": value}, f"case-{index}.json", 3, metadata={"case": index}
        ).finalize()
        for index, value in enumerate(values)
    ]
    return root, artifacts, append_metric_attempt(root, artifacts, step_number=3)


def test_failed_criterion_is_successful_full_population_inspection(
    tmp_path, make_app, invoke_result
):
    root, artifacts, selection = _population(tmp_path, [0.80001, 0.2, None])
    result = invoke_result(
        make_app(delta_root=root),
        "artisan_assess_outcomes",
        {**selection.model_dump(), "spec": _criteria()},
    )
    payload = result.structured_content
    assert result.is_error is False
    assert payload["assessment_status"] == "incomplete"
    assert payload["observation_count"] == len(artifacts)
    assert payload["checks"][0]["fail_count"] == 1
    assert payload["checks"][0]["unassessed_count"] == 1
    assert payload["checks"][0]["coverage"] == 2 / 3
    assert payload["selection"]["step_run_id"] == selection.step_run_id
    assert "items" not in payload
    assert json.loads(result.content[0].text) == payload


def test_assessment_pages_keep_exact_attempt_and_full_counts(
    tmp_path, make_app, invoke
):
    root, artifacts, selection = _population(tmp_path, [0.8, 0.2, 0.9])
    app = make_app(delta_root=root)
    request = {
        **selection.model_dump(exclude={"step_run_id"}),
        "spec": _criteria(),
        "view": "observations",
        "limit": 1,
    }
    page = invoke(app, "artisan_assess_outcomes", request)
    first_view = page["view_id"]
    seen = []
    while True:
        seen.extend(row["artifact_id"] for row in page["items"])
        assert page["view_id"] == first_view
        assert page["observation_count"] == 3
        assert page["checks"][0]["pass_count"] == 2
        assert page["total_items"] == 3
        assert len(encode_json(page)) <= MAX_PAYLOAD_BYTES
        if not page["has_more"]:
            break
        follow_up = page["follow_up"]
        assert follow_up["arguments"]["step_run_id"] == selection.step_run_id
        page = invoke(
            app,
            "artisan_" + follow_up["capability"],
            {**follow_up["arguments"], "limit": 2},
        )
    assert seen == sorted(item.artifact_id for item in artifacts)


def test_assessment_continuation_rejects_missing_attempt_and_changed_spec(
    tmp_path, make_app, invoke, invoke_result
):
    root, _, selection = _population(tmp_path, [0.8, 0.2])
    app = make_app(delta_root=root)
    page = invoke(
        app,
        "artisan_assess_outcomes",
        {
            **selection.model_dump(),
            "spec": _criteria(),
            "view": "observations",
            "limit": 1,
        },
    )
    arguments = page["follow_up"]["arguments"]
    missing = {key: value for key, value in arguments.items() if key != "step_run_id"}
    changed = {
        **arguments,
        "spec": {**arguments["spec"], "label": "different criteria"},
    }
    for request in (missing, changed):
        result = invoke_result(app, "artisan_assess_outcomes", request)
        assert result.is_error
        assert result.structured_content["code"] == "invalid_evidence_cursor"


def test_assessment_cursor_does_not_follow_new_current_attempt(
    tmp_path, make_app, invoke
):
    root, _, selection = _population(tmp_path, [0.8, 0.9])
    app = make_app(delta_root=root)
    page = invoke(
        app,
        "artisan_assess_outcomes",
        {
            **selection.model_dump(exclude={"step_run_id"}),
            "spec": _criteria(),
            "view": "observations",
            "limit": 1,
        },
    )
    append_metric_attempt(root, [], step_number=3, status="failed")
    follow_up = page["follow_up"]
    continued = invoke(
        app, "artisan_" + follow_up["capability"], follow_up["arguments"]
    )
    assert continued["selection"]["step_run_id"] == selection.step_run_id
    assert continued["assessment_status"] == "pass"
    current = invoke(
        app,
        "artisan_assess_outcomes",
        {**selection.model_dump(exclude={"step_run_id"}), "spec": _criteria()},
    )
    assert current["population_status"] == "unavailable"


def test_assessment_advertised_schema_and_large_integer(tmp_path, make_app):
    value = 2**53 + 1
    root, _, selection = _population(tmp_path, [value])
    app = make_app(delta_root=root)

    async def check():
        async with Client(app) as client:
            schema = next(
                tool.outputSchema
                for tool in await client.list_tools()
                if tool.name == "artisan_assess_outcomes"
            )
            result = await client.call_tool(
                "artisan_assess_outcomes",
                {
                    **selection.model_dump(),
                    "spec": _criteria(),
                    "view": "observations",
                },
            )
            jsonschema.validate(result.structured_content, schema)
            observed = result.structured_content["items"][0]["checks"][0]["value"]
            assert type(observed) is int
            assert observed == value
            error = await client.call_tool(
                "artisan_assess_outcomes",
                {**selection.model_dump(), "step_name": "missing", "spec": _criteria()},
                raise_on_error=False,
            )
            assert error.is_error
            jsonschema.validate(error.structured_content, schema)

    asyncio.run(check())


def test_oversized_observation_is_an_error_without_skipping_it(
    tmp_path, make_app, invoke_result
):
    root = tmp_path / "delta"
    artifact = MetricArtifact.draft(
        {"score": 0.9}, "x" * (MAX_PAYLOAD_BYTES + 1) + ".json", 0
    ).finalize()
    selection = append_metric_attempt(root, [artifact])
    result = invoke_result(
        make_app(delta_root=root),
        "artisan_assess_outcomes",
        {
            **selection.model_dump(),
            "spec": _criteria(),
            "view": "observations",
            "limit": 1,
        },
    )
    assert result.is_error
    assert result.structured_content["code"] == "evidence_item_too_large"
    assert len(encode_json(result.structured_content)) <= MAX_PAYLOAD_BYTES


@pytest.mark.parametrize("bad_number", [True, "3", -1])
def test_assessment_rejects_invalid_step_number(
    tmp_path, make_app, invoke_result, bad_number
):
    root, _, selection = _population(tmp_path, [0.8])
    result = invoke_result(
        make_app(delta_root=root),
        "artisan_assess_outcomes",
        {**selection.model_dump(), "step_number": bad_number, "spec": _criteria()},
    )
    assert result.is_error


@pytest.mark.parametrize("limit", [0, -1, 101])
def test_assessment_summary_rejects_invalid_limit_before_store_read(
    make_app, invoke_result, limit
):
    result = invoke_result(
        make_app(),
        "artisan_assess_outcomes",
        {
            "pipeline_run_id": "run",
            "step_name": "measure",
            "role": "metrics",
            "spec": _criteria(),
            "limit": limit,
        },
    )
    assert result.is_error
    assert result.structured_content is None
