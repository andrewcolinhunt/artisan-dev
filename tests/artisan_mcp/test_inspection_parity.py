"""Normal Python, CLI and MCP calls share evidence and portable continuation."""

from __future__ import annotations

import asyncio
import json

import pytest
from fastmcp import Client
from fixtures.outcome_store import append_metric_attempt

from artisan.cli import main
from artisan.errors import ArtisanError, CommitError
from artisan.inspection._boundary import error_payload
from artisan.inspection._evidence import json_ready
from artisan.inspection.api import inspect_evidence
from artisan.registry import bootstrap_applications, discover
from artisan.schemas import MetricArtifact


@pytest.fixture
def cli_payload(tmp_path, capfd):
    """Call the actual CLI command handler and capture its one JSON object."""

    def call(capability, arguments, root=None):
        request = tmp_path / "inspection-request.json"
        request.write_text(json.dumps(arguments), encoding="utf-8")
        args = ["inspect", "run", capability, "--request", str(request), "--json"]
        if root is not None:
            args.extend(["--delta-root", str(root)])
        capfd.readouterr()
        status = main(args)
        output = capfd.readouterr()
        assert output.out.endswith("\n")
        return status, json.loads(output.out)

    return call


@pytest.mark.parametrize(
    ("capability", "arguments"),
    [
        ("capabilities", {}),
        ("list_operations", {"detail": "identity", "limit": 2}),
        ("describe_operation", {"name": "data_generator", "section": "params"}),
        ("list_runs", {}),
        ("get_run_status", {"pipeline_run_id": "run-1", "limit": 1}),
        ("diagnose_run", {"pipeline_run_id": "run-1"}),
        ("query_artifacts", {"pipeline_run_id": "run-1", "metadata_keys": ["seed"]}),
        (
            "get_step_result",
            {"pipeline_run_id": "run-1", "step_name": "generate", "limit": 1},
        ),
        (
            "get_step_logs",
            {"pipeline_run_id": "run-1", "step_name": "transform", "tail_lines": 3},
        ),
        ("get_provenance_graph", {"artifact_id": "a" * 32}),
    ],
)
def test_all_catalogue_and_store_views_agree(
    capability, arguments, make_app, seeded_run, cli_payload
):
    app = make_app(delta_root=seeded_run.delta_root)
    bootstrap_applications(["artisan.operations.examples"])
    discovery = discover(["artisan.operations.examples"], include_environment=False)
    expected = json_ready(
        inspect_evidence(
            capability,
            arguments,
            delta_root=str(seeded_run.delta_root),
            discovery=discovery,
        )
    )
    status, actual = cli_payload(capability, arguments, seeded_run.delta_root)
    assert status == 0
    assert actual == expected

    async def check():
        async with Client(app) as client:
            result = await client.call_tool("artisan_" + capability, arguments)
            assert not result.is_error
            assert result.structured_content == expected
            assert json.loads(result.content[0].text) == expected

    asyncio.run(check())


@pytest.fixture
def populations(tmp_path):
    root = tmp_path / "metrics"
    selections = []
    for run_id in ("baseline", "candidate"):
        artifacts = [
            MetricArtifact.draft(
                {"score": value}, f"{run_id}-{index}.json", 0, metadata={"case": index}
            ).finalize()
            for index, value in enumerate([0.9, 0.2, 0.7])
        ]
        selections.append(
            append_metric_attempt(root, artifacts, pipeline_run_id=run_id)
        )
    assessment = {
        "label": "minimum",
        "checks": [
            {
                "check_id": "score",
                "metric_path": ["score"],
                "operator": "ge",
                "threshold": 0.8,
            }
        ],
    }
    comparison = {
        "label": "same cases",
        "matching": {"kind": "metadata_key", "key": "case"},
        "metrics": [
            {
                "metric_id": "score",
                "metric_path": ["score"],
                "direction": "higher",
                "measurement_id": "score",
                "measurement_version": "1",
                "unit": "unitless",
            }
        ],
        "assessment": assessment,
    }
    return root, {
        "assess_outcomes": {**selections[0].model_dump(), "spec": assessment},
        "compare_runs": {
            "baseline": selections[0].model_dump(),
            "candidate": selections[1].model_dump(),
            "spec": comparison,
        },
    }


@pytest.mark.parametrize("capability", ["assess_outcomes", "compare_runs"])
def test_full_population_summaries_agree(
    capability, populations, make_app, cli_payload
):
    root, requests = populations
    arguments = requests[capability]
    expected = json_ready(inspect_evidence(capability, arguments, delta_root=str(root)))
    status, actual = cli_payload(capability, arguments, root)
    assert status == 0
    assert actual == expected

    async def check():
        async with Client(make_app(delta_root=root)) as client:
            result = await client.call_tool("artisan_" + capability, arguments)
            assert result.structured_content == expected
            assert not result.is_error

    asyncio.run(check())


@pytest.mark.parametrize(
    ("capability", "view"),
    [("assess_outcomes", "observations"), ("compare_runs", "pairs")],
)
def test_exact_attempt_pages_continue_python_to_cli_to_mcp(
    capability, view, populations, make_app, cli_payload
):
    root, requests = populations
    arguments = {**requests[capability], "view": view, "limit": 1}
    if capability == "assess_outcomes":
        arguments.pop("step_run_id")
    else:
        for side in ("baseline", "candidate"):
            arguments[side].pop("step_run_id")
    first = inspect_evidence(capability, arguments, delta_root=str(root)).model_dump(
        mode="json"
    )
    follow = first["follow_up"]
    assert follow["capability"] == capability
    status, second = cli_payload(follow["capability"], follow["arguments"], root)
    assert status == 0
    assert second["view_id"] == first["view_id"]

    async def check():
        async with Client(make_app(delta_root=root)) as client:
            follow = second["follow_up"]
            result = await client.call_tool(
                "artisan_" + follow["capability"], follow["arguments"]
            )
            third = result.structured_content
            assert third["view_id"] == first["view_id"]
            assert not third["has_more"]
            assert len(first["items"] + second["items"] + third["items"]) == 3

    asyncio.run(check())


@pytest.mark.parametrize("change", ["root", "spec", "view"])
def test_portable_cursor_rejects_changed_context(
    change, populations, make_app, cli_payload
):
    root, requests = populations
    request = {**requests["compare_runs"], "view": "pairs", "limit": 1}
    first = inspect_evidence("compare_runs", request, delta_root=str(root))
    arguments = dict(first.follow_up.arguments)
    if change == "root":
        # The same filesystem with a different spelling intentionally has a new scope.
        root = str(root) + "/"
    elif change == "spec":
        arguments["spec"]["label"] = "changed"
    else:
        arguments["view"] = "unmatched"
    with pytest.raises(ArtisanError) as caught:
        inspect_evidence("compare_runs", arguments, delta_root=str(root))
    expected = error_payload(caught.value)
    assert expected["code"] == "invalid_evidence_cursor"
    status, actual = cli_payload("compare_runs", arguments, root)
    assert status == 1
    assert actual == expected

    async def check():
        async with Client(make_app(delta_root=root)) as client:
            result = await client.call_tool(
                "artisan_compare_runs", arguments, raise_on_error=False
            )
            assert result.is_error
            assert result.structured_content == expected

    asyncio.run(check())


def test_specialized_known_error_parity_preserves_extensions(
    monkeypatch, make_app, cli_payload, tmp_path
):
    from artisan.inspection import runs

    error = CommitError(
        logical_commit_id="commit",
        table="steps",
        plan_key="plans/commit",
        verified_tables=["steps"],
        staging_objects=["staging/object"],
        message="Repair required",
    )

    def fail(**kwargs):
        raise error from RuntimeError("PRIVATE-CAUSE")

    monkeypatch.setattr(runs, "runs_payload", fail)
    expected = error_payload(error)
    status, actual = cli_payload("list_runs", {}, tmp_path)
    assert status == 1
    assert actual == expected

    async def check():
        async with Client(make_app(delta_root=tmp_path)) as client:
            result = await client.call_tool("artisan_list_runs", raise_on_error=False)
            assert result.is_error
            assert result.structured_content == expected

    asyncio.run(check())


def test_changed_evidence_rejects_portable_cursor_everywhere(
    monkeypatch, make_app, cli_payload, tmp_path
):
    import importlib

    import polars as pl

    reader = importlib.import_module("artisan.orchestration.run_history")
    rows = [
        {
            "pipeline_run_id": run_id,
            "step_count": 1,
            "last_status": "running",
            "started_at": None,
            "ended_at": None,
        }
        for run_id in ("first", "second")
    ]
    monkeypatch.setattr(reader, "list_runs", lambda root: pl.DataFrame(rows))
    first = inspect_evidence("list_runs", {"limit": 1}, delta_root=str(tmp_path))
    arguments = first.follow_up.arguments
    rows[1]["last_status"] = "failed"
    with pytest.raises(ArtisanError) as caught:
        inspect_evidence("list_runs", arguments, delta_root=str(tmp_path))
    expected = error_payload(caught.value)
    assert expected["code"] == "evidence_view_changed"
    status, actual = cli_payload("list_runs", arguments, tmp_path)
    assert status == 1
    assert actual == expected

    async def check():
        async with Client(make_app(delta_root=tmp_path)) as client:
            result = await client.call_tool(
                "artisan_list_runs", arguments, raise_on_error=False
            )
            assert result.is_error
            assert result.structured_content == expected

    asyncio.run(check())
