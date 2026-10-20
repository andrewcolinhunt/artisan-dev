"""Selected evidence remains whole, ordered and honest about its coverage."""

from __future__ import annotations

import importlib
import json
from datetime import UTC, datetime

import polars as pl
import pytest

import artisan.registry
from artisan.errors import ArtisanError, ErrorCode
from artisan.operations.base.operation_example import OperationExample
from artisan.registry.models import (
    DiscoveryReport,
    DiscoverySource,
    InputSpecMetadata,
    OperationMetadata,
    OutputSpecMetadata,
)
from artisan.storage.core.artifact_query import ArtifactRef
from artisan_mcp.tools.artifacts import artifact_payload
from artisan_mcp.tools.catalog import (
    capability_payload,
    operation_list_payload,
    operation_payload,
)
from artisan_mcp.tools.runs import diagnosis_payload, runs_payload


@pytest.fixture
def metadata(monkeypatch):
    value = OperationMetadata(
        name="test_operation",
        kind="creator",
        description="Description",
        input_roles=["z", "a"],
        output_roles=["result"],
        tags=["z", "a"],
        inputs={
            role: InputSpecMetadata(
                artifact_type="data", required=True, description=role, materialize=False
            )
            for role in ["z", "a"]
        },
        outputs={
            "result": OutputSpecMetadata(
                artifact_type="data", required=True, description="Output"
            )
        },
        params_schema={
            "$defs": {
                "Nested": {
                    "type": "object",
                    "properties": {"value": {"type": "number"}},
                }
            },
            "properties": {"nested": {"$ref": "#/$defs/Nested"}},
        },
        examples=[
            OperationExample(description="First"),
            OperationExample(description="Second"),
        ],
        source_module="test.module",
    )
    monkeypatch.setattr(artisan.registry, "describe", lambda _name: value)
    return value


def test_operation_sections_preserve_complete_schema_and_declared_order(metadata):
    all_fields = operation_payload("test_operation")
    assert all_fields["params_schema"] == metadata.params_schema
    assert (
        operation_payload("test_operation", "params")["params_schema"]
        == metadata.params_schema
    )
    summary = operation_payload("test_operation", "summary")
    assert (
        summary["input_count"],
        summary["output_count"],
        summary["example_count"],
        summary["tag_count"],
    ) == (2, 1, 2, 2)
    assert "params_schema" not in summary
    inputs = operation_payload("test_operation", "inputs", limit=1)
    assert inputs["items"][0]["role"] == "a"
    next_inputs = operation_payload(
        "test_operation", "inputs", limit=2, cursor=inputs["next_cursor"]
    )
    assert [row["role"] for row in next_inputs["items"]] == ["z"]
    assert (
        operation_payload("test_operation", "outputs")["items"][0]["role"] == "result"
    )
    assert operation_payload("test_operation", "tags")["items"] == ["a", "z"]
    assert [
        row["description"]
        for row in operation_payload("test_operation", "examples")["items"]
    ] == ["First", "Second"]


def test_operation_oversize_requires_actual_section_selection(metadata):
    metadata.params_schema = {"description": "x" * 60_000}
    for section in ["all", "params"]:
        with pytest.raises(ArtisanError) as caught:
            operation_payload("test_operation", section)
        assert caught.value.code == ErrorCode.EVIDENCE_RESPONSE_TOO_LARGE
    assert operation_payload("test_operation", "inputs")["total_items"] == 2
    metadata.examples = [OperationExample(description="x" * 60_000)]
    with pytest.raises(ArtisanError) as caught:
        operation_payload("test_operation", "examples")
    assert caught.value.code == ErrorCode.EVIDENCE_ITEM_TOO_LARGE


def test_identity_projection_rescues_large_catalogue_row(monkeypatch, metadata):
    metadata.description = "x" * 60_000
    monkeypatch.setattr(
        artisan.registry, "list_operations", lambda **kwargs: [metadata]
    )
    with pytest.raises(ArtisanError) as caught:
        operation_list_payload(None, None, None, "summary", 50, None)
    assert caught.value.code == ErrorCode.EVIDENCE_ITEM_TOO_LARGE
    result = operation_list_payload(None, None, None, "identity", 50, None)
    assert result["items"] == [{"name": "test_operation", "kind": "creator"}]
    assert "items.description" in result["omitted_fields"]


def test_capability_summary_omits_indivisible_large_discovery_source():
    report = DiscoveryReport(
        operations_count=1,
        sources=[
            DiscoverySource(
                kind="manual", module="test.module", classes_registered=["x" * 60_000]
            ),
        ],
    )
    summary = capability_payload({"discovery": report})
    assert summary["source_count"] == 1
    assert "sources" not in summary
    with pytest.raises(ArtisanError) as caught:
        capability_payload({"discovery": report}, view="sources")
    assert caught.value.code == ErrorCode.EVIDENCE_ITEM_TOO_LARGE


def test_metadata_selection_preserves_authored_values_and_occurrences(monkeypatch):
    module = importlib.import_module("artisan.storage.core.artifact_query")
    authored = {"token": "authored metric metadata", "nested": ["a", {"key": "雪"}]}
    refs = [
        ArtifactRef(
            artifact_id="a",
            artifact_type="metric",
            origin_step_number=1,
            current_step_number=number,
            pipeline_run_id="run",
            metadata={"selected": authored, "unselected": "x" * 60_000},
        )
        for number in [1, 2]
    ]
    monkeypatch.setattr(module, "query_artifacts", lambda *args, **kwargs: refs)
    identity = artifact_payload("/store", None, "run", None, 50, None)
    assert len(identity["items"]) == 2
    assert all("metadata" not in row for row in identity["items"])
    first = artifact_payload("/store", None, "run", ["selected", "selected"], 1, None)
    assert first["items"][0]["metadata"] == {"selected": authored}
    refs[1].metadata["unselected"] = "changed outside selected projection"
    second = artifact_payload(
        "/store", None, "run", ["selected"], 1, first["next_cursor"]
    )
    assert second["items"][0]["current_step_number"] == 2
    refs[1].metadata["selected"] = "changed"
    with pytest.raises(ArtisanError) as caught:
        artifact_payload("/store", None, "run", ["selected"], 1, first["next_cursor"])
    assert caught.value.code == ErrorCode.EVIDENCE_VIEW_CHANGED


def test_metadata_narrowing_does_not_clip_one_selected_value(monkeypatch):
    module = importlib.import_module("artisan.storage.core.artifact_query")
    refs = [
        ArtifactRef(
            artifact_id="a",
            artifact_type="data",
            origin_step_number=1,
            metadata={"large": {"nested": "x" * 60_000}},
        )
    ]
    monkeypatch.setattr(module, "query_artifacts", lambda *args, **kwargs: refs)
    with pytest.raises(ArtisanError) as caught:
        artifact_payload("/store", None, None, ["large"], 1, None)
    assert caught.value.code == ErrorCode.EVIDENCE_ITEM_TOO_LARGE
    assert artifact_payload("/store", None, None, [], 1, None)["returned_items"] == 1


def test_run_timestamp_ties_use_identity_and_nulls_sort_last(monkeypatch):
    module = importlib.import_module("artisan.orchestration.run_history")
    when = datetime(2026, 10, 20, tzinfo=UTC)
    rows = [
        {
            "pipeline_run_id": name,
            "started_at": timestamp,
            "ended_at": None,
            "last_status": None,
            "step_count": 0,
        }
        for name, timestamp in [("z", when), ("null", None), ("a", when)]
    ]
    monkeypatch.setattr(module, "list_runs", lambda _root: pl.DataFrame(rows))
    first = runs_payload("/store", 1)
    second = runs_payload("/store", 2, first["next_cursor"])
    assert [
        first["items"][0]["pipeline_run_id"],
        *[row["pipeline_run_id"] for row in second["items"]],
    ] == ["a", "z", "null"]


def test_failed_status_without_execution_failure_does_not_claim_cleanup(monkeypatch):
    module = importlib.import_module("artisan.visualization.inspect")
    diagnosis = module.RunDiagnosis(
        pipeline_run_id="run",
        last_status="failed",
        failed_steps=[],
        similar_runs=[],
        upstream_edges=[],
        suggested_actions=[],
        matching_failed_run_count=0,
        upstream_scope=module.DiagnosisUpstreamScope(),
    )
    monkeypatch.setattr(module, "diagnose_run", lambda *args: diagnosis)
    result = diagnosis_payload("/store", "run")
    assert result["last_status"] == "failed"
    assert result["failed_execution_count"] == 0
    assert "do not prove workload termination" in result["evidence_note"]


def test_failure_identity_narrows_without_reconstructing_redacted_diagnostics(
    monkeypatch,
):
    module = importlib.import_module("artisan.visualization.inspect")
    row = {
        "step": 1,
        "operation": "test",
        "execution_run_id": "execution",
        "timestamp_start": datetime(2026, 10, 20, tzinfo=UTC),
        "code": "commit_failed",
        "recovery_hint": "CHECK_INPUT",
        "field": "token",
        "log": "failure.log",
        "suggestions": ["Keep [REDACTED]"],
        "error": "[REDACTED]",
    }
    diagnosis = module.RunDiagnosis(
        pipeline_run_id="run",
        last_status="failed",
        failed_steps=[row],
        similar_runs=[],
        upstream_edges=[],
        suggested_actions=[],
        matching_failed_run_count=0,
        upstream_scope=module.DiagnosisUpstreamScope(),
    )
    monkeypatch.setattr(module, "diagnose_run", lambda *args: diagnosis)
    assert (
        diagnosis_payload("/store", "run", "failures")["items"][0]["error"]
        == "[REDACTED]"
    )
    diagnosis.failed_steps[0]["error"] = "x" * 60_000
    with pytest.raises(ArtisanError) as caught:
        diagnosis_payload("/store", "run", "failures")
    assert caught.value.code == ErrorCode.EVIDENCE_ITEM_TOO_LARGE
    identity = diagnosis_payload("/store", "run", "failures", "identity")
    assert "error" not in identity["items"][0]
    assert identity["items"][0]["code"] == "commit_failed"


def test_resource_continuation_is_usable_by_tool(
    make_app, read_resource, invoke, monkeypatch, tmp_path
):
    module = importlib.import_module("artisan.orchestration.run_history")
    rows = [
        {
            "pipeline_run_id": f"run-{index:03d}",
            "started_at": None,
            "ended_at": None,
            "last_status": None,
            "step_count": 0,
        }
        for index in range(27)
    ]
    monkeypatch.setattr(module, "list_runs", lambda _root: pl.DataFrame(rows))
    app = make_app(delta_root=tmp_path)
    first = json.loads(read_resource(app, "artisan://runs").text)
    follow = first["follow_up"]
    second = invoke(app, follow["tool"], follow["arguments"])
    assert first["view_id"] == second["view_id"]
    assert first["items"] + second["items"] == rows
