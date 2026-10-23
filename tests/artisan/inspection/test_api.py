"""Shared requests validate before I/O and preserve full reader semantics."""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from fixtures.outcome_store import append_metric_attempt
from pydantic import ValidationError

from artisan.errors import ArtisanError, CommitError, ErrorCode
from artisan.inspection._boundary import error_payload
from artisan.inspection._evidence import decode_json_object, encode_json
from artisan.inspection.api import inspect_evidence, validate_request
from artisan.inspection.models import AssessmentSummary
from artisan.registry.models import DiscoveryError, DiscoveryReport
from artisan.schemas import MetricArtifact


@pytest.mark.parametrize(
    "arguments",
    [
        None,
        [],
        "{}",
        {"limit": True},
        {"limit": "2"},
        {"limit": 0},
        {"limit": 101},
        {"extra": "private"},
        {"limit": float("nan")},
        {"cursor": Path("private")},
    ],
)
def test_invalid_requests_fail_before_store_access(arguments, monkeypatch):
    module = importlib.import_module("artisan.inspection.runs")
    monkeypatch.setattr(
        module, "runs_payload", lambda **kwargs: pytest.fail("reader invoked")
    )
    with pytest.raises(ArtisanError) as caught:
        inspect_evidence("list_runs", arguments, delta_root="/unopened")
    assert caught.value.code == ErrorCode.PARAM_TYPE_MISMATCH
    assert "private" not in encode_json(error_payload(caught.value)).decode()


def test_unknown_capability_is_not_dynamic_dispatch():
    with pytest.raises(ArtisanError) as caught:
        inspect_evidence("artisan.inspection.runs.runs_payload", {})
    assert caught.value.code == ErrorCode.UNKNOWN_INSPECTION


@pytest.mark.parametrize(
    ("capability", "arguments"),
    [
        ("capabilities", {"cursor": "private"}),
        ("describe_operation", {"name": "x", "section": "params", "cursor": "private"}),
        ("diagnose_run", {"pipeline_run_id": "x", "detail": "identity"}),
        (
            "get_step_logs",
            {"pipeline_run_id": "x", "step_name": "x", "tail_lines": 1001},
        ),
        ("get_provenance_graph", {"artifact_id": "x", "depth": True}),
    ],
)
def test_cross_field_and_range_validation_precede_io(capability, arguments):
    with pytest.raises(ArtisanError) as caught:
        inspect_evidence(capability, arguments)
    assert caught.value.code == ErrorCode.PARAM_TYPE_MISMATCH
    assert "private" not in encode_json(error_payload(caught.value)).decode()


def test_request_normalization_and_context_requirements():
    assert validate_request(
        "query_artifacts", {"metadata_keys": ["z", "a", "z"]}
    ).metadata_keys == ["a", "z"]
    assert validate_request("list_operations", {"query": "TEST"}).query == "test"
    assert validate_request("list_runs", {}).limit == 20
    with pytest.raises(ArtisanError, match="discovery"):
        inspect_evidence("capabilities", {})
    with pytest.raises(ArtisanError) as caught:
        inspect_evidence("list_runs", {})
    assert caught.value.code == ErrorCode.DELTA_ROOT_UNSET


def test_failed_discovery_remains_inspectable_without_store():
    report = DiscoveryReport(
        sources=[],
        operations_count=0,
        errors=[
            DiscoveryError(
                module="broken", error_type="ImportError", error_message="Unavailable"
            )
        ],
    )
    result = inspect_evidence("capabilities", {}, discovery=report)
    assert result.error_count == 1
    assert result.delta_root is None
    assert result.projection_version == "2"
    assert "server_version" not in result.model_dump()


def test_reader_validation_is_an_internal_defect(monkeypatch):
    module = importlib.import_module("artisan.inspection.runs")
    monkeypatch.setattr(
        module, "runs_payload", lambda **kwargs: {"code": "param_type_mismatch"}
    )
    with pytest.raises(ValidationError):
        inspect_evidence("list_runs", {}, delta_root="/store")


def test_missing_store_preserves_native_cause_and_sanitizes_envelope(monkeypatch):
    module = importlib.import_module("artisan.inspection.runs")

    def missing(**kwargs):
        message = "PRIVATE filesystem location"
        raise FileNotFoundError(message)

    monkeypatch.setattr(module, "runs_payload", missing)
    with pytest.raises(ArtisanError) as caught:
        inspect_evidence("list_runs", {}, delta_root="/store")
    assert caught.value.code == ErrorCode.STORE_NOT_FOUND
    assert isinstance(caught.value.__cause__, FileNotFoundError)
    assert "PRIVATE" not in encode_json(error_payload(caught.value)).decode()


def test_known_commit_errors_keep_repair_fields_and_bounded_fallback():
    error = CommitError(
        logical_commit_id="commit",
        table="steps",
        plan_key="plan",
        verified_tables=["steps"],
        staging_objects=["staged"],
        message="Repair required",
    )
    error.__cause__ = RuntimeError("PRIVATE")
    payload = error_payload(error)
    assert payload["plan_key"] == "plan"
    assert payload["staging_objects"] == ["staged"]
    assert "cause" not in payload
    error.staging_objects = ("x" * 60_000,)
    assert error_payload(error)["code"] == ErrorCode.EVIDENCE_RESPONSE_TOO_LARGE


def test_complete_assessment_summary_retains_failures_and_missing_measurements(
    tmp_path,
):
    root = tmp_path / "delta"
    artifacts = [
        MetricArtifact.draft({"score": score}, f"case-{index}.json", 0).finalize()
        for index, score in enumerate([0.9, 0.2, None])
    ]
    selection = append_metric_attempt(root, artifacts)
    result = inspect_evidence(
        "assess_outcomes",
        {
            **selection.model_dump(),
            "spec": {
                "label": "minimum",
                "checks": [
                    {
                        "check_id": "score",
                        "metric_path": ["score"],
                        "operator": "ge",
                        "threshold": 0.8,
                    }
                ],
            },
        },
        delta_root=str(root),
    )
    assert isinstance(result, AssessmentSummary)
    assert result.observation_count == 3
    assert result.assessment_status == "incomplete"
    assert result.checks[0].fail_count == 1
    assert result.checks[0].unassessed_count == 1
    assert result.follow_up is None


@pytest.mark.parametrize(
    "source",
    [
        b"",
        b"\xff",
        b"[]",
        b"{} {}",
        b'{"a": 1, "a": 2}',
        b'{"nested":{"a":1,"a":2}}',
        b'{"a": NaN}',
        b'{"a": Infinity}',
        b'{"a": 1e999}',
    ],
)
def test_strict_json_decoder_rejects_malformed_documents(source):
    with pytest.raises(ValueError, match="valid UTF-8 JSON object"):
        decode_json_object(source)


def test_strict_json_decoder_preserves_unicode_and_numbers():
    source = '{"text":"雪", "value":9007199254740993, "null":null}'
    assert decode_json_object(source.encode()) == {
        "text": "雪",
        "value": 9007199254740993,
        "null": None,
    }


def test_cyclic_request_is_rejected_as_non_json_before_store_access():
    arguments = {}
    arguments["cycle"] = arguments
    with pytest.raises(ArtisanError) as caught:
        inspect_evidence("list_runs", arguments)
    assert caught.value.code == ErrorCode.PARAM_TYPE_MISMATCH
