"""Discovery advertises the exact fixed request and response contracts."""

from __future__ import annotations

import jsonschema
import pytest

from artisan.errors import ArtisanError, ErrorCode
from artisan.inspection._boundary import error_payload
from artisan.inspection._evidence import MAX_PAYLOAD_BYTES, encode_json
from artisan.inspection.api import inspect_evidence
from artisan.inspection.catalog import (
    DEFINITIONS,
    describe_inspection,
    list_inspections,
)
from artisan.inspection.models import output_schema
from artisan.registry.models import DiscoveryReport

_NAMES = {
    "capabilities",
    "list_operations",
    "describe_operation",
    "list_runs",
    "get_run_status",
    "diagnose_run",
    "query_artifacts",
    "get_step_result",
    "get_step_logs",
    "get_provenance_graph",
    "assess_outcomes",
    "compare_runs",
}


def test_fixed_catalogue_context_and_separate_local_export():
    result = list_inspections()
    assert {item.name for item in result.inspections} == _NAMES
    assert len(result.inspections) == 12
    assert result.projection_version == "2"
    assert all(item.read_only for item in result.inspections)
    assert {item.name for item in result.inspections if not item.requires_store} == {
        "capabilities",
        "list_operations",
        "describe_operation",
    }
    assert {item.name for item in result.inspections if item.requires_discovery} == {
        "capabilities"
    }
    assert result.local_interfaces.model_dump() == {
        "evidence_export": {
            "format_version": "1",
            "report_kinds": ["assessment", "comparison"],
        }
    }


@pytest.mark.parametrize("item", DEFINITIONS, ids=lambda item: item.name)
def test_complete_descriptions_fit_and_use_exact_runtime_models(item):
    result = describe_inspection(item.name)
    assert result.request_schema == item.request_type.model_json_schema()
    assert result.response_schema == output_schema(item.response_type)
    assert len(encode_json(result)) <= MAX_PAYLOAD_BYTES
    assert "$defs" in result.response_schema
    jsonschema.Draft202012Validator.check_schema(result.request_schema)
    jsonschema.Draft202012Validator.check_schema(result.response_schema)
    error = ArtisanError(
        ErrorCode.PARAM_TYPE_MISMATCH, "Invalid request", error_type="validation"
    )
    jsonschema.validate(error_payload(error), result.response_schema)


def test_discovery_schema_validates_actual_success():
    result = inspect_evidence(
        "capabilities", {}, discovery=DiscoveryReport(sources=[], operations_count=0)
    )
    jsonschema.validate(
        result.model_dump(mode="json"),
        describe_inspection("capabilities").response_schema,
    )


def test_describe_unknown_inspection_is_known_failure():
    with pytest.raises(ArtisanError) as caught:
        describe_inspection("unknown")
    assert caught.value.code == ErrorCode.UNKNOWN_INSPECTION
