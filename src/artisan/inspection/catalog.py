"""Fixed inspection definitions and complete generated discovery contracts."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, Field

from artisan import __version__
from artisan.errors import ArtisanError, ErrorCode
from artisan.inspection import (
    artifacts,
    logs,
    models,
    operations,
    outcomes,
    provenance,
    requests,
    runs,
)
from artisan.inspection._evidence import ensure_fits, json_ready


@dataclass(frozen=True)
class InspectionDefinition:
    """Bind one request, response and reader without dynamic public dispatch."""

    name: str
    description: str
    request_type: type[BaseModel]
    response_type: Any
    handler: Callable[..., dict[str, Any]]
    requires_store: bool = True
    requires_discovery: bool = False


ArtifactPage = models.PageResponse[models.ArtifactIdentity | models.ArtifactSelected]
DEFINITIONS = (
    InspectionDefinition(
        "capabilities",
        "Inspect supplied application discovery and package versions.",
        requests.CapabilitiesRequest,
        models.CapabilityResult,
        lambda **kwargs: operations.capability_payload(**kwargs),
        False,
        True,
    ),
    InspectionDefinition(
        "list_operations",
        "List registered operations and their declared roles.",
        requests.ListOperationsRequest,
        models.OperationList,
        lambda **kwargs: operations.operation_list_payload(**kwargs),
        False,
    ),
    InspectionDefinition(
        "describe_operation",
        "Describe one registered operation's complete contract or section.",
        requests.DescribeOperationRequest,
        models.OperationDescription,
        lambda **kwargs: operations.operation_payload(**kwargs),
        False,
    ),
    InspectionDefinition(
        "list_runs",
        "List persisted pipeline runs in deterministic recency order.",
        requests.ListRunsRequest,
        models.PageResponse[models.RunRow],
        lambda **kwargs: runs.runs_payload(**kwargs),
    ),
    InspectionDefinition(
        "get_run_status",
        "Inspect a run rollup and its current step states.",
        requests.RunStatusRequest,
        models.StatusResponse,
        lambda **kwargs: runs.status_payload(**kwargs),
    ),
    InspectionDefinition(
        "diagnose_run",
        "Inspect failure evidence with explicit source coverage.",
        requests.DiagnoseRunRequest,
        models.DiagnosisResult,
        lambda **kwargs: runs.diagnosis_payload(**kwargs),
    ),
    InspectionDefinition(
        "query_artifacts",
        "Inspect accepted artifact references and selected metadata.",
        requests.QueryArtifactsRequest,
        ArtifactPage,
        lambda **kwargs: artifacts.artifact_payload(**kwargs),
    ),
    InspectionDefinition(
        "get_step_result",
        "Inspect a named step's accepted output references.",
        requests.StepResultRequest,
        ArtifactPage,
        lambda **kwargs: artifacts.artifact_payload(**kwargs),
    ),
    InspectionDefinition(
        "get_step_logs",
        "Read a bounded excerpt of a step's failure logs.",
        requests.StepLogsRequest,
        models.LogsResponse,
        lambda **kwargs: logs.logs_payload(**kwargs),
    ),
    InspectionDefinition(
        "get_provenance_graph",
        "Inspect a depth-bounded artifact provenance walk.",
        requests.ProvenanceRequest,
        models.ProvenanceResponse,
        lambda **kwargs: provenance.provenance_payload(**kwargs),
    ),
    InspectionDefinition(
        "assess_outcomes",
        "Assess a complete selected metric population against explicit criteria.",
        requests.AssessmentRequest,
        models.AssessmentResult,
        lambda **kwargs: outcomes.assessment_inspection(**kwargs),
    ),
    InspectionDefinition(
        "compare_runs",
        "Compare declared metric populations using explicit correspondence.",
        requests.ComparisonRequest,
        models.ComparisonResult,
        lambda **kwargs: outcomes.comparison_inspection(**kwargs),
    ),
)


class InspectionDescriptor(models.Projection):
    """Installed read-only capability and its caller-supplied context needs."""

    name: str
    description: str
    requires_store: bool
    requires_discovery: bool
    read_only: Literal[True] = True


class EvidenceExportInterface(models.Projection):
    """Availability of the separate local complete-report writer and loader."""

    format_version: Literal["1"] = "1"
    report_kinds: list[Literal["assessment", "comparison"]] = Field(
        default=["assessment", "comparison"]
    )


class LocalInterfaces(models.Projection):
    """Local interfaces do not add write capabilities to the inspection table."""

    evidence_export: EvidenceExportInterface = Field(
        default_factory=EvidenceExportInterface
    )


class InspectionCatalog(models.Projection):
    """Small installed catalogue requiring no discovery or store access."""

    projection_version: Literal["2"] = "2"
    artisan_version: str = __version__
    inspections: list[InspectionDescriptor]
    local_interfaces: LocalInterfaces = Field(default_factory=LocalInterfaces)


class InspectionDescription(InspectionDescriptor):
    """Complete schemas from the exact validators used by dispatch."""

    projection_version: Literal["2"] = "2"
    artisan_version: str = __version__
    request_schema: dict[str, Any]
    response_schema: dict[str, Any]


def definition(name: str) -> InspectionDefinition:
    """Resolve only installed inspection names, never imports or expressions."""
    for item in DEFINITIONS:
        if name == item.name:
            return item
    raise ArtisanError(
        ErrorCode.UNKNOWN_INSPECTION,
        "Unknown inspection capability",
        error_type="validation",
        field="capability",
        hint="Choose a capability from list_inspections().",
        recovery_hint="CHECK_INPUT",
    )


def _descriptor(item: InspectionDefinition) -> dict[str, Any]:
    return {
        key: getattr(item, key)
        for key in ("name", "description", "requires_store", "requires_discovery")
    }


def list_inspections() -> InspectionCatalog:
    """Return installed inspection metadata without importing applications or reading stores."""
    result = InspectionCatalog(
        inspections=[InspectionDescriptor(**_descriptor(item)) for item in DEFINITIONS]
    )
    ensure_fits(json_ready(result))
    return result


def describe_inspection(name: str) -> InspectionDescription:
    """Return one complete request/success/error schema within the payload limit."""
    item = definition(name)
    result = InspectionDescription(
        **_descriptor(item),
        request_schema=item.request_type.model_json_schema(),
        response_schema=models.output_schema(item.response_type),
    )
    ensure_fits(json_ready(result))
    return result
