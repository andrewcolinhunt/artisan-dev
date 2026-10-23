"""Typed inspection projections and schemas, independent of protocol result objects."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, JsonValue, TypeAdapter

from artisan.inspection._boundary import CommitErrorPayload, ErrorPayload
from artisan.operations.base.operation_example import OperationExample
from artisan.orchestration.run_status import RunStepStatus
from artisan.registry.models import (
    DiscoveryError,
    DiscoverySource,
    InputSpecMetadata,
    NameCollision,
    OperationMetadata,
    OperationSummary,
    OutputSpecMetadata,
)
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.visualization.comparison import (
    ComparisonCoverage,
    ComparisonDiagnostic,
    ComparisonMetricSummary,
    ComparisonPair,
    ComparisonSide,
    ComparisonSpec,
    ComparisonStatus,
)
from artisan.visualization.inspect import DiagnosisUpstreamScope
from artisan.visualization.outcomes import (
    AssessmentCheckSummary,
    AssessmentObservation,
    AssessmentSpec,
    CountAssessment,
    CountingBasis,
    ResolvedOutcomeSelection,
)


class Projection(BaseModel):
    """A concrete transport projection rejects unexpected fields."""

    model_config = ConfigDict(extra="forbid")


class FollowUp(Projection):
    """A portable capability invocation continuing the selected evidence."""

    capability: str
    arguments: dict[str, JsonValue]


class Response(Projection):
    """Transport contract version, distinct from a registry schema version."""

    projection_version: Literal["2"] = "2"
    follow_up: FollowUp | None = None


class Page[T](Projection):
    """One selected collection; totals precede response pagination."""

    items: list[T]
    returned_items: int
    total_items: int | None
    has_more: bool
    next_cursor: str | None
    view_id: str
    omitted_fields: list[str]


class PageResponse[T](Page[T], Response):
    """A response whose sole paged collection is top-level items."""


class OperationIdentity(Projection):
    """Smallest useful registered-operation identity."""

    name: str
    kind: Literal["creator", "curator"]


class OperationList(PageResponse[OperationSummary | OperationIdentity]):
    """Selected catalogue summaries or identities."""

    detail: Literal["summary", "identity"]


class CapabilityBase(Response):
    """Installed package identity and bounded discovery counts."""

    artisan_version: str
    read_only: Literal[True]
    delta_root: None = None
    operations_count: int
    source_count: int
    error_count: int
    name_collision_count: int


class CapabilitySummary(CapabilityBase):
    """Discovery summary without arbitrary module/exception lists."""

    view: Literal["summary"] = "summary"


class CapabilitySources(CapabilityBase, Page[DiscoverySource]):
    """One page of imported discovery sources."""

    view: Literal["sources"] = "sources"


class CapabilityErrors(CapabilityBase, Page[DiscoveryError]):
    """One page of discovery import errors."""

    view: Literal["errors"] = "errors"


class CapabilityCollisions(CapabilityBase, Page[NameCollision]):
    """One page of operation-name collisions."""

    view: Literal["name_collisions"] = "name_collisions"


CapabilityResult = (
    CapabilitySummary | CapabilitySources | CapabilityErrors | CapabilityCollisions
)


class OperationAll(OperationMetadata, Response):
    """Complete registry metadata in a versioned inspection projection."""

    section: Literal["all"] = "all"


class OperationHeader(Response):
    """Identity needed to interpret one metadata section."""

    name: str
    kind: Literal["creator", "curator"]
    source_module: str


class OperationOverview(OperationHeader):
    """Operation identity, description and section counts."""

    section: Literal["summary"] = "summary"
    description: str
    input_count: int
    output_count: int
    example_count: int
    tag_count: int


class OperationParameters(OperationHeader):
    """An indivisible parameter schema retaining its references."""

    section: Literal["params"] = "params"
    params_schema: dict[str, JsonValue]


class InputRole(Projection):
    """One named input contract."""

    role: str
    spec: InputSpecMetadata


class OutputRole(Projection):
    """One named output contract."""

    role: str
    spec: OutputSpecMetadata


class OperationInputs(OperationHeader, Page[InputRole]):
    """One page of named input contracts."""

    section: Literal["inputs"] = "inputs"


class OperationOutputs(OperationHeader, Page[OutputRole]):
    """One page of named output contracts."""

    section: Literal["outputs"] = "outputs"


class OperationExamples(OperationHeader, Page[OperationExample]):
    """One page of declared usage examples."""

    section: Literal["examples"] = "examples"


class OperationTags(OperationHeader, Page[str]):
    """One page of operation tags."""

    section: Literal["tags"] = "tags"


OperationDescription = (
    OperationAll
    | OperationOverview
    | OperationParameters
    | OperationInputs
    | OperationOutputs
    | OperationExamples
    | OperationTags
)


class ArtifactIdentity(Projection):
    """Artifact occurrence with explicitly selected metadata only."""

    artifact_id: str
    artifact_type: str
    origin_step_number: int
    current_step_number: int | None
    pipeline_run_id: str | None


class ArtifactSelected(ArtifactIdentity):
    """An artifact occurrence carrying only requested metadata keys."""

    metadata: dict[str, JsonValue]


class RunRow(Projection):
    """An authoritative run rollup, not an outcome assessment."""

    pipeline_run_id: str
    step_count: int
    last_status: str | None
    started_at: datetime | None
    ended_at: datetime | None


class StatusResponse(RunRow, Response):
    """A run rollup and one page of current steps."""

    steps: Page[RunStepStatus]


class Edge(Projection):
    """One directed artifact provenance edge."""

    source_artifact_id: str
    target_artifact_id: str


class ProvenanceResponse(Response):
    """Paged selected edges with a separate depth-coverage flag."""

    artifact_id: str
    direction: Literal["backward", "forward"]
    depth: int
    traversal_truncated: bool
    edges: Page[Edge]


class FailureIdentity(Projection):
    """Failure reference without potentially large diagnostics."""

    step: int
    operation: str
    execution_run_id: str
    timestamp_start: datetime
    code: str | None
    recovery_hint: str | None
    field: str | None
    log: str


class FailureRow(FailureIdentity):
    """Failure identity plus its complete selected diagnostics."""

    suggestions: list[str] | None
    error: str | None


class DiagnosisHeader(Response):
    """Coverage facts shared by every diagnosis view."""

    pipeline_run_id: str
    last_status: str | None
    failed_execution_count: int
    affected_step_count: int
    similar_run_count: int
    matching_failed_run_count: int
    similar_run_limit: int
    upstream_edge_count: int
    upstream_scope: DiagnosisUpstreamScope


class DiagnosisSummary(DiagnosisHeader):
    """Diagnosis counts and advisory actions without hidden evidence samples."""

    view: Literal["summary"] = "summary"
    suggested_actions: list[str]
    detail_views: list[Literal["failures", "similar_runs", "upstream_edges"]]
    evidence_note: str


class DiagnosisFailures(DiagnosisHeader, Page[FailureRow | FailureIdentity]):
    """One page of selected failed executions."""

    view: Literal["failures"] = "failures"
    detail: Literal["full", "identity"]


class DiagnosisRuns(DiagnosisHeader, Page[RunRow]):
    """A page within the most recent matching failed-run window."""

    view: Literal["similar_runs"] = "similar_runs"


class DiagnosisEdges(DiagnosisHeader, Page[Edge]):
    """A page within the selected seed/depth provenance union."""

    view: Literal["upstream_edges"] = "upstream_edges"


DiagnosisResult = DiagnosisSummary | DiagnosisFailures | DiagnosisRuns | DiagnosisEdges


class LogsResponse(Response):
    """A bounded failure-log excerpt, never a complete-log assertion."""

    lines: list[str]
    truncated: bool
    truncation_reasons: list[Literal["bytes", "lines", "files"]]
    partial_first_line: bool


class AssessmentFacts(Projection):
    """Full-population assessment facts without criterion or observation copies."""

    spec_id: str
    evidence_id: str
    reader_version: Literal["1"]
    evaluator_version: Literal["1"]
    selection: ResolvedOutcomeSelection
    execution_status: StepStatus
    population_status: Literal["available", "unavailable"]
    role_validation: Literal["validated", "deferred", "unavailable"]
    counting_basis: CountingBasis
    observation_count: int | None
    checks: list[AssessmentCheckSummary]
    count_check: CountAssessment | None
    assessment_status: Literal["pass", "fail", "incomplete", "unassessed"]
    reason: str | None


class AssessmentHeader(AssessmentFacts, Response):
    """Declared criteria and aggregate facts shared by assessment views."""

    spec: AssessmentSpec


class AssessmentSummary(AssessmentHeader):
    """Complete assessment aggregates without supporting observation rows."""

    view: Literal["summary"] = "summary"


class AssessmentObservations(AssessmentHeader, Page[AssessmentObservation]):
    """One evidence page; aggregates still describe the complete population."""

    view: Literal["observations"] = "observations"


AssessmentResult = AssessmentSummary | AssessmentObservations


class ComparisonHeader(Response):
    """Complete comparison aggregates; each optional assessment shares one spec."""

    spec: ComparisonSpec
    spec_id: str
    evidence_id: str
    reader_version: str
    evaluator_version: str
    comparability_basis: Literal["caller_declared"]
    counting_basis: CountingBasis
    baseline: ComparisonSide
    candidate: ComparisonSide
    comparison_status: ComparisonStatus
    coverage: ComparisonCoverage
    metrics: list[ComparisonMetricSummary]
    assessment_population: Literal["complete_selected_role"] | None
    baseline_assessment: AssessmentFacts | None
    candidate_assessment: AssessmentFacts | None


class ComparisonSummary(ComparisonHeader):
    """Comparison counts and numeric summaries without supporting rows."""

    view: Literal["summary"] = "summary"


class ComparisonPairs(ComparisonHeader, Page[ComparisonPair]):
    """One page of structural matches, including incomparable measurements."""

    view: Literal["pairs"] = "pairs"


class ComparisonUnmatched(ComparisonHeader, Page[ComparisonDiagnostic]):
    """One page of observations outside the explicit correspondence."""

    view: Literal["unmatched"] = "unmatched"


class ComparisonInvalid(ComparisonHeader, Page[ComparisonDiagnostic]):
    """One page of invalid or ambiguous matching keys."""

    view: Literal["invalid"] = "invalid"


ComparisonResult = (
    ComparisonSummary | ComparisonPairs | ComparisonUnmatched | ComparisonInvalid
)


def output_schema(success_type: Any) -> dict[str, Any]:
    """Generate the actual success/error schema advertised to MCP clients."""
    schema = TypeAdapter(success_type | ErrorPayload | CommitErrorPayload).json_schema()
    return {**schema, "type": "object"}
