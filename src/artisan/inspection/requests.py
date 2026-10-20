"""Authoritative strict requests for the fixed bounded inspection surface."""

from __future__ import annotations

from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from artisan.errors import ErrorCode
from artisan.inspection._boundary import invalid_input
from artisan.inspection._evidence import evidence_error
from artisan.inspection._pagination import reject_cursor
from artisan.visualization.comparison import ComparisonSpec
from artisan.visualization.outcomes import AssessmentSpec, OutcomeSelection

PageLimit = Annotated[int, Field(strict=True, ge=1, le=100)]
TailLines = Annotated[int, Field(strict=True, ge=1, le=1_000)]
ProvenanceDepth = Annotated[int, Field(strict=True, ge=1, le=10)]
CapabilityView = Literal["summary", "sources", "errors", "name_collisions"]
OperationSection = Literal[
    "all", "summary", "inputs", "outputs", "params", "examples", "tags"
]
DiagnosisView = Literal["summary", "failures", "similar_runs", "upstream_edges"]


class Request(BaseModel):
    """Reject coercion and undeclared caller fields before any reads."""

    model_config = ConfigDict(extra="forbid", strict=True, validate_default=True)


class PagedRequest(Request):
    """Shared page controls, excluded from evidence selection identity."""

    limit: PageLimit = 50
    cursor: str | None = None


class CapabilitiesRequest(PagedRequest):
    """Select a startup discovery summary or one complete collection."""

    view: CapabilityView = "summary"

    @model_validator(mode="after")
    def validate_view(self) -> Self:
        if self.view == "summary":
            reject_cursor(self.cursor)
        return self


class ListOperationsRequest(PagedRequest):
    """Select registered operations with normalized case-insensitive queries."""

    query: str | None = None
    kind: Literal["creator", "curator"] | None = None
    tag: str | None = None
    detail: Literal["summary", "identity"] = "summary"

    @field_validator("query")
    @classmethod
    def normalize_query(cls, value: str | None) -> str | None:
        return value.lower() if value is not None else None


class DescribeOperationRequest(PagedRequest):
    """Select complete operation metadata or one paged metadata section."""

    name: str
    section: OperationSection = "all"

    @model_validator(mode="after")
    def validate_section(self) -> Self:
        if self.section in {"all", "summary", "params"}:
            reject_cursor(self.cursor)
        return self


class ListRunsRequest(PagedRequest):
    """Select persisted run rollups."""

    limit: PageLimit = 20


class RunStatusRequest(PagedRequest):
    """Select one run's current status and steps."""

    pipeline_run_id: str


class DiagnoseRunRequest(RunStatusRequest):
    """Select diagnosis evidence without treating status as termination proof."""

    view: DiagnosisView = "summary"
    detail: Literal["full", "identity"] = "full"

    @model_validator(mode="after")
    def validate_view(self) -> Self:
        if self.view == "summary":
            reject_cursor(self.cursor)
        if self.detail == "identity" and self.view != "failures":
            invalid_input(
                "detail", "Identity detail is available only in the failures view"
            )
        return self


class QueryArtifactsRequest(PagedRequest):
    """Select accepted artifact references and exact metadata keys."""

    artifact_type: str | None = None
    pipeline_run_id: str | None = None
    metadata_keys: list[str] | None = None

    @field_validator("metadata_keys")
    @classmethod
    def normalize_keys(cls, value: list[str] | None) -> list[str]:
        return sorted(set(value or []))


class StepResultRequest(QueryArtifactsRequest):
    """Select the first matching step's accepted artifact references."""

    pipeline_run_id: str
    step_name: str


class StepLogsRequest(Request):
    """Request an explicitly bounded failure-log excerpt."""

    pipeline_run_id: str
    step_name: str
    tail_lines: TailLines = 200


class ProvenanceRequest(PagedRequest):
    """Select a depth-bounded walk independently of edge pagination."""

    artifact_id: str
    direction: Literal["backward", "forward"] = "backward"
    depth: ProvenanceDepth = 3


class AssessmentRequest(OutcomeSelection, PagedRequest):
    """Reuse the domain selection contract with explicit assessment criteria."""

    spec: AssessmentSpec
    view: Literal["summary", "observations"] = "summary"

    @model_validator(mode="after")
    def validate_view(self) -> Self:
        _validate_attempts(self.view, self.cursor, self)
        return self


class ComparisonRequest(PagedRequest):
    """Compare declared populations, preserving both exact attempt selectors."""

    baseline: OutcomeSelection
    candidate: OutcomeSelection
    spec: ComparisonSpec
    view: Literal["summary", "pairs", "unmatched", "invalid"] = "summary"

    @model_validator(mode="after")
    def validate_view(self) -> Self:
        _validate_attempts(self.view, self.cursor, self.baseline, self.candidate)
        return self


def _validate_attempts(
    view: str, cursor: str | None, *selections: OutcomeSelection
) -> None:
    if view == "summary":
        reject_cursor(cursor)
    elif cursor is not None and any(item.step_run_id is None for item in selections):
        raise evidence_error(
            ErrorCode.INVALID_EVIDENCE_CURSOR,
            field="cursor",
            hint="Continue with every exact step_run_id from the previous response.",
        )
