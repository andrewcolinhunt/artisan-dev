"""Strict contracts for complete local evidence bundles and bounded receipts."""

from __future__ import annotations

from datetime import timedelta
from typing import Annotated, Any, Literal, Self

from pydantic import AwareDatetime, Field, field_validator, model_validator

from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.visualization.comparison import (
    ComparisonCoverage,
    ComparisonSide,
    ComparisonStatus,
)
from artisan.visualization.outcomes import (
    AssessmentCheckSummary,
    AssessmentStatus,
    CountAssessment,
    CountingBasis,
    ResolvedOutcomeSelection,
    StrictModel,
)

type ReportKind = Literal["assessment", "comparison"]
type CollectionCount = Annotated[int, Field(ge=0)]
type Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]


class AssessmentMetadata(StrictModel):
    """Assessment identities and coverage copied without reevaluation."""

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
    assessment_status: AssessmentStatus
    reason: str | None


class ComparisonMetadata(StrictModel):
    """Comparison identities, pairing coverage and optional assessment metadata."""

    spec_id: str
    evidence_id: str
    reader_version: Literal["1"]
    evaluator_version: Literal["1"]
    comparability_basis: Literal["caller_declared"]
    counting_basis: CountingBasis
    baseline: ComparisonSide
    candidate: ComparisonSide
    comparison_status: ComparisonStatus
    coverage: ComparisonCoverage
    assessment_population: Literal["complete_selected_role"] | None
    baseline_assessment: AssessmentMetadata | None
    candidate_assessment: AssessmentMetadata | None


class AssessmentCollectionCounts(StrictModel):
    """Stored detail count, independent of population availability."""

    observations: CollectionCount


class ComparisonCollectionCounts(StrictModel):
    """Stored list lengths; absent nested assessments retain null counts."""

    pairs: CollectionCount
    unmatched: CollectionCount
    invalid: CollectionCount
    baseline_assessment_observations: CollectionCount | None
    candidate_assessment_observations: CollectionCount | None


class ExportFileIdentity(StrictModel):
    """Exact file-byte identity, separate from semantic evidence identity."""

    digest_algorithm: Literal["xxh3_128"] = "xxh3_128"
    digest: Digest
    size_bytes: CollectionCount


class _ExportRecords(StrictModel):
    format_version: Literal["1"] = "1"
    report_kind: ReportKind
    report_metadata: AssessmentMetadata | ComparisonMetadata
    collection_counts: AssessmentCollectionCounts | ComparisonCollectionCounts
    report_complete: Literal[True] = True

    @model_validator(mode="after")
    def _matching_records(self) -> Self:
        expected = (
            (AssessmentMetadata, AssessmentCollectionCounts)
            if self.report_kind == "assessment"
            else (ComparisonMetadata, ComparisonCollectionCounts)
        )
        if not isinstance(self.report_metadata, expected[0]) or not isinstance(
            self.collection_counts, expected[1]
        ):
            msg = "Export records must match the report kind"
            raise ValueError(msg)
        return self


class EvidenceExportManifest(_ExportRecords):
    """Completion marker for the fixed report and archival-schema files."""

    artisan_version: str
    exported_at: AwareDatetime
    capture_consistency: Literal["reader_defined"] = "reader_defined"
    files: dict[Literal["report.json", "schema.json"], ExportFileIdentity]

    @field_validator("files")
    @classmethod
    def _fixed_files(
        cls, value: dict[Literal["report.json", "schema.json"], ExportFileIdentity]
    ) -> dict[Literal["report.json", "schema.json"], ExportFileIdentity]:
        if set(value) != {"report.json", "schema.json"}:
            msg = "An export must declare exactly report.json and schema.json"
            raise ValueError(msg)
        return value

    @field_validator("exported_at")
    @classmethod
    def _utc_timestamp(cls, value: AwareDatetime) -> AwareDatetime:
        if value.utcoffset() != timedelta(0):
            msg = "Export timestamp must use UTC"
            raise ValueError(msg)
        return value


class EvidenceExportReceipt(_ExportRecords):
    """Bounded result of a completed explicit local export.

    The summary is complete when included. An omitted summary is marked
    ``omitted_size_limit``; all report details remain in the verified bundle.
    ``report_complete`` describes saved report fields, not source coverage.
    """

    destination: str
    manifest_path: str
    manifest_digest_algorithm: Literal["xxh3_128"] = "xxh3_128"
    manifest_digest: Digest
    summary: dict[str, Any] | None
    summary_status: Literal["included", "omitted_size_limit"]

    @model_validator(mode="after")
    def _summary_presence(self) -> Self:
        if (self.summary is not None) != (self.summary_status == "included"):
            msg = "Summary presence must match summary_status"
            raise ValueError(msg)
        return self
