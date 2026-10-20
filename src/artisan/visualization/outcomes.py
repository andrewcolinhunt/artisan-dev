"""Assess explicitly selected accepted metrics without rounding their evidence."""

from __future__ import annotations

import json
import math
import operator
from typing import Annotated, Any, Literal, Self, cast

import polars as pl
from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    JsonValue,
    ValidationError,
    model_validator,
)

from artisan.errors import (
    ArtifactIntegrityError,
    AssessmentSpecError,
    OutcomeSelectionError,
)
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_state import StepState
from artisan.storage.core.committed_scan import scan_committed
from artisan.storage.core.run_scope import load_accepted_outputs
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


def _utf8_bound(value: str, maximum: int = 256) -> str:
    if len(value.encode("utf-8")) > maximum:
        msg = f"String exceeds {maximum} UTF-8 bytes"
        raise ValueError(msg)
    return value


def _short_text(value: str) -> str:
    return _utf8_bound(value, 64)


def _number(value: Any) -> int | float:
    if type(value) not in {int, float}:
        msg = "Expected an integer or finite float, excluding bool"
        raise ValueError(msg)
    if isinstance(value, float) and not math.isfinite(value):
        msg = "Expected a finite number"
        raise ValueError(msg)
    return cast(int | float, value)


def _version(value: Any) -> int:
    if type(value) is not int or value != 1:
        msg = "schema_version must be integer 1"
        raise ValueError(msg)
    return value


Numeric = Annotated[int | float, BeforeValidator(_number)]
Label = Annotated[str, Field(strict=True, min_length=1), AfterValidator(_utf8_bound)]
MetadataKey = Annotated[str, Field(strict=True), AfterValidator(_utf8_bound)]
ShortLabel = Annotated[
    str, Field(strict=True, min_length=1), AfterValidator(_short_text)
]
MetricPath = Annotated[list[MetadataKey], Field(min_length=1, max_length=16)]
ArtifactId = Annotated[
    str, Field(strict=True, pattern=r"^[0-9a-f]{32}$", min_length=32, max_length=32)
]
SchemaVersion = Annotated[Literal[1], BeforeValidator(_version)]
CountingBasis = Literal["unique_attempt_role_artifact"]
CheckStatus = Literal["pass", "fail", "unassessed", "evaluation_error"]
AssessmentStatus = Literal["pass", "fail", "incomplete", "unassessed"]


class StrictModel(BaseModel):
    """Reject coercion and unknown fields in observation contracts."""

    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        validate_default=True,
        revalidate_instances="always",
    )


def _validate_spec_size(spec: BaseModel) -> None:
    if len(canonical_json_bytes(spec.model_dump(mode="python"))) > 16 * 1024:
        msg = "Normalized specification exceeds 16 KiB"
        raise ValueError(msg)


def _spec_fingerprint(domain: str, spec: BaseModel) -> str:
    return compute_content_digest(
        canonical_json_bytes({"domain": domain, "spec": spec.model_dump(mode="python")})
    )


class OutcomeSelection(StrictModel):
    """Select a run, named step and role, optionally fixing position or history."""

    pipeline_run_id: Label
    step_name: Label
    role: Label
    step_number: Annotated[int, Field(strict=True, ge=0)] | None = None
    step_run_id: ArtifactId | None = None


class ResolvedOutcomeSelection(OutcomeSelection):
    """Observed exact attempt and its independent lifecycle acknowledgements."""

    step_number: Annotated[int, Field(strict=True, ge=0)]
    step_run_id: ArtifactId
    execution_status: StepStatus
    state_sequence: int
    disposition: StepDisposition | None = None
    cancellation_status: CancellationStatus | None = None


class AssessmentCheck(StrictModel):
    """One literal numeric comparison over a metric JSON object path."""

    check_id: ShortLabel
    metric_path: MetricPath
    operator: Literal["lt", "le", "gt", "ge", "eq", "ne"]
    threshold: Numeric
    unit: MetadataKey | None = None
    evaluator_label: MetadataKey | None = None


class AssessmentSpec(StrictModel):
    """Bounded criteria with optional count equality; no executable expressions."""

    schema_version: SchemaVersion = 1
    label: Label
    checks: Annotated[list[AssessmentCheck], Field(max_length=20)] = Field(
        default_factory=list
    )
    expected_count: Annotated[int, Field(strict=True, ge=0)] | None = None

    @model_validator(mode="after")
    def validate_spec(self) -> Self:
        """Require unique criteria and a bounded canonical specification."""
        if len({check.check_id for check in self.checks}) != len(self.checks):
            msg = "check_id values must be unique"
            raise ValueError(msg)
        _validate_spec_size(self)
        return self


class MetricMeasurement(StrictModel):
    """A requested raw number, or an explicit reason it cannot be evaluated."""

    metric_path: list[str]
    status: Literal["valid", "unassessed", "evaluation_error"]
    value: Numeric | None = None
    reason: str | None = None


class SelectedMetadata(StrictModel):
    """An explicit metadata projection distinguishing absent and present null."""

    status: Literal["present", "missing", "invalid"]
    value: JsonValue = None
    reason: str | None = None


class MetricEvidenceRef(StrictModel):
    """An accepted artifact's exact membership, independent of origin."""

    pipeline_run_id: str
    step_name: str
    step_number: int
    step_run_id: str
    role: str
    artifact_id: str
    origin_step_number: int
    original_name: str | None = None


class MetricObservation(MetricEvidenceRef):
    """Unrounded requested measurements and explicitly selected case metadata."""

    measurements: list[MetricMeasurement]
    metadata: dict[str, SelectedMetadata] = Field(default_factory=dict)


class MetricObservationSet(StrictModel):
    """Complete selected population with a deterministic evidence identity."""

    selection: ResolvedOutcomeSelection
    population_status: Literal["available", "unavailable"]
    role_validation: Literal["validated", "deferred", "unavailable"]
    reason: str | None = None
    reader_version: Literal["1"] = "1"
    metric_paths: list[list[str]]
    metadata_keys: list[str]
    counting_basis: CountingBasis = "unique_attempt_role_artifact"
    observation_count: int | None
    observations: list[MetricObservation]
    evidence_id: str


class AssessmentCheckResult(StrictModel):
    """One criterion's result and the exact numeric evidence used."""

    check_id: str
    metric_path: list[str]
    status: CheckStatus
    value: Numeric | None
    threshold: Numeric
    reason: str | None = None


class AssessmentObservation(MetricEvidenceRef):
    """Criterion results for one complete accepted-artifact membership."""

    checks: list[AssessmentCheckResult]


class AssessmentCheckSummary(StrictModel):
    """Full-population counts, or unavailable counts before assessment."""

    check_id: str
    pass_count: int | None
    fail_count: int | None
    unassessed_count: int | None
    evaluation_error_count: int | None
    evaluated_count: int | None
    coverage: float | None


class CountAssessment(StrictModel):
    """Count equality does not establish subject identity or numeric quality."""

    expected_count: int
    observed_count: int | None
    status: Literal["pass", "fail", "unassessed"]
    reason: str | None = None


class OutcomeAssessment(StrictModel):
    """Read-only criteria report over the complete selected metric role."""

    spec: AssessmentSpec
    spec_id: str
    evidence_id: str
    reader_version: Literal["1"] = "1"
    evaluator_version: Literal["1"] = "1"
    selection: ResolvedOutcomeSelection
    execution_status: StepStatus
    population_status: Literal["available", "unavailable"]
    role_validation: Literal["validated", "deferred", "unavailable"]
    counting_basis: CountingBasis = "unique_attempt_role_artifact"
    observation_count: int | None
    checks: list[AssessmentCheckSummary]
    count_check: CountAssessment | None
    assessment_status: AssessmentStatus
    reason: str | None = None
    observations: list[AssessmentObservation]


class _Projection(StrictModel):
    metric_paths: list[MetricPath]
    metadata_keys: list[MetadataKey]

    @model_validator(mode="after")
    def normalize(self) -> Self:
        self.metric_paths = [
            list(path) for path in sorted({tuple(path) for path in self.metric_paths})
        ]
        self.metadata_keys = sorted(set(self.metadata_keys))
        if len(self.metric_paths) > 40 or len(self.metadata_keys) > 20:
            msg = "At most 40 distinct paths and 20 metadata keys may be selected"
            raise ValueError(msg)
        return self


def _resolve_selection(
    tracker: StepTracker, selection: OutcomeSelection
) -> tuple[StepState, ResolvedOutcomeSelection]:
    states = (
        tracker.load_all_current_states()
        if selection.step_run_id is not None
        else tracker.load_current_states(selection.pipeline_run_id)
    )
    matches = [
        state
        for state in states
        if state.pipeline_run_id == selection.pipeline_run_id
        and state.step_name == selection.step_name
        and (
            selection.step_number is None or state.step_number == selection.step_number
        )
        and (
            selection.step_run_id is None or state.step_run_id == selection.step_run_id
        )
    ]
    if not matches:
        msg = "No attempt matches the supplied run and step"
        raise OutcomeSelectionError(msg)
    if len(matches) != 1:
        msg = "The step name is ambiguous; specify step_number or exact step_run_id"
        raise OutcomeSelectionError(msg)
    state = matches[0]
    if state.replay_of_execution_run_id is not None:
        msg = "Diagnostic replay attempts are not assessable"
        raise OutcomeSelectionError(msg)
    resolved = ResolvedOutcomeSelection(
        **selection.model_dump(exclude={"step_number", "step_run_id"}),
        step_number=state.step_number,
        step_run_id=state.step_run_id,
        execution_status=state.status,
        state_sequence=state.state_sequence,
        disposition=state.disposition,
        cancellation_status=state.cancellation_status,
    )
    return state, resolved


def _population_state(state: StepState, role: str) -> tuple[str, str, str | None]:
    if state.status in {StepStatus.PENDING, StepStatus.RUNNING}:
        return "unavailable", "deferred", "execution_incomplete"
    if state.status not in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}:
        return "unavailable", "unavailable", "execution_has_no_accepted_outputs"
    if role not in state.output_roles:
        msg = "The selected attempt has no such output role"
        raise OutcomeSelectionError(msg)
    if state.output_types.get(role) not in {None, "any", "metric"}:
        msg = "The selected role does not contain metric artifacts"
        raise OutcomeSelectionError(msg)
    return "available", "validated", None


class _DuplicateJSONKey(ValueError):
    pass


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            msg = "Duplicate JSON object key"
            raise _DuplicateJSONKey(msg)
        result[key] = value
    return result


def _hydrate_metric(row: dict[str, Any], origin: int) -> MetricArtifact:
    try:
        if row.get("metadata"):
            metadata = json.loads(row["metadata"], object_pairs_hook=_unique_object)
            if not isinstance(metadata, dict):
                msg = "Metadata must be a JSON object"
                raise ValueError(msg)
        artifact = MetricArtifact.from_row(row)
    except (ValueError, TypeError, RecursionError) as exc:
        msg = "Invalid persisted metric artifact row"
        raise ArtifactIntegrityError(msg) from exc
    if artifact.origin_step_number != origin:
        msg = "Metric origin contradicts accepted membership"
        raise ArtifactIntegrityError(msg)
    return artifact


def _decode_content(content: bytes) -> tuple[dict[str, Any] | None, str | None]:
    try:
        value = json.loads(content.decode("utf-8"), object_pairs_hook=_unique_object)
    except UnicodeError:
        return None, "invalid_utf8"
    except _DuplicateJSONKey:
        return None, "duplicate_object_key"
    except (ValueError, RecursionError):
        return None, "invalid_json"
    if not isinstance(value, dict):
        return None, "non_object_root"
    return value, None


def _measurement(
    values: dict[str, Any] | None, path: list[str], parsing_reason: str | None
) -> MetricMeasurement:
    if parsing_reason is not None:
        return MetricMeasurement(
            metric_path=path, status="evaluation_error", reason=parsing_reason
        )
    value: Any = values
    for key in path:
        if not isinstance(value, dict):
            return _unassessed(path, "non_object_path")
        if key not in value:
            return _unassessed(path, "missing_key")
        value = value[key]
    if value is None:
        return _unassessed(path, "null_value")
    if isinstance(value, (dict, list)):
        return _unassessed(path, "non_scalar_value")
    if type(value) not in {int, float}:
        return _unassessed(path, "non_numeric_value")
    if isinstance(value, float) and not math.isfinite(value):
        return _unassessed(path, "nonfinite_value")
    return MetricMeasurement(metric_path=path, status="valid", value=value)


def _unassessed(path: list[str], reason: str) -> MetricMeasurement:
    return MetricMeasurement(metric_path=path, status="unassessed", reason=reason)


def _selected_metadata(metadata: dict[str, Any], key: str) -> SelectedMetadata:
    if key not in metadata:
        return SelectedMetadata(status="missing", reason="missing_key")
    value = metadata[key]
    try:
        json.dumps(value, allow_nan=False, ensure_ascii=False).encode("utf-8")
    except UnicodeError:
        return SelectedMetadata(status="invalid", reason="invalid_unicode")
    except RecursionError:
        return SelectedMetadata(status="invalid", reason="metadata_depth_exceeded")
    except ValueError:
        return SelectedMetadata(status="invalid", reason="nonfinite_value")
    try:
        return SelectedMetadata(status="present", value=value)
    except ValidationError:
        return SelectedMetadata(status="invalid", reason="unsupported_metadata_value")


def _reference(
    artifact: MetricArtifact, selection: ResolvedOutcomeSelection
) -> dict[str, Any]:
    return {
        **selection.model_dump(
            include={
                "pipeline_run_id",
                "step_name",
                "step_number",
                "step_run_id",
                "role",
            }
        ),
        "artifact_id": artifact.artifact_id,
        "origin_step_number": artifact.origin_step_number,
        "original_name": artifact.original_name,
    }


def _read_observations(
    delta_root: str,
    selection: ResolvedOutcomeSelection,
    projection: _Projection,
    storage: StorageConfig,
) -> list[MetricObservation]:
    fs, options = storage.filesystem(), storage.delta_storage_options()
    membership = load_accepted_outputs(
        delta_root,
        fs=fs,
        storage_options=options,
        pipeline_run_id=selection.pipeline_run_id,
        step_run_id=selection.step_run_id,
        role=selection.role,
    )
    if membership.is_empty():
        return []
    if membership.filter(pl.col("artifact_type") != "metric").height:
        msg = "Every artifact in the selected role must be a metric"
        raise OutcomeSelectionError(msg)
    origins = dict(membership.select("artifact_id", "origin_step_number").iter_rows())
    rows = (
        scan_committed(
            delta_root,
            ArtifactTypeDef.get_table_path("metric"),
            fs=fs,
            storage_options=options,
        )
        .filter(pl.col("artifact_id").is_in(list(origins)))
        .collect()
    )
    counts = rows.group_by("artifact_id").len()
    if (
        set(counts["artifact_id"]) != set(origins)
        or counts.filter(pl.col("len") != 1).height
    ):
        msg = "Accepted metric rows are missing or duplicated"
        raise ArtifactIntegrityError(msg)
    observations = []
    for row in rows.sort("artifact_id").iter_rows(named=True):
        artifact = _hydrate_metric(row, origins[row["artifact_id"]])
        assert artifact.content is not None
        values, reason = (
            _decode_content(artifact.content)
            if projection.metric_paths
            else (None, None)
        )
        observations.append(
            MetricObservation(
                **_reference(artifact, selection),
                measurements=[
                    _measurement(values, path, reason)
                    for path in projection.metric_paths
                ],
                metadata={
                    key: _selected_metadata(artifact.metadata, key)
                    for key in projection.metadata_keys
                },
            )
        )
    return observations


def read_metric_observations(
    delta_root: str,
    selection: OutcomeSelection,
    metric_paths: list[list[str]],
    *,
    metadata_keys: list[str] | None = None,
    storage: StorageConfig | None = None,
) -> MetricObservationSet:
    """Read precise accepted metric evidence from one exact step attempt.

    Args:
        delta_root: Store containing Artisan's committed tables.
        selection: Explicit run, step and metric-capable output role.
        metric_paths: At most 40 distinct literal object-key paths.
        metadata_keys: At most 20 explicit top-level artifact metadata keys.
        storage: Filesystem and storage options; local by default.

    Returns:
        Complete observations or an unavailable population with lifecycle evidence.

    Raises:
        OutcomeSelectionError: Request is invalid, ambiguous or not a metric role.
        ArtifactIntegrityError: Selected persisted metric evidence is inconsistent.
    """
    try:
        selection = OutcomeSelection.model_validate(selection)
        projection = _Projection(
            metric_paths=metric_paths,
            metadata_keys=[] if metadata_keys is None else metadata_keys,
        )
    except ValidationError as exc:
        msg = "Invalid observation selection or projection"
        raise OutcomeSelectionError(msg) from exc
    storage = storage or StorageConfig()
    tracker = StepTracker(
        delta_root,
        fs=storage.filesystem(),
        storage_options=storage.delta_storage_options(),
    )
    state, resolved = _resolve_selection(tracker, selection)
    population, role_validation, reason = _population_state(state, selection.role)
    observations = (
        _read_observations(delta_root, resolved, projection, storage)
        if population == "available"
        else []
    )
    payload = {
        "selection": resolved.model_dump(mode="json"),
        "population_status": population,
        "role_validation": role_validation,
        "reason": reason,
        "reader_version": "1",
        **projection.model_dump(),
        "counting_basis": "unique_attempt_role_artifact",
        "observation_count": len(observations) if population == "available" else None,
        "observations": [
            observation.model_dump(mode="json") for observation in observations
        ],
    }
    evidence_id = compute_content_digest(
        canonical_json_bytes({"domain": "metric-observations-v1", **payload})
    )
    return MetricObservationSet(
        **{**payload, "selection": resolved, "observations": observations},
        evidence_id=evidence_id,
    )


def _check_result(
    measurement: MetricMeasurement, check: AssessmentCheck
) -> AssessmentCheckResult:
    status: CheckStatus
    if measurement.status == "valid":
        assert measurement.value is not None
        status = (
            "pass"
            if getattr(operator, check.operator)(measurement.value, check.threshold)
            else "fail"
        )
    else:
        status = measurement.status
    return AssessmentCheckResult(
        check_id=check.check_id,
        metric_path=check.metric_path,
        status=status,
        value=measurement.value,
        threshold=check.threshold,
        reason=measurement.reason,
    )


def _assessed_observations(
    source: MetricObservationSet, spec: AssessmentSpec
) -> list[AssessmentObservation]:
    result = []
    for observation in source.observations:
        measurements = {
            tuple(item.metric_path): item for item in observation.measurements
        }
        if any(tuple(check.metric_path) not in measurements for check in spec.checks):
            msg = "Assessment paths were not loaded in this evidence"
            raise AssessmentSpecError(msg)
        result.append(
            AssessmentObservation(
                **observation.model_dump(exclude={"measurements", "metadata"}),
                checks=[
                    _check_result(measurements[tuple(check.metric_path)], check)
                    for check in spec.checks
                ],
            )
        )
    return result


def _check_summary(
    check_id: str, rows: list[AssessmentObservation], count: int | None
) -> AssessmentCheckSummary:
    statuses: list[str] = [
        check.status
        for row in rows
        for check in row.checks
        if check.check_id == check_id
    ]
    counts = {
        f"{status}_count": None if count is None else statuses.count(status)
        for status in ("pass", "fail", "unassessed", "evaluation_error")
    }
    evaluated = (
        None if count is None else statuses.count("pass") + statuses.count("fail")
    )
    return AssessmentCheckSummary(
        check_id=check_id,
        **counts,
        evaluated_count=evaluated,
        coverage=evaluated / count if count and evaluated is not None else None,
    )


def _assessment_status(
    source: MetricObservationSet,
    spec: AssessmentSpec,
    checks: list[AssessmentCheckSummary],
    count: CountAssessment | None,
) -> tuple[AssessmentStatus, str | None]:
    if source.population_status == "unavailable":
        return "unassessed", source.reason
    if not spec.checks and spec.expected_count is None:
        return "unassessed", "no_criteria"
    if source.observation_count == 0:
        return "unassessed", "empty_population"
    if any(check.unassessed_count or check.evaluation_error_count for check in checks):
        return "incomplete", "incomplete_measurements"
    if any(check.fail_count for check in checks) or (count and count.status == "fail"):
        return "fail", "criteria_failed"
    return "pass", None


def _evaluate_observations(
    source: MetricObservationSet, spec: AssessmentSpec
) -> OutcomeAssessment:
    """Evaluate an already loaded population without a second store read."""
    try:
        spec = AssessmentSpec.model_validate(spec)
    except ValidationError as exc:
        msg = "Invalid assessment specification"
        raise AssessmentSpecError(msg) from exc
    rows = _assessed_observations(source, spec)
    checks = [
        _check_summary(check.check_id, rows, source.observation_count)
        for check in spec.checks
    ]
    count = None
    if spec.expected_count is not None:
        count = CountAssessment(
            expected_count=spec.expected_count,
            observed_count=source.observation_count,
            status=(
                "unassessed"
                if source.observation_count is None
                else "pass"
                if source.observation_count == spec.expected_count
                else "fail"
            ),
            reason=source.reason if source.observation_count is None else None,
        )
    status, reason = _assessment_status(source, spec, checks, count)
    return OutcomeAssessment(
        spec=spec,
        spec_id=_spec_fingerprint("outcome-spec-v1", spec),
        evidence_id=source.evidence_id,
        selection=source.selection,
        execution_status=source.selection.execution_status,
        population_status=source.population_status,
        role_validation=source.role_validation,
        observation_count=source.observation_count,
        checks=checks,
        count_check=count,
        assessment_status=status,
        reason=reason,
        observations=rows,
    )


def assess_outcomes(
    delta_root: str,
    selection: OutcomeSelection,
    spec: AssessmentSpec,
    *,
    storage: StorageConfig | None = None,
) -> OutcomeAssessment:
    """Assess the complete selected metric role under explicit numeric criteria.

    Args:
        delta_root: Store containing Artisan's committed tables.
        selection: Run, step and output role; exact history is optional.
        spec: Bounded numeric checks and optional expected observation count.
        storage: Filesystem and storage options; local by default.

    Returns:
        Exact criterion results with population coverage and execution state.

    Raises:
        AssessmentSpecError: Criteria are invalid, before any storage read.
        OutcomeSelectionError: The selected attempt/role is invalid or ambiguous.
        ArtifactIntegrityError: Selected metric evidence fails integrity checks.
    """
    try:
        spec = AssessmentSpec.model_validate(spec)
    except ValidationError as exc:
        msg = "Invalid assessment specification"
        raise AssessmentSpecError(msg) from exc
    source = read_metric_observations(
        delta_root,
        selection,
        [check.metric_path for check in spec.checks],
        storage=storage,
    )
    return _evaluate_observations(source, spec)
