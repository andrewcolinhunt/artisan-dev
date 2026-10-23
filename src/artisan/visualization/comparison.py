"""Read-only paired comparison of explicitly selected metric populations."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from fractions import Fraction
from typing import Annotated, Any, Literal

from pydantic import Field, ValidationError, field_validator, model_validator

from artisan.errors import ComparisonSpecError, OutcomeSelectionError
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest
from artisan.visualization.outcomes import (
    ArtifactId,
    AssessmentSpec,
    Label,
    MetadataKey,
    MetricEvidenceRef,
    MetricMeasurement,
    MetricObservation,
    MetricObservationSet,
    MetricPath,
    Numeric,
    OutcomeAssessment,
    OutcomeSelection,
    ResolvedOutcomeSelection,
    SchemaVersion,
    ShortLabel,
    StrictModel,
    _evaluate_observations,
    _spec_fingerprint,
    _validate_spec_size,
    read_metric_observations,
)

COMPARISON_EVALUATOR_VERSION = "1"
type CaseKey = int | str
type Direction = Literal["higher", "lower", "neutral"]
type Classification = Literal["improved", "regressed", "unchanged", "changed"]
type ComparisonStatus = Literal["unavailable", "ambiguous", "no_pairs", "compared"]
type _Side = Literal["baseline", "candidate"]
_SIDES: tuple[_Side, _Side] = ("baseline", "candidate")


class ComparisonMetric(StrictModel):
    """One declared measurement and its directional comparison rule."""

    metric_id: ShortLabel
    metric_path: MetricPath
    direction: Direction
    absolute_tolerance: Numeric = 0
    measurement_id: Label
    measurement_version: Label
    unit: Label

    @field_validator("absolute_tolerance")
    @classmethod
    def _nonnegative_tolerance(cls, value: Numeric) -> Numeric:
        if value < 0:
            msg = "absolute_tolerance must be nonnegative"
            raise ValueError(msg)
        return value


class MetadataKeyMatch(StrictModel):
    """Match observations by one exact, typed artifact metadata value."""

    kind: Literal["metadata_key"] = "metadata_key"
    key: MetadataKey


class ExplicitPair(StrictModel):
    """One caller-declared correspondence between selected artifacts."""

    case_label: ShortLabel
    baseline_artifact_id: ArtifactId
    candidate_artifact_id: ArtifactId


class ExplicitPairsMatch(StrictModel):
    """A bounded one-to-one correspondence, sorted by case label."""

    kind: Literal["explicit_pairs"] = "explicit_pairs"
    pairs: list[ExplicitPair] = Field(min_length=1, max_length=100)

    @field_validator("pairs")
    @classmethod
    def _unique_pairs(cls, pairs: list[ExplicitPair]) -> list[ExplicitPair]:
        for name in ("case_label", "baseline_artifact_id", "candidate_artifact_id"):
            values = [getattr(pair, name) for pair in pairs]
            if len(values) != len(set(values)):
                msg = f"Explicit pairs require unique {name} values"
                raise ValueError(msg)
        return sorted(pairs, key=lambda pair: pair.case_label)


class ComparisonSpec(StrictModel):
    """Bounded comparison declarations; these do not verify comparability."""

    schema_version: SchemaVersion = 1
    label: Label
    matching: Annotated[
        MetadataKeyMatch | ExplicitPairsMatch, Field(discriminator="kind")
    ]
    metrics: list[ComparisonMetric] = Field(min_length=1, max_length=20)
    assessment: AssessmentSpec | None = None

    @model_validator(mode="after")
    def _validate_spec(self) -> ComparisonSpec:
        ids = [metric.metric_id for metric in self.metrics]
        if len(ids) != len(set(ids)):
            msg = "Comparison metric IDs must be unique"
            raise ValueError(msg)
        _validate_spec_size(self)
        return self


class ComparisonSide(StrictModel):
    """Resolved state and evidence identity for one complete population."""

    selection: ResolvedOutcomeSelection
    population_status: Literal["available", "unavailable"]
    role_validation: Literal["validated", "deferred", "unavailable"]
    observation_count: int | None
    reason: str | None
    evidence_id: str


class ComparisonCoverage(StrictModel):
    """Disjoint membership categories and structural pairing coverage."""

    matched_pair_count: int | None = None
    baseline_unmatched_count: int | None = None
    candidate_unmatched_count: int | None = None
    baseline_invalid_key_count: int | None = None
    candidate_invalid_key_count: int | None = None
    baseline_ambiguous_count: int | None = None
    candidate_ambiguous_count: int | None = None
    ambiguous_key_count: int | None = None
    baseline_structural_coverage: float | None = None
    candidate_structural_coverage: float | None = None


class ComparisonMetricResult(StrictModel):
    """One metric's exact-evidence comparison within a structural pair."""

    metric_id: str
    baseline: MetricMeasurement
    candidate: MetricMeasurement
    comparison_status: Literal["comparable", "incomparable", "not_evaluated"]
    reason: str | None = None
    delta: Numeric | None = None
    delta_rounded: bool | None = None
    classification: Classification | None = None


class ComparisonPair(StrictModel):
    """A structural match with both exact references and metric results."""

    key: CaseKey | None = None
    case_label: str | None = None
    baseline: MetricEvidenceRef
    candidate: MetricEvidenceRef
    metrics_evaluation: Literal["evaluated", "not_evaluated"]
    metrics: list[ComparisonMetricResult]


class ComparisonDiagnostic(StrictModel):
    """One unmatched, invalid-key or ambiguous-key observation."""

    side: _Side
    category: Literal["unmatched", "invalid_key", "ambiguous_key"]
    observation: MetricEvidenceRef
    reason: str
    key: CaseKey | None = None


class ComparisonMetricSummary(StrictModel):
    """Full-pair numeric coverage and classifications for one metric."""

    metric_id: str
    comparable_count: int | None = None
    incomparable_count: int | None = None
    improved_count: int | None = None
    regressed_count: int | None = None
    unchanged_count: int | None = None
    changed_count: int | None = None
    paired_numeric_coverage: float | None = None
    baseline_numeric_coverage: float | None = None
    candidate_numeric_coverage: float | None = None
    mean_delta: Numeric | None = None
    mean_delta_rounded: bool | None = None
    mean_status: Literal[
        "available", "arithmetic_overflow", "no_comparable_pairs", "not_evaluated"
    ] = "not_evaluated"
    reason: str | None = None


class RunComparison(StrictModel):
    """A reproducible comparison of complete selected populations."""

    spec: ComparisonSpec
    spec_id: str
    evidence_id: str
    reader_version: str
    evaluator_version: str = COMPARISON_EVALUATOR_VERSION
    comparability_basis: Literal["caller_declared"] = "caller_declared"
    counting_basis: Literal["unique_attempt_role_artifact"] = (
        "unique_attempt_role_artifact"
    )
    baseline: ComparisonSide
    candidate: ComparisonSide
    comparison_status: ComparisonStatus
    coverage: ComparisonCoverage
    metrics: list[ComparisonMetricSummary]
    pairs: list[ComparisonPair]
    unmatched: list[ComparisonDiagnostic]
    invalid: list[ComparisonDiagnostic]
    assessment_population: Literal["complete_selected_role"] | None = None
    baseline_assessment: OutcomeAssessment | None = None
    candidate_assessment: OutcomeAssessment | None = None


@dataclass
class _StructuralPair:
    baseline: MetricObservation
    candidate: MetricObservation
    key: CaseKey | None = None
    case_label: str | None = None


@dataclass
class _Matches:
    pairs: list[_StructuralPair] = field(default_factory=list)
    unmatched: list[ComparisonDiagnostic] = field(default_factory=list)
    invalid: list[ComparisonDiagnostic] = field(default_factory=list)
    ambiguous_key_count: int = 0


def compare_runs(
    delta_root: str,
    baseline: OutcomeSelection,
    candidate: OutcomeSelection,
    spec: ComparisonSpec,
    *,
    storage: StorageConfig | None = None,
) -> RunComparison:
    """Compare declared paired measurements from two selected metric roles.

    Args:
        delta_root: Artisan store shared by both selections.
        baseline: Explicit baseline run, step and role, optionally an exact attempt.
        candidate: Explicit candidate run, step and role, optionally an exact attempt.
        spec: Matching rule, measurement declarations and optional assessment.
        storage: Store access configuration.

    Returns:
        Complete paired evidence and coverage, with no automatic overall winner.

    Raises:
        ComparisonSpecError: The spec or explicit pair membership is invalid.
        OutcomeSelectionError: A selection is invalid or ambiguous.
        ArtifactIntegrityError: Persisted metric evidence is inconsistent.
    """
    spec = _validated_spec(spec)
    try:
        selections = [
            OutcomeSelection.model_validate(selection)
            for selection in (baseline, candidate)
        ]
    except ValidationError as exc:
        msg = "Invalid comparison population selection"
        raise OutcomeSelectionError(msg) from exc
    paths = [metric.metric_path for metric in spec.metrics]
    if spec.assessment is not None:
        paths.extend(check.metric_path for check in spec.assessment.checks)
    metadata_keys = (
        [spec.matching.key] if isinstance(spec.matching, MetadataKeyMatch) else []
    )
    sides = [
        read_metric_observations(
            delta_root, selection, paths, metadata_keys=metadata_keys, storage=storage
        )
        for selection in selections
    ]
    return _compare_observations(sides[0], sides[1], spec)


def _validated_spec(spec: ComparisonSpec) -> ComparisonSpec:
    try:
        return ComparisonSpec.model_validate(spec)
    except ValidationError as exc:
        msg = "Invalid comparison specification"
        raise ComparisonSpecError(
            msg,
            field=".".join(str(part) for part in exc.errors()[0]["loc"]) or "spec",
        ) from exc


def _compare_observations(
    baseline: MetricObservationSet,
    candidate: MetricObservationSet,
    spec: ComparisonSpec,
) -> RunComparison:
    available = all(
        side.population_status == "available" for side in (baseline, candidate)
    )
    matches = (
        _match_observations(baseline, candidate, spec.matching)
        if available
        else _Matches()
    )
    status: ComparisonStatus = (
        "unavailable"
        if not available
        else "ambiguous"
        if matches.ambiguous_key_count
        else "compared"
        if matches.pairs
        else "no_pairs"
    )
    pairs = [_evaluate_pair(pair, spec.metrics, status) for pair in matches.pairs]
    spec_id = _spec_fingerprint("comparison-spec-v1", spec)
    evidence_id = compute_content_digest(
        canonical_json_bytes(
            {
                "domain": "comparison-evidence-v1",
                "baseline": baseline.evidence_id,
                "candidate": candidate.evidence_id,
                "spec_id": spec_id,
                "evaluator_version": COMPARISON_EVALUATOR_VERSION,
            }
        )
    )
    return RunComparison(
        spec=spec,
        spec_id=spec_id,
        evidence_id=evidence_id,
        reader_version=baseline.reader_version,
        baseline=_side_summary(baseline),
        candidate=_side_summary(candidate),
        comparison_status=status,
        coverage=_coverage(matches, baseline, candidate)
        if available
        else ComparisonCoverage(),
        metrics=[
            _metric_summary(
                metric,
                pairs,
                status,
                baseline.observation_count,
                candidate.observation_count,
            )
            for metric in spec.metrics
        ],
        pairs=pairs,
        unmatched=matches.unmatched,
        invalid=matches.invalid,
        assessment_population="complete_selected_role"
        if spec.assessment is not None
        else None,
        baseline_assessment=_evaluate_observations(baseline, spec.assessment)
        if spec.assessment is not None
        else None,
        candidate_assessment=_evaluate_observations(candidate, spec.assessment)
        if spec.assessment is not None
        else None,
    )


def _side_summary(observations: MetricObservationSet) -> ComparisonSide:
    return ComparisonSide(
        **{name: getattr(observations, name) for name in ComparisonSide.model_fields}
    )


def _reference(observation: MetricObservation) -> MetricEvidenceRef:
    return MetricEvidenceRef(
        **{name: getattr(observation, name) for name in MetricEvidenceRef.model_fields}
    )


def _match_observations(
    baseline: MetricObservationSet,
    candidate: MetricObservationSet,
    rule: MetadataKeyMatch | ExplicitPairsMatch,
) -> _Matches:
    if isinstance(rule, ExplicitPairsMatch):
        matches = _match_explicit(baseline.observations, candidate.observations, rule)
    else:
        matches = _match_metadata(
            baseline.observations, candidate.observations, rule.key
        )
    matches.unmatched.sort(
        key=lambda row: (row.side != "baseline", row.observation.artifact_id)
    )
    matches.invalid.sort(
        key=lambda row: (
            row.side != "baseline",
            row.category,
            row.observation.artifact_id,
        )
    )
    return matches


def _diagnostic(
    observation: MetricObservation,
    side: _Side,
    category: Literal["unmatched", "invalid_key", "ambiguous_key"],
    reason: str,
    key: CaseKey | None = None,
) -> ComparisonDiagnostic:
    return ComparisonDiagnostic(
        side=side,
        category=category,
        observation=_reference(observation),
        reason=reason,
        key=key,
    )


def _key_groups(
    observations: list[MetricObservation],
    side: _Side,
    metadata_key: str,
    matches: _Matches,
) -> dict[CaseKey, list[MetricObservation]]:
    groups: dict[CaseKey, list[MetricObservation]] = {}
    for observation in observations:
        selected = observation.metadata[metadata_key]
        value = selected.value
        if selected.status == "present" and (
            type(value) is int or (isinstance(value, str) and value)
        ):
            groups.setdefault(value, []).append(observation)
        else:
            reason = selected.reason or (
                "missing_key" if selected.status == "missing" else "invalid_key_type"
            )
            matches.invalid.append(
                _diagnostic(observation, side, "invalid_key", reason)
            )
    return groups


def _match_metadata(
    baseline: list[MetricObservation], candidate: list[MetricObservation], key: str
) -> _Matches:
    matches = _Matches()
    baseline_groups = _key_groups(baseline, "baseline", key, matches)
    candidate_groups = _key_groups(candidate, "candidate", key, matches)
    keys = sorted(
        baseline_groups.keys() | candidate_groups.keys(),
        key=lambda value: (isinstance(value, str), value),
    )
    for value in keys:
        left, right = baseline_groups.get(value, []), candidate_groups.get(value, [])
        if len(left) > 1 or len(right) > 1:
            matches.ambiguous_key_count += 1
            for side, observations in zip(_SIDES, (left, right), strict=True):
                matches.invalid.extend(
                    _diagnostic(item, side, "ambiguous_key", "duplicate_key", value)
                    for item in observations
                )
        elif left and right:
            matches.pairs.append(_StructuralPair(left[0], right[0], key=value))
        else:
            side = "baseline" if left else "candidate"
            observations = left if left else right
            matches.unmatched.append(
                _diagnostic(
                    observations[0],
                    side,
                    "unmatched",
                    "key_absent_on_other_side",
                    value,
                )
            )
    return matches


def _match_explicit(
    baseline: list[MetricObservation],
    candidate: list[MetricObservation],
    rule: ExplicitPairsMatch,
) -> _Matches:
    matches = _Matches()
    indexes = [
        {item.artifact_id: item for item in side} for side in (baseline, candidate)
    ]
    used: list[set[str]] = [set(), set()]
    for pair in rule.pairs:
        ids = (pair.baseline_artifact_id, pair.candidate_artifact_id)
        for index, artifact_id in enumerate(ids):
            if artifact_id not in indexes[index]:
                missing_side = "baseline" if index == 0 else "candidate"
                msg = f"Explicit pair artifact is outside the {missing_side} selection"
                raise ComparisonSpecError(
                    msg,
                    field="matching.pairs",
                )
            used[index].add(artifact_id)
        matches.pairs.append(
            _StructuralPair(
                indexes[0][ids[0]], indexes[1][ids[1]], case_label=pair.case_label
            )
        )
    for index, side in enumerate(_SIDES):
        matches.unmatched.extend(
            _diagnostic(item, side, "unmatched", "not_explicitly_paired")
            for artifact_id, item in indexes[index].items()
            if artifact_id not in used[index]
        )
    return matches


def _fraction(numerator: int, denominator: int | None) -> float | None:
    return numerator / denominator if denominator else None


def _coverage(
    matches: _Matches, baseline: MetricObservationSet, candidate: MetricObservationSet
) -> ComparisonCoverage:
    counts = {
        f"{side}_{category}_count": sum(
            row.side == side and row.category == category for row in rows
        )
        for side in ("baseline", "candidate")
        for category, rows in (
            ("unmatched", matches.unmatched),
            ("invalid_key", matches.invalid),
            ("ambiguous_key", matches.invalid),
        )
    }
    counts["baseline_ambiguous_count"] = counts.pop("baseline_ambiguous_key_count")
    counts["candidate_ambiguous_count"] = counts.pop("candidate_ambiguous_key_count")
    return ComparisonCoverage(
        **counts,
        matched_pair_count=len(matches.pairs),
        ambiguous_key_count=matches.ambiguous_key_count,
        baseline_structural_coverage=_fraction(
            len(matches.pairs), baseline.observation_count
        ),
        candidate_structural_coverage=_fraction(
            len(matches.pairs), candidate.observation_count
        ),
    )


def _evaluate_pair(
    pair: _StructuralPair, metrics: list[ComparisonMetric], status: ComparisonStatus
) -> ComparisonPair:
    values = [
        {tuple(value.metric_path): value for value in item.measurements}
        for item in (pair.baseline, pair.candidate)
    ]
    return ComparisonPair(
        key=pair.key,
        case_label=pair.case_label,
        baseline=_reference(pair.baseline),
        candidate=_reference(pair.candidate),
        metrics_evaluation="not_evaluated" if status == "ambiguous" else "evaluated",
        metrics=[
            _compare_metric(
                metric,
                values[0][tuple(metric.metric_path)],
                values[1][tuple(metric.metric_path)],
                status,
            )
            for metric in metrics
        ],
    )


def _compare_metric(
    metric: ComparisonMetric,
    baseline: MetricMeasurement,
    candidate: MetricMeasurement,
    status: ComparisonStatus,
) -> ComparisonMetricResult:
    common: dict[str, Any] = {
        "metric_id": metric.metric_id,
        "baseline": baseline,
        "candidate": candidate,
    }
    if status == "ambiguous":
        return ComparisonMetricResult(
            **common, comparison_status="not_evaluated", reason="ambiguous_matching"
        )
    if baseline.status != "valid" or candidate.status != "valid":
        return ComparisonMetricResult(
            **common, comparison_status="incomparable", reason="invalid_measurement"
        )
    difference = _measurement_difference(baseline, candidate)
    delta: Numeric | None
    rounded: bool | None
    if type(baseline.value) is int and type(candidate.value) is int:
        delta, rounded = int(difference), False
    else:
        delta, rounded = _finite_float(difference)
        if delta is None:
            return ComparisonMetricResult(
                **common, comparison_status="incomparable", reason="arithmetic_overflow"
            )
    return ComparisonMetricResult(
        **common,
        comparison_status="comparable",
        delta=delta,
        delta_rounded=rounded,
        classification=_classify(difference, metric),
    )


def _measurement_difference(
    baseline: MetricMeasurement, candidate: MetricMeasurement
) -> Fraction:
    assert baseline.value is not None
    assert candidate.value is not None
    return Fraction(candidate.value) - Fraction(baseline.value)


def _finite_float(value: Fraction) -> tuple[float | None, bool | None]:
    try:
        represented = float(value)
    except OverflowError:
        return None, None
    if not math.isfinite(represented):
        return None, None
    return represented, Fraction(represented) != value


def _classify(difference: Fraction, metric: ComparisonMetric) -> Classification:
    if abs(difference) <= Fraction(metric.absolute_tolerance):
        return "unchanged"
    if metric.direction == "neutral":
        return "changed"
    return (
        "improved"
        if (difference > 0) == (metric.direction == "higher")
        else "regressed"
    )


def _metric_summary(
    metric: ComparisonMetric,
    pairs: list[ComparisonPair],
    status: ComparisonStatus,
    baseline_count: int | None,
    candidate_count: int | None,
) -> ComparisonMetricSummary:
    if status in {"unavailable", "ambiguous"}:
        return ComparisonMetricSummary(
            metric_id=metric.metric_id,
            reason="population_unavailable"
            if status == "unavailable"
            else "ambiguous_matching",
        )
    results = [
        next(result for result in pair.metrics if result.metric_id == metric.metric_id)
        for pair in pairs
    ]
    comparable = [
        result for result in results if result.comparison_status == "comparable"
    ]
    counts = {
        f"{classification}_count": sum(
            result.classification == classification for result in comparable
        )
        for classification in ("improved", "regressed", "unchanged", "changed")
    }
    mean, rounded, mean_status = _mean_delta(comparable)
    return ComparisonMetricSummary(
        metric_id=metric.metric_id,
        comparable_count=len(comparable),
        incomparable_count=len(results) - len(comparable),
        **counts,
        paired_numeric_coverage=_fraction(len(comparable), len(results)),
        baseline_numeric_coverage=_fraction(len(comparable), baseline_count)
        if pairs
        else None,
        candidate_numeric_coverage=_fraction(len(comparable), candidate_count)
        if pairs
        else None,
        mean_delta=mean,
        mean_delta_rounded=rounded,
        mean_status=mean_status,
        reason="no_matched_pairs" if status == "no_pairs" else None,
    )


def _mean_delta(
    comparable: list[ComparisonMetricResult],
) -> tuple[
    Numeric | None,
    bool | None,
    Literal["available", "arithmetic_overflow", "no_comparable_pairs"],
]:
    if not comparable:
        return None, None, "no_comparable_pairs"
    exact = sum(
        (
            _measurement_difference(result.baseline, result.candidate)
            for result in comparable
        ),
        Fraction(),
    ) / len(comparable)
    if exact.denominator == 1:
        return exact.numerator, False, "available"
    value, rounded = _finite_float(exact)
    return value, rounded, "arithmetic_overflow" if value is None else "available"
