"""Exact retained metric populations with unchanged assessment and comparison math."""

from __future__ import annotations

import polars as pl
from pydantic import ValidationError

from artisan.errors import AssessmentSpecError, OutcomeSelectionError
from artisan.orchestration.engine.retained_artifact_query import (
    _references,
    _step_exports,
)
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_state import StepState
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_read import RetainedReadSnapshot
from artisan.visualization.comparison import (
    ComparisonSpec,
    MetadataKeyMatch,
    RunComparison,
    _compare_observations,
    _validated_spec,
)
from artisan.visualization.outcomes import (
    AssessmentSpec,
    MetricObservation,
    MetricObservationSet,
    OutcomeAssessment,
    OutcomeSelection,
    ResolvedOutcomeSelection,
    _evaluate_observations,
    _observation_set,
    _observations_from_rows,
    _population_state,
    _Projection,
    _resolve_selection,
)


def read_retained_metric_observations(
    committer: RetainedCommitter,
    selection: OutcomeSelection,
    metric_paths: list[list[str]],
    *,
    metadata_keys: list[str] | None = None,
) -> MetricObservationSet:
    """Read a selected role's inline metrics under one fresh metadata proof.

    Exact historical attempts use their own terminal selection. Otherwise the
    latest attempt masks prior results, including when it has no usable outputs.
    No unrelated artifact payloads, external carriers or diagnostics are read.
    """
    return _read_populations(
        committer, (selection,), metric_paths, metadata_keys=metadata_keys
    )[0]


def assess_retained_outcomes(
    committer: RetainedCommitter,
    selection: OutcomeSelection,
    spec: AssessmentSpec,
) -> OutcomeAssessment:
    """Apply the existing exact numeric assessment to one verified population."""
    try:
        spec = AssessmentSpec.model_validate(spec)
    except ValidationError as error:
        msg = "Invalid assessment specification"
        raise AssessmentSpecError(msg) from error
    observed = read_retained_metric_observations(
        committer, selection, [check.metric_path for check in spec.checks]
    )
    return _evaluate_observations(observed, spec)


def compare_retained_runs(
    committer: RetainedCommitter,
    baseline: OutcomeSelection,
    candidate: OutcomeSelection,
    spec: ComparisonSpec,
) -> RunComparison:
    """Compare two populations selected and acquired from the same fresh read."""
    spec = _validated_spec(spec)
    paths = [metric.metric_path for metric in spec.metrics]
    if spec.assessment is not None:
        paths.extend(check.metric_path for check in spec.assessment.checks)
    metadata_keys = (
        [spec.matching.key] if isinstance(spec.matching, MetadataKeyMatch) else []
    )
    sides = _read_populations(
        committer, (baseline, candidate), paths, metadata_keys=metadata_keys
    )
    return _compare_observations(sides[0], sides[1], spec)


def _read_populations(
    committer: RetainedCommitter,
    selections: tuple[OutcomeSelection, ...],
    metric_paths: list[list[str]],
    *,
    metadata_keys: list[str] | None,
) -> tuple[MetricObservationSet, ...]:
    try:
        selections = tuple(OutcomeSelection.model_validate(item) for item in selections)
        projection = _Projection(
            metric_paths=metric_paths,
            metadata_keys=[] if metadata_keys is None else metadata_keys,
        )
    except ValidationError as error:
        msg = "Invalid observation selection or projection"
        raise OutcomeSelectionError(msg) from error
    with committer._read_snapshot() as snapshot:
        tracker = RetainedStepTracker(committer, "")
        _, attempts = tracker._read_lifecycle(snapshot)
        resolved = [
            _resolve_selection(
                attempts
                if item.step_run_id is not None
                else tracker._select_current_states(attempts, item.pipeline_run_id),
                item,
            )
            for item in selections
        ]
        if any(
            _population_state(state, item.role)[0] == "available"
            for state, item in resolved
        ):
            snapshot.snapshot(TablePath.ARTIFACT_INDEX.value)
            snapshot.snapshot(ArtifactTypeDef.get_table_path("metric"))
        return tuple(
            _read_population(snapshot, state, item, projection)
            for state, item in resolved
        )


def _read_population(
    snapshot: RetainedReadSnapshot,
    state: StepState,
    selection: ResolvedOutcomeSelection,
    projection: _Projection,
) -> MetricObservationSet:
    population, role_validation, reason = _population_state(state, selection.role)
    observed = (
        _read_observations(snapshot, state, selection, projection)
        if population == "available"
        else []
    )
    return _observation_set(
        selection,
        projection,
        observed,
        population_status=population,
        role_validation=role_validation,
        reason=reason,
    )


def _read_observations(
    snapshot: RetainedReadSnapshot,
    state: StepState,
    selection: ResolvedOutcomeSelection,
    projection: _Projection,
) -> list[MetricObservation]:
    selected = {}
    for item in _step_exports(snapshot, state, role=selection.role):
        if item.artifact_type != "metric":
            msg = "Every artifact in the selected role must be a metric"
            raise OutcomeSelectionError(msg)
        selected[state.step_number, item.artifact_id] = item.artifact_type
    if not selected:
        return []
    references = _references(snapshot, selected, selection.pipeline_run_id)
    origins = {item.artifact_id: item.origin_step_number for item in references}
    path = ArtifactTypeDef.get_table_path("metric")
    physical = snapshot.snapshot(path)
    frames = [
        frame.filter(pl.col("artifact_id").is_in(list(origins)))
        for frame in physical.iter_frames()
    ]
    rows = pl.concat(frames) if frames else pl.DataFrame(schema=physical.schema)
    for owner in rows["logical_commit_id"].unique():
        snapshot.verify_effect(snapshot.completed_control_plan(owner), path)
    return _observations_from_rows(rows, origins, selection, projection)
