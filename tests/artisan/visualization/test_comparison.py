"""Paired comparisons preserve populations, identity and decoded precision."""

from __future__ import annotations

import json
import math
import sys
from fractions import Fraction
from pathlib import Path
from unittest.mock import patch

import pytest
from fixtures.outcome_store import append_metric_attempt
from fixtures.run_outputs import commit_outputs
from fixtures.store_format import publish_test_store
from fsspec.implementations.local import LocalFileSystem
from pydantic import ValidationError

from artisan.errors import (
    ArtifactIntegrityError,
    ComparisonSpecError,
    OutcomeSelectionError,
)
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.visualization.comparison import (
    ComparisonMetric,
    ComparisonSpec,
    ExplicitPair,
    ExplicitPairsMatch,
    MetadataKeyMatch,
    compare_runs,
)
from artisan.visualization.outcomes import (
    AssessmentCheck,
    AssessmentSpec,
    MetricMeasurement,
    MetricObservation,
    MetricObservationSet,
    OutcomeSelection,
    ResolvedOutcomeSelection,
    SelectedMetadata,
    read_metric_observations,
)


def _metric(**changes) -> ComparisonMetric:
    return ComparisonMetric(
        **{
            "metric_id": "score",
            "metric_path": ["score"],
            "direction": "higher",
            "measurement_id": "score",
            "measurement_version": "1",
            "unit": "unitless",
            **changes,
        }
    )


def _spec(**changes) -> ComparisonSpec:
    return ComparisonSpec(
        **{
            "label": "Declared comparison",
            "matching": MetadataKeyMatch(key="case"),
            "metrics": [_metric()],
            **changes,
        }
    )


def _selection(run: str) -> OutcomeSelection:
    return OutcomeSelection(pipeline_run_id=run, step_name="step-0", role="output")


def _population(
    run: str, values: list[object], keys: list[object] | None = None
) -> MetricObservationSet:
    selection = ResolvedOutcomeSelection(
        **_selection(run).model_dump(exclude={"step_number", "step_run_id"}),
        step_run_id=("b" if run == "baseline" else "c") * 32,
        step_number=0,
        execution_status=StepStatus.SUCCEEDED,
        state_sequence=2,
    )
    observations = []
    for index, value in enumerate(values):
        valid = type(value) in {int, float} and (
            type(value) is int or math.isfinite(value)
        )
        observations.append(
            MetricObservation(
                pipeline_run_id=run,
                step_name="step-0",
                step_number=0,
                step_run_id=selection.step_run_id,
                role="output",
                artifact_id=f"{index + 1:032x}",
                origin_step_number=0,
                original_name=f"metric-{index}.json",
                measurements=[
                    MetricMeasurement(
                        metric_path=["score"],
                        status="valid" if valid else "unassessed",
                        value=value if valid else None,
                        reason=None if valid else "not_numeric",
                    )
                ],
                metadata={
                    "case": SelectedMetadata(
                        status="present", value=index if keys is None else keys[index]
                    )
                },
            )
        )
    return MetricObservationSet(
        selection=selection,
        population_status="available",
        role_validation="validated",
        metric_paths=[["score"]],
        metadata_keys=["case"],
        observation_count=len(values),
        observations=observations,
        evidence_id=run,
    )


def _compare(baseline, candidate, spec=None):
    with patch(
        "artisan.visualization.comparison.read_metric_observations",
        side_effect=[baseline, candidate],
    ) as reader:
        report = compare_runs(
            "unused", _selection("baseline"), _selection("candidate"), spec or _spec()
        )
    assert reader.call_count == 2
    return report


@pytest.mark.parametrize(
    ("baseline", "candidate", "direction", "tolerance", "classification"),
    [
        (1, 2, "higher", 0, "improved"),
        (2, 1, "higher", 0, "regressed"),
        (1, 2, "lower", 0, "regressed"),
        (2, 1, "lower", 0, "improved"),
        (1, 2, "neutral", 0, "changed"),
        (1, 2, "higher", 1, "unchanged"),
        (2, 1, "lower", 1, "unchanged"),
        (1, 1, "neutral", 0, "unchanged"),
    ],
)
def test_compare_runs_classifies_exact_values(
    baseline, candidate, direction, tolerance, classification
):
    report = _compare(
        _population("baseline", [baseline]),
        _population("candidate", [candidate]),
        _spec(metrics=[_metric(direction=direction, absolute_tolerance=tolerance)]),
    )
    result = report.pairs[0].metrics[0]
    assert result.classification == classification
    assert result.delta == candidate - baseline
    assert result.delta_rounded is False
    assert report.metrics[0].comparable_count == 1
    assert report.metrics[0].paired_numeric_coverage == 1
    assert getattr(report.metrics[0], f"{classification}_count") == 1


def test_compare_runs_exposes_hidden_regression_and_full_population_assessment():
    spec = _spec(
        assessment=AssessmentSpec(
            label="Minimum score",
            checks=[
                AssessmentCheck(
                    check_id="minimum",
                    metric_path=["score"],
                    operator="ge",
                    threshold=0.4,
                )
            ],
            expected_count=3,
        )
    )
    report = _compare(
        _population("baseline", [0.9, 0.1]),
        _population("candidate", [0.8, 0.5, 1.0]),
        spec,
    )
    assert report.metrics[0].mean_delta > 0
    assert report.metrics[0].regressed_count == 1
    assert report.metrics[0].improved_count == 1
    assert report.coverage.candidate_unmatched_count == 1
    assert report.metrics[0].candidate_numeric_coverage == 2 / 3
    assert report.candidate_assessment.observation_count == 3
    assert report.candidate_assessment.assessment_status == "pass"
    assert report.baseline_assessment.count_check.status == "fail"
    assert report.assessment_population == "complete_selected_role"
    assert report.comparability_basis == "caller_declared"
    assert not hasattr(report, "winner")


@pytest.mark.parametrize(
    ("baseline", "candidate", "expected_delta", "rounded"),
    [
        (float(2**53), 2**53 + 1, 1.0, False),
        (10**400, 10**400 + 7, 7, False),
        (0, 10**400, 10**400, False),
        (1, float(2**54), float(2**54), True),
    ],
)
def test_compare_runs_retains_large_and_mixed_numeric_precision(
    baseline, candidate, expected_delta, rounded
):
    report = _compare(
        _population("baseline", [baseline]), _population("candidate", [candidate])
    )
    result = report.pairs[0].metrics[0]
    assert result.delta == expected_delta
    assert result.delta_rounded is rounded
    assert result.classification == "improved"
    assert type(result.delta) is type(expected_delta)
    assert report.metrics[0].mean_delta == Fraction(candidate) - Fraction(baseline)


def test_compare_runs_classifies_before_rounding_returned_delta():
    report = _compare(
        _population("baseline", [1]),
        _population("candidate", [float(2**54)]),
        _spec(metrics=[_metric(absolute_tolerance=2**54 - 1)]),
    )
    result = report.pairs[0].metrics[0]
    assert result.delta > 2**54 - 1
    assert result.delta_rounded
    assert result.classification == "unchanged"


def test_compare_runs_separates_subtraction_overflow_from_missing_values():
    report = _compare(
        _population("baseline", [-sys.float_info.max, 1, 2]),
        _population("candidate", [sys.float_info.max, None, 4]),
    )
    results = [pair.metrics[0] for pair in report.pairs]
    assert [result.reason for result in results] == [
        "arithmetic_overflow",
        "invalid_measurement",
        None,
    ]
    assert results[1].candidate.reason == "not_numeric"
    assert report.metrics[0].comparable_count == 1
    assert report.metrics[0].incomparable_count == 2
    assert report.metrics[0].paired_numeric_coverage == 1 / 3
    assert report.metrics[0].mean_delta == 2
    json.dumps(report.model_dump(mode="json"), allow_nan=False)


def test_compare_runs_sums_exact_differences_without_intermediate_overflow():
    value = sys.float_info.max
    report = _compare(
        _population("baseline", [0, 0, value]),
        _population("candidate", [value, value, 0]),
    )
    assert report.metrics[0].mean_delta == float(Fraction(value) / 3)
    assert report.metrics[0].mean_status == "available"
    assert report.metrics[0].improved_count == 2
    assert report.metrics[0].regressed_count == 1


def test_compare_runs_reports_final_nonintegral_mean_overflow():
    report = _compare(
        _population("baseline", [0, 0]),
        _population("candidate", [10**400, 10**400 + 1]),
    )
    summary = report.metrics[0]
    assert summary.comparable_count == summary.improved_count == 2
    assert summary.mean_status == "arithmetic_overflow"
    assert summary.mean_delta is summary.mean_delta_rounded is None
    assert all(pair.metrics[0].delta is not None for pair in report.pairs)


def test_compare_runs_marks_mean_underflow_without_reclassifying_pairs():
    report = _compare(
        _population("baseline", [0, 0, 0]),
        _population("candidate", [math.ulp(0.0), 0, 0]),
    )
    assert report.metrics[0].mean_delta == 0.0
    assert report.metrics[0].mean_delta_rounded is True
    assert report.metrics[0].improved_count == 1
    assert report.metrics[0].unchanged_count == 2


def test_compare_runs_zero_comparable_pairs_preserves_numeric_denominator():
    report = _compare(
        _population("baseline", [True, None]), _population("candidate", [1, 2])
    )
    summary = report.metrics[0]
    assert report.comparison_status == "compared"
    assert summary.comparable_count == 0
    assert summary.incomparable_count == 2
    assert summary.paired_numeric_coverage == summary.baseline_numeric_coverage == 0
    assert summary.mean_status == "no_comparable_pairs"


def test_compare_runs_matches_typed_keys_and_partitions_all_observations():
    left = _population("baseline", [0] * 8, [1, "1", None, True, 1.0, "", "left", "x"])
    left.observations[-1].metadata["case"] = SelectedMetadata(status="missing")
    right = _population("candidate", [1] * 4, ["1", 1, "right", None])
    report = _compare(left, right)
    assert [pair.key for pair in report.pairs] == [1, "1"]
    assert [type(pair.key) for pair in report.pairs] == [int, str]
    assert report.coverage.matched_pair_count == 2
    assert report.coverage.baseline_unmatched_count == 1
    assert report.coverage.baseline_invalid_key_count == 5
    assert (
        report.coverage.candidate_unmatched_count
        == report.coverage.candidate_invalid_key_count
        == 1
    )
    assert report.coverage.baseline_structural_coverage == 2 / 8
    for side in ("baseline", "candidate"):
        coverage = report.coverage
        assert getattr(report, side).observation_count == (
            coverage.matched_pair_count
            + getattr(coverage, f"{side}_unmatched_count")
            + getattr(coverage, f"{side}_invalid_key_count")
            + getattr(coverage, f"{side}_ambiguous_count")
        )


def test_compare_runs_duplicate_keys_suppress_every_numeric_pair():
    report = _compare(
        _population(
            "baseline",
            [1, 2, 3, 4, 5],
            ["duplicate", "duplicate", "alone", "alone", "unique"],
        ),
        _population("candidate", [7, 8], ["duplicate", "unique"]),
        _spec(assessment=AssessmentSpec(label="Count", expected_count=5)),
    )
    assert report.comparison_status == "ambiguous"
    assert report.coverage.ambiguous_key_count == 2
    assert report.coverage.baseline_ambiguous_count == 4
    assert report.coverage.candidate_ambiguous_count == 1
    assert report.coverage.matched_pair_count == 1
    assert report.pairs[0].metrics_evaluation == "not_evaluated"
    assert report.pairs[0].metrics[0].reason == "ambiguous_matching"
    assert report.metrics[0].comparable_count is None
    assert report.metrics[0].reason == "ambiguous_matching"
    assert report.baseline_assessment.assessment_status == "pass"
    assert len(report.invalid) == 5


@pytest.mark.parametrize("empty", [False, True])
def test_compare_runs_no_pairs_reports_zero_counts_and_null_ratios(empty):
    report = _compare(
        _population("baseline", [] if empty else [1], [] if empty else ["left"]),
        _population("candidate", [2], ["right"]),
    )
    assert report.comparison_status == "no_pairs"
    assert (
        report.metrics[0].comparable_count == report.metrics[0].incomparable_count == 0
    )
    assert report.metrics[0].paired_numeric_coverage is None
    assert report.metrics[0].baseline_numeric_coverage is None
    assert report.metrics[0].reason == "no_matched_pairs"
    assert report.coverage.baseline_structural_coverage == (None if empty else 0)


def test_compare_runs_explicit_subset_is_sorted_and_preserves_unmatched():
    pairs = [
        ExplicitPair(
            case_label=label,
            baseline_artifact_id=f"{index:032x}",
            candidate_artifact_id=f"{index:032x}",
        )
        for label, index in (("z", 2), ("a", 1))
    ]
    spec = _spec(matching=ExplicitPairsMatch(pairs=pairs))
    report = _compare(
        _population("baseline", [1, 2, 3]), _population("candidate", [4, 5, 6, 7]), spec
    )
    assert [pair.case_label for pair in report.pairs] == ["a", "z"]
    assert report.coverage.matched_pair_count == 2
    assert report.coverage.baseline_unmatched_count == 1
    assert report.coverage.candidate_unmatched_count == 2
    assert (
        report.coverage.baseline_invalid_key_count
        == report.coverage.baseline_ambiguous_count
        == 0
    )
    assert all(
        pair.baseline.artifact_id == pair.candidate.artifact_id for pair in report.pairs
    )
    reordered = _compare(
        _population("baseline", [1, 2, 3]),
        _population("candidate", [4, 5, 6, 7]),
        _spec(matching=ExplicitPairsMatch(pairs=list(reversed(pairs)))),
    )
    assert reordered.spec_id == report.spec_id
    assert reordered.evidence_id == report.evidence_id


@pytest.mark.parametrize(
    "field", ["case_label", "baseline_artifact_id", "candidate_artifact_id"]
)
def test_explicit_pairs_rejects_duplicate_side_membership_and_labels(field):
    first = ExplicitPair(
        case_label="a", baseline_artifact_id="1" * 32, candidate_artifact_id="2" * 32
    )
    second = ExplicitPair(
        case_label="b", baseline_artifact_id="3" * 32, candidate_artifact_id="4" * 32
    )
    second = second.model_copy(update={field: getattr(first, field)})
    with pytest.raises(ValidationError, match="unique"):
        ExplicitPairsMatch(pairs=[first, second])


def test_compare_runs_rejects_explicit_artifact_outside_selection():
    spec = _spec(
        matching=ExplicitPairsMatch(
            pairs=[
                ExplicitPair(
                    case_label="a",
                    baseline_artifact_id="1" * 32,
                    candidate_artifact_id="2" * 32,
                )
            ]
        )
    )
    with pytest.raises(ComparisonSpecError, match="outside the baseline"):
        _compare(_population("baseline", [1]), _population("candidate", [2]), spec)


def test_compare_runs_unavailable_sides_defer_pair_membership_validation():
    left = _population("baseline", [1])
    right = _population("candidate", [])
    right.population_status = "unavailable"
    right.role_validation = "deferred"
    right.observation_count = None
    right.reason = "execution_incomplete"
    right.selection.execution_status = StepStatus.RUNNING
    spec = _spec(
        matching=ExplicitPairsMatch(
            pairs=[
                ExplicitPair(
                    case_label="a",
                    baseline_artifact_id="1" * 32,
                    candidate_artifact_id="2" * 32,
                )
            ]
        )
    )
    report = _compare(left, right, spec)
    assert report.comparison_status == "unavailable"
    assert report.baseline.observation_count == 1
    assert report.candidate.observation_count is None
    assert all(value is None for value in report.coverage.model_dump().values())
    assert report.pairs == report.invalid == report.unmatched == []
    assert report.metrics[0].comparable_count is None


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("schema_version",), True),
        (("extra",), 1),
        (("label",), "x" * 257),
        (("metrics", 0, "absolute_tolerance"), True),
        (("metrics", 0, "absolute_tolerance"), -1),
        (("metrics", 0, "absolute_tolerance"), "1"),
        (("metrics", 0, "absolute_tolerance"), float("nan")),
        (("metrics", 0, "absolute_tolerance"), float("inf")),
        (("metrics", 0, "metric_path"), []),
        (("metrics", 0, "metric_path"), ["x"] * 17),
        (("metrics", 0, "unit"), ""),
    ],
)
def test_compare_runs_invalid_spec_fails_before_store_io(path, value):
    spec = _spec().model_dump()
    target = spec
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value
    with (
        patch("artisan.visualization.comparison.read_metric_observations") as reader,
        pytest.raises(ComparisonSpecError),
    ):
        compare_runs("unused", _selection("baseline"), _selection("candidate"), spec)
    reader.assert_not_called()


def test_comparison_spec_byte_and_cardinality_bounds():
    metrics = [
        _metric(metric_id=f"metric-{index}", metric_path=["x" * 256] * 16)
        for index in range(20)
    ]
    with pytest.raises(ValidationError, match="16 KiB"):
        _spec(metrics=metrics)
    with pytest.raises(ValidationError, match="unique"):
        _spec(metrics=[_metric(), _metric()])
    with pytest.raises(ValidationError):
        _spec(metrics=[_metric(metric_id=str(index)) for index in range(21)])
    with pytest.raises(ValidationError):
        ExplicitPairsMatch(
            pairs=[
                ExplicitPair(
                    case_label=str(index),
                    baseline_artifact_id=f"{index:032x}",
                    candidate_artifact_id=f"{index:032x}",
                )
                for index in range(101)
            ]
        )


def test_compare_runs_requests_union_and_reuses_each_side_for_assessment():
    spec = _spec(
        assessment=AssessmentSpec(
            label="Complete",
            checks=[
                AssessmentCheck(
                    check_id="positive",
                    metric_path=["nested", ""],
                    operator="gt",
                    threshold=0,
                )
            ],
        )
    )
    sides = [_population("baseline", [1, 2]), _population("candidate", [3, 4])]
    for side in sides:
        for observation in side.observations:
            observation.measurements.append(
                MetricMeasurement(metric_path=["nested", ""], status="valid", value=1)
            )
    with patch(
        "artisan.visualization.comparison.read_metric_observations", side_effect=sides
    ) as reader:
        report = compare_runs(
            "unused", _selection("baseline"), _selection("candidate"), spec
        )
    assert reader.call_count == 2
    assert all(
        call.kwargs["metadata_keys"] == ["case"] for call in reader.call_args_list
    )
    assert all(
        {tuple(path) for path in call.args[2]} == {("score",), ("nested", "")}
        for call in reader.call_args_list
    )
    assert report.baseline_assessment.evidence_id == report.baseline.evidence_id
    assert report.candidate_assessment.evidence_id == report.candidate.evidence_id


def test_compare_runs_validates_both_selections_before_reading_either():
    candidate = _selection("candidate")
    candidate.role = "x" * 257
    with (
        patch("artisan.visualization.comparison.read_metric_observations") as reader,
        pytest.raises(OutcomeSelectionError),
    ):
        compare_runs("unused", _selection("baseline"), candidate, _spec())
    reader.assert_not_called()


def test_compare_runs_revalidates_mutated_nested_spec_before_reading():
    spec = _spec()
    spec.metrics[0].absolute_tolerance = True
    with (
        patch("artisan.visualization.comparison.read_metric_observations") as reader,
        pytest.raises(ComparisonSpecError),
    ):
        compare_runs("unused", _selection("baseline"), _selection("candidate"), spec)
    reader.assert_not_called()


def test_compare_runs_one_invalid_metric_does_not_hide_other_metrics():
    baseline, candidate = _population("baseline", [None]), _population("candidate", [1])
    for side, value in ((baseline, 2), (candidate, 3)):
        side.observations[0].measurements.append(
            MetricMeasurement(metric_path=["other"], status="valid", value=value)
        )
    report = _compare(
        baseline,
        candidate,
        _spec(metrics=[_metric(), _metric(metric_id="other", metric_path=["other"])]),
    )
    assert report.metrics[0].incomparable_count == 1
    assert report.metrics[1].comparable_count == report.metrics[1].improved_count == 1
    assert [result.comparison_status for result in report.pairs[0].metrics] == [
        "incomparable",
        "comparable",
    ]


@pytest.mark.parametrize(
    "status", ["pending", "running", "failed", "cancelled", "skipped"]
)
def test_compare_runs_real_unavailable_lifecycles_defer_role_validation(
    tmp_path, status
):
    root = tmp_path / "store"
    baseline = append_metric_attempt(root, [], pipeline_run_id="baseline")
    candidate = append_metric_attempt(
        root, [], pipeline_run_id="candidate", status=status
    )
    candidate.role = "not-yet-known"
    report = compare_runs(str(root), baseline, candidate, _spec())
    assert report.comparison_status == "unavailable"
    assert report.baseline.observation_count == 0
    assert report.candidate.observation_count is None
    assert report.candidate.selection.execution_status.value == status
    assert report.candidate.role_validation == (
        "deferred" if status in {"pending", "running"} else "unavailable"
    )


def test_compare_runs_real_partial_acceptance_and_malformed_measurements(tmp_path):
    root = tmp_path / "store"
    malformed = MetricArtifact(
        content=b'{"score":1,"score":2}',
        original_name="duplicate-key",
        extension=".json",
        origin_step_number=0,
        metadata={"case": "a"},
    ).finalize()
    baseline = append_metric_attempt(
        root, [malformed], pipeline_run_id="baseline", status="partial"
    )
    candidate_metric = MetricArtifact.draft(
        {"score": 3}, "candidate.json", 0, metadata={"case": "a"}
    ).finalize()
    candidate = append_metric_attempt(
        root, [candidate_metric], pipeline_run_id="candidate"
    )
    report = compare_runs(str(root), baseline, candidate, _spec())
    assert report.baseline.selection.execution_status is StepStatus.PARTIAL
    assert report.coverage.matched_pair_count == 1
    assert report.metrics[0].incomparable_count == 1
    assert report.pairs[0].metrics[0].baseline.status == "evaluation_error"
    assert report.pairs[0].metrics[0].baseline.reason == "duplicate_object_key"


def test_compare_runs_fingerprint_binds_both_sides_and_spec():
    left, right = _population("baseline", [1]), _population("candidate", [2])
    report = _compare(left, right)
    revised_spec = _compare(left, right, _spec(metrics=[_metric(direction="lower")]))
    assert revised_spec.spec_id != report.spec_id
    assert revised_spec.evidence_id != report.evidence_id
    right.evidence_id = "new-state"
    revised_evidence = _compare(left, right)
    assert revised_evidence.spec_id == report.spec_id
    assert revised_evidence.evidence_id != report.evidence_id


@pytest.fixture
def store(tmp_path: Path) -> str:
    root = str(tmp_path / "delta")
    publish_test_store(root, LocalFileSystem())
    return root


def test_compare_runs_real_accepted_metadata_and_cached_origin(store):
    baseline_metric = MetricArtifact.draft(
        {"score": 0.12341}, "baseline.json", 3, metadata={"case": "case-a"}
    ).finalize()
    candidate_metric = MetricArtifact.draft(
        {"score": 0.12349}, "candidate.json", 7, metadata={"case": "case-a"}
    ).finalize()
    _, source_execution = commit_outputs(
        store, run_id="source", number=3, artifacts=[baseline_metric]
    )
    baseline_attempt, _ = commit_outputs(
        store,
        run_id="baseline",
        number=0,
        reused_execution_id=source_execution,
        output_ids={"output": [baseline_metric.artifact_id]},
    )
    commit_outputs(store, run_id="candidate", number=7, artifacts=[candidate_metric])
    with patch(
        "artisan.visualization.comparison.read_metric_observations",
        wraps=read_metric_observations,
    ) as reader:
        report = compare_runs(
            store,
            _selection("baseline"),
            OutcomeSelection(
                pipeline_run_id="candidate", step_name="step-7", role="output"
            ),
            _spec(),
        )
    assert reader.call_count == 2
    assert report.pairs[0].baseline.step_run_id == baseline_attempt
    assert report.pairs[0].baseline.origin_step_number == 3
    assert report.pairs[0].candidate.origin_step_number == 7
    assert report.pairs[0].metrics[0].classification == "improved"
    assert report.pairs[0].metrics[0].baseline.value == 0.12341


def test_compare_runs_current_failed_retry_does_not_fall_back(store):
    metric = MetricArtifact.draft(
        {"score": 1}, "baseline.json", 0, metadata={"case": "a"}
    ).finalize()
    earlier, _ = commit_outputs(store, run_id="baseline", artifacts=[metric])
    latest, _ = commit_outputs(
        store, run_id="baseline", status="failed", execution_success=False
    )
    commit_outputs(
        store, run_id="candidate", output_ids={"output": [metric.artifact_id]}
    )
    report = compare_runs(
        store, _selection("baseline"), _selection("candidate"), _spec()
    )
    assert report.comparison_status == "unavailable"
    assert report.baseline.selection.step_run_id == latest
    historic = _selection("baseline").model_copy(update={"step_run_id": earlier})
    report = compare_runs(store, historic, _selection("candidate"), _spec())
    assert report.comparison_status == "compared"
    assert report.pairs[0].metrics[0].classification == "unchanged"


def test_compare_runs_shared_integrity_failure_is_not_a_quality_verdict():
    with (
        patch(
            "artisan.visualization.comparison.read_metric_observations",
            side_effect=ArtifactIntegrityError("Corrupt metadata"),
        ),
        pytest.raises(ArtifactIntegrityError, match="Corrupt metadata"),
    ):
        compare_runs("unused", _selection("baseline"), _selection("candidate"), _spec())
