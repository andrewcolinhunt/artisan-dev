"""Precision, lifecycle and integrity checks for accepted metric assessment."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import arro3.core as ac
import polars as pl
import pytest
from deltalake import write_deltalake
from fixtures.outcome_store import append_metric_attempt
from pydantic import ValidationError

from artisan.errors import (
    ArtifactIntegrityError,
    AssessmentSpecError,
    OutcomeSelectionError,
)
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas import CSVArtifact, MetricArtifact, TablePath
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.utils.hashing import digest_utf8
from artisan.visualization import outcomes
from artisan.visualization.outcomes import (
    AssessmentCheck,
    AssessmentSpec,
    OutcomeSelection,
    assess_outcomes,
    read_metric_observations,
)


def _metric(
    values: dict | bytes, name: str = "case", *, metadata: dict | None = None
) -> MetricArtifact:
    if isinstance(values, bytes):
        return MetricArtifact(
            content=values,
            original_name=name,
            extension=".json",
            origin_step_number=0,
            metadata=metadata or {},
        ).finalize()
    return MetricArtifact.draft(values, f"{name}.json", 0, metadata=metadata).finalize()


def _spec(
    *,
    threshold: int | float = 0.8,
    operator: str = "ge",
    path: list[str] | None = None,
    expected_count: int | None = None,
) -> AssessmentSpec:
    return AssessmentSpec(
        label="quality",
        checks=[
            AssessmentCheck(
                check_id="quality",
                metric_path=path or ["score"],
                operator=operator,
                threshold=threshold,
            )
        ],
        expected_count=expected_count,
    )


def _rewrite_metrics(
    root: Path, transform: Callable[[pl.DataFrame], pl.DataFrame]
) -> None:
    table = root / "artifacts" / "metrics"
    rows = pl.read_delta(str(table))
    write_deltalake(str(table), ac.Table.from_arrow(transform(rows)), mode="overwrite")


def test_assessment_uses_raw_precision_and_complete_population(tmp_path) -> None:
    selection = append_metric_attempt(
        tmp_path / "store",
        [
            _metric({"score": 0.80001}, "pass"),
            _metric({"score": 0.79999}, "fail"),
            _metric({"other": 1}, "missing"),
        ],
    )
    report = assess_outcomes(
        str(tmp_path / "store"), selection, _spec(expected_count=3)
    )
    assert report.assessment_status == "incomplete"
    assert report.count_check.status == "pass"
    check = report.checks[0]
    assert (check.pass_count, check.fail_count, check.unassessed_count) == (1, 1, 1)
    assert check.evaluation_error_count == 0
    assert check.evaluated_count == 2
    assert check.coverage == 2 / 3
    assert {item.checks[0].value for item in report.observations} == {
        0.80001,
        0.79999,
        None,
    }
    assert len({item.artifact_id for item in report.observations}) == 3


@pytest.mark.parametrize(
    ("content", "path", "threshold", "verdict", "reason"),
    [
        ({"score": 9007199254740993}, ["score"], 9007199254740992.0, "pass", None),
        ({"nested": {"": 3}}, ["nested", ""], 3, "fail", None),
        ({"score": True}, ["score"], 0, "unassessed", "non_numeric_value"),
        ({"score": "1"}, ["score"], 0, "unassessed", "non_numeric_value"),
        ({"score": None}, ["score"], 0, "unassessed", "null_value"),
        ({"score": []}, ["score"], 0, "unassessed", "non_scalar_value"),
        ({"score": {}}, ["score"], 0, "unassessed", "non_scalar_value"),
        ({"score": 1}, ["score", "x"], 0, "unassessed", "non_object_path"),
        ({"other": 1}, ["score"], 0, "unassessed", "missing_key"),
        (b'{"score":NaN}', ["score"], 0, "unassessed", "nonfinite_value"),
        (b'{"score":1e999}', ["score"], 0, "unassessed", "nonfinite_value"),
        (b'{"score":2,"unused":NaN}', ["score"], 0, "pass", None),
        (
            b'{"score":1,"score":2}',
            ["score"],
            0,
            "evaluation_error",
            "duplicate_object_key",
        ),
        (b'{"score":', ["score"], 0, "evaluation_error", "invalid_json"),
        (b'{"score":"\xff"}', ["score"], 0, "evaluation_error", "invalid_utf8"),
        (b"[1,2]", ["score"], 0, "evaluation_error", "non_object_root"),
    ],
)
def test_measurements_preserve_numeric_and_invalid_evidence(
    tmp_path, content, path, threshold, verdict, reason
) -> None:
    selection = append_metric_attempt(tmp_path / "store", [_metric(content)])
    report = assess_outcomes(
        str(tmp_path / "store"),
        selection,
        _spec(threshold=threshold, operator="gt", path=path),
    )
    result = report.observations[0].checks[0]
    assert (result.status, result.reason) == (verdict, reason)
    serialized = report.model_dump_json()
    json.loads(serialized, parse_constant=lambda value: pytest.fail(value))
    if isinstance(content, dict) and content.get("score") == 9007199254740993:
        assert type(result.value) is int
        assert result.value == 9007199254740993


@pytest.mark.parametrize(
    ("status", "role_validation"),
    [
        ("pending", "deferred"),
        ("running", "deferred"),
        ("failed", "unavailable"),
        ("cancelled", "unavailable"),
        ("skipped", "unavailable"),
    ],
)
def test_unavailable_attempts_do_not_validate_cleared_or_dynamic_roles(
    tmp_path, monkeypatch, status, role_validation
) -> None:
    selection = append_metric_attempt(tmp_path / "store", [], status=status)
    monkeypatch.setattr(
        outcomes,
        "load_accepted_outputs",
        lambda *args, **kwargs: pytest.fail(
            "Unavailable attempts must not scan membership"
        ),
    )
    report = assess_outcomes(
        str(tmp_path / "store"), selection, _spec(expected_count=0)
    )
    assert report.population_status == "unavailable"
    assert report.role_validation == role_validation
    assert report.observation_count is None
    assert report.checks[0].evaluated_count is None
    assert report.count_check.status == "unassessed"
    assert report.assessment_status == "unassessed"
    assert report.execution_status.value == status


def test_partial_assessment_preserves_execution_state(tmp_path) -> None:
    selection = append_metric_attempt(
        tmp_path / "store", [_metric({"score": 1})], status="partial"
    )
    report = assess_outcomes(str(tmp_path / "store"), selection, _spec())
    assert report.assessment_status == "pass"
    assert report.execution_status is StepStatus.PARTIAL
    assert report.observation_count == 1


@pytest.mark.parametrize("expected", [None, 0, 3])
def test_empty_population_never_passes_overall(tmp_path, expected) -> None:
    selection = append_metric_attempt(tmp_path / "store", [])
    report = assess_outcomes(
        str(tmp_path / "store"),
        selection,
        AssessmentSpec(label="empty", expected_count=expected),
    )
    assert report.observation_count == 0
    assert report.assessment_status == "unassessed"
    assert report.reason == ("no_criteria" if expected is None else "empty_population")
    if expected is not None:
        assert report.count_check.status == ("pass" if expected == 0 else "fail")


def test_count_only_does_not_judge_malformed_content(tmp_path) -> None:
    selection = append_metric_attempt(tmp_path / "store", [_metric(b"bad-json")])
    report = assess_outcomes(
        str(tmp_path / "store"),
        selection,
        AssessmentSpec(label="count", expected_count=1),
    )
    assert report.assessment_status == "pass"
    assert report.checks == []


def test_current_retry_does_not_fall_back_to_success(tmp_path) -> None:
    root = tmp_path / "store"
    old = append_metric_attempt(root, [_metric({"score": 1})])
    latest = append_metric_attempt(root, [], status="failed")
    current = old.model_copy(update={"step_run_id": None})
    assert (
        assess_outcomes(str(root), current, _spec()).selection.step_run_id
        == latest.step_run_id
    )
    assert (
        assess_outcomes(str(root), current, _spec()).population_status == "unavailable"
    )
    historical = assess_outcomes(str(root), old, _spec())
    assert historical.assessment_status == "pass"
    assert historical.selection.step_run_id == old.step_run_id


def test_repeated_names_need_position_but_history_uses_its_own_state(tmp_path) -> None:
    root = tmp_path / "store"
    old = append_metric_attempt(root, [_metric({"score": 1})])
    append_metric_attempt(root, [], step_number=1)
    ambiguous = OutcomeSelection(
        pipeline_run_id="run", step_name="score", role="metrics"
    )
    with pytest.raises(OutcomeSelectionError, match="ambiguous"):
        assess_outcomes(str(root), ambiguous, _spec())
    append_metric_attempt(root, [], step_name="renamed", status="failed")
    assert assess_outcomes(str(root), old, _spec()).selection.step_name == "score"
    mismatch = old.model_copy(update={"step_number": 1})
    with pytest.raises(OutcomeSelectionError, match="No attempt"):
        assess_outcomes(str(root), mismatch, _spec())


def test_cached_membership_keeps_current_and_origin_distinct(tmp_path) -> None:
    root = tmp_path / "store"
    source = append_metric_attempt(
        root, [_metric({"score": 1})], pipeline_run_id="source"
    )
    current = append_metric_attempt(
        root,
        [],
        pipeline_run_id="current",
        step_number=5,
        reuse_execution_ids=[digest_utf8(f"{source.step_run_id}:execution")],
    )
    report = assess_outcomes(str(root), current, _spec())
    assert report.selection.disposition.value == "cache_hit"
    assert report.observation_count == 1
    row = report.observations[0]
    assert row.pipeline_run_id == "current"
    assert (row.step_number, row.origin_step_number) == (5, 0)
    assert row.step_run_id == current.step_run_id


def test_repeated_output_edges_count_one_artifact_membership(tmp_path) -> None:
    root = tmp_path / "store"
    selection = append_metric_attempt(root, [_metric({"score": 1})])
    table = root / TablePath.EXECUTION_EDGES.value
    rows = pl.read_delta(str(table))
    write_deltalake(
        str(table), ac.Table.from_arrow(pl.concat([rows, rows])), mode="overwrite"
    )
    report = assess_outcomes(str(root), selection, _spec(expected_count=1))
    assert report.observation_count == 1
    assert report.counting_basis == "unique_attempt_role_artifact"
    assert report.count_check.status == "pass"


def test_diagnostic_attempt_is_excluded(tmp_path) -> None:
    root = tmp_path / "store"
    selection = append_metric_attempt(root, [], status="running")
    table = root / TablePath.STEPS.value
    rows = pl.read_delta(str(table)).with_columns(
        pl.lit("a" * 32).alias("replay_of_execution_run_id")
    )
    write_deltalake(str(table), ac.Table.from_arrow(rows), mode="overwrite")
    with pytest.raises(OutcomeSelectionError, match="Diagnostic"):
        assess_outcomes(str(root), selection, _spec())


@pytest.mark.parametrize("declared_type", ["csv", "any"])
def test_non_metric_declared_or_actual_role_is_rejected(
    tmp_path, declared_type
) -> None:
    artifact = CSVArtifact.draft(b"value\n1\n", "data.csv", 0).finalize()
    selection = append_metric_attempt(
        tmp_path / "store", [artifact], declared_type=declared_type
    )
    with pytest.raises(OutcomeSelectionError, match="metric"):
        read_metric_observations(str(tmp_path / "store"), selection, [["score"]])


def test_selected_metadata_is_authored_data_and_presence_is_explicit(tmp_path) -> None:
    artifact = _metric(
        {"score": 1},
        metadata={"authorship": "case-7", "null": None, "bad": [float("nan")]},
    )
    selection = append_metric_attempt(tmp_path / "store", [artifact])
    report = read_metric_observations(
        str(tmp_path / "store"),
        selection,
        [["score"]],
        metadata_keys=["authorship", "null", "missing", "bad"],
    )
    metadata = report.observations[0].metadata
    assert metadata["authorship"].value == "case-7"
    assert metadata["null"].status == "present"
    assert metadata["null"].value is None
    assert metadata["missing"].status == "missing"
    assert metadata["bad"].status == "invalid"
    json.loads(
        report.model_dump_json(), parse_constant=lambda value: pytest.fail(value)
    )


@pytest.mark.parametrize(
    "mutation",
    ["duplicate", "missing", "content", "metadata", "deep_metadata", "origin"],
)
def test_persisted_artifact_inconsistency_is_not_a_quality_verdict(
    tmp_path, mutation
) -> None:
    root = tmp_path / "store"
    selection = append_metric_attempt(
        root, [_metric({"score": 1}, metadata={"case": "a"})]
    )
    transforms = {
        "duplicate": lambda rows: pl.concat([rows, rows]),
        "missing": lambda rows: rows.clear(),
        "content": lambda rows: rows.with_columns(
            pl.lit(b'{"score":2}').alias("content")
        ),
        "metadata": lambda rows: rows.with_columns(
            pl.lit('{"case":"a","case":"a"}').alias("metadata")
        ),
        "deep_metadata": lambda rows: rows.with_columns(
            pl.lit('{"case":' + "[" * 10000 + "0" + "]" * 10000 + "}").alias("metadata")
        ),
        "origin": lambda rows: rows.with_columns(
            pl.lit(3, dtype=pl.Int32).alias("origin_step_number")
        ),
    }
    _rewrite_metrics(root, transforms[mutation])
    with pytest.raises(ArtifactIntegrityError):
        assess_outcomes(str(root), selection, _spec())


@pytest.mark.parametrize("value", ["\ud800", {"nested": ["\udfff"]}, {"\ud800": 1}])
def test_unencodable_selected_metadata_is_explicitly_invalid(tmp_path, value) -> None:
    root = tmp_path / "store"
    artifact = _metric({"score": 1}, metadata={"case": value})
    selection = append_metric_attempt(root, [artifact])
    result = read_metric_observations(
        str(root), selection, [["score"]], metadata_keys=["case"]
    )
    item = result.observations[0].metadata["case"]
    assert item.status == "invalid"
    assert item.reason == "invalid_unicode"
    assert item.value is None
    json.loads(result.model_dump_json())
    assert artifact.metadata["case"] == value


@pytest.mark.parametrize(
    "change",
    [
        {"threshold": True},
        {"threshold": "0.8"},
        {"threshold": float("inf")},
        {"threshold": float("nan")},
        {"metric_path": []},
        {"metric_path": ["x"] * 17},
        {"check_id": "é" * 33},
        {"metric_path": ["é" * 129]},
    ],
)
def test_criteria_reject_lossy_or_unbounded_inputs(change) -> None:
    data = {
        "check_id": "score",
        "metric_path": ["score"],
        "operator": "ge",
        "threshold": 0.8,
    }
    with pytest.raises(ValidationError):
        AssessmentCheck(**{**data, **change})


def test_invalid_specs_and_projection_fail_before_io(monkeypatch) -> None:
    monkeypatch.setattr(
        outcomes, "StepTracker", lambda *args, **kwargs: pytest.fail("I/O")
    )
    selection = OutcomeSelection(
        pipeline_run_id="run", step_name="score", role="metrics"
    )
    broken = _spec().model_copy(update={"expected_count": True})
    with pytest.raises(AssessmentSpecError):
        assess_outcomes("unused", selection, broken)
    with pytest.raises(OutcomeSelectionError):
        read_metric_observations("unused", selection, [[f"k{i}"] for i in range(41)])
    for invalid in (False, "", {}, ()):
        with pytest.raises(OutcomeSelectionError):
            read_metric_observations("unused", selection, [], metadata_keys=invalid)
    with pytest.raises(ValidationError):
        AssessmentSpec(
            label="too large",
            checks=[
                AssessmentCheck(
                    check_id=str(i),
                    metric_path=["x" * 256] * 16,
                    operator="eq",
                    threshold=0,
                )
                for i in range(20)
            ],
        )
    with pytest.raises(ValidationError):
        AssessmentSpec(label="duplicate", checks=[_spec().checks[0]] * 2)
    with pytest.raises(ValidationError):
        AssessmentSpec(label="bad version", schema_version=True)


def test_evidence_identity_binds_projection_but_not_check_criteria(tmp_path) -> None:
    root = tmp_path / "store"
    selection = append_metric_attempt(
        root, [_metric({"score": 1}, metadata={"case": "a"})]
    )
    first = read_metric_observations(str(root), selection, [["score"]])
    reordered = read_metric_observations(str(root), selection, [["score"], ["score"]])
    metadata = read_metric_observations(
        str(root), selection, [["score"]], metadata_keys=["case"]
    )
    assert first.evidence_id == reordered.evidence_id
    assert first.evidence_id != metadata.evidence_id
    one = assess_outcomes(str(root), selection, _spec(threshold=0))
    two = assess_outcomes(str(root), selection, _spec(threshold=2))
    assert one.evidence_id == two.evidence_id
    assert one.spec_id != two.spec_id


def test_unknown_cancellation_is_not_cancelled_and_changes_evidence(tmp_path) -> None:
    root = tmp_path / "store"
    selection = append_metric_attempt(root, [], status="running")
    tracker = StepTracker(str(root))
    before = read_metric_observations(str(root), selection, [])
    tracker.record_cancellation(
        selection.step_run_id,
        StepStatus.RUNNING,
        CancellationAcknowledgement(CancellationStatus.REQUESTED),
    )
    requested = read_metric_observations(str(root), selection, [])
    tracker.record_cancellation(
        selection.step_run_id,
        StepStatus.RUNNING,
        CancellationAcknowledgement(CancellationStatus.UNKNOWN),
    )
    tracker.transition(
        selection.step_run_id,
        StepStatus.RUNNING,
        StepStatus.FAILED,
        result=StepResult(
            step_name="score",
            step_number=0,
            step_run_id=selection.step_run_id,
            status=StepStatus.FAILED,
            cancellation_status=CancellationStatus.UNKNOWN,
            error="closure not proven",
        ),
    )
    failed = read_metric_observations(str(root), selection, [])
    assert len({before.evidence_id, requested.evidence_id, failed.evidence_id}) == 3
    assert requested.selection.cancellation_status is CancellationStatus.REQUESTED
    assert failed.selection.execution_status is StepStatus.FAILED
    assert failed.selection.cancellation_status is CancellationStatus.UNKNOWN
    assert failed.population_status == "unavailable"


def test_rejected_cancellation_remains_running_unavailable(tmp_path) -> None:
    root = tmp_path / "store"
    selection = append_metric_attempt(root, [], status="running")
    tracker = StepTracker(str(root))
    for status in (CancellationStatus.REQUESTED, CancellationStatus.REJECTED):
        tracker.record_cancellation(
            selection.step_run_id,
            StepStatus.RUNNING,
            CancellationAcknowledgement(status),
        )
    result = read_metric_observations(str(root), selection, [])
    assert result.selection.execution_status is StepStatus.RUNNING
    assert result.selection.cancellation_status is CancellationStatus.REJECTED
    assert result.observation_count is None
