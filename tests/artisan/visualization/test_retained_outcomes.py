"""Exact metric-role membership and content share one retained read boundary."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from functools import partial
from pathlib import Path

import polars as pl
import pytest
from fixtures.candidate_outputs import prepare_scientific
from fixtures.curator_cache import STAMPS
from fixtures.retained_curators import admit_curator
from fixtures.retained_results import (
    RetainedCase,
    _derive,
    boundary_artifact,
    retained_case,
)

from artisan.errors import (
    ArtifactIntegrityError,
    OutcomeSelectionError,
    PersistenceIntegrityError,
    StoreIntegrityError,
)
from artisan.execution.executors.curator import run_curator_flow
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.curator.merge import Merge
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas import (
    ExecutionConfigArtifact,
    FileRefArtifact,
    MetricArtifact,
    OutputSpec,
)
from artisan.schemas.enums import CachePolicy, TablePath
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_config import WorkerConfig
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.orchestration.step_state import StepState
from artisan.schemas.provenance.projection import ProjectedOccurrence
from artisan.storage.cache.curator_dependencies import CuratorReadRecorder
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.cache.retained_reuse import verify_cache_source
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.curator_staging import read_staged_curator_result
from artisan.storage.core.curator_view import CuratorView
from artisan.storage.core.projection_schemas import PROJECTION_TABLES
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_read import RetainedControlProof, RetainedReadSnapshot
from artisan.storage.io.retained_store import create_retained_store
from artisan.storage.io.step_result_plan import StepUnitFailure
from artisan.utils.hashing import compute_content_digest
from artisan.utils.path import uri_join
from artisan.visualization import retained_outcomes
from artisan.visualization.comparison import (
    ComparisonMetric,
    ComparisonSpec,
    MetadataKeyMatch,
)
from artisan.visualization.outcomes import (
    AssessmentCheck,
    AssessmentSpec,
    MetricObservationSet,
    OutcomeSelection,
    ResolvedOutcomeSelection,
)
from artisan.visualization.retained_outcomes import (
    assess_retained_outcomes,
    compare_retained_runs,
    read_retained_metric_observations,
)


@pytest.fixture
def committer(tmp_path: Path) -> RetainedCommitter:
    return create_retained_store(
        str(tmp_path / "store"),
        str(tmp_path / "files"),
        fs=StorageConfig().filesystem(),
    )


def _case(
    *, score: int = 3, empty: bool = False, external: Path | None = None
) -> RetainedCase:
    graph = OccurrenceGraph()
    seed = graph.add_input("data", boundary_artifact())
    metric = _derive(
        graph,
        "score",
        MetricArtifact.draft(
            {"score": score}, "score.json", 7, metadata={"case": "one"}
        ).finalize(),
        [seed],
    )
    hidden = _derive(
        graph,
        "support",
        MetricArtifact.draft({"score": -100}, "support.json", 7).finalize(),
        [seed],
    )
    references = {"hidden": {"$artifact": graph.nodes[hidden].artifact.artifact_id}}
    parents = [hidden]
    if external is not None:
        payload = b"unrelated carrier bytes"
        external.write_bytes(payload)
        file = _derive(
            graph,
            "external",
            FileRefArtifact.draft(
                str(external), compute_content_digest(payload), len(payload), 7
            ).finalize(),
            [seed],
        )
        parents.append(file)
        references["file"] = {"$artifact": graph.nodes[file].artifact.artifact_id}
    config = _derive(
        graph,
        "config",
        ExecutionConfigArtifact.draft(
            references,
            "config.json",
            7,
        ).finalize(),
        parents,
    )
    projection = graph.project(
        {"config": [config], "metrics": [] if empty else [metric, metric]},
        {"config": OutputSpec(), "metrics": OutputSpec(required=not empty)},
    )
    return RetainedCase(
        projection,
        {seed: graph.nodes[seed].artifact},
        {
            item.address: graph.nodes[item.address].artifact
            for item in projection.occurrences
            if item.address != seed
        },
    )


def _selection(
    *, step_run_id: str | None = None, role: str = "metrics", name: str = "produce"
) -> OutcomeSelection:
    return OutcomeSelection(
        pipeline_run_id="synthetic-pipeline",
        step_name=name,
        role=role,
        step_run_id=step_run_id,
    )


def _finish(
    committer: RetainedCommitter, case: RetainedCase, step_run_id: str = "a" * 32
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, case, step_run_id=step_run_id
    )
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)


def _assessment() -> AssessmentSpec:
    return AssessmentSpec(
        label="quality",
        checks=[
            AssessmentCheck(
                check_id="score", metric_path=["score"], operator="ge", threshold=3
            )
        ],
        expected_count=1,
    )


def _comparison() -> ComparisonSpec:
    return ComparisonSpec(
        label="same read",
        matching=MetadataKeyMatch(key="case"),
        metrics=[
            ComparisonMetric(
                metric_id="score",
                metric_path=["score"],
                direction="higher",
                measurement_id="score",
                measurement_version="1",
                unit="unitless",
            )
        ],
        assessment=_assessment(),
    )


def test_role_population_deduplicates_exports_and_excludes_required_support(
    committer: RetainedCommitter,
) -> None:
    _finish(committer, _case(score=9007199254740993))
    report = assess_retained_outcomes(committer, _selection(), _assessment())
    assert report.observation_count == 1
    assert report.count_check is not None
    assert report.count_check.status == "pass"
    assert report.assessment_status == "pass"
    observed = report.observations[0]
    assert observed.checks[0].value == 9007199254740993
    assert type(observed.checks[0].value) is int
    assert observed.origin_step_number == observed.step_number == 7
    with pytest.raises(OutcomeSelectionError, match="metric"):
        read_retained_metric_observations(committer, _selection(role="config"), [])
    with pytest.raises(OutcomeSelectionError, match="no such output"):
        read_retained_metric_observations(committer, _selection(role="absent"), [])


def test_empty_declared_role_is_available_without_support_metrics(
    committer: RetainedCommitter,
) -> None:
    _finish(committer, _case(empty=True))
    result = assess_retained_outcomes(committer, _selection(), _assessment())
    assert result.population_status == "available"
    assert result.observation_count == 0
    assert result.observations == []
    assert result.assessment_status != "pass"


@pytest.mark.parametrize(
    "status", ["pending", "running", "failed", "cancelled", "skipped"]
)
def test_latest_unusable_attempt_masks_history_but_exact_selection_survives(
    committer: RetainedCommitter, status: str
) -> None:
    case = _case()
    _finish(committer, case)
    original = read_retained_metric_observations(
        committer, _selection(step_run_id="a" * 32), [["score"]]
    )
    current, admission, _ = prepare_scientific(
        committer,
        case,
        step_run_id="b" * 32,
        count=0 if status == "skipped" else 1,
        admit=status not in {"pending", "cancelled"},
    )
    if status == "failed":
        current.finish(
            admission.step_run_id, failures={0: StepUnitFailure(error="tool failed")}
        )
    elif status == "cancelled":
        for acknowledgement in (
            CancellationStatus.REQUESTED,
            CancellationStatus.CONFIRMED,
        ):
            current.tracker.record_cancellation(
                admission.step_run_id,
                StepStatus.PENDING,
                CancellationAcknowledgement(acknowledgement),
            )
        current.tracker.transition(
            admission.step_run_id,
            StepStatus.PENDING,
            StepStatus.CANCELLED,
            result=StepResult(
                step_run_id=admission.step_run_id,
                step_number=7,
                step_name="produce",
                status=StepStatus.CANCELLED,
                cancellation_status=CancellationStatus.CONFIRMED,
            ),
        )
    latest = read_retained_metric_observations(
        committer, _selection(role="cleared-or-dynamic"), [["score"]]
    )
    assert latest.population_status == "unavailable"
    assert latest.observation_count is None
    assert latest.selection.step_run_id == "b" * 32
    assert latest.selection.execution_status == StepStatus(status)
    assert (
        read_retained_metric_observations(
            committer, _selection(step_run_id="a" * 32), [["score"]]
        )
        == original
    )


@pytest.mark.parametrize("whole", [False, True])
def test_cached_population_preserves_exact_source_and_partial_state(
    committer: RetainedCommitter, whole: bool
) -> None:
    case = _case()
    results, admission, receipts = prepare_scientific(committer, case, count=2)
    committer.accept(receipts[0])
    results.finish(
        admission.step_run_id, failures={1: StepUnitFailure(error="failed slot")}
    )
    current, admitted, _ = prepare_scientific(
        committer, case, step_run_id="b" * 32, count=2 if whole else 1
    )
    if whole:
        hit = current.lookup(admitted.step_run_id, CachePolicy.STEP_COMPLETED)
        assert hit is not None
        current.reuse(hit)
    else:
        committer.journal.reuse(
            admitted.assignments[0],
            receipts[0],
            verify_source=partial(
                verify_cache_source, reader=RetainedReader(committer)
            ),
        )
        current.finish(admitted.step_run_id)
    observed = read_retained_metric_observations(committer, _selection(), [["score"]])
    assert observed.selection.step_run_id == "b" * 32
    assert observed.selection.execution_status == (
        StepStatus.PARTIAL if whole else StepStatus.SUCCEEDED
    )
    assert observed.observation_count == 1
    assert observed.observations[0].measurements[0].value == 3


def test_comparison_uses_one_proof_and_one_lifecycle_snapshot(
    committer: RetainedCommitter, monkeypatch: pytest.MonkeyPatch
) -> None:
    _finish(committer, _case(score=3))
    _finish(committer, _case(score=5), "b" * 32)
    prove = committer._control_proof
    calls = 0

    def counted() -> RetainedControlProof:
        nonlocal calls
        calls += 1
        return prove()

    monkeypatch.setattr(committer, "_control_proof", counted)
    original = retained_outcomes._read_population
    inserted = False

    def insert_retry(
        snapshot: RetainedReadSnapshot,
        state: StepState,
        selection: ResolvedOutcomeSelection,
        projection: retained_outcomes._Projection,
    ) -> MetricObservationSet:
        nonlocal inserted
        result = original(snapshot, state, selection, projection)
        if not inserted:
            inserted = True
            path = uri_join(committer.delta_root, TablePath.STEPS.value)
            rows = pl.read_delta(path)
            pending = rows.filter(
                (pl.col("step_run_id") == "b" * 32) & (pl.col("state_sequence") == 0)
            ).with_columns(
                pl.lit("c" * 32).alias("step_run_id"),
                pl.lit(datetime.now(UTC)).alias("timestamp"),
            )
            pending.write_delta(path, mode="append")
        return result

    monkeypatch.setattr(retained_outcomes, "_read_population", insert_retry)
    report = compare_retained_runs(
        committer, _selection(step_run_id="a" * 32), _selection(), _comparison()
    )
    assert report.baseline.selection.step_run_id == "a" * 32
    assert report.candidate.selection.step_run_id == "b" * 32
    assert report.pairs[0].metrics[0].classification == "improved"
    assert calls == 1
    latest = read_retained_metric_observations(committer, _selection(), [["score"]])
    assert latest.selection.step_run_id == "c" * 32
    assert latest.population_status == "unavailable"
    assert calls == 2


def test_selected_content_version_is_fixed_and_next_read_detects_tampering(
    committer: RetainedCommitter, monkeypatch: pytest.MonkeyPatch
) -> None:
    _finish(committer, _case())
    original = retained_outcomes._step_exports
    changed = False

    def mutate(
        snapshot: RetainedReadSnapshot, state: StepState, *, role: str | None = None
    ) -> Iterator[ProjectedOccurrence]:
        nonlocal changed
        if not changed:
            changed = True
            path = uri_join(committer.delta_root, "artifacts/metrics")
            pl.read_delta(path).with_columns(
                pl.lit(b'{"score":999}').alias("content")
            ).write_delta(path, mode="overwrite")
        return original(snapshot, state, role=role)

    monkeypatch.setattr(retained_outcomes, "_step_exports", mutate)
    observed = read_retained_metric_observations(committer, _selection(), [["score"]])
    assert observed.observations[0].measurements[0].value == 3
    with pytest.raises((StoreIntegrityError, ArtifactIntegrityError)):
        read_retained_metric_observations(committer, _selection(), [["score"]])


@pytest.mark.parametrize(
    "changed", ["terminal", "index", "metric", "missing_metric", "projection", "owner"]
)
def test_protected_selected_evidence_rejects_tampering(
    committer: RetainedCommitter, changed: str
) -> None:
    _finish(committer, _case())
    assert (
        read_retained_metric_observations(committer, _selection(), []).observation_count
        == 1
    )
    if changed == "terminal":
        path = TablePath.STEPS.value
        expression = (
            pl.when(pl.col("status") == "succeeded")
            .then(pl.lit(42.0))
            .otherwise(pl.col("duration_seconds"))
            .alias("duration_seconds")
        )
    elif changed == "index":
        path, expression = (
            TablePath.ARTIFACT_INDEX.value,
            pl.lit(9).cast(pl.Int32).alias("origin_step_number"),
        )
    elif changed == "projection":
        path, expression = (
            PROJECTION_TABLES[0].table_path,
            pl.lit("f" * 32).alias("artifact_id"),
        )
    elif changed == "owner":
        path, expression = (
            "artifacts/metrics",
            pl.lit("retained_result:" + "f" * 32).alias("logical_commit_id"),
        )
    else:
        path, expression = "artifacts/metrics", pl.lit(b"changed").alias("content")
    uri = uri_join(committer.delta_root, path)
    rows = pl.read_delta(uri)
    if changed == "missing_metric":
        rows = rows.head(0)
    else:
        rows = rows.with_columns(expression)
    rows.write_delta(uri, mode="overwrite")
    with pytest.raises(
        (StoreIntegrityError, ArtifactIntegrityError, PersistenceIntegrityError)
    ):
        read_retained_metric_observations(committer, _selection(), [["score"]])


@pytest.mark.parametrize("metric", [True, False])
def test_curator_metric_reuse_keeps_current_membership_and_original_origin(
    committer: RetainedCommitter,
    tmp_path: Path,
    metric: bool,
) -> None:
    case = _case() if metric else retained_case(tmp_path, kind="alias")
    results, admission, receipts = prepare_scientific(committer, case)
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    inputs = {
        "selected": RetainedReader(committer)
        .read(receipts[0])
        .output("metrics" if metric else "data")
    }
    operation = Merge()
    curator, admitted, reference = admit_curator(
        committer, operation, inputs, step_run_id="b" * 32, **STAMPS
    )
    view = CuratorView(committer, admitted.view)
    staged = run_curator_flow(
        ExecutionUnit(
            operation=operation,
            inputs={
                "selected": [item.artifact.artifact_id for item in inputs["selected"]]
            },
            step_number=8,
            step_run_id=reference.step_run_id,
            execution_spec_id=admitted.digest,
            replay_snapshot=ReplaySnapshot.unavailable("fixture"),
        ),
        WorkerConfig(
            delta_root=committer.delta_root,
            files_root=committer.files_root,
            staging_root=curator.staging.staging_dir,
        ),
        curator_view=view,
        read_recorder=CuratorReadRecorder(view, admitted),
    )
    terminal = read_staged_curator_result(
        admitted,
        staged.execution_run_id,
        staging_root=curator.staging.staging_dir,
        committer=committer,
    )
    curator.finish(reference, staged.execution_run_id, terminal)
    if not metric:
        with pytest.raises(OutcomeSelectionError, match="Every artifact"):
            read_retained_metric_observations(
                committer, _selection(name="merge", role="merged"), []
            )
        return
    first = read_retained_metric_observations(
        committer,
        _selection(name="merge", role="merged", step_run_id="b" * 32),
        [["score"]],
    )
    assert first.observation_count == 1
    _, _, current = admit_curator(
        committer, operation, inputs, step_run_id="c" * 32, step_number=9, **STAMPS
    )
    hit = lookup_curator(current, reader=CuratorReader(committer))
    assert hit is not None
    curator.reuse(hit)
    observed = read_retained_metric_observations(
        committer,
        _selection(name="merge", role="merged", step_run_id="c" * 32),
        [["score"]],
    )
    assert observed.observations[0].step_number == 9
    assert observed.observations[0].origin_step_number == 7
    assert observed.observations[0].artifact_id == first.observations[0].artifact_id
    with pytest.raises(OutcomeSelectionError, match="ambiguous"):
        read_retained_metric_observations(
            committer, _selection(name="merge", role="merged"), []
        )


def test_invalid_comparison_selection_fails_before_any_store_read(
    committer: RetainedCommitter, monkeypatch: pytest.MonkeyPatch
) -> None:
    def unexpected() -> RetainedControlProof:
        pytest.fail("invalid request must not read authority")

    monkeypatch.setattr(committer, "_control_proof", unexpected)
    invalid = _selection().model_copy(update={"pipeline_run_id": ""})
    with pytest.raises(OutcomeSelectionError):
        compare_retained_runs(committer, _selection(), invalid, _comparison())


def test_selected_inline_metrics_do_not_hydrate_required_external_support(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    case = _case(external=tmp_path / "support.bin")
    results, admission, receipts = prepare_scientific(committer, case)
    plan = committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    assert len(plan.carriers) == 1
    committer.fs.rm(uri_join(committer.files_root, plan.carriers[0].relative_path))
    observed = read_retained_metric_observations(committer, _selection(), [["score"]])
    assert observed.observations[0].measurements[0].value == 3
    with pytest.raises(FileNotFoundError):
        RetainedReader(committer).read(receipts[0])


def test_diagnostic_attempt_is_not_an_assessable_population(
    committer: RetainedCommitter,
) -> None:
    tracker = RetainedStepTracker(committer, "synthetic-pipeline")
    tracker.create_attempt(
        StepStartRecord(
            step_run_id="d" * 32,
            step_number=7,
            step_name="produce",
            operation_class="synthetic.Operation",
            params_json="{}",
            input_refs_json="{}",
            compute_backend="local",
            compute_options_json="{}",
            output_roles_json="[]",
            output_types_json="{}",
            replay_of_execution_run_id="a" * 32,
        )
    )
    with pytest.raises(OutcomeSelectionError, match="Diagnostic"):
        read_retained_metric_observations(
            committer, _selection(step_run_id="d" * 32), []
        )
