"""Curator reuse owns current lifecycle while preserving original producer scope."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import polars as pl
import pytest
from fixtures.curator_cache import STAMPS, DynamicCurator, MeasureCSV, stage_observed
from fixtures.retained_acceptance import open_committer
from fixtures.retained_curators import admit_curator
from fixtures.retained_readers import Backend, admit_read_case
from fixtures.retained_results import RetainedCase, _derive

from artisan.composites.base.compiler import compile_definition
from artisan.errors import StoreIntegrityError
from artisan.execution.composition import execute_compiled_plan
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.executor import Executor
from artisan.execution.executors.curator import run_curator_flow
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.curator.merge import Merge
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.orchestration.engine.retained_inputs import prepare_step_inputs
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas import (
    CSVArtifact,
    ExecutionConfigArtifact,
    MetricArtifact,
    OutputSpec,
)
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.assignment import AssignmentAttempt
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.worker_config import WorkerConfig
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.storage.cache.curator_dependencies import CuratorReadRecorder
from artisan.storage.cache.curator_identity import curator_cache_key
from artisan.storage.cache.curator_lookup import CuratorCacheHit, lookup_curator
from artisan.storage.cache.retained_identity import retained_cache_key
from artisan.storage.cache.retained_lookup import lookup_assignment
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.curator_view import CuratorView
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.admission import publish_admission
from artisan.storage.io.curator_admission import read_curator_admission
from artisan.storage.io.curator_plan import read_curator_plan
from artisan.storage.io.curator_reuse_plan import CuratorReusePlan
from artisan.storage.io.retained_commit import RetainedCommitter


def _hit(
    backend: Backend, *, empty: bool = False
) -> tuple[RetainedCommitter, CuratorResults, CuratorCacheHit]:
    fs, storage, root = backend
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    operation = DynamicCurator()
    operation.params.empty = empty
    coordinator, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(
        coordinator, admission, operation, storage=storage
    )
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    _, _, current = admit_curator(
        committer, operation, {}, step_run_id="b" * 32, step_number=9, **STAMPS
    )
    hit = lookup_curator(current, reader=CuratorReader(committer))
    assert hit is not None
    return committer, coordinator, hit


@pytest.mark.parametrize("empty", [False, True])
def test_reuse_preserves_original_outputs_and_publishes_only_current_membership(
    raw_backend_fs: Backend, monkeypatch: pytest.MonkeyPatch, empty: bool
) -> None:
    committer, coordinator, hit = _hit(raw_backend_fs, empty=empty)
    monkeypatch.setattr(
        DynamicCurator,
        "execute_curator",
        lambda *_: pytest.fail("A cache hit must not execute curator code"),
    )
    result = coordinator.reuse(hit)
    assert result.status == StepStatus.SUCCEEDED
    assert result.disposition == StepDisposition.CACHE_HIT
    assert result.step_run_id == hit.admission.step_run_id
    assert result.step_number == 9
    assert result.metadata == hit.source.result.metadata
    assert result.output_roles == hit.source.result.output_roles
    assert coordinator.reuse(hit) == result
    plan = read_curator_plan(
        committer.delta_root, hit.admission.step_run_id, committer.fs
    )
    assert isinstance(plan, CuratorReusePlan)
    assert {item.table_path for item in plan.tables} == {
        TablePath.STEPS.value,
        TablePath.CACHE_REUSE.value,
    }
    accepted = CuratorReader(committer).read_result(plan.step_run_id, plan.plan_digest)
    assert accepted.result == result
    assert accepted.outputs == hit.source.outputs
    assert accepted.projection == hit.source.projection
    executions = pl.read_delta(
        f"{committer.delta_root}/{TablePath.EXECUTIONS.value}",
        storage_options=committer.storage_options,
    )
    assert executions.height == 1
    if not empty:
        original = accepted.output("found")[0].reference
        assert CuratorReader(committer).read(original).plan == hit.source.plan
        forged = original.model_copy(
            update={"step_run_id": plan.step_run_id, "plan_digest": plan.plan_digest}
        )
        with pytest.raises(StoreIntegrityError, match="occurrence|producer"):
            CuratorReader(committer).read(forged)


@pytest.mark.parametrize(
    "boundary",
    [
        "plan",
        "planned",
        "table:orchestration/cache_reuse",
        "table:orchestration/steps",
        "complete",
    ],
)
def test_reuse_recovers_exact_source_after_controller_process_death(
    raw_backend_fs: Backend, boundary: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    committer, coordinator, hit = _hit(raw_backend_fs)
    _, storage, _ = raw_backend_fs
    process = subprocess.run(
        [sys.executable, "-m", "fixtures.curator_reuse"],
        input=json.dumps(
            {
                "storage": storage.model_dump(mode="json"),
                "root": committer.delta_root,
                "staging": coordinator.staging.staging_dir,
                "boundary": boundary,
                "reference": hit.admission.model_dump(mode="json"),
            }
        ),
        text=True,
        capture_output=True,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).parents[2])},
        timeout=120,
        check=False,
    )
    assert process.returncode == 41, process.stderr
    reopened = open_committer(committer.delta_root, committer.fs, storage)
    recovery = CuratorResults(
        RetainedStepTracker(reopened, "synthetic-pipeline"),
        coordinator.staging.staging_dir,
    )
    if boundary == "table:orchestration/steps":
        rows = recovery.tracker._rows_for_attempt(hit.admission.step_run_id)
        terminal = rows.sort("state_sequence").row(-1, named=True)
        conflicting = rows.with_columns(
            pl.when(pl.col("state_sequence") == terminal["state_sequence"])
            .then(pl.lit("different terminal metadata"))
            .otherwise(pl.col("step_name"))
            .alias("step_name")
        )
        with monkeypatch.context() as patch:
            patch.setattr(recovery.tracker, "_rows_for_attempt", lambda _: conflicting)
            with pytest.raises(StoreIntegrityError, match="different evidence"):
                recovery._validate_terminal_history(terminal)
    recovered = recovery.recover(hit.admission.step_run_id)
    assert recovered.disposition == StepDisposition.CACHE_HIT
    plan = read_curator_plan(
        reopened.delta_root, hit.admission.step_run_id, reopened.fs
    )
    assert isinstance(plan, CuratorReusePlan)
    assert (
        CuratorReader(reopened).read_result(plan.step_run_id, plan.plan_digest).outputs
        == hit.source.outputs
    )


def test_reuse_cancellation_fence_and_plan_first_ordering(
    raw_backend_fs: Backend,
) -> None:
    committer, coordinator, hit = _hit(raw_backend_fs)
    requested = CancellationAcknowledgement(status=CancellationStatus.REQUESTED)
    confirmed = CancellationAcknowledgement(status=CancellationStatus.CONFIRMED)
    coordinator.record_cancellation(hit.admission.step_run_id, requested)
    coordinator.record_cancellation(hit.admission.step_run_id, confirmed)
    with pytest.raises(StoreIntegrityError, match="cancellation fence"):
        coordinator.reuse(hit)
    assert (
        read_curator_plan(committer.delta_root, hit.admission.step_run_id, committer.fs)
        is None
    )
    _, _, reference = admit_curator(
        committer, DynamicCurator(), {}, step_run_id="c" * 32, step_number=10, **STAMPS
    )
    second = lookup_curator(reference, reader=CuratorReader(committer))
    assert second is not None

    def interrupt(event: str) -> None:
        if event == "plan":
            message = "selection interrupted"
            raise RuntimeError(message)

    coordinator.checkpoint = interrupt
    with pytest.raises(RuntimeError, match="selection interrupted"):
        coordinator.reuse(second)
    coordinator.checkpoint = None
    state = coordinator.record_cancellation(reference.step_run_id, confirmed)
    assert state.status == StepStatus.SUCCEEDED
    assert state.disposition == StepDisposition.CACHE_HIT
    assert state.cancellation_status is None


def test_reuse_rechecks_lookup_key_and_selected_source(raw_backend_fs: Backend) -> None:
    committer, coordinator, hit = _hit(raw_backend_fs)
    with pytest.raises(StoreIntegrityError, match="lookup key"):
        coordinator.reuse(replace(hit, cache_key="0" * 32))
    source = replace(
        hit.source, plan=hit.source.plan.model_copy(update={"plan_digest": "0" * 32})
    )
    with pytest.raises(StoreIntegrityError, match="matching immutable"):
        coordinator.reuse(replace(hit, source=source))
    assert (
        read_curator_plan(committer.delta_root, hit.admission.step_run_id, committer.fs)
        is None
    )


def test_curator_reuse_preserves_downstream_creator_cache_key_and_hit(
    raw_backend_fs: Backend,
) -> None:
    committer, coordinator, hit = _hit(raw_backend_fs)
    reader = RetainedReader(committer)
    plan = compile_definition(MeasureCSV)
    first = prepare_step_inputs(
        plan.recipe,
        {"source": hit.source.output("found")},
        step_run_id="c" * 32,
        reader=reader,
    )[0]
    with Executor() as executor:
        execution = execute_compiled_plan(
            plan,
            {
                "source": [
                    first.artifacts[address]
                    for address in first.boundary.input_roles()["source"]
                ]
            },
            executor,
            invocation_id=first.assignment.assignment_id,
            execute_adapter=lambda *_: LocalExecuteRouter(),
            frozen_context=first.boundary.frozen_context,
            input_attribution=first.boundary.input_attribution,
        )
        case = RetainedCase(
            execution.projection, first.artifacts, execution.retained_artifacts
        )
        admitted, receipt = admit_read_case(
            committer, case, step_run_id="c" * 32, recipe=plan.recipe
        )
        committer.accept(receipt)
    coordinator.reuse(hit)
    selected = read_curator_plan(
        committer.delta_root, hit.admission.step_run_id, committer.fs
    )
    assert isinstance(selected, CuratorReusePlan)
    reused = CuratorReader(committer).read_result(
        selected.step_run_id, selected.plan_digest
    )
    second = prepare_step_inputs(
        plan.recipe,
        {"source": reused.output("found")},
        step_run_id="d" * 32,
        reader=reader,
    )[0]
    attempt = AssignmentAttempt(
        assignment=second.assignment,
        attempt=0,
        generation=0,
        recipe_digest=plan.recipe.digest,
        software_digest="c" * 64,
        input_context_digest=second.boundary.input_context_digest,
    )
    current = publish_admission(
        attempt,
        plan.recipe,
        second.boundary,
        second.artifacts,
        step_number=10,
        delta_root=committer.delta_root,
        files_root=committer.files_root,
        fs=committer.fs,
        verify_producer=committer._verify_producer,
    )
    committer.journal.admit(attempt, admission_digest=current.record_digest)
    assert retained_cache_key(
        plan.recipe, admitted.attempt, first.boundary
    ) == retained_cache_key(plan.recipe, attempt, second.boundary)
    downstream = lookup_assignment(current, reader=reader)
    assert downstream is not None
    assert downstream.receipt == receipt


@pytest.mark.parametrize(
    "change", ["diagnostic", "uncacheable", "unqualified", "software_ineligible"]
)
def test_reuse_rejects_forced_ineligible_current_admission(
    raw_backend_fs: Backend, change: str
) -> None:
    committer, coordinator, hit = _hit(raw_backend_fs)
    current = read_curator_admission(
        hit.admission, delta_root=committer.delta_root, fs=committer.fs
    )
    record = current.record.model_copy(
        update={"step_run_id": "c" * 32, "step_number": 10}
    )
    updates: dict[str, object] = {"record": record}
    if change == "diagnostic":
        updates["record"] = record.model_copy(
            update={"replay_of_execution_run_id": "e" * 32}
        )
    elif change == "uncacheable":
        updates["cacheable"] = False
        assert current.definition is not None
        updates["definition"] = current.definition.model_copy(
            update={"cacheable": False}
        )
    elif change == "software_ineligible":
        updates["software_cacheable"] = False
    else:
        updates.update(definition=None, software_digest=None)
    admission = current.model_copy(update=updates)
    reference = coordinator.admit(admission)
    assert lookup_curator(reference, reader=CuratorReader(committer)) is None
    forced = replace(
        hit, admission=reference, cache_key=curator_cache_key(admission) or "0" * 32
    )
    with pytest.raises(StoreIntegrityError, match="eligible|lookup key"):
        coordinator.reuse(forced)
    assert (
        read_curator_plan(committer.delta_root, reference.step_run_id, committer.fs)
        is None
    )


def test_reuse_preserves_source_sets_frozen_queries_and_required_support(
    raw_backend_fs: Backend,
) -> None:
    fs, storage, root = raw_backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    reader = RetainedReader(committer)
    graph = OccurrenceGraph()
    primary = CSVArtifact.draft(b"value\n1\n", "source.csv").finalize()
    boundary = graph.add_input("data", primary)
    payload = CSVArtifact.draft(b"value\n2\n", "support.csv").finalize()
    support = _derive(graph, "support", payload, [boundary])
    config = ExecutionConfigArtifact.draft(
        {"input": {"$artifact": payload.artifact_id}}, "config.json"
    ).finalize()
    exported = _derive(graph, "configure", config, [boundary])
    metric = MetricArtifact.draft({"score": 1}, "score.json").finalize()
    measure = _derive(graph, "measure", metric, [exported])
    case = RetainedCase(
        graph.project(
            {"config": [exported], "metrics": [measure]},
            {"config": OutputSpec(), "metrics": OutputSpec()},
        ),
        {boundary: primary},
        {support: payload, exported: config, measure: metric},
    )
    _, receipt = admit_read_case(committer, case, step_run_id="a" * 32)
    committer.accept(receipt)
    supplied = reader.read(receipt).output("config")
    operation = Merge()
    coordinator, admission, reference = admit_curator(
        committer, operation, {"data": supplied}, step_run_id="b" * 32, **STAMPS
    )
    view = CuratorView(committer, admission.view)
    staged = run_curator_flow(
        ExecutionUnit(
            operation=operation,
            inputs={"data": [config.artifact_id]},
            step_number=admission.record.step_number,
            step_run_id=reference.step_run_id,
            execution_spec_id=admission.digest,
            replay_snapshot=ReplaySnapshot.unavailable("curator-reuse-fixture"),
        ),
        WorkerConfig(
            delta_root=committer.delta_root,
            staging_root=coordinator.staging.staging_dir,
            files_root=committer.files_root,
            storage=storage,
        ),
        curator_view=view,
        read_recorder=CuratorReadRecorder(view, admission),
    )
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    coordinator.finish(
        reference,
        staged.execution_run_id,
        StepResult(
            step_run_id=reference.step_run_id,
            step_number=admission.record.step_number,
            step_name=operation.name,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=1,
            succeeded_count=1,
            output_roles=frozenset({"merged"}),
            output_types={"merged": "any"},
        ),
    )
    _, _, current = admit_curator(
        committer,
        operation,
        {"data": supplied},
        step_run_id="c" * 32,
        step_number=9,
        **STAMPS,
    )
    hit = lookup_curator(current, reader=CuratorReader(committer))
    assert hit is not None
    assert hit.source.projection.references
    assert any(
        item.artifact_type == "metric" and item.members
        for item in hit.source.projection.queries
    )
    assert any(member.sources for member in hit.source.projection.occurrences)
    coordinator.reuse(hit)
    plan = read_curator_plan(committer.delta_root, current.step_run_id, fs)
    assert isinstance(plan, CuratorReusePlan)
    reused = CuratorReader(committer).read_result(plan.step_run_id, plan.plan_digest)
    assert reused.projection == hit.source.projection
    assert reused.outputs == hit.source.outputs
    assert {item.artifact_id for item in reused.artifacts.values()} == {
        config.artifact_id,
        payload.artifact_id,
        metric.artifact_id,
    }
    capture = reused.projection.references[0]
    assert capture.direct_parents == ()
    assert [reused.artifacts[address].artifact_id for address in capture.members] == [
        payload.artifact_id
    ]
