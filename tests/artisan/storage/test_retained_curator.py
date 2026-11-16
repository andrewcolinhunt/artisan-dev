"""Curator completion preserves existing content selection without fake winners."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest
from fixtures.retained_acceptance import admit_case, open_committer
from fixtures.retained_curators import admit_curator, stage_curator
from fixtures.retained_readers import Backend, admit_read_case
from fixtures.retained_results import RetainedCase, _derive, boundary_artifact
from fsspec import AbstractFileSystem
from pydantic import PrivateAttr

from artisan.errors import PersistenceIntegrityError, StoreIntegrityError
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.curator.filter import Criterion, Filter
from artisan.operations.curator.ingest_csv import IngestCSV
from artisan.operations.curator.merge import Merge
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.artifact.appendable import AppendableArtifact
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_cache import CuratorCacheProof
from artisan.schemas.execution.curator_result import ArtifactResult, PassthroughResult
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.provenance.occurrence import ReferenceContext, RequiredReference
from artisan.schemas.provenance.projection import (
    AcceptedCuratorOccurrence,
    BoundaryInputAttribution,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.cache.curator_identity import curator_cache_key
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.core.retained_reader import AcceptedArtifact, RetainedReader
from artisan.storage.core.table_schemas import get_physical_schema
from artisan.storage.io.curator_admission import publish_curator_admission
from artisan.storage.io.curator_carriers import curator_carrier_directory
from artisan.storage.io.curator_plan import read_curator_plan
from artisan.storage.io.delta_files import append_table_files
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.utils.hashing import compute_content_digest


def test_curator_ingest_stream_publishes_own_completed_content_authority(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_case(committer, tmp_path, kind="file")
    committer.accept(receipt)
    supplied = RetainedReader(committer).read(receipt).support
    operation = IngestCSV()
    coordinator, admission, reference = admit_curator(
        committer, operation, {"file": supplied}, step_run_id="b" * 32
    )
    assert coordinator.admit(admission) == reference
    staged, terminal = stage_curator(coordinator, admission, operation)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    assert (
        coordinator.finish(reference, staged.execution_run_id, terminal).status
        == StepStatus.SUCCEEDED
    )
    plan = read_curator_plan(committer.delta_root, reference.step_run_id, fs)
    assert plan is not None
    selected = AcceptedCuratorOccurrence(
        step_run_id=plan.step_run_id,
        plan_digest=plan.plan_digest,
        address=plan.projection.outputs[0].occurrences[0],
    )
    result = CuratorReader(committer).read(selected)
    assert result.output("csv")
    assert all(item.artifact.origin_step_number == 8 for item in result.output("csv"))
    assert result.projection.lineage
    assert all(
        item.reference.step_run_id == reference.step_run_id
        for item in result.output("csv")
    )
    output = result.output("csv")[0]
    graph = OccurrenceGraph()
    slot = graph.add_input("data", output.artifact)
    case = RetainedCase(
        graph.project(
            {"result": [slot]},
            {"result": OutputSpec()},
            input_attribution=(
                BoundaryInputAttribution(boundary=slot, producer=output.reference),
            ),
        ),
        {slot: output.artifact},
        {},
    )
    _, child = admit_read_case(committer, case, step_run_id="c" * 32)
    committer.accept(child)
    child_result = RetainedReader(committer).read(child)
    assert child_result.projection.input_attribution[0].producer == output.reference


def test_curator_merge_deduplicates_content_and_keeps_all_supplied_sources(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_case(committer, tmp_path, kind="alias")
    committer.accept(receipt)
    original = RetainedReader(committer).read(receipt).output("data")[0]
    operation = Merge()
    coordinator, admission, reference = admit_curator(
        committer,
        operation,
        {"first": (original,), "second": (original,)},
        step_run_id="b" * 32,
    )
    staged, terminal = stage_curator(coordinator, admission, operation)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    plan = read_curator_plan(committer.delta_root, reference.step_run_id, fs)
    assert plan is not None
    assert len(plan.projection.outputs[0].occurrences) == 1
    assert plan.projection.occurrences[0].sources == (original.reference,)
    assert not plan.projection.lineage


def test_curator_appendable_carrier_tamper_blocks_downstream_admission(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_case(committer, tmp_path, kind="appendable")
    committer.accept(receipt)
    original = RetainedReader(committer).read(receipt).support[0]
    assert isinstance(original.artifact, AppendableArtifact)
    coordinator, admission, reference = admit_curator(
        committer, Merge(), {"first": (original,)}, step_run_id="b" * 32
    )
    staged, terminal = stage_curator(coordinator, admission, Merge())
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    plan = read_curator_plan(committer.delta_root, reference.step_run_id, fs)
    assert plan is not None
    selected = CuratorReader(committer).read_result(plan.step_run_id, plan.plan_digest)
    output = selected.outputs[0].occurrences[0]
    path = original.artifact.external_path
    assert path is not None
    with fs.open(path, "rb") as stream:
        content = stream.read()
    with fs.open(path, "wb") as stream:
        stream.write(content + b'{"record_id":"unregistered","value":999}\n')
    original.artifact.verify_external_content(fs=fs)
    graph = OccurrenceGraph()
    slot = graph.add_input("data", output.artifact)
    case = RetainedCase(
        graph.project(
            {"result": [slot]},
            {"result": OutputSpec()},
            input_attribution=(
                BoundaryInputAttribution(
                    boundary=slot,
                    producer=output.reference,
                ),
            ),
        ),
        {slot: output.artifact},
        {},
    )
    with pytest.raises(StoreIntegrityError, match="full carrier changed"):
        admit_read_case(committer, case, step_run_id="c" * 32)


def test_curator_rejects_detached_cache_proof(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_case(committer, tmp_path, kind="alias")
    committer.accept(receipt)
    original = RetainedReader(committer).read(receipt).output("data")[0]
    coordinator, admission, reference = admit_curator(
        committer,
        Merge(),
        {"first": (original,)},
        step_run_id="b" * 32,
        software_digest="e" * 64,
    )
    staged, terminal = stage_curator(coordinator, admission, Merge())
    assert staged.execution_run_id is not None
    key = curator_cache_key(admission)
    assert key is not None
    detached = CuratorCacheProof(
        definition_digest=admission.definition_digest,
        software_digest="e" * 64,
        cache_key=key,
        snapshot_digest=admission.view.digest,
        views=(),
    )
    with pytest.raises(StoreIntegrityError, match="differs from its sealed"):
        coordinator.finish(
            reference, staged.execution_run_id, terminal, cache_proof=detached
        )
    assert read_curator_plan(committer.delta_root, reference.step_run_id, fs) is None


def test_curator_rejects_seal_for_another_admission_spec(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_case(committer, tmp_path, kind="alias")
    committer.accept(receipt)
    original = RetainedReader(committer).read(receipt).output("data")[0]
    coordinator, admission, reference = admit_curator(
        committer, Merge(), {"first": (original,)}, step_run_id="b" * 32
    )
    other = admission.model_copy(update={"operation_version": "different"})
    staged, terminal = stage_curator(coordinator, other, Merge())
    assert staged.execution_run_id is not None
    with pytest.raises(StoreIntegrityError, match="sealed execution"):
        coordinator.finish(reference, staged.execution_run_id, terminal)
    assert read_curator_plan(committer.delta_root, reference.step_run_id, fs) is None


@pytest.mark.parametrize("kind", ["alias", "file"])
def test_failed_curator_seal_cannot_publish_output_or_artifact_effects(
    backend_fs: Backend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    from artisan.execution.executors import curator_stream
    from artisan.execution.recording import parquet_writer

    write_record = parquet_writer._write_execution_record

    def failed_record(**kwargs: Any) -> None:
        write_record(
            **{
                **kwargs,
                "success": False,
                "error": "failed worker",
                "curator_carriers": None,
            }
        )

    monkeypatch.setattr(parquet_writer, "_write_execution_record", failed_record)
    monkeypatch.setattr(curator_stream, "_write_execution_record", failed_record)
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_case(committer, tmp_path, kind=kind)
    committer.accept(receipt)
    accepted = RetainedReader(committer).read(receipt)
    operation = Merge() if kind == "alias" else IngestCSV()
    inputs = (
        {"first": accepted.output("data")}
        if kind == "alias"
        else {
            "file": accepted.support,
        }
    )
    coordinator, admission, reference = admit_curator(
        committer, operation, inputs, step_run_id="b" * 32
    )
    staged, terminal = stage_curator(coordinator, admission, operation)
    assert staged.execution_run_id is not None
    failed = terminal.model_copy(
        update={
            "status": StepStatus.FAILED,
            "disposition": None,
            "succeeded_count": 0,
            "failed_count": 1,
            "output_roles": frozenset(),
            "output_types": {},
            "error": "failed worker",
        }
    )
    with pytest.raises(StoreIntegrityError, match="must not publish"):
        coordinator.finish(reference, staged.execution_run_id, failed)
    assert read_curator_plan(committer.delta_root, reference.step_run_id, fs) is None


def test_diagnostic_curator_edges_are_excluded_from_ordinary_discovery(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_case(committer, tmp_path, kind="file")
    committer.accept(receipt)
    supplied = RetainedReader(committer).read(receipt).support
    operation = IngestCSV()
    coordinator, initial, _ = admit_curator(
        committer, operation, {"file": supplied}, step_run_id="b" * 32
    )
    diagnostic = initial.model_copy(
        update={
            "record": initial.record.model_copy(
                update={"step_run_id": "c" * 32, "replay_of_execution_run_id": "f" * 32}
            )
        }
    )
    reference = coordinator.admit(diagnostic)
    staged, terminal = stage_curator(coordinator, diagnostic, operation)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    plan = read_curator_plan(committer.delta_root, reference.step_run_id, fs)
    assert plan is not None
    assert plan.projection.lineage
    view = CuratorView(committer, capture_curator_view(committer))
    assert (
        view.artifact_edges(direct=True)
        .filter(pl.col("execution_run_id") == staged.execution_run_id)
        .collect()
        .is_empty()
    )
    assert (
        view.scan(TablePath.EXECUTION_EDGES)
        .filter(pl.col("execution_run_id") == staged.execution_run_id)
        .collect()
        .is_empty()
    )
    selected = CuratorReader(committer).read_result(plan.step_run_id, plan.plan_digest)
    next_coordinator, next_admission, next_reference = admit_curator(
        committer, Merge(), {"first": selected.output("csv")}, step_run_id="d" * 32
    )
    next_staged, next_terminal = stage_curator(
        next_coordinator, next_admission, Merge()
    )
    assert next_staged.execution_run_id is not None
    next_coordinator.finish(next_reference, next_staged.execution_run_id, next_terminal)
    next_plan = read_curator_plan(committer.delta_root, next_reference.step_run_id, fs)
    assert next_plan is not None
    assert next_plan.projection.occurrences[0].sources == (
        selected.output("csv")[0].reference,
    )


@pytest.mark.parametrize("diagnostic", [False, True])
def test_curator_preserves_required_sibling_reference_without_parent_edge(
    backend_fs: Backend,
    diagnostic: bool,
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    graph = OccurrenceGraph()
    source = graph.add_input("data", boundary_artifact())
    payload = CSVArtifact.draft(b"value\n2\n", "support.csv", 7).finalize()
    support = _derive(graph, "support", payload, [source])
    config = ExecutionConfigArtifact.draft(
        {"input": {"$artifact": payload.artifact_id}}, "config.json", 7
    ).finalize()
    exported = _derive(graph, "config", config, [source])
    case = RetainedCase(
        graph.project({"config": [exported]}, {"config": OutputSpec()}),
        {source: graph.nodes[source].artifact},
        {support: payload, exported: config},
    )
    _, receipt = admit_read_case(
        committer,
        case,
        step_run_id="a" * 32,
        replay_of="f" * 32 if diagnostic else None,
    )
    committer.accept(receipt)
    original = RetainedReader(committer).read(receipt).output("config")[0]
    coordinator, admission, reference = admit_curator(
        committer, Merge(), {"first": (original,)}, step_run_id="b" * 32
    )
    staged, terminal = stage_curator(coordinator, admission, Merge())
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    plan = read_curator_plan(committer.delta_root, reference.step_run_id, fs)
    assert plan is not None
    result = CuratorReader(committer).read_result(plan.step_run_id, plan.plan_digest)
    capture = result.projection.references[0]
    assert {result.artifacts[key].artifact_id for key in capture.members} == {
        payload.artifact_id
    }
    assert capture.members[0] not in capture.direct_parents
    assert not result.projection.lineage


def test_curator_ambiguous_content_reference_policy_rejects_arbitrary_source(
    backend_fs: Backend, monkeypatch: pytest.MonkeyPatch
) -> None:
    def policy(
        _artifact: Artifact, context: ReferenceContext
    ) -> tuple[RequiredReference, ...]:
        if len(context.direct_parents) != 1:
            msg = "Metric requires exactly one source parent"
            raise ValueError(msg)
        parent = context.direct_parents[0]
        return (
            RequiredReference(
                address=parent,
                artifact_id=context.artifacts[parent].artifact_id,
                artifact_type="csv",
            ),
        )

    monkeypatch.setattr(
        ArtifactTypeDef.get("metric"), "required_references", staticmethod(policy)
    )
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    inputs = {}
    for owner, value in (("a", 1), ("b", 2)):
        graph = OccurrenceGraph(boundary_invocation=owner)
        artifact = CSVArtifact.draft(
            f"value\n{value}\n".encode(), "data.csv", 7
        ).finalize()
        source = graph.add_input("data", artifact)
        metric = MetricArtifact.draft({"score": 1}, "metric.json", 7).finalize()
        target = _derive(graph, "measure", metric, [source])
        case = RetainedCase(
            graph.project({"metric": [target]}, {"metric": OutputSpec()}),
            {source: artifact},
            {target: metric},
        )
        _, receipt = admit_read_case(committer, case, step_run_id=owner * 32)
        committer.accept(receipt)
        inputs[owner] = RetainedReader(committer).read(receipt).output("metric")
    coordinator, admission, reference = admit_curator(
        committer, Merge(), inputs, step_run_id="c" * 32
    )
    staged, terminal = stage_curator(coordinator, admission, Merge())
    assert staged.execution_run_id is not None
    with pytest.raises(ValueError, match="exactly one source"):
        coordinator.finish(reference, staged.execution_run_id, terminal)
    assert read_curator_plan(committer.delta_root, reference.step_run_id, fs) is None


@pytest.mark.parametrize("table_kind", ["index", "content"])
def test_curator_read_rejects_changed_reused_content_origin(
    backend_fs: Backend, tmp_path: Path, table_kind: str
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_case(committer, tmp_path, kind="alias")
    committer.accept(receipt)
    original = RetainedReader(committer).read(receipt).output("data")[0]
    coordinator, admission, reference = admit_curator(
        committer, Merge(), {"first": (original,)}, step_run_id="b" * 32
    )
    staged, terminal = stage_curator(coordinator, admission, Merge())
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    plan = read_curator_plan(committer.delta_root, reference.step_run_id, fs)
    assert plan is not None
    table_path = (
        TablePath.ARTIFACT_INDEX.value
        if table_kind == "index"
        else ArtifactTypeDef.get_table_path(original.artifact.artifact_type)
    )
    path = f"{committer.delta_root}/{table_path}"
    frame = pl.read_delta(path, storage_options=storage.delta_storage_options())
    frame.with_columns(
        pl.lit(99, dtype=pl.Int32).alias("origin_step_number")
    ).write_delta(
        path, mode="overwrite", storage_options=storage.delta_storage_options()
    )
    with pytest.raises(StoreIntegrityError, match="origin|conflicting"):
        CuratorReader(committer).read_result(plan.step_run_id, plan.plan_digest)


def _measured(
    committer: RetainedCommitter,
    step: str,
    field: str,
    *,
    replay_of: str | None = None,
) -> AcceptedArtifact:
    graph = OccurrenceGraph(boundary_invocation=f"input-{step}")
    data = boundary_artifact()
    slot = graph.add_input("data", data)
    metric = MetricArtifact.draft({field: 1}, f"{field}.json", 7).finalize()
    measured = _derive(graph, "measure", metric, [slot])
    case = RetainedCase(
        graph.project(
            {"data": [slot], "metric": [measured]},
            {"data": OutputSpec(), "metric": OutputSpec()},
        ),
        {slot: data},
        {measured: metric},
    )
    _, receipt = admit_read_case(
        committer, case, step_run_id=step * 32, replay_of=replay_of
    )
    committer.accept(receipt)
    return RetainedReader(committer).read(receipt).output("data")[0]


def test_curator_explicit_diagnostic_input_does_not_expand_ordinary_queries(
    backend_fs: Backend,
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    diagnostic = _measured(committer, "a", "debug_score", replay_of="f" * 32)
    coordinator, admission, reference = admit_curator(
        committer,
        Merge(),
        {"first": (diagnostic,)},
        step_run_id="b" * 32,
    )
    staged, terminal = stage_curator(coordinator, admission, Merge())
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    plan = read_curator_plan(committer.delta_root, reference.step_run_id, fs)
    assert plan is not None
    reader = CuratorReader(committer)
    result = reader.read_result(plan.step_run_id, plan.plan_digest)
    selected = result.outputs[0].occurrences[0].reference
    assert isinstance(selected, AcceptedCuratorOccurrence)
    assert reader.parents(selected) == (diagnostic.reference,)
    assert reader.associated(selected, "metric") == ()


@pytest.mark.parametrize("operation_name", ["merge", "filter"])
def test_curator_source_sets_do_not_invent_global_query_derivation(
    backend_fs: Backend, operation_name: str
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    first, second = (
        _measured(committer, "a", "score_a"),
        _measured(committer, "b", "score_b"),
    )
    operation = (
        Merge()
        if operation_name == "merge"
        else Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score_b", operator="gt", value=0)]
            )
        )
    )
    inputs = (
        {"first": (first,), "second": (second,)}
        if operation_name == "merge"
        else {"passthrough": (first,)}
    )
    coordinator, admission, reference = admit_curator(
        committer, operation, inputs, step_run_id="c" * 32
    )
    staged, terminal = stage_curator(coordinator, admission, operation)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    plan = read_curator_plan(committer.delta_root, reference.step_run_id, fs)
    assert plan is not None
    address = plan.projection.outputs[0].occurrences[0]
    selected = AcceptedCuratorOccurrence(
        step_run_id=plan.step_run_id, plan_digest=plan.plan_digest, address=address
    )
    reader = CuratorReader(committer)
    assert len(reader.associated(selected, "metric")) == 2
    expected = (
        {first.reference, second.reference}
        if operation_name == "merge"
        else {first.reference}
    )
    assert set(reader.parents(selected)) == expected
    assert not plan.projection.lineage
    with pytest.raises(StoreIntegrityError, match="no matching"):
        reader.read(selected.model_copy(update={"plan_digest": "f" * 32}))


def test_curator_frozen_plan_wins_against_later_cancellation(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_case(committer, tmp_path, kind="alias")
    committer.accept(receipt)
    original = RetainedReader(committer).read(receipt).output("data")[0]
    operation = Merge()
    coordinator, admission, reference = admit_curator(
        committer, operation, {"first": (original,)}, step_run_id="b" * 32
    )
    staged, terminal = stage_curator(coordinator, admission, operation)
    assert staged.execution_run_id is not None

    def stop(event: str) -> None:
        if event == "plan":
            msg = "controller interrupted after terminal selection"
            raise InterruptedError(msg)

    coordinator.checkpoint = stop
    with pytest.raises(InterruptedError):
        coordinator.finish(reference, staged.execution_run_id, terminal)
    cancellation = CancellationAcknowledgement(
        status=CancellationStatus.CONFIRMED, message="later cancellation"
    )
    with pytest.raises(PersistenceIntegrityError, match="Recover the frozen"):
        coordinator.tracker.record_cancellation(
            reference.step_run_id, StepStatus.RUNNING, cancellation
        )
    coordinator.checkpoint = None
    assert (
        coordinator.record_cancellation(reference.step_run_id, cancellation).status
        == StepStatus.SUCCEEDED
    )
    assert (
        coordinator.tracker.current_state(reference.step_run_id).cancellation_status
        is None
    )


@pytest.mark.parametrize(
    "boundary",
    [
        "plan",
        "planned",
        "table:provenance/execution_edges",
        "table:orchestration/steps",
        "complete",
    ],
)
def test_curator_recovers_after_controller_process_death(
    backend_fs: Backend, tmp_path: Path, boundary: str
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_case(committer, tmp_path, kind="alias")
    committer.accept(receipt)
    original = RetainedReader(committer).read(receipt).output("data")[0]
    operation = Merge()
    coordinator, admission, reference = admit_curator(
        committer, operation, {"first": (original,)}, step_run_id="b" * 32
    )
    staged, terminal = stage_curator(coordinator, admission, operation)
    assert staged.execution_run_id is not None
    request = {
        "storage": storage.model_dump(mode="json"),
        "root": f"{root}/candidate",
        "staging": coordinator.staging.staging_dir,
        "boundary": boundary,
        "reference": reference.model_dump(mode="json"),
        "execution_run_id": staged.execution_run_id,
        "result": terminal.model_dump(mode="json"),
    }
    process = subprocess.run(
        [sys.executable, "-m", "fixtures.retained_curators"],
        input=json.dumps(request),
        text=True,
        capture_output=True,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).parents[2])},
        timeout=120,
        check=False,
    )
    assert process.returncode == 41, process.stderr
    reopened = open_committer(f"{root}/candidate", fs, storage)
    recovered = CuratorResults(
        RetainedStepTracker(reopened, "synthetic-pipeline"),
        coordinator.staging.staging_dir,
    ).recover(reference.step_run_id)
    assert recovered.status == StepStatus.SUCCEEDED


def test_curator_cancel_before_selection_rejects_late_seal(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_case(committer, tmp_path, kind="alias")
    committer.accept(receipt)
    original = RetainedReader(committer).read(receipt).output("data")[0]
    coordinator, admission, reference = admit_curator(
        committer, Merge(), {"first": (original,)}, step_run_id="b" * 32
    )
    staged, terminal = stage_curator(coordinator, admission, Merge())
    assert staged.execution_run_id is not None
    coordinator.record_cancellation(
        reference.step_run_id, CancellationAcknowledgement(CancellationStatus.REQUESTED)
    )
    coordinator.record_cancellation(
        reference.step_run_id, CancellationAcknowledgement(CancellationStatus.CONFIRMED)
    )
    with pytest.raises(StoreIntegrityError, match="cancellation fence"):
        coordinator.finish(reference, staged.execution_run_id, terminal)
    assert read_curator_plan(committer.delta_root, reference.step_run_id, fs) is None
    assert (
        coordinator.tracker.current_state(reference.step_run_id).cancellation_status
        == CancellationStatus.CONFIRMED
    )
    final = coordinator.finish_without_execution(
        reference, error="cancelled before result selection"
    )
    assert final.status == StepStatus.CANCELLED
    assert (final.total_count, final.succeeded_count, final.failed_count) == (0, 0, 0)
    plan = read_curator_plan(committer.delta_root, reference.step_run_id, fs)
    assert plan is not None
    assert plan.effects.table(TablePath.EXECUTIONS.value) is None
    assert (
        CuratorReader(committer).read_result(plan.step_run_id, plan.plan_digest).outputs
        == ()
    )


@pytest.mark.parametrize("acknowledgement", [None, CancellationStatus.UNKNOWN])
def test_curator_unselected_infrastructure_failure_has_no_execution(
    backend_fs: Backend, acknowledgement: CancellationStatus | None
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    coordinator, _, reference = admit_curator(
        committer, Merge(), {}, step_run_id="a" * 32
    )
    if acknowledgement is not None:
        coordinator.record_cancellation(
            reference.step_run_id,
            CancellationAcknowledgement(CancellationStatus.REQUESTED),
        )
        coordinator.record_cancellation(
            reference.step_run_id, CancellationAcknowledgement(acknowledgement)
        )
    final = coordinator.finish_without_execution(
        reference, error="worker ownership lost"
    )
    assert final.status == StepStatus.FAILED
    assert (final.total_count, final.failed_count) == (1, 1)
    assert not final.output_roles


def test_curator_empty_optional_output_has_complete_result_authority(
    backend_fs: Backend,
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    first = _measured(committer, "a", "score")
    operation = Filter(
        params=Filter.Params(
            criteria=[Criterion(metric="score", operator="gt", value=100)]
        )
    )
    coordinator, admission, reference = admit_curator(
        committer, operation, {"passthrough": (first,)}, step_run_id="b" * 32
    )
    staged, terminal = stage_curator(coordinator, admission, operation)
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    plan = read_curator_plan(committer.delta_root, reference.step_run_id, fs)
    assert plan is not None
    result = CuratorReader(committer).read_result(plan.step_run_id, plan.plan_digest)
    assert result.output("passthrough") == ()
    assert not result.projection.occurrences


class _FailingIngest(IngestCSV):
    name = "failing_ingest_fixture"
    _converted: int = PrivateAttr(default=0)

    def convert_file(
        self,
        file_ref: FileRefArtifact,
        step_number: int,
        *,
        fs: AbstractFileSystem | None = None,
    ) -> CSVArtifact:
        self._converted += 1
        if self._converted == 2:
            msg = "conversion failed after one unpublished artifact"
            raise ValueError(msg)
        return super().convert_file(file_ref, step_number, fs=fs)


def test_curator_failed_stream_does_not_publish_converted_prefix(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_case(committer, tmp_path, kind="file")
    committer.accept(receipt)
    supplied = RetainedReader(committer).read(receipt).support
    operation = _FailingIngest()
    coordinator, admission, reference = admit_curator(
        committer, operation, {"file": supplied}, step_run_id="b" * 32
    )
    staged, terminal = stage_curator(coordinator, admission, operation)
    assert not staged.success
    assert staged.execution_run_id is not None
    final = coordinator.finish(reference, staged.execution_run_id, terminal)
    assert final.status == StepStatus.FAILED
    plan = read_curator_plan(committer.delta_root, reference.step_run_id, fs)
    assert plan is not None
    assert plan.effects.table("artifacts/csvs") is None
    assert (
        not CuratorReader(committer)
        .read_result(plan.step_run_id, plan.plan_digest)
        .outputs
    )


def test_curator_admission_rejects_source_accepted_after_frozen_view(
    backend_fs: Backend,
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    original = _measured(committer, "a", "first")
    _, admission, _ = admit_curator(
        committer, Merge(), {"data": (original,)}, step_run_id="b" * 32
    )
    later = _measured(committer, "c", "later")
    member = (
        admission.inputs[0]
        .members[0]
        .model_copy(update={"producers": (later.reference,)})
    )
    changed = admission.model_copy(
        update={
            "record": admission.record.model_copy(update={"step_run_id": "d" * 32}),
            "inputs": (admission.inputs[0].model_copy(update={"members": (member,)}),),
        }
    )
    with pytest.raises(StoreIntegrityError, match="admitted completed view"):
        publish_curator_admission(changed, committer)


def test_curator_reader_rejects_unplanned_owned_rows(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_case(committer, tmp_path, kind="alias")
    committer.accept(receipt)
    original = RetainedReader(committer).read(receipt).output("data")[0]
    coordinator, admission, reference = admit_curator(
        committer, Merge(), {"first": (original,)}, step_run_id="b" * 32
    )
    staged, terminal = stage_curator(coordinator, admission, Merge())
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    plan = read_curator_plan(committer.delta_root, reference.step_run_id, fs)
    assert plan is not None
    row = {
        "execution_run_id": staged.execution_run_id,
        "source_artifact_id": original.artifact.artifact_id,
        "target_artifact_id": original.artifact.artifact_id,
        "source_artifact_type": "csv",
        "target_artifact_type": "csv",
        "source_role": "first",
        "target_role": "merged",
        "group_id": None,
        "step_boundary": True,
        "logical_commit_id": plan.logical_commit_id,
    }
    append_table_files(
        committer._snapshot(TablePath.ARTIFACT_EDGES.value),
        [pl.DataFrame([row], schema=get_physical_schema(TablePath.ARTIFACT_EDGES))],
        step_number=8,
        statistics_columns=["logical_commit_id"],
    )
    with pytest.raises(StoreIntegrityError, match="unplanned"):
        CuratorReader(committer).read_result(plan.step_run_id, plan.plan_digest)


class _DynamicEmpty(OperationDefinition):
    name = "empty_dynamic_curator_fixture"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {}

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> PassthroughResult:
        return PassthroughResult(passthrough={}, metadata={"selection": "empty"})


class _DynamicData(OperationDefinition):
    name = "diagnostic_dynamic_curator_fixture"

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        return ArtifactResult(
            artifacts={"data": [CSVArtifact.draft(b"value\n7\n", "result.csv")]},
            lineage={"data": []},
        )


def test_dynamic_diagnostic_curator_keeps_exact_terminal_result_evidence(
    backend_fs: Backend,
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    operation = _DynamicData()
    coordinator, initial, _ = admit_curator(
        committer, operation, {}, step_run_id="a" * 32
    )
    diagnostic = initial.model_copy(
        update={
            "record": initial.record.model_copy(
                update={"step_run_id": "b" * 32, "replay_of_execution_run_id": "f" * 32}
            )
        }
    )
    reference = coordinator.admit(diagnostic)
    staged, terminal = stage_curator(coordinator, diagnostic, operation)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    terminal = terminal.model_copy(
        update={
            "output_roles": frozenset({"data"}),
            "output_types": {"data": "csv"},
        }
    )
    result = coordinator.finish(reference, staged.execution_run_id, terminal)
    assert result.status == StepStatus.SUCCEEDED
    assert result.output_roles == frozenset({"data"})
    assert coordinator.recover(reference.step_run_id) == result
    assert (
        coordinator.tracker.current_state(reference.step_run_id).to_step_result()
        == result
    )


class _ExternalCurator(OperationDefinition):
    name = "external_curator_fixture"

    class OutputRole(StrEnum):
        file = "file"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "file": OutputSpec(artifact_type="file_ref", derives_from={"inputs": []})
    }
    _artifact: Artifact = PrivateAttr()

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        result = ArtifactResult()
        result.add_artifact("file", self._artifact, sources={})
        return result


def test_curator_external_output_has_verified_managed_carrier_after_source_deletion(
    backend_fs: Backend,
    tmp_path: Path,
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    payload = b"external curator output\n"
    path = tmp_path / "worker-output.bin"
    path.write_bytes(payload)
    operation = _ExternalCurator()
    operation._artifact = FileRefArtifact.draft(
        str(path),
        compute_content_digest(payload),
        len(payload),
        8,
    )
    coordinator, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32
    )
    staged, terminal = stage_curator(coordinator, admission, operation)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    path.unlink()
    coordinator.finish(reference, staged.execution_run_id, terminal)
    plan = read_curator_plan(committer.delta_root, reference.step_run_id, fs)
    assert plan is not None
    occurrence = plan.projection.occurrences[0].occurrence
    producer = AcceptedCuratorOccurrence(
        step_run_id=plan.step_run_id,
        plan_digest=plan.plan_digest,
        address=occurrence.address,
    )
    assert plan.carrier_inventory is not None
    assert len(plan.carrier_inventory.carriers) == 1
    committer._verify_producer(producer, occurrence)
    selected = CuratorReader(committer).read(producer).output("file")[0]
    assert isinstance(selected.artifact, FileRefArtifact)
    assert selected.artifact.path.startswith(committer.files_root)
    assert selected.artifact.read_content(fs=fs) == payload
    graph = OccurrenceGraph()
    slot = graph.add_input("data", selected.artifact)
    case = RetainedCase(
        graph.project(
            {"result": [slot]},
            {"result": OutputSpec()},
            input_attribution=(
                BoundaryInputAttribution(boundary=slot, producer=selected.reference),
            ),
        ),
        {slot: selected.artifact},
        {},
    )
    _, child = admit_read_case(committer, case, step_run_id="c" * 32)
    committer.accept(child)
    assert RetainedReader(committer).read(child).output("result")


class _AppendableCurator(_ExternalCurator):
    name = "appendable_curator_fixture"
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "file": OutputSpec(artifact_type="appendable", derives_from={"inputs": []})
    }


def test_curator_selects_valid_alternate_carrier_for_same_appendable_record(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    record = b'{"record_id":"selected","value":1}'
    plans = []
    for owner in ("b", "c"):
        payload = record + f'\n{{"record_id":"{owner}","value":2}}\n'.encode()
        path = tmp_path / f"{owner}.jsonl"
        path.write_bytes(payload)
        operation = _AppendableCurator()
        operation._artifact = AppendableArtifact.draft(
            "selected", compute_content_digest(record), len(record), 8, str(path)
        )
        coordinator, admission, reference = admit_curator(
            committer, operation, {}, step_run_id=owner * 32
        )
        staged, terminal = stage_curator(coordinator, admission, operation)
        assert staged.success, staged.error
        assert staged.execution_run_id is not None
        coordinator.finish(reference, staged.execution_run_id, terminal)
        plan = read_curator_plan(committer.delta_root, reference.step_run_id, fs)
        assert plan is not None
        plans.append(plan)
    locations = sorted(
        plan.table(TablePath.ARTIFACT_LOCATIONS.value).row_keys[0][1] for plan in plans
    )
    assert len(set(locations)) == 2
    fs.rm(locations[0])
    selected = (
        CuratorReader(committer)
        .read_result(plans[0].step_run_id, plans[0].plan_digest)
        .output("file")[0]
    )
    assert selected.artifact.external_path == locations[1]
    assert selected.reference.step_run_id == plans[0].step_run_id
    with fs.open(locations[1], "rb") as stream:
        payload = stream.read()
    with fs.open(locations[1], "wb") as stream:
        stream.write(payload.replace(b'"value":2', b'"value":3'))
    selected.artifact.verify_external_content(fs=fs)
    with pytest.raises(StoreIntegrityError, match="full carrier changed"):
        CuratorReader(committer).read_result(plans[0].step_run_id, plans[0].plan_digest)


@pytest.mark.parametrize("phase", ["staging", "managed"])
def test_curator_created_appendable_binds_nonselected_records(
    backend_fs: Backend, tmp_path: Path, phase: str
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    record = b'{"record_id":"selected","value":1}'
    payload = record + b'\n{"record_id":"other","value":2}\n'
    source = tmp_path / "worker.jsonl"
    source.write_bytes(payload)
    operation = _AppendableCurator()
    operation._artifact = AppendableArtifact.draft(
        "selected", compute_content_digest(record), len(record), 8, str(source)
    )
    coordinator, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="b" * 32
    )
    staged, terminal = stage_curator(coordinator, admission, operation)
    assert staged.success, staged.error
    assert staged.staging_path is not None
    assert staged.execution_run_id is not None
    source.unlink()
    digest = compute_content_digest(payload)
    if phase == "staging":
        target = (
            f"{curator_carrier_directory(staged.staging_path)}/carrier-{digest}.bin"
        )
    else:
        coordinator.finish(reference, staged.execution_run_id, terminal)
        target = f"{committer.files_root}/carriers/{digest[:2]}/{digest}.bin"
    with fs.open(target, "wb") as stream:
        stream.write(payload.replace(b'"value":2', b'"value":3'))
    operation._artifact.external_path = target
    operation._artifact.verify_external_content(fs=fs)
    with pytest.raises(StoreIntegrityError, match="carrier.*changed"):
        if phase == "staging":
            coordinator.finish(reference, staged.execution_run_id, terminal)
        else:
            plan = read_curator_plan(committer.delta_root, reference.step_run_id, fs)
            assert plan is not None
            CuratorReader(committer).read_result(plan.step_run_id, plan.plan_digest)


@pytest.mark.parametrize(
    "boundary", ["plan", "carriers", f"table:{TablePath.ARTIFACT_LOCATIONS.value}"]
)
def test_curator_carriers_recover_after_controller_death_and_source_removal(
    backend_fs: Backend, tmp_path: Path, boundary: str
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    payload = b"worker-owned complete bytes\n"
    source = tmp_path / "worker.bin"
    source.write_bytes(payload)
    operation = _ExternalCurator()
    operation._artifact = FileRefArtifact.draft(
        str(source), compute_content_digest(payload), len(payload), 8
    )
    coordinator, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="b" * 32
    )
    staged, terminal = stage_curator(coordinator, admission, operation)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    source.unlink()
    process = subprocess.run(
        [sys.executable, "-m", "fixtures.retained_curators"],
        input=json.dumps(
            {
                "storage": storage.model_dump(mode="json"),
                "root": f"{root}/candidate",
                "staging": coordinator.staging.staging_dir,
                "boundary": boundary,
                "reference": reference.model_dump(mode="json"),
                "execution_run_id": staged.execution_run_id,
                "result": terminal.model_dump(mode="json"),
            }
        ),
        text=True,
        capture_output=True,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).parents[2])},
        timeout=120,
        check=False,
    )
    assert process.returncode == 41, process.stderr
    reopened = open_committer(f"{root}/candidate", fs, storage)
    plan = read_curator_plan(reopened.delta_root, reference.step_run_id, fs)
    assert plan is not None
    assert plan.managed_locations is not None
    if boundary == "plan":
        assert not fs.exists(plan.managed_locations.row_keys[0][1])
    recovered = CuratorResults(
        RetainedStepTracker(reopened, "synthetic-pipeline"),
        coordinator.staging.staging_dir,
    ).recover(reference.step_run_id)
    assert recovered.status == StepStatus.SUCCEEDED
    result = CuratorReader(reopened).read_result(plan.step_run_id, plan.plan_digest)
    artifact = result.output("file")[0].artifact
    assert isinstance(artifact, FileRefArtifact)
    assert artifact.read_content(fs=fs) == payload


def test_dynamic_empty_curator_uses_candidate_result_validation(
    backend_fs: Backend,
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    operation = _DynamicEmpty()
    coordinator, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32
    )
    staged, terminal = stage_curator(coordinator, admission, operation)
    assert staged.success
    assert staged.execution_run_id is not None
    result = coordinator.finish(reference, staged.execution_run_id, terminal)
    assert result.status == StepStatus.SUCCEEDED
    assert result.metadata == {"selection": "empty"}
    plan = read_curator_plan(committer.delta_root, reference.step_run_id, fs)
    assert plan is not None
    accepted = CuratorReader(committer).read_result(plan.step_run_id, plan.plan_digest)
    assert accepted.result == result
    assert accepted.outputs == ()
