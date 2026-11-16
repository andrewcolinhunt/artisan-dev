"""Inspection proves current membership and index metadata without payload reads."""

from __future__ import annotations

from functools import partial
from pathlib import Path

import polars as pl
import pytest
from deltalake.exceptions import TableNotFoundError
from fixtures.candidate_outputs import external_case, prepare_scientific
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_curators import admit_curator
from fixtures.retained_readers import Backend
from fixtures.retained_results import (
    RetainedCase,
    _derive,
    boundary_artifact,
    retained_case,
)

from artisan.errors import PersistenceIntegrityError, StoreIntegrityError
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.orchestration.engine.retained_artifact_query import (
    query_retained_artifacts,
)
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.enums import CachePolicy, TablePath
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.cache.retained_reuse import verify_cache_source
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.projection_schemas import PROJECTION_TABLES
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_read import RetainedControlProof
from artisan.storage.io.retained_store import create_retained_store
from artisan.storage.io.step_result_plan import StepUnitFailure
from artisan.utils.path import uri_join


@pytest.fixture
def committer(tmp_path: Path) -> RetainedCommitter:
    return create_retained_store(
        str(tmp_path / "store"),
        str(tmp_path / "files"),
        fs=StorageConfig().filesystem(),
    )


def _query(committer: RetainedCommitter) -> list[tuple[int | None, str, int]]:
    return [
        (item.current_step_number, item.artifact_id, item.origin_step_number)
        for item in query_retained_artifacts(
            committer, pipeline_run_id="synthetic-pipeline"
        )
    ]


def test_artifact_query_shares_lifecycle_and_membership_proof(
    committer: RetainedCommitter,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = retained_case(tmp_path, kind="alias")
    results, admission, receipts = prepare_scientific(committer, case)
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    prove = committer._control_proof
    calls = 0

    def counted() -> RetainedControlProof:
        nonlocal calls
        calls += 1
        return prove()

    monkeypatch.setattr(committer, "_control_proof", counted)
    assert _query(committer)
    assert calls == 1
    assert _query(committer)
    assert calls == 2


def test_artifact_query_rejects_removed_selected_terminal(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias")
    )
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    assert _query(committer)
    path = uri_join(committer.delta_root, TablePath.STEPS.value)
    pl.read_delta(path).filter(pl.col("status") != "succeeded").write_delta(
        path, mode="overwrite"
    )
    with pytest.raises(StoreIntegrityError):
        _query(committer)


def test_inspection_deduplicates_only_current_step_and_preserves_unit_reuse(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    case = retained_case(tmp_path, kind="alias")
    results, admission, receipts = prepare_scientific(committer, case, count=2)
    for receipt in receipts:
        committer.accept(receipt)
    results.finish(admission.step_run_id)
    artifact = next(iter(case.boundary.values()))
    assert _query(committer) == [(7, artifact.artifact_id, 1)]
    assert (
        len(read_current_step_outputs(committer, admission.step_run_id)[0].occurrences)
        == 4
    )
    current, admitted, _ = prepare_scientific(committer, case, step_run_id="b" * 32)
    committer.journal.reuse(
        admitted.assignments[0],
        receipts[0],
        verify_source=partial(verify_cache_source, reader=RetainedReader(committer)),
    )
    current.finish(admitted.step_run_id)
    assert _query(committer) == [(7, artifact.artifact_id, 1)]
    assert (
        len(read_current_step_outputs(committer, admitted.step_run_id)[0].occurrences)
        == 2
    )
    assert query_retained_artifacts(committer, pipeline_run_id="other") == []
    assert (
        query_retained_artifacts(
            committer, pipeline_run_id="synthetic-pipeline", artifact_type="metric"
        )
        == []
    )


def test_partial_whole_step_reuse_lists_only_original_successful_units(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    case = retained_case(tmp_path, kind="alias")
    results, admission, receipts = prepare_scientific(committer, case, count=2)
    committer.accept(receipts[0])
    result = results.finish(
        admission.step_run_id, failures={1: StepUnitFailure(error="tool failed")}
    )
    assert result.status == StepStatus.PARTIAL
    expected = _query(committer)
    current, admitted, _ = prepare_scientific(
        committer, case, step_run_id="b" * 32, count=2
    )
    hit = current.lookup(admitted.step_run_id, CachePolicy.STEP_COMPLETED)
    assert hit is not None
    current.reuse(hit)
    assert _query(committer) == expected
    assert (
        len(read_current_step_outputs(committer, admitted.step_run_id)[0].occurrences)
        == 2
    )


@pytest.mark.parametrize("empty", [False, True])
def test_curator_reuse_lists_current_steps_with_original_origins(
    committer: RetainedCommitter, empty: bool
) -> None:
    operation = DynamicCurator()
    operation.params.empty = empty
    results, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(results, admission, operation)
    results.finish(reference, staged.execution_run_id, terminal)
    original = _query(committer)
    _, _, current = admit_curator(
        committer, operation, {}, step_run_id="b" * 32, step_number=9, **STAMPS
    )
    hit = lookup_curator(current, reader=CuratorReader(committer))
    assert hit is not None
    results.reuse(hit)
    assert _query(committer) == original + [
        (9, artifact_id, origin) for _, artifact_id, origin in original
    ]


@pytest.mark.parametrize(
    "latest", ["failed", "skipped", "pending", "running", "cancelled"]
)
def test_latest_unusable_attempt_masks_previous_success(
    committer: RetainedCommitter, tmp_path: Path, latest: str
) -> None:
    case = retained_case(tmp_path, kind="alias")
    results, admission, receipts = prepare_scientific(committer, case)
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    current, admitted, _ = prepare_scientific(
        committer,
        case,
        step_run_id="b" * 32,
        count=0 if latest == "skipped" else 1,
        admit=latest not in {"pending", "cancelled"},
    )
    if latest == "failed":
        current.finish(
            admitted.step_run_id, failures={0: StepUnitFailure(error="tool failed")}
        )
    elif latest == "cancelled":
        for status in (CancellationStatus.REQUESTED, CancellationStatus.CONFIRMED):
            current.tracker.record_cancellation(
                admitted.step_run_id,
                StepStatus.PENDING,
                CancellationAcknowledgement(status),
            )
        current.tracker.transition(
            admitted.step_run_id,
            StepStatus.PENDING,
            StepStatus.CANCELLED,
            result=StepResult(
                step_run_id=admitted.step_run_id,
                step_number=7,
                step_name="produce",
                status=StepStatus.CANCELLED,
                cancellation_status=CancellationStatus.CONFIRMED,
            ),
        )
    assert _query(committer) == []


def test_incomplete_selection_is_not_recovered_or_exposed(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias")
    )
    committer.accept(receipts[0])

    def interrupt(event: str) -> None:
        if event == "planned":
            raise InterruptedError(event)

    results.checkpoint = interrupt
    with pytest.raises(InterruptedError):
        results.finish(admission.step_run_id)
    before = committer._controls()
    assert _query(committer) == []
    assert committer._controls().equals(before)
    assert (
        results.tracker.current_state(admission.step_run_id).status
        == StepStatus.RUNNING
    )


def test_support_only_content_is_not_listed(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    case = retained_case(tmp_path)
    results, admission, receipts = prepare_scientific(committer, case)
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    assert len(case.projection.support) == 2
    selected = query_retained_artifacts(committer, pipeline_run_id="synthetic-pipeline")
    assert [item.artifact_type for item in selected] == ["config"]


def test_metric_exports_preserve_direct_associations_and_scientific_duplicate_order(
    committer: RetainedCommitter,
) -> None:
    graph = OccurrenceGraph()
    seed = graph.add_input("data", boundary_artifact())
    final = _derive(
        graph, "final", CSVArtifact.draft(b"x\n2\n", "final.csv", 7).finalize(), [seed]
    )
    hidden = _derive(
        graph,
        "hidden",
        CSVArtifact.draft(b"x\n3\n", "hidden.csv", 7).finalize(),
        [final],
    )
    direct = _derive(
        graph,
        "direct",
        MetricArtifact.draft({"score": 1}, "direct.json", 7).finalize(),
        [final],
    )
    indirect = _derive(
        graph,
        "indirect",
        MetricArtifact.draft({"score": 2}, "indirect.json", 7).finalize(),
        [hidden],
    )
    projection = graph.project(
        {"data": [final], "metrics": [indirect, direct, indirect]},
        {"data": OutputSpec(), "metrics": OutputSpec()},
    )
    case = RetainedCase(
        projection,
        {seed: graph.nodes[seed].artifact},
        {
            item.address: graph.nodes[item.address].artifact
            for item in projection.occurrences
            if item.address != seed
        },
    )
    results, admission, receipts = prepare_scientific(committer, case)
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    selected = query_retained_artifacts(
        committer, pipeline_run_id="synthetic-pipeline", artifact_type="metric"
    )
    assert {item.artifact_id for item in selected} == {
        graph.nodes[direct].artifact.artifact_id,
        graph.nodes[indirect].artifact.artifact_id,
    }
    reader = RetainedReader(committer)
    accepted = reader.read(receipts[0])
    assert [item.reference.address for item in accepted.output("metrics")] == [
        indirect,
        direct,
        indirect,
    ]
    assert [
        item.reference.address
        for item in reader.associated(accepted.output("data")[0].reference, "metric")
    ] == [direct]
    assert accepted.projection == projection


@pytest.mark.parametrize("changed", ["missing", "bytes", "payload_table"])
def test_metadata_never_proves_payload_availability_even_within_same_snapshot(
    committer: RetainedCommitter, tmp_path: Path, changed: str
) -> None:
    _unavailable_payload(committer, tmp_path, changed)


def _unavailable_payload(
    committer: RetainedCommitter, scratch: Path, changed: str
) -> None:
    case = external_case(scratch)
    results, admission, receipts = prepare_scientific(committer, case)
    plan = committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    expected = _query(committer)
    carrier = uri_join(committer.files_root, plan.carriers[0].relative_path)
    if changed == "missing":
        committer.fs.rm(carrier)
    elif changed == "bytes":
        with committer.fs.open(carrier, "wb") as stream:
            stream.write(b"changed")
    else:
        committer.fs.rm(
            uri_join(committer.delta_root, "artifacts/file_refs"), recursive=True
        )
    assert _query(committer) == expected
    with committer._read_snapshot() as snapshot:
        assert snapshot.projection_metadata(receipts[0]) == case.projection
        with pytest.raises(
            (StoreIntegrityError, FileNotFoundError, TableNotFoundError)
        ):
            snapshot.projection(receipts[0])
    with pytest.raises((StoreIntegrityError, FileNotFoundError, TableNotFoundError)):
        read_current_step_outputs(committer, admission.step_run_id)
    with pytest.raises((StoreIntegrityError, FileNotFoundError, TableNotFoundError)):
        verify_cache_source(
            admission.assignments[0], receipts[0], reader=RetainedReader(committer)
        )


@pytest.mark.parametrize("changed", ["terminal", "index", "projection", "owner"])
def test_metadata_rejects_changed_exact_evidence(
    committer: RetainedCommitter, tmp_path: Path, changed: str
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias")
    )
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    assert _query(committer)
    if changed == "terminal":
        table, expression = (
            TablePath.STEPS.value,
            pl.when(pl.col("status") == "succeeded")
            .then(pl.lit("changed"))
            .otherwise(pl.col("step_name"))
            .alias("step_name"),
        )
    elif changed == "projection":
        table, expression = (
            PROJECTION_TABLES[0].table_path,
            pl.lit("f" * 32).alias("artifact_id"),
        )
    elif changed == "owner":
        table, expression = (
            TablePath.ARTIFACT_INDEX.value,
            pl.lit("retained_result:" + "f" * 32).alias("logical_commit_id"),
        )
    else:
        table, expression = (
            TablePath.ARTIFACT_INDEX.value,
            pl.lit('{"changed": true}').alias("metadata"),
        )
    path = uri_join(committer.delta_root, table)
    pl.read_delta(path, storage_options=committer.storage_options).with_columns(
        expression
    ).write_delta(path, mode="overwrite", storage_options=committer.storage_options)
    with pytest.raises((StoreIntegrityError, PersistenceIntegrityError)):
        _query(committer)


def test_metadata_external_boundary_on_s3(s3_fs: Backend, tmp_path: Path) -> None:
    fs, storage, root = s3_fs
    committer = create_retained_store(
        f"{root}/candidate",
        f"{root}/files",
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )
    _unavailable_payload(committer, tmp_path, "bytes")
