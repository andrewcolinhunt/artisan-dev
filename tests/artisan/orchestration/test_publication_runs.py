"""Portable runs preserve selections without recreating live acceptance authority."""

from __future__ import annotations

import json
import shutil
from functools import partial
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from fixtures.candidate_outputs import external_case, prepare_scientific
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_acceptance import admit_case
from fixtures.retained_curators import admit_curator
from fixtures.retained_results import retained_case
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.operations.curator.merge import Merge
from artisan.orchestration.engine import publication_runs as inspection
from artisan.orchestration.engine.retained_artifact_query import (
    query_retained_artifacts,
)
from artisan.orchestration.engine.retained_run_status import (
    list_retained_runs,
    retained_run_status,
)
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import CachePolicy, TablePath
from artisan.schemas.execution.assignment import DeliveryIdentity, SealedDelivery
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage._publication_policy import PublicationPolicy
from artisan.storage._publication_runs import InspectedCurrentUnits, InspectedWholeReuse
from artisan.storage._retained_publication import (
    PublicationExport,
    RetainedInspectionManifest,
    _inspection_digest,
)
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.cache.retained_reuse import verify_cache_source
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_store import create_retained_store
from artisan.storage.io.step_result_plan import StepUnitFailure
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


@pytest.fixture
def committer(tmp_path: Path) -> RetainedCommitter:
    return create_retained_store(
        str(tmp_path / "store"), str(tmp_path / "files"), fs=LocalFileSystem()
    )


def _export(committer: RetainedCommitter, target: Path) -> PublicationExport:
    return inspection.export_retained_runs(RetainedReader(committer), target)


def _rewrite_manifest(publication: PublicationExport, payload: dict[str, Any]) -> None:
    payload.pop("inspection_digest", None)
    payload["inspection_digest"] = _inspection_digest(payload)
    (publication.directory / "inspection.json").write_bytes(
        canonical_json_bytes(payload)
    )


def _lifecycle_only(
    committer: RetainedCommitter,
    *,
    identity: str = "e" * 32,
    run: str = "synthetic-pipeline",
    pending: bool = False,
    error: str = "preparation failed",
    input_refs: str = "{}",
) -> None:
    tracker = RetainedStepTracker(committer, run)
    tracker.create_attempt(
        StepStartRecord(
            step_run_id=identity,
            step_number=7,
            step_name="latest",
            operation_class="synthetic.Operation",
            params_json="{}",
            input_refs_json=input_refs,
            compute_backend="local",
            compute_options_json="{}",
            output_roles_json='["data"]',
            output_types_json='{"data":"csv"}',
        )
    )
    if not pending:
        tracker.transition(identity, StepStatus.PENDING, StepStatus.RUNNING)
        tracker.transition(
            identity,
            StepStatus.RUNNING,
            StepStatus.FAILED,
            result=StepResult(
                step_run_id=identity,
                step_number=7,
                step_name="latest",
                status=StepStatus.FAILED,
                total_count=1,
                succeeded_count=0,
                failed_count=1,
                error=error,
            ),
        )


def test_whole_partial_keeps_original_failed_delivery_and_moved_run_history(
    committer: RetainedCommitter,
    tmp_path: Path,
) -> None:
    case = retained_case(tmp_path, kind="alias")
    original, admission, receipts = prepare_scientific(committer, case, count=3)
    committer.accept(receipts[0])
    failed = DeliveryIdentity.new(admission.assignments[1].attempt)
    original.finish(
        admission.step_run_id,
        failures={
            1: StepUnitFailure(
                error="private-token authored failure", failed_delivery=failed
            ),
            2: StepUnitFailure(error="no closed delivery"),
        },
    )
    current, admitted, _ = prepare_scientific(
        committer, case, step_run_id="b" * 32, count=3
    )
    hit = current.lookup(admitted.step_run_id, CachePolicy.STEP_COMPLETED)
    assert hit is not None
    current.reuse(hit)
    expected_status = retained_run_status(committer, "synthetic-pipeline")
    expected_history = list_retained_runs(committer)
    expected_refs = query_retained_artifacts(
        committer, pipeline_run_id="synthetic-pipeline"
    )
    publication = inspection.export_retained_runs(
        RetainedReader(committer), tmp_path / "bundle", redact=("private-token",)
    )
    runs = publication.metadata().runs
    assert runs is not None
    original_selection, reused = runs.selections
    assert isinstance(original_selection.selection, InspectedCurrentUnits)
    assert [unit.execution_run_id for unit in original_selection.selection.units] == [
        receipts[0].delivery.delivery_id,
        failed.delivery_id,
        None,
    ]
    assert isinstance(reused.selection, InspectedWholeReuse)
    assert reused.selection.source_step_run_id == admission.step_run_id
    assert runs.failed_sources[0].delivery == failed
    assert original_selection.selection.units[1].error == "[redacted] authored failure"
    assert any(
        marker.location == "unit" and marker.index == 1 for marker in runs.redactions
    )
    assert not tuple((publication.directory).rglob("*seal*"))
    moved = tmp_path / "moved"
    shutil.move(publication.directory, moved)
    shutil.rmtree(committer.delta_root)
    if Path(committer.files_root).exists():
        shutil.rmtree(committer.files_root)
    publication = PublicationExport(moved)
    assert (
        inspection.publication_run_status(publication, "synthetic-pipeline")
        == expected_status
    )
    assert inspection.list_publication_runs(publication).equals(expected_history)
    assert (
        inspection.query_publication_artifacts(
            publication, pipeline_run_id="synthetic-pipeline"
        )
        == expected_refs
    )
    payload = publication.metadata().model_dump(mode="json")
    payload["runs"]["selections"].pop(0)
    _rewrite_manifest(publication, payload)
    with pytest.raises(ValueError, match="original eligible selection"):
        inspection.list_publication_runs(publication)


def test_repeated_unit_selections_preserve_order_but_inspection_deduplicates(
    committer: RetainedCommitter,
    tmp_path: Path,
) -> None:
    case = retained_case(tmp_path, kind="alias")
    original, admission, receipts = prepare_scientific(committer, case, count=2)
    for receipt in receipts:
        committer.accept(receipt)
    original.finish(admission.step_run_id)
    current, admitted, _ = prepare_scientific(
        committer, case, step_run_id="b" * 32, count=3
    )
    selected = (receipts[1], receipts[0], receipts[1])
    for reference, receipt in zip(admitted.assignments, selected, strict=True):
        committer.journal.reuse(
            reference,
            receipt,
            verify_source=partial(
                verify_cache_source, reader=RetainedReader(committer)
            ),
        )
    current.finish(admitted.step_run_id)
    publication = _export(committer, tmp_path / "bundle")
    runs = publication.metadata().runs
    assert runs is not None
    selection = runs.selections[1].selection
    assert isinstance(selection, InspectedCurrentUnits)
    assert [unit.execution_run_id for unit in selection.units] == [
        receipt.delivery.delivery_id for receipt in selected
    ]
    references = inspection.query_publication_artifacts(
        publication, pipeline_run_id="synthetic-pipeline"
    )
    assert len(references) == 1
    assert all(item.current_step_number == 7 for item in references)


@pytest.mark.parametrize("empty", [False, True])
def test_curator_reuse_keeps_one_original_execution_and_terminal_roles(
    committer: RetainedCommitter,
    tmp_path: Path,
    empty: bool,
) -> None:
    operation = DynamicCurator(params=DynamicCurator.Params(empty=empty))
    results, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(results, admission, operation)
    results.finish(reference, staged.execution_run_id, terminal)
    _, _, current = admit_curator(
        committer, operation, {}, step_run_id="b" * 32, step_number=9, **STAMPS
    )
    hit = lookup_curator(current, reader=CuratorReader(committer))
    assert hit is not None
    results.reuse(hit)
    expected = retained_run_status(committer, "synthetic-pipeline")
    publication = _export(committer, tmp_path / "bundle")
    manifest = publication.metadata()
    assert len(manifest.curator_sources) == 1
    assert manifest.runs is not None
    assert len(manifest.runs.selections) == 2
    assert (
        inspection.publication_run_status(publication, "synthetic-pipeline") == expected
    )
    assert len(
        inspection.query_publication_artifacts(
            publication, pipeline_run_id="synthetic-pipeline"
        )
    ) == (0 if empty else 2)


def test_latest_failure_masks_exports_and_preserves_prior_attempts_and_runs(
    committer: RetainedCommitter,
    tmp_path: Path,
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias")
    )
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    _lifecycle_only(committer)
    _lifecycle_only(committer, identity="f" * 32, run="another-run")
    expected = list_retained_runs(committer)
    publication = _export(committer, tmp_path / "bundle")
    assert inspection.list_publication_runs(publication).equals(expected)
    assert (
        inspection.query_publication_artifacts(
            publication, pipeline_run_id="synthetic-pipeline"
        )
        == []
    )
    assert (
        inspection.publication_run_status(publication, "synthetic-pipeline")
        .steps[0]
        .name
        == "latest"
    )


def test_metadata_index_does_not_hydrate_content_or_read_changed_carrier(
    committer: RetainedCommitter,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, external_case(tmp_path / "external")
    )
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    publication = _export(committer, tmp_path / "bundle")
    expected = inspection.query_publication_artifacts(
        publication, pipeline_run_id="synthetic-pipeline"
    )
    manifest = publication.metadata()
    carrier = publication.directory / manifest.carriers[0].relative_path
    carrier.write_bytes(b"changed complete carrier")

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("metadata query attempted artifact hydration")

    with monkeypatch.context() as scope:
        scope.setattr(ArtifactStore, "get_artifact", forbidden)
        scope.setattr(ArtifactTypeDef, "get_all", forbidden)
        assert (
            inspection.query_publication_artifacts(
                publication, pipeline_run_id="synthetic-pipeline"
            )
            == expected
        )
    with pytest.raises(StoreIntegrityError, match="carrier"), publication.artifacts():
        pass
    assert publication.artifact_index().height > len(expected)


def test_full_content_read_crosschecks_protected_index_rows(
    committer: RetainedCommitter,
    tmp_path: Path,
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias")
    )
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    publication = _export(committer, tmp_path / "bundle")
    payload = publication.metadata().model_dump(mode="json")
    descriptor = payload["artifact_index"]
    path = publication.directory / descriptor["filename"]
    pl.read_parquet(path).with_columns(
        pl.lit(99, dtype=pl.Int32).alias("origin_step_number")
    ).write_parquet(path)
    encoded = path.read_bytes()
    descriptor.update(digest=compute_content_digest(encoded), size_bytes=len(encoded))
    _rewrite_manifest(publication, payload)
    with (
        pytest.raises(StoreIntegrityError, match="protected artifact metadata"),
        publication.artifacts(),
    ):
        pass


def test_empty_quiescent_store_has_portable_empty_metadata(
    committer: RetainedCommitter,
    tmp_path: Path,
) -> None:
    publication = _export(committer, tmp_path / "bundle")
    assert publication.artifact_index().is_empty()
    assert inspection.list_publication_runs(publication).equals(
        list_retained_runs(committer)
    )
    assert inspection.publication_run_status(
        publication, "absent"
    ) == retained_run_status(committer, "absent")
    assert (
        inspection.query_publication_artifacts(publication, pipeline_run_id="absent")
        == []
    )


@pytest.mark.parametrize(
    "acknowledgement", [None, CancellationStatus.CONFIRMED, CancellationStatus.UNKNOWN]
)
def test_curator_without_execution_exports_only_closed_terminal_lifecycle(
    committer: RetainedCommitter,
    tmp_path: Path,
    acknowledgement: CancellationStatus | None,
) -> None:
    results, _, reference = admit_curator(committer, Merge(), {}, step_run_id="a" * 32)
    if acknowledgement is not None:
        for status in (CancellationStatus.REQUESTED, acknowledgement):
            results.record_cancellation(
                reference.step_run_id, CancellationAcknowledgement(status)
            )
    terminal = results.finish_without_execution(
        reference, error="worker closed without a result"
    )
    target = tmp_path / "bundle"
    if acknowledgement == CancellationStatus.UNKNOWN:
        with pytest.raises(StoreIntegrityError, match="quiescent"):
            _export(committer, target)
        assert not target.exists()
        return
    publication = _export(committer, target)
    manifest = publication.metadata()
    assert not manifest.curator_sources
    assert manifest.runs is not None
    assert not manifest.runs.selections
    assert inspection.list_publication_runs(publication)["last_status"].to_list() == [
        terminal.status.value
    ]
    assert (
        inspection.query_publication_artifacts(
            publication, pipeline_run_id="synthetic-pipeline"
        )
        == []
    )


def test_failed_curator_keeps_original_execution_and_no_exports(
    committer: RetainedCommitter,
    tmp_path: Path,
) -> None:
    operation = DynamicCurator(params=DynamicCurator.Params(fail=True))
    results, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(results, admission, operation)
    results.finish(reference, staged.execution_run_id, terminal)
    publication = _export(committer, tmp_path / "bundle")
    manifest = publication.metadata()
    assert manifest.curator_sources[0].execution_run_id == staged.execution_run_id
    assert not manifest.curator_sources[0].success
    assert manifest.runs is not None
    assert len(manifest.runs.selections) == 1
    assert (
        inspection.query_publication_artifacts(
            publication, pipeline_run_id="synthetic-pipeline"
        )
        == []
    )


@pytest.mark.parametrize("field", ["timestamp", "succeeded_count"])
def test_export_rejects_structurally_valid_changed_selected_terminal(
    committer: RetainedCommitter,
    tmp_path: Path,
    field: str,
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias")
    )
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    path = f"{committer.delta_root}/{TablePath.STEPS.value}"
    rows = pl.read_delta(path)
    terminal = pl.col("status") == "succeeded"
    if field == "timestamp":
        rows = rows.with_columns(
            pl.when(terminal)
            .then(pl.col(field) + pl.duration(seconds=1))
            .otherwise(pl.col(field))
            .alias(field)
        )
    else:
        rows = rows.with_columns(
            *(
                pl.when(terminal).then(2).otherwise(pl.col(name)).alias(name)
                for name in ("total_count", "succeeded_count")
            )
        )
    rows.write_delta(path, mode="overwrite")
    with pytest.raises(StoreIntegrityError):
        _export(committer, tmp_path / "bundle")
    assert not (tmp_path / "bundle").exists()


def test_unresolved_claim_is_not_inferred_complete_from_worker_seal(
    committer: RetainedCommitter,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, receipt = admit_case(committer, tmp_path, kind="alias")

    def stop(event: str, selected: SealedDelivery, path: str | None) -> None:
        if event == "claimed":
            msg = "stopped after durable claim"
            raise RuntimeError(msg)

    with monkeypatch.context() as scope:
        scope.setattr(committer, "_checkpoint", stop)
        with pytest.raises(RuntimeError, match="durable claim"):
            committer.accept(receipt)
    with pytest.raises(StoreIntegrityError, match="unresolved"):
        _export(committer, tmp_path / "bundle")
    assert not (tmp_path / "bundle").exists()


def test_run_publication_rejects_hidden_pending_attempt_and_cleans_mutated_copy(
    committer: RetainedCommitter,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _lifecycle_only(committer, pending=True)
    _lifecycle_only(committer, identity="f" * 32)
    target = tmp_path / "blocked"
    with pytest.raises(StoreIntegrityError, match="quiescent"):
        _export(committer, target)
    assert not target.exists()
    tracker = RetainedStepTracker(committer, "synthetic-pipeline")
    tracker.skip_pending("e" * 32, reason="empty_inputs")
    write = inspection._write_inspection

    def mutate(
        reader: RetainedReader, path: Path, policy: PublicationPolicy
    ) -> RetainedInspectionManifest:
        copied = write(reader, path, policy)
        _lifecycle_only(committer, identity="d" * 32, run="new-run")
        return copied

    monkeypatch.setattr(inspection, "_write_inspection", mutate)
    with pytest.raises(StoreIntegrityError, match="changed"):
        _export(committer, target)
    assert not target.exists()


def test_raw_locator_and_error_redaction_preserve_source_ids_and_mark_observations(
    committer: RetainedCommitter,
    tmp_path: Path,
) -> None:
    _lifecycle_only(
        committer,
        error="private-token failed",
        input_refs=json.dumps(
            {"raw_paths": ["/private-token/input", "/private-token/input"]}
        ),
    )
    publication = inspection.export_retained_runs(
        RetainedReader(committer), tmp_path / "bundle", redact=("private-token",)
    )
    runs = publication.metadata().runs
    assert runs is not None
    assert runs.redactions
    assert {marker.field for marker in runs.redactions} == {"error", "input_refs_json"}
    rows = pl.read_parquet(publication.directory / runs.lifecycle.filename)
    assert "private-token" not in str(rows.to_dicts())
    assert rows["step_run_id"].unique().to_list() == ["e" * 32]
    assert (
        json.loads(rows["input_refs_json"][0])["raw_paths"] == ["/[redacted]/input"] * 2
    )


@pytest.mark.parametrize(
    "tamper", ["count", "execution", "order", "lifecycle", "index"]
)
def test_bundle_rejects_inconsistent_protected_selection_and_table_evidence(
    committer: RetainedCommitter,
    tmp_path: Path,
    tamper: str,
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias")
    )
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    publication = _export(committer, tmp_path / "bundle")
    payload = publication.metadata().model_dump(mode="json")
    if tamper in {"lifecycle", "index"}:
        descriptor = (
            payload["runs"]["lifecycle"]
            if tamper == "lifecycle"
            else payload["artifact_index"]
        )
        path = publication.directory / descriptor["filename"]
        path.write_bytes(path.read_bytes() + b"changed")
    else:
        selected = payload["runs"]["selections"][0]
        if tamper == "count":
            selected["result"]["total_count"] = 2
            selected["result"]["succeeded_count"] = 2
        elif tamper == "execution":
            selected["selection"]["units"][0]["execution_run_id"] = "f" * 32
        else:
            selected["selection"]["units"][0]["unit_index"] = 2
        _rewrite_manifest(publication, payload)
    with pytest.raises((StoreIntegrityError, ValueError)):
        inspection.query_publication_artifacts(
            publication, pipeline_run_id="synthetic-pipeline"
        )
