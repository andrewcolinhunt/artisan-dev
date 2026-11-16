"""Curator completion reuses evidence only between authority mutations."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import polars as pl
import pytest
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_acceptance import admit_case, open_committer
from fixtures.retained_curators import admit_curator, stage_curator
from fixtures.retained_readers import Backend

from artisan.errors import PersistenceIntegrityError, StoreIntegrityError
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.operations.curator.merge import Merge
from artisan.orchestration.engine import retained_curator
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.schemas.artifact.appendable import AppendableArtifact
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_admission import CuratorAdmission
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.commit_contract import LogicalEffect
from artisan.storage.io.curator_plan import CuratorCommitPlan, read_curator_plan
from artisan.storage.io.retained_read import RetainedControlProof
from artisan.utils.hashing import compute_content_digest
from artisan.utils.path import uri_join

from .test_retained_curator import _AppendableCurator


def _prepare(
    backend: Backend,
    tmp_path: Path,
    *,
    kind: Literal["static", "dynamic", "appendable"] = "static",
) -> tuple[CuratorResults, CuratorAdmission, StagingResult, StepResult]:
    fs, storage, root = backend
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    if kind == "dynamic":
        operation = DynamicCurator()
        results, admission, _ = admit_curator(
            committer, operation, {}, step_run_id="b" * 32, **STAMPS
        )
        staged, terminal, _ = stage_observed(
            results, admission, operation, storage=storage
        )
    elif kind == "appendable":
        record = b'{"record_id":"selected","value":1}'
        path = tmp_path / "records.jsonl"
        path.write_bytes(record + b'\n{"record_id":"other","value":2}\n')
        external = _AppendableCurator()
        external._artifact = AppendableArtifact.draft(
            "selected", compute_content_digest(record), len(record), 8, str(path)
        )
        results, admission, _ = admit_curator(
            committer, external, {}, step_run_id="b" * 32
        )
        staged, terminal = stage_curator(results, admission, external)
        path.unlink()
    else:
        _, receipt = admit_case(committer, tmp_path, kind="alias")
        committer.accept(receipt)
        original = RetainedReader(committer).read(receipt).output("data")[0]
        merge = Merge()
        results, admission, _ = admit_curator(
            committer, merge, {"data": (original,)}, step_run_id="b" * 32
        )
        staged, terminal = stage_curator(results, admission, merge)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    return results, admission, staged, terminal


def _finish(
    results: CuratorResults,
    admission: CuratorAdmission,
    staged: StagingResult,
    terminal: StepResult,
) -> StepResult:
    assert staged.execution_run_id is not None
    return results.finish(results.admit(admission), staged.execution_run_id, terminal)


@pytest.mark.parametrize("kind", ["static", "dynamic"])
@pytest.mark.parametrize("complete", [False, True])
def test_completion_uses_one_proof_per_unchanged_phase(
    raw_backend_fs: Backend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: Literal["static", "dynamic"],
    complete: bool,
) -> None:
    results, admission, staged, terminal = _prepare(raw_backend_fs, tmp_path, kind=kind)
    reference = results.admit(admission)
    assert staged.execution_run_id is not None
    if complete:
        results.finish(reference, staged.execution_run_id, terminal)
    original = results.committer._control_proof
    proofs = []

    def counted() -> RetainedControlProof:
        proof = original()
        proofs.append(proof)
        return proof

    with monkeypatch.context() as scope:
        scope.setattr(results.committer, "_control_proof", counted)
        result = (
            results.recover(reference.step_run_id)
            if complete
            else results.finish(reference, staged.execution_run_id, terminal)
        )
    assert result.status == StepStatus.SUCCEEDED
    assert len(proofs) == (1 if complete else 5)
    assert (
        results.tracker.current_state(reference.step_run_id).to_step_result() == result
    )


def test_complete_helpers_reject_another_store_binding(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    results, admission, staged, terminal = _prepare(raw_backend_fs, tmp_path)
    _finish(results, admission, staged, terminal)
    committer = results.committer
    plan = read_curator_plan(
        committer.delta_root, admission.record.step_run_id, committer.fs
    )
    assert isinstance(plan, CuratorCommitPlan)
    other = open_committer(committer.delta_root, committer.fs, raw_backend_fs[1])
    with (
        other._read_snapshot() as read,
        pytest.raises(StoreIntegrityError, match="another store binding"),
    ):
        results._completed_result(plan, read)


@pytest.mark.parametrize(
    "boundary", ["carriers", "planned", "table:orchestration/steps"]
)
def test_complete_proof_rechecks_full_published_carrier(
    raw_backend_fs: Backend, tmp_path: Path, boundary: str
) -> None:
    results, admission, staged, terminal = _prepare(
        raw_backend_fs, tmp_path, kind="appendable"
    )
    committer = results.committer
    saved: dict[str, bytes] = {}

    def corrupt(event: str) -> None:
        if event == boundary:
            plan = read_curator_plan(
                committer.delta_root, admission.record.step_run_id, committer.fs
            )
            assert isinstance(plan, CuratorCommitPlan)
            assert plan.managed_locations is not None
            path = plan.managed_locations.row_keys[0][1]
            saved[path] = committer.fs.cat_file(path)
            committer.fs.pipe_file(path, saved[path] + b"unselected remainder changed")

    results.checkpoint = corrupt
    with pytest.raises(StoreIntegrityError, match="carrier"):
        _finish(results, admission, staged, terminal)
    results.checkpoint = None
    assert saved
    for path, payload in saved.items():
        committer.fs.pipe_file(path, payload)
    expected = results.recover(admission.record.step_run_id)
    assert expected.status == StepStatus.SUCCEEDED
    path, payload = next(iter(saved.items()))
    committer.fs.pipe_file(path, payload + b"changed after completed recovery")
    with pytest.raises(StoreIntegrityError, match="carrier"):
        results.recover(admission.record.step_run_id)
    committer.fs.pipe_file(path, payload)
    assert results.recover(admission.record.step_run_id) == expected


def test_completion_reopens_terminal_table_after_control_write(
    raw_backend_fs: Backend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    results, admission, staged, terminal = _prepare(raw_backend_fs, tmp_path)
    committer = results.committer
    complete = retained_curator.complete_control
    path = uri_join(committer.delta_root, TablePath.STEPS.value)
    saved = []

    def change_after_complete(
        plan: LogicalEffect, control_path: str, *, storage_options: dict[str, str]
    ) -> None:
        complete(plan, control_path, storage_options=storage_options)
        rows = pl.read_delta(path, storage_options=storage_options)
        saved.append(rows)
        rows.with_columns(
            pl.when(pl.col("logical_commit_id") == plan.logical_commit_id)
            .then(pl.lit(99.0))
            .otherwise(pl.col("duration_seconds"))
            .alias("duration_seconds")
        ).write_delta(path, mode="overwrite", storage_options=storage_options)

    with monkeypatch.context() as scope:
        scope.setattr(retained_curator, "complete_control", change_after_complete)
        with pytest.raises(StoreIntegrityError):
            _finish(results, admission, staged, terminal)
    saved[0].write_delta(
        path, mode="overwrite", storage_options=committer.storage_options
    )
    assert results.recover(admission.record.step_run_id).status == StepStatus.SUCCEEDED


def test_cancellation_between_candidate_and_selection_is_not_hidden(
    raw_backend_fs: Backend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    results, admission, staged, terminal = _prepare(raw_backend_fs, tmp_path)
    stage = results.staging.stage_orchestrator_dataframe

    def cancel_after_stage(*args: object, **kwargs: object) -> object:
        value = stage(*args, **kwargs)  # type: ignore[arg-type]
        results.tracker.record_cancellation(
            admission.record.step_run_id,
            StepStatus.RUNNING,
            CancellationAcknowledgement(status=CancellationStatus.CONFIRMED),
        )
        return value

    monkeypatch.setattr(
        results.staging, "stage_orchestrator_dataframe", cancel_after_stage
    )
    with pytest.raises((StoreIntegrityError, PersistenceIntegrityError)):
        _finish(results, admission, staged, terminal)
    assert (
        read_curator_plan(
            results.committer.delta_root,
            admission.record.step_run_id,
            results.committer.fs,
        )
        is None
    )
