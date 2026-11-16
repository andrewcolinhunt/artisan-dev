"""Finished worker staging cannot override cancellation before selection."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from artisan.operations.examples import DataGenerator
from artisan.orchestration import PipelineManager, StepStatus
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.storage.core.execution_source import read_execution_source
from artisan.storage.io.repair import repair_store
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_plan import RetainedCommitPlan

pytestmark = pytest.mark.integration


def _cancel_before_selection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    delta_root: str,
    staging_root: str,
    files_root: str,
    storage: StorageConfig,
    preserve: bool,
) -> None:
    manager = PipelineManager.create(
        "cancelled-source",
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=str(tmp_path / "worker"),
        files_root=files_root,
        storage=storage,
        preserve_staging=preserve,
    )
    receipts: list[SealedDelivery] = []
    accept = manager.committer.accept

    def cancel(
        receipt: SealedDelivery, *, preserve_staging: bool = False
    ) -> RetainedCommitPlan:
        receipts.append(receipt)
        manager.cancel()
        return accept(receipt, preserve_staging=preserve_staging)

    with monkeypatch.context() as patch, manager:
        patch.setattr(manager.committer, "accept", cancel)
        result = manager.run(DataGenerator, params={"count": 1, "seed": 91})
    assert result.status is StepStatus.CANCELLED, result.error
    assert manager.ownership_state() is OwnershipState.CLOSED
    assert len(receipts) == 1
    receipt = receipts[0]
    committer = manager.committer
    assignment = receipt.delivery.attempt.assignment
    decision = committer.journal.state(assignment)
    assert decision is not None
    assert decision.cancelled
    assert decision.winner is None
    assert committer.fs.exists(receipt.seal_uri)
    assert read_execution_source(committer, receipt.delivery.delivery_id) is None
    evidence = _inventory(committer, staging_root)
    assert evidence
    report = repair_store(
        delta_root=delta_root,
        staging_root=staging_root,
        files_root=files_root,
        fs=committer.fs,
        storage_options=committer.storage_options,
        apply=True,
        preserve_staging=preserve,
    )
    assert not report.blocking
    assert committer.journal.state(assignment) == decision
    assert _inventory(committer, staging_root) == evidence
    with PipelineManager.create(
        "reopened",
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=str(tmp_path / "reopened-worker"),
        files_root=files_root,
        storage=storage,
    ) as reopened:
        assert reopened.committer.journal.state(assignment) == decision
        assert (
            read_execution_source(reopened.committer, receipt.delivery.delivery_id)
            is None
        )
    assert _inventory(committer, staging_root) == evidence
    assert (
        manager.tracker.current_state(result.step_run_id).status is StepStatus.CANCELLED
    )


def _inventory(committer: RetainedCommitter, root: str) -> dict[str, str]:
    return {
        path: hashlib.sha256(committer.fs.cat_file(path)).hexdigest()
        for path in committer.fs.find(root)
    }


@pytest.mark.parametrize("preserve", [False, True])
def test_repair_and_new_pipeline_preserve_cancelled_unclaimed_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, preserve: bool
) -> None:
    _cancel_before_selection(
        tmp_path,
        monkeypatch,
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        files_root=str(tmp_path / "files"),
        storage=StorageConfig(),
        preserve=preserve,
    )


@pytest.mark.parametrize("preserve", [False, True])
def test_s3_repair_preserves_cancelled_unclaimed_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, s3_fs, preserve: bool
) -> None:
    _fs, storage, root = s3_fs
    _cancel_before_selection(
        tmp_path,
        monkeypatch,
        delta_root=f"{root}/delta",
        staging_root=f"{root}/staging",
        files_root=f"{root}/files",
        storage=storage,
        preserve=preserve,
    )
