"""Repeated selected-result recovery performs no additional storage writes."""

from __future__ import annotations

from pathlib import Path

import pytest
from deltalake import DeltaTable
from fixtures.candidate_outputs import prepare_scientific
from fixtures.retained_results import retained_case
from fsspec.implementations.local import LocalFileSystem

from artisan.schemas.execution.assignment import SealedDelivery
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.storage.io.repair import repair_store
from artisan.storage.io.retained_store import create_retained_store
from integration.test_staging_recovery import _inventory

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("count", [1, 10])
def test_recovery_finishes_selected_units_without_rewriting_completed_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, count: int
) -> None:
    committer = create_retained_store(
        str(tmp_path / "delta"), str(tmp_path / "files"), fs=LocalFileSystem()
    )
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path / "worker", kind="alias"), count=count + 1
    )

    def interrupt(event: str, _receipt: SealedDelivery, _table: str | None) -> None:
        if event == "claimed":
            message = "selected result awaiting publication"
            raise InterruptedError(message)

    with monkeypatch.context() as patch:
        patch.setattr(committer, "_checkpoint", interrupt)
        for receipt in receipts[:-1]:
            with pytest.raises(InterruptedError, match="awaiting publication"):
                committer.accept(receipt, preserve_staging=True)
    staging = f"{committer.delta_root}/staging"
    report = repair_store(
        delta_root=committer.delta_root,
        files_root=committer.files_root,
        staging_root=staging,
        fs=committer.fs,
        apply=True,
    )
    assert not report.blocking
    assert report.unresolved
    for receipt in receipts[:-1]:
        assert committer.accepted_projection(receipt).exports
        assert not committer.fs.exists(receipt.seal_uri)
    sibling = committer.journal.state(receipts[-1].delivery.attempt.assignment)
    assert sibling is not None
    assert sibling.winner is None
    assert committer.fs.exists(receipts[-1].seal_uri)
    assert (
        results.tracker.current_state(admission.step_run_id).status
        is StepStatus.RUNNING
    )
    tables = [path.parent for path in Path(committer.delta_root).rglob("_delta_log")]
    versions = {path: DeltaTable(str(path)).version() for path in tables}
    evidence = _inventory(committer, committer.delta_root)
    for _ in range(2):
        assert (
            repair_store(
                delta_root=committer.delta_root,
                files_root=committer.files_root,
                staging_root=staging,
                fs=committer.fs,
                apply=True,
            )
            == report
        )
        assert {path: DeltaTable(str(path)).version() for path in tables} == versions
        assert _inventory(committer, committer.delta_root) == evidence
