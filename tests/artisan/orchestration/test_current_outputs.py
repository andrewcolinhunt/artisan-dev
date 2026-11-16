"""Current output selection preserves exact scientific and curator authority."""

from __future__ import annotations

from functools import partial
from pathlib import Path

import pytest
from fixtures.candidate_outputs import prepare_scientific
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_acceptance import open_committer
from fixtures.retained_curators import admit_curator
from fixtures.retained_results import retained_case

from artisan.errors import PersistenceIntegrityError, StoreIntegrityError
from artisan.orchestration.engine.current_outputs import (
    read_current_step_outputs,
    read_source_run_outputs,
)
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.cache.retained_reuse import verify_cache_source
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.step_result_plan import StepUnitFailure


@pytest.fixture
def committer(tmp_path: Path) -> RetainedCommitter:
    storage = StorageConfig()
    return open_committer(
        str(tmp_path / "store"), storage.filesystem(), storage, initialize=True
    )


def test_scientific_direct_and_reuse_preserve_order_and_duplicate_occurrences(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    case = retained_case(tmp_path, kind="alias")
    results, admission, receipts = prepare_scientific(committer, case, count=2)
    for receipt in receipts:
        committer.accept(receipt)
    results.finish(admission.step_run_id)
    direct = read_current_step_outputs(committer, admission.step_run_id)
    assert [item.reference.delivery_id for item in direct[0].occurrences] == [
        receipt.delivery.delivery_id for receipt in receipts for _ in range(2)
    ]
    reused, current, _ = prepare_scientific(committer, case, step_run_id="b" * 32)
    committer.journal.reuse(
        current.assignments[0],
        receipts[0],
        verify_source=partial(verify_cache_source, reader=RetainedReader(committer)),
    )
    reused.finish(current.step_run_id)
    selected = read_source_run_outputs(
        committer, pipeline_run_id="synthetic-pipeline", step_number=7
    )
    assert len(selected) == 1
    assert selected[0].state.step_run_id == current.step_run_id
    assert selected[0].outputs[0].occurrences == direct[0].occurrences[:2]


@pytest.mark.parametrize("empty", [False, True])
def test_curator_direct_and_reuse_keep_original_producer(
    committer: RetainedCommitter, empty: bool
) -> None:
    operation = DynamicCurator()
    operation.params.empty = empty
    coordinator, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(coordinator, admission, operation)
    assert staged.success, staged.error
    coordinator.finish(reference, staged.execution_run_id, terminal)
    direct = read_current_step_outputs(committer, reference.step_run_id)
    _, _, current = admit_curator(
        committer, operation, {}, step_run_id="b" * 32, step_number=9, **STAMPS
    )
    hit = lookup_curator(current, reader=CuratorReader(committer))
    assert hit is not None
    coordinator.reuse(hit)
    assert read_current_step_outputs(committer, current.step_run_id) == direct
    selected = read_source_run_outputs(
        committer,
        pipeline_run_id="synthetic-pipeline",
        step_number=9,
        include_prior_steps=True,
    )
    assert [item.state.step_number for item in selected] == [8, 9]
    assert [item.outputs for item in selected] == [direct, direct]


@pytest.mark.parametrize("latest", ["failed", "skipped", "pending", "running"])
def test_latest_attempt_never_falls_back_to_old_success(
    committer: RetainedCommitter, tmp_path: Path, latest: str
) -> None:
    case = retained_case(tmp_path, kind="alias")
    results, old, receipts = prepare_scientific(committer, case)
    committer.accept(receipts[0])
    results.finish(old.step_run_id)
    results, current, _ = prepare_scientific(
        committer,
        case,
        step_run_id="b" * 32,
        count=0 if latest == "skipped" else 1,
        admit=latest != "pending",
    )
    if latest == "failed":
        results.finish(
            current.step_run_id, failures={0: StepUnitFailure(error="science failed")}
        )
    if latest in {"running", "pending"}:
        with pytest.raises(StoreIntegrityError, match="not terminal"):
            read_source_run_outputs(
                committer, pipeline_run_id="synthetic-pipeline", step_number=7
            )
        return
    selected = read_source_run_outputs(
        committer, pipeline_run_id="synthetic-pipeline", step_number=7
    )
    assert selected[0].state.step_run_id == current.step_run_id
    assert selected[0].outputs == ()


def test_successful_empty_membership_is_distinct_from_unknown_step(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="empty")
    )
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    outputs = read_current_step_outputs(committer, admission.step_run_id)
    assert [(role.role, role.occurrences) for role in outputs] == [("optional", ())]
    with pytest.raises(PersistenceIntegrityError, match="Unknown step"):
        read_current_step_outputs(committer, "f" * 32)
    with pytest.raises(ValueError, match="boundary"):
        read_source_run_outputs(committer, pipeline_run_id="unknown", step_number=7)


def test_incomplete_terminal_plan_is_not_recovered_by_read(
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
    with pytest.raises(StoreIntegrityError, match="not terminal"):
        read_current_step_outputs(committer, admission.step_run_id)
    assert (
        results.tracker.current_state(admission.step_run_id).status
        == StepStatus.RUNNING
    )
