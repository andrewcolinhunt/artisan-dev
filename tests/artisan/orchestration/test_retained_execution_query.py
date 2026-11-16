"""Current execution inspection preserves exact sources without payload reads."""

from __future__ import annotations

from functools import partial
from pathlib import Path

import polars as pl
import pytest
from fixtures.candidate_outputs import external_case, prepare_scientific
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_curators import admit_curator
from fixtures.retained_readers import Backend
from fixtures.retained_results import retained_case
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.operations.curator.merge import Merge
from artisan.orchestration.engine import retained_execution_query as query
from artisan.schemas.enums import CachePolicy, FailurePolicy, TablePath
from artisan.schemas.execution.assignment import DeliveryIdentity
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.cache.retained_reuse import verify_cache_source
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.execution_source import (
    CuratorExecutionSource,
    ExecutionSource,
    FailedScientificExecutionSource,
    ScientificExecutionSource,
)
from artisan.storage.core.projection_schemas import PROJECTION_TABLES
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.admission import admission_directory
from artisan.storage.io.delta_files import DeltaSnapshot
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_read import RetainedControlProof, RetainedReadSnapshot
from artisan.storage.io.retained_store import create_retained_store
from artisan.storage.io.step_result_plan import StepUnitFailure


@pytest.fixture(params=["local", pytest.param("s3", marks=pytest.mark.s3)])
def backend_fs(request: pytest.FixtureRequest, tmp_path: Path) -> Backend:
    if request.param == "local":
        return LocalFileSystem(), StorageConfig(), str(tmp_path)
    return request.getfixturevalue("s3_fs")


@pytest.fixture
def committer(backend_fs: Backend) -> RetainedCommitter:
    fs, storage, root = backend_fs
    return create_retained_store(
        f"{root}/store",
        f"{root}/files",
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )


def _query(committer: RetainedCommitter) -> tuple[query.ExecutionMembership, ...]:
    return query.read_retained_execution_membership(
        committer, pipeline_run_id="synthetic-pipeline"
    )


@pytest.mark.parametrize(
    "acknowledgement", [None, CancellationStatus.CONFIRMED, CancellationStatus.UNKNOWN]
)
def test_curator_without_execution_has_no_execution_membership(
    committer: RetainedCommitter, acknowledgement: CancellationStatus | None
) -> None:
    curator, _, reference = admit_curator(committer, Merge(), {}, step_run_id="a" * 32)
    if acknowledgement is not None:
        for status in (CancellationStatus.REQUESTED, acknowledgement):
            curator.record_cancellation(
                reference.step_run_id, CancellationAcknowledgement(status)
            )
    curator.finish_without_execution(
        reference, error="worker ended before producing a result"
    )
    assert _query(committer) == ()


def test_ordered_repeated_unit_reuse_resolves_each_original_once_per_call(
    committer: RetainedCommitter, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
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
    calls: list[str] = []
    resolve = query._read_execution_source

    def counted(
        snapshot: RetainedReadSnapshot, identity: str
    ) -> ExecutionSource | None:
        calls.append(identity)
        return resolve(snapshot, identity)

    monkeypatch.setattr(query, "_read_execution_source", counted)
    members = _query(committer)
    assert [item.unit_index for item in members] == [0, 1, 2]
    assert all(
        item.cache_hit and item.state.step_run_id == admitted.step_run_id
        for item in members
    )
    for member, receipt in zip(members, selected, strict=True):
        assert isinstance(member.source, ScientificExecutionSource)
        assert member.source.plan.receipt == receipt
        assert member.source.admission.attempt == receipt.delivery.attempt
    assert members[0].source is members[2].source
    assert calls == [receipts[1].delivery.delivery_id, receipts[0].delivery.delivery_id]
    assert _query(committer) == members
    assert len(calls) == 4


def test_whole_partial_reuse_keeps_original_failure_and_omits_infrastructure_slot(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    case = retained_case(tmp_path, kind="alias")
    original, admission, receipts = prepare_scientific(committer, case, count=3)
    committer.accept(receipts[0])
    failed = DeliveryIdentity.new(admission.assignments[1].attempt)
    original.finish(
        admission.step_run_id,
        failures={
            1: StepUnitFailure(error="authored failure", failed_delivery=failed),
            2: StepUnitFailure(error="worker never produced a closed delivery"),
        },
    )
    direct = _query(committer)
    assert len(direct) == 2
    assert [item.unit_index for item in direct] == [0, 1]
    assert not any(item.cache_hit for item in direct)
    current, admitted, _ = prepare_scientific(
        committer, case, step_run_id="b" * 32, count=3
    )
    hit = current.lookup(admitted.step_run_id, CachePolicy.STEP_COMPLETED)
    assert hit is not None
    current.reuse(hit)
    reused = _query(committer)
    assert [item.source for item in reused] == [item.source for item in direct]
    assert all(
        item.cache_hit and item.state.status == StepStatus.PARTIAL for item in reused
    )
    assert all(item.state.step_run_id == admitted.step_run_id for item in reused)
    failure = reused[1].source
    assert isinstance(failure, FailedScientificExecutionSource)
    assert failure.unit.failed_delivery == failed
    assert failure.unit.error == "authored failure"
    assert failure.plan.step_run_id == admission.step_run_id


@pytest.mark.parametrize("empty", [False, True])
def test_curator_reuse_keeps_original_execution_even_without_exports(
    committer: RetainedCommitter, backend_fs: Backend, empty: bool
) -> None:
    operation = DynamicCurator(params=DynamicCurator.Params(empty=empty))
    results, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(
        results, admission, operation, storage=backend_fs[1]
    )
    results.finish(reference, staged.execution_run_id, terminal)
    _, _, current = admit_curator(
        committer, operation, {}, step_run_id="b" * 32, step_number=9, **STAMPS
    )
    hit = lookup_curator(current, reader=CuratorReader(committer))
    assert hit is not None
    results.reuse(hit)
    members = _query(committer)
    assert [member.state.step_number for member in members] == [8, 9]
    assert [member.cache_hit for member in members] == [False, True]
    assert all(member.unit_index is None for member in members)
    assert members[0].source is members[1].source
    source = members[1].source
    assert isinstance(source, CuratorExecutionSource)
    assert source.plan.step_run_id == reference.step_run_id
    assert source.execution_record["execution_run_id"] == staged.execution_run_id


def test_failed_curator_remains_an_execution_without_fabricating_scientific_units(
    committer: RetainedCommitter,
    backend_fs: Backend,
) -> None:
    operation = DynamicCurator(params=DynamicCurator.Params(fail=True))
    results, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(
        results, admission, operation, storage=backend_fs[1]
    )
    results.finish(reference, staged.execution_run_id, terminal)
    (member,) = _query(committer)
    assert member.state.status == StepStatus.FAILED
    assert member.unit_index is None
    assert not member.cache_hit
    assert isinstance(member.source, CuratorExecutionSource)
    assert member.source.execution_record["success"] is False
    assert not committer.journal.all_states()


@pytest.mark.parametrize(
    "latest", ["pending", "running", "unknown", "skipped", "failed", "cancelled"]
)
def test_latest_attempt_masks_earlier_execution_without_inventing_work(
    committer: RetainedCommitter, tmp_path: Path, latest: str
) -> None:
    case = retained_case(tmp_path, kind="alias")
    original, admission, receipts = prepare_scientific(committer, case)
    committer.accept(receipts[0])
    original.finish(admission.step_run_id)
    current, admitted, _ = prepare_scientific(
        committer,
        case,
        step_run_id="b" * 32,
        count=0 if latest == "skipped" else 1,
        admit=latest not in {"pending", "cancelled"},
    )
    if latest == "failed":
        current.finish(
            admitted.step_run_id, failures={0: StepUnitFailure(error="startup failed")}
        )
    elif latest in {"cancelled", "unknown"}:
        expected = StepStatus.PENDING if latest == "cancelled" else StepStatus.RUNNING
        status = (
            CancellationStatus.CONFIRMED
            if latest == "cancelled"
            else CancellationStatus.UNKNOWN
        )
        for acknowledgement in (CancellationStatus.REQUESTED, status):
            current.tracker.record_cancellation(
                admitted.step_run_id,
                expected,
                CancellationAcknowledgement(acknowledgement),
            )
        if latest == "cancelled":
            current.tracker.transition(
                admitted.step_run_id,
                expected,
                StepStatus.CANCELLED,
                result=StepResult(
                    step_run_id=admitted.step_run_id,
                    step_number=7,
                    step_name="produce",
                    status=StepStatus.CANCELLED,
                    cancellation_status=status,
                ),
            )
    assert _query(committer) == ()


def test_incomplete_selection_is_not_exposed_or_recovered(
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
    assert _query(committer) == ()
    assert committer._controls().equals(before)
    assert (
        results.tracker.current_state(admission.step_run_id).status
        == StepStatus.RUNNING
    )


def test_failed_outer_step_still_lists_its_actual_successful_execution(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias"), count=2
    )
    committer.accept(receipts[0])
    results.finish(
        admission.step_run_id,
        failures={1: StepUnitFailure(error="unowned failure")},
        failure_policy=FailurePolicy.FAIL_FAST,
    )
    (member,) = _query(committer)
    assert member.state.status == StepStatus.FAILED
    assert member.unit_index == 0
    assert isinstance(member.source, ScientificExecutionSource)
    assert member.source.plan.receipt == receipts[0]


def test_metadata_membership_never_reads_carriers_or_optional_diagnostics(
    committer: RetainedCommitter, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, external_case(tmp_path / "scratch")
    )
    plan = committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    committer.fs.rm(f"{committer.files_root}/{plan.carriers[0].relative_path}")
    diagnostic = f"{committer.delta_root}/_artisan/replay/{receipts[0].delivery.delivery_id}/configuration.json"
    committer.fs.makedirs(diagnostic.rsplit("/", 1)[0], exist_ok=True)
    committer.fs.pipe_file(diagnostic, b"invalid optional diagnostic")

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Metadata query attempted payload hydration or recovery")

    monkeypatch.setattr(ArtifactStore, "get_artifact", forbidden)
    monkeypatch.setattr(committer, "recover", forbidden)
    (member,) = _query(committer)
    assert isinstance(member.source, ScientificExecutionSource)
    assert member.source.plan == plan
    with pytest.raises(FileNotFoundError):
        RetainedReader(committer).read(receipts[0])


@pytest.mark.parametrize("target", ["terminal", "projection", "admission"])
def test_fresh_read_rejects_changed_required_metadata_after_success(
    committer: RetainedCommitter, tmp_path: Path, target: str
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias")
    )
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    assert len(_query(committer)) == 1
    if target == "admission":
        path = (
            admission_directory(committer.delta_root, receipts[0].delivery.attempt)
            + "/admission.json"
        )
        committer.fs.pipe_file(path, b"{}")
    else:
        table = (
            TablePath.STEPS.value
            if target == "terminal"
            else PROJECTION_TABLES[0].table_path
        )
        path = f"{committer.delta_root}/{table}"
        rows = pl.read_delta(path, storage_options=committer.storage_options)
        column = "duration_seconds" if target == "terminal" else "artifact_id"
        value = 123.0 if target == "terminal" else "f" * 32
        rows.with_columns(
            pl.when(pl.col("status") == "succeeded")
            .then(pl.lit(value))
            .otherwise(pl.col(column))
            .alias(column)
            if target == "terminal"
            else pl.lit(value).alias(column)
        ).write_delta(path, mode="overwrite", storage_options=committer.storage_options)
    with pytest.raises(StoreIntegrityError):
        _query(committer)


def test_run_scope_and_one_captured_latest_attempt_selection(
    committer: RetainedCommitter, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias")
    )
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    assert (
        query.read_retained_execution_membership(committer, pipeline_run_id="other")
        == ()
    )
    with pytest.raises(ValueError, match="explicit pipeline"):
        query.read_retained_execution_membership(committer, pipeline_run_id="")
    snapshot = RetainedReadSnapshot.snapshot
    versions: list[int] = []
    submitted = False

    def capture_then_submit(
        read: RetainedReadSnapshot, table: str, *, version: int | None = None
    ) -> DeltaSnapshot:
        nonlocal submitted
        physical = snapshot(read, table, version=version)
        if table != TablePath.STEPS.value:
            return physical
        versions.append(physical.delta.version())
        if not submitted:
            submitted = True
            results.tracker.create_attempt(
                StepStartRecord(
                    step_run_id="b" * 32,
                    step_number=7,
                    step_name="replacement",
                    operation_class="synthetic.Operation",
                    params_json="{}",
                    input_refs_json="{}",
                    compute_backend="local",
                    compute_options_json="{}",
                    output_roles_json="[]",
                    output_types_json="{}",
                )
            )
        return physical

    with monkeypatch.context() as context:
        context.setattr(RetainedReadSnapshot, "snapshot", capture_then_submit)
        (member,) = _query(committer)
    assert len(set(versions)) == 1
    assert member.state.step_run_id == admission.step_run_id
    assert _query(committer) == ()


@pytest.mark.parametrize("kind", ["scientific", "dynamic_curator"])
def test_lifecycle_and_sources_share_one_fresh_global_proof(
    committer: RetainedCommitter,
    backend_fs: Backend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    if kind == "scientific":
        results, admission, receipts = prepare_scientific(
            committer, retained_case(tmp_path, kind="alias")
        )
        committer.accept(receipts[0])
        results.finish(admission.step_run_id)
    else:
        operation = DynamicCurator()
        curator, admitted, reference = admit_curator(
            committer, operation, {}, step_run_id="a" * 32, **STAMPS
        )
        staged, terminal, _ = stage_observed(
            curator, admitted, operation, storage=backend_fs[1]
        )
        curator.finish(reference, staged.execution_run_id, terminal)
    calls = 0
    prove = committer._control_proof

    def counted() -> RetainedControlProof:
        nonlocal calls
        calls += 1
        return prove()

    monkeypatch.setattr(committer, "_control_proof", counted)
    assert len(_query(committer)) == 1
    assert calls == 1
    assert len(_query(committer)) == 1
    assert calls == 2
