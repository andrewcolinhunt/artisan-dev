"""Timing observes exact outer membership without inventing child executions."""

from __future__ import annotations

from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from fixtures.candidate_outputs import external_case, prepare_scientific
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_curators import admit_curator
from fixtures.retained_results import retained_case

from artisan.errors import StoreIntegrityError
from artisan.execution.recording.delivery_diagnostics import (
    _path,
    publish_delivery_diagnostics,
)
from artisan.schemas.enums import CachePolicy, FailurePolicy, TablePath
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.delivery_diagnostics import (
    DeliveryDiagnostics,
    InvocationDiagnostic,
)
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.cache.retained_reuse import verify_cache_source
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_read import RetainedControlProof
from artisan.storage.io.retained_store import create_retained_store
from artisan.storage.io.step_result_plan import StepUnitFailure
from artisan.utils.hashing import canonical_json_bytes
from artisan.utils.path import uri_join
from artisan.visualization import retained_timing
from artisan.visualization.retained_timing import read_retained_timings


@pytest.fixture
def committer(tmp_path: Path) -> RetainedCommitter:
    return create_retained_store(
        str(tmp_path / "store"),
        str(tmp_path / "files"),
        fs=StorageConfig().filesystem(),
    )


def _accepted(committer: RetainedCommitter, scratch: Path) -> SealedDelivery:
    results, admission, receipts = prepare_scientific(committer, external_case(scratch))
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    return receipts[0]


def _invocation(identity: str) -> InvocationDiagnostic:
    recording = CommandRecording.model_validate(
        {
            "status": "complete",
            "missing_invocations": [],
            "omitted_commands": 0,
            "omitted_missing_invocations": 0,
            "unavailable_reason": None,
            "commands": [
                {
                    "invocation": 0,
                    "sequence": 0,
                    "location": "local",
                    "requested_argv": ["tool"],
                    "argv": ["tool"],
                    "cwd": "/work",
                    "tool": None,
                    "environment": {
                        "type": "native",
                        "identity": {},
                        "variable_names": [],
                    },
                    "outcome": "succeeded",
                    "returncode": 0,
                    "redacted_fields": [],
                    "required_environment": [],
                    "launch_seconds": 0.123456789,
                }
            ],
        }
    )
    return InvocationDiagnostic(
        invocation_id=identity,
        call_path=(identity,),
        operation_name="leaf",
        operation_version="1",
        succeeded=True,
        cleanup="closed",
        duration_seconds=0.25,
        timings={"execute": 0.2},
        command_recording=recording,
        tool_output=None,
        phase=None,
        error=None,
    )


def _diagnostic(
    receipt: SealedDelivery,
    *,
    groups: tuple[InvocationDiagnostic, ...] = (),
    omitted: int = 0,
) -> DeliveryDiagnostics:
    return DeliveryDiagnostics(
        timestamp_start=datetime(2024, 1, 1, tzinfo=UTC),
        delivery=receipt.delivery,
        seal_digest=receipt.seal_digest,
        outcome="success",
        cleanup="closed",
        duration_seconds=0.75,
        invocations=groups,
        omitted_invocations=omitted,
        phase=None,
        call_path=(),
        error=None,
    )


def _publish(committer: RetainedCommitter, diagnostic: DeliveryDiagnostics) -> None:
    publish_delivery_diagnostics(
        diagnostic, delta_root=committer.delta_root, fs=committer.fs
    )


def test_absent_and_observed_empty_timings_keep_outer_identity_and_nullable_values(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    receipt = _accepted(committer, tmp_path)
    absent = read_retained_timings(committer, pipeline_run_id="synthetic-pipeline")
    frame = absent.execution_timings(7)
    assert frame["execution_run_id"].to_list() == [receipt.delivery.delivery_id]
    assert frame["duration_seconds"].to_list() == [None]
    assert frame["diagnostics_status"].to_list() == ["unavailable"]
    assert absent.execution_stats(7).is_empty()
    commands = absent.command_timings(7)
    assert commands["entry_type"].to_list() == ["delivery_coverage"]
    assert commands["coverage_status"].to_list() == ["unavailable"]
    assert commands["omitted_invocations"].to_list() == [None]
    _publish(committer, _diagnostic(receipt))
    present = read_retained_timings(committer)
    assert present.execution_timings(7)["duration_seconds"].to_list() == [0.75]
    assert present.command_timings(7)["coverage_status"].to_list() == ["complete"]
    assert present.command_timings(7)["omitted_invocations"].to_list() == [0]
    assert present.data["steps"][0]["executions"][0]["invocations"] == []


def test_nested_timings_and_commands_keep_original_groups_and_no_phase_sums(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    receipt = _accepted(committer, tmp_path)
    _publish(
        committer,
        _diagnostic(
            receipt, groups=(_invocation("second"), _invocation("first")), omitted=1
        ),
    )
    with committer._read_snapshot() as read:
        plan = read.plan_for_delivery(receipt.delivery.delivery_id)
    committer.fs.rm(uri_join(committer.files_root, plan.carriers[0].relative_path))
    result = read_retained_timings(committer)
    frame = result.execution_timings(7)
    assert frame.height == 1
    assert "execute" not in frame.columns
    assert result.execution_stats(7).is_empty()
    invocations = result.data["steps"][0]["executions"][0]["invocations"]
    assert [item["invocation_id"] for item in invocations] == ["second", "first"]
    assert [item["timings"] for item in invocations] == [{"execute": 0.2}] * 2
    commands = result.command_timings(7)
    assert commands["invocation_id"].to_list() == ["second", "first", None]
    assert commands["sequence"].to_list() == [0, 0, None]
    assert commands["invocation"].to_list() == [0, 0, None]
    assert commands["entry_type"].to_list() == [
        "command",
        "command",
        "delivery_coverage",
    ]
    assert commands["launch_seconds"].to_list() == [0.123456789, 0.123456789, None]
    assert commands["omitted_invocations"].to_list() == [1, 1, 1]
    assert "argv" not in commands.columns


@pytest.mark.parametrize("whole", [False, True])
def test_cache_selection_never_adds_historical_worker_duration(
    committer: RetainedCommitter, tmp_path: Path, whole: bool
) -> None:
    case = retained_case(tmp_path, kind="alias")
    results, admission, receipts = prepare_scientific(committer, case, count=2)
    committer.accept(receipts[0])
    results.finish(
        admission.step_run_id, failures={1: StepUnitFailure(error="failed sibling")}
    )
    _publish(committer, _diagnostic(receipts[0]))
    assert read_retained_timings(committer).execution_timings(7).height == 1
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
    timing = read_retained_timings(committer)
    assert timing.execution_timings(7).is_empty()
    assert timing.command_timings(7).is_empty()


def test_pending_retry_masks_timing_without_reading_older_diagnostics(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    _accepted(committer, tmp_path)
    prepare_scientific(
        committer,
        retained_case(tmp_path, kind="alias"),
        step_run_id="b" * 32,
        admit=False,
    )
    with pytest.raises(ValueError, match="No usable terminal"):
        read_retained_timings(committer)


def test_failed_outer_step_excludes_its_successful_unit_observation(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias"), count=2
    )
    committer.accept(receipts[0])
    results.finish(
        admission.step_run_id,
        failures={1: StepUnitFailure(error="failed sibling")},
        failure_policy=FailurePolicy.FAIL_FAST,
    )
    _publish(committer, _diagnostic(receipts[0]))

    with pytest.raises(ValueError, match="No usable terminal"):
        read_retained_timings(committer)


def test_timing_lifecycle_and_sources_use_one_proof_and_next_call_is_fresh(
    committer: RetainedCommitter, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    receipt = _accepted(committer, tmp_path)
    prove = committer._control_proof
    calls = 0

    def counted() -> RetainedControlProof:
        nonlocal calls
        calls += 1
        return prove()

    monkeypatch.setattr(committer, "_control_proof", counted)
    observations = retained_timing._observations
    appended = False

    def append_pending(
        store: RetainedCommitter, member: retained_timing.ExecutionMembership
    ) -> dict[str, Any]:
        nonlocal appended
        observed = observations(store, member)
        if not appended:
            appended = True
            path = uri_join(store.delta_root, TablePath.STEPS.value)
            pending = (
                pl.read_delta(path)
                .filter(pl.col("state_sequence") == 0)
                .with_columns(
                    pl.lit("b" * 32).alias("step_run_id"),
                    pl.lit(datetime.now(UTC)).alias("timestamp"),
                )
            )
            pending.write_delta(path, mode="append")
        return observed

    monkeypatch.setattr(retained_timing, "_observations", append_pending)
    result = read_retained_timings(committer)
    assert result.execution_timings(7)["execution_run_id"].to_list() == [
        receipt.delivery.delivery_id
    ]
    assert calls == 1
    with pytest.raises(ValueError, match="No usable terminal"):
        read_retained_timings(committer)
    assert calls == 2


@pytest.mark.parametrize("tamper", ["seal", "owner", "malformed"])
def test_present_diagnostics_must_match_exact_source(
    committer: RetainedCommitter, tmp_path: Path, tamper: str
) -> None:
    receipt = _accepted(committer, tmp_path)
    diagnostic = _diagnostic(receipt)
    _publish(committer, diagnostic)
    if tamper == "seal":
        diagnostic = diagnostic.model_copy(update={"seal_digest": "f" * 32})
    elif tamper == "owner":
        diagnostic = diagnostic.model_copy(
            update={
                "delivery": diagnostic.delivery.model_copy(
                    update={"delivery_id": "e" * 32}
                )
            }
        )
    committer.fs.pipe_file(
        _path(committer.delta_root, receipt.delivery),
        b"sensitive malformed bytes"
        if tamper == "malformed"
        else canonical_json_bytes(diagnostic.model_dump(mode="json")),
    )
    with pytest.raises(StoreIntegrityError) as error:
        read_retained_timings(committer)
    assert "sensitive" not in str(error.value)


def test_accepted_source_keeps_postseal_infrastructure_timing(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    receipt = _accepted(committer, tmp_path)
    observed = _diagnostic(receipt).model_copy(
        update={"outcome": "infrastructure", "error": "reply transport failed"}
    )
    _publish(committer, observed)
    timing = read_retained_timings(committer)
    assert timing.execution_timings(7)["duration_seconds"].to_list() == [0.75]


def test_curator_uses_original_execution_observations_and_reuse_has_no_worker_time(
    committer: RetainedCommitter,
) -> None:
    operation = DynamicCurator()
    coordinator, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(coordinator, admission, operation)
    terminal = terminal.model_copy(
        update={
            "metadata": {**terminal.metadata, "timings": {"execute": 1.25}},
            "duration_seconds": 2.0,
        }
    )
    coordinator.finish(reference, staged.execution_run_id, terminal)
    timing = read_retained_timings(committer)
    assert timing.data["steps"][0]["timings"] == {"execute": 1.25}
    assert timing.data["steps"][0]["duration_seconds"] == 2.0
    frame = timing.execution_timings(8)
    assert frame["execution_run_id"].to_list() == [staged.execution_run_id]
    assert frame["duration_seconds"][0] is not None
    assert frame["duration_seconds"][0] >= 0
    assert timing.command_timings(8)["entry_type"].to_list() == ["empty_recording"]
    _, _, current = admit_curator(
        committer, operation, {}, step_run_id="b" * 32, step_number=9, **STAMPS
    )
    hit = lookup_curator(current, reader=CuratorReader(committer))
    assert hit is not None
    coordinator.reuse(hit)
    timing = read_retained_timings(committer)
    assert timing.execution_timings(8).height == 1
    assert timing.execution_timings(9).is_empty()
    assert timing.data["steps"][1]["timings"] == {}
    assert timing.data["steps"][1]["duration_seconds"] is None
