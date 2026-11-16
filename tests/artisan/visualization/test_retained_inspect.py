"""Retained diagnostics preserve exact outer sources without inventing observations."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest
from fixtures.candidate_outputs import external_case, prepare_scientific
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_curators import admit_curator
from fixtures.retained_results import retained_case

from artisan.errors import ArtisanError, StoreIntegrityError
from artisan.execution.recording.delivery_diagnostics import (
    _path,
    publish_delivery_diagnostics,
)
from artisan.execution.recording.recorder import _write_failure_log
from artisan.schemas.enums import CachePolicy
from artisan.schemas.execution.assignment import DeliveryIdentity, SealedDelivery
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.delivery_diagnostics import (
    DeliveryDiagnostics,
    InvocationDiagnostic,
)
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.execution_source import (
    CuratorExecutionSource,
    FailedScientificExecutionSource,
    read_execution_source,
)
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_store import create_retained_store
from artisan.storage.io.step_result_plan import StepUnitFailure
from artisan.utils.hashing import canonical_json_bytes
from artisan.utils.log_paths import failure_log_relative_path
from artisan.utils.path import uri_join, uri_parent
from artisan.visualization.inspect import _FAILURES_SCHEMA
from artisan.visualization.retained_inspect import retained_commands, retained_failures


class StructuredFailureCurator(DynamicCurator):
    name = "structured_failure_curator"

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        code = "synthetic_input_error"
        raise ArtisanError(
            code,
            "required field is unavailable",
            error_type="validation",
            field="params.value",
            recovery_hint="CHECK_INPUT",
            suggestions=["provide value"],
        )


@pytest.fixture
def committer(tmp_path: Path) -> RetainedCommitter:
    return create_retained_store(
        str(tmp_path / "store"),
        str(tmp_path / "files"),
        fs=StorageConfig().filesystem(),
    )


def _recording(label: str) -> CommandRecording:
    return CommandRecording.model_validate(
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
                    "requested_argv": ["tool", label],
                    "argv": ["tool", label],
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
                    "launch_seconds": 0.1,
                }
            ],
        }
    )


def _observed(identity: str) -> InvocationDiagnostic:
    return InvocationDiagnostic(
        invocation_id=identity,
        call_path=(identity,),
        operation_name="synthetic",
        operation_version="1",
        succeeded=True,
        cleanup="closed",
        duration_seconds=0.1,
        timings={},
        command_recording=_recording(identity),
        tool_output=None,
        phase=None,
        error=None,
    )


def _diagnostic(
    delivery: DeliveryIdentity,
    *,
    seal: str | None,
    invocations: tuple[InvocationDiagnostic, ...] = (),
    omitted: int = 0,
) -> DeliveryDiagnostics:
    return DeliveryDiagnostics(
        timestamp_start=datetime(2024, 1, 1, tzinfo=UTC),
        delivery=delivery,
        seal_digest=seal,
        outcome="success" if seal else "science",
        cleanup="closed",
        duration_seconds=0.2,
        invocations=invocations,
        omitted_invocations=omitted,
        phase=None if seal else "execute",
        call_path=(),
        error=None if seal else "optional observed text",
    )


def _accepted(committer: RetainedCommitter, tmp_path: Path) -> SealedDelivery:
    results, admission, receipts = prepare_scientific(
        committer, external_case(tmp_path)
    )
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    return receipts[0]


def test_missing_and_present_empty_scientific_commands_are_distinct(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    receipt = _accepted(committer, tmp_path)
    absent = retained_commands(committer, receipt.delivery.delivery_id)
    assert absent.status == "unavailable"
    assert absent.omitted_invocations is None
    publish_delivery_diagnostics(
        _diagnostic(receipt.delivery, seal=receipt.seal_digest),
        delta_root=committer.delta_root,
        fs=committer.fs,
    )
    present = retained_commands(committer, receipt.delivery.delivery_id)
    assert present.status == "complete"
    assert present.omitted_invocations == 0
    assert present.invocations == absent.invocations == ()
    assert present.recording is None


def test_scientific_command_groups_keep_order_indices_and_omissions_without_payload_reads(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    receipt = _accepted(committer, tmp_path)
    diagnostic = _diagnostic(
        receipt.delivery,
        seal=receipt.seal_digest,
        invocations=(_observed("second"), _observed("first")),
        omitted=1,
    )
    publish_delivery_diagnostics(
        diagnostic, delta_root=committer.delta_root, fs=committer.fs
    )
    with committer._read_snapshot() as read:
        plan = read.plan_for_delivery(receipt.delivery.delivery_id)
    committer.fs.rm(uri_join(committer.files_root, plan.carriers[0].relative_path))
    result = retained_commands(committer, receipt.delivery.delivery_id)
    assert result.status == "partial"
    assert result.omitted_invocations == 1
    assert [item.invocation_id for item in result.invocations] == ["second", "first"]
    assert [item.recording for item in result.invocations] == [
        item.command_recording for item in diagnostic.invocations
    ]
    assert [item.recording.commands[0].sequence for item in result.invocations] == [
        0,
        0,
    ]


@pytest.mark.parametrize("tamper", ["owner", "seal", "malformed"])
def test_malformed_or_mismatched_scientific_observations_are_not_silently_absent(
    committer: RetainedCommitter, tmp_path: Path, tamper: str
) -> None:
    receipt = _accepted(committer, tmp_path)
    diagnostic = _diagnostic(receipt.delivery, seal=receipt.seal_digest)
    publish_delivery_diagnostics(
        diagnostic, delta_root=committer.delta_root, fs=committer.fs
    )
    path = _path(committer.delta_root, receipt.delivery)
    if tamper == "malformed":
        encoded = b"{}"
    else:
        payload = diagnostic.model_dump(mode="json")
        if tamper == "owner":
            payload["delivery"] = DeliveryIdentity.new(
                receipt.delivery.attempt
            ).model_dump(mode="json")
        else:
            payload["seal_digest"] = "f" * 32
        encoded = canonical_json_bytes(payload)
    committer.fs.pipe_file(path, encoded)
    with pytest.raises(StoreIntegrityError):
        retained_commands(committer, receipt.delivery.delivery_id)


def test_whole_partial_failures_keep_required_identity_and_error_without_infrastructure_rows(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    case = retained_case(tmp_path, kind="alias")
    source, admitted, receipts = prepare_scientific(committer, case, count=3)
    committer.accept(receipts[0])
    failed = DeliveryIdentity.new(admitted.assignments[1].attempt)
    source.finish(
        admitted.step_run_id,
        failures={
            1: StepUnitFailure(
                error="required scientific failure", failed_delivery=failed
            ),
            2: StepUnitFailure(error="infrastructure without a delivery"),
        },
    )
    original = retained_failures(committer, pipeline_run_id="synthetic-pipeline")
    assert original.schema == _FAILURES_SCHEMA
    assert original.height == 1
    assert original["execution_run_id"][0] == failed.delivery_id
    assert original["error"][0] == "required scientific failure"
    assert original["timestamp_start"][0] is original["log"][0] is None
    publish_delivery_diagnostics(
        _diagnostic(failed, seal=None), delta_root=committer.delta_root, fs=committer.fs
    )
    observed = retained_failures(committer, pipeline_run_id="synthetic-pipeline")
    assert observed.drop("timestamp_start", "log").equals(
        original.drop("timestamp_start", "log")
    )
    assert observed["timestamp_start"][0] == datetime(2024, 1, 1, tzinfo=UTC)
    assert observed["log"][0] == failure_log_relative_path(
        failed.delivery_id, datetime(2024, 1, 1, tzinfo=UTC)
    )
    original = observed
    current, selected, _ = prepare_scientific(
        committer, case, step_run_id="b" * 32, count=3
    )
    hit = current.lookup(selected.step_run_id, CachePolicy.STEP_COMPLETED)
    assert hit is not None
    current.reuse(hit)
    assert retained_failures(committer, pipeline_run_id="synthetic-pipeline").equals(
        original
    )
    committer.fs.pipe_file(_path(committer.delta_root, failed), b"{}")
    with pytest.raises(StoreIntegrityError):
        retained_failures(committer, pipeline_run_id="synthetic-pipeline")


def test_curator_failure_and_commands_preserve_original_physical_observations(
    committer: RetainedCommitter,
) -> None:
    operation = DynamicCurator(params=DynamicCurator.Params(fail=True))
    results, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(results, admission, operation)
    results.finish(reference, staged.execution_run_id, terminal)
    assert staged.execution_run_id is not None
    source = read_execution_source(committer, staged.execution_run_id)
    assert isinstance(source, CuratorExecutionSource)
    failed = retained_failures(committer, pipeline_run_id="synthetic-pipeline").row(
        0, named=True
    )
    assert failed["timestamp_start"] == source.execution_record["timestamp_start"]
    assert failed["error"] == source.execution_record["error"]
    assert failed["log"] is None
    root = uri_join(uri_parent(committer.delta_root), "logs", "failures")
    _write_failure_log(
        root,
        staged.execution_run_id,
        failed["timestamp_start"],
        operation.name,
        failed["step"],
        "local",
        failed["error"],
        fs=committer.fs,
    )
    relative = failure_log_relative_path(
        staged.execution_run_id, failed["timestamp_start"]
    )
    assert retained_failures(committer)["log"].to_list() == [relative]
    committer.fs.rm(uri_join(root, relative))
    assert retained_failures(committer)["log"].to_list() == [None]
    commands = retained_commands(committer, staged.execution_run_id)
    assert commands.invocations == ()
    assert commands.omitted_invocations == 0
    assert commands.recording is not None
    assert (
        commands.recording.model_dump_json()
        == source.execution_record["command_recording"]
    )


def test_unknown_execution_and_empty_run_do_not_invent_observations(
    committer: RetainedCommitter,
) -> None:
    failures = retained_failures(committer, pipeline_run_id="unknown")
    assert failures.is_empty()
    assert failures.schema == _FAILURES_SCHEMA
    with pytest.raises(FileNotFoundError):
        retained_commands(committer, "missing")
    with pytest.raises(ValueError, match="explicit"):
        retained_failures(committer, pipeline_run_id="")


def test_curator_structured_error_fields_remain_typed_observations(
    committer: RetainedCommitter,
) -> None:
    operation = StructuredFailureCurator()
    results, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(results, admission, operation)
    results.finish(reference, staged.execution_run_id, terminal)
    row = retained_failures(committer, pipeline_run_id="synthetic-pipeline").row(
        0, named=True
    )
    assert row["code"] == "synthetic_input_error"
    assert row["recovery_hint"] == "CHECK_INPUT"
    assert row["field"] == "params.value"
    assert row["suggestions"] == ["provide value"]


@pytest.mark.parametrize("contradiction", ["success", "seal", "outcome", "cleanup"])
def test_failed_source_rejects_contradictory_optional_outcomes_without_losing_authority(
    committer: RetainedCommitter, tmp_path: Path, contradiction: str
) -> None:
    results, admitted, _ = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias")
    )
    delivery = DeliveryIdentity.new(admitted.assignments[0].attempt)
    results.finish(
        admitted.step_run_id,
        failures={
            0: StepUnitFailure(error="required failure", failed_delivery=delivery)
        },
    )
    diagnostic = _diagnostic(
        delivery, seal="f" * 32 if contradiction == "success" else None
    )
    payload = diagnostic.model_dump(mode="json")
    if contradiction == "seal":
        payload["seal_digest"] = "f" * 32
    elif contradiction == "outcome":
        payload["outcome"] = "infrastructure"
    elif contradiction == "cleanup":
        payload["cleanup"] = "unknown"
    publish_delivery_diagnostics(
        DeliveryDiagnostics.model_validate(payload),
        delta_root=committer.delta_root,
        fs=committer.fs,
    )
    with pytest.raises(StoreIntegrityError, match="contradicts"):
        retained_commands(committer, delivery.delivery_id)
    with pytest.raises(StoreIntegrityError, match="contradicts"):
        retained_failures(committer, pipeline_run_id="synthetic-pipeline")
    source = read_execution_source(committer, delivery.delivery_id)
    assert isinstance(source, FailedScientificExecutionSource)
    assert source.unit.error == "required failure"


def test_accepted_seal_may_have_later_infrastructure_diagnostics(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    receipt = _accepted(committer, tmp_path)
    payload = _diagnostic(
        receipt.delivery, seal=receipt.seal_digest, invocations=(_observed("child"),)
    ).model_dump(mode="json")
    # Session file cleanup can fail after the result seal was already emitted.
    payload.update(outcome="infrastructure", error="post-seal file cleanup failed")
    publish_delivery_diagnostics(
        DeliveryDiagnostics.model_validate(payload),
        delta_root=committer.delta_root,
        fs=committer.fs,
    )
    commands = retained_commands(committer, receipt.delivery.delivery_id)
    assert commands.status == "complete"
    assert commands.invocations[0].invocation_id == "child"
    assert retained_failures(committer, pipeline_run_id="synthetic-pipeline").is_empty()
