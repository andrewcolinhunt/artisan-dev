"""Private retained diagnostic projections; optional observations grant no authority."""

from __future__ import annotations

from collections.abc import Mapping

import polars as pl
from pydantic import ValidationError

from artisan.errors import ArtisanErrorEnvelope, StoreIntegrityError
from artisan.execution.recording.delivery_diagnostics import read_delivery_diagnostics
from artisan.inspection.models import (
    ExecutionCommands,
    FailureRow,
    InvocationCommands,
    _command_coverage,
)
from artisan.orchestration.engine.retained_execution_query import (
    ExecutionMembership,
    _execution_membership,
)
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.delivery_diagnostics import DeliveryDiagnostics
from artisan.storage.core.execution_source import (
    CuratorExecutionSource,
    ExecutionSource,
    FailedScientificExecutionSource,
    ScientificExecutionSource,
    read_execution_source,
)
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.utils.log_paths import failure_log_relative_path
from artisan.utils.path import uri_join, uri_parent
from artisan.visualization.inspect import _FAILURES_SCHEMA


def retained_failures(
    committer: RetainedCommitter, *, pipeline_run_id: str | None = None
) -> pl.DataFrame:
    """Preserve selected outer failures even when optional observations are absent.

    Current step and ordered unit membership come from the completed selection.
    Source delivery identities survive reuse. Missing execution timestamps/logs
    remain null, and infrastructure-only step failures acquire no execution row.
    """
    if pipeline_run_id is not None and not pipeline_run_id.strip():
        msg = "Failure inspection needs an explicit nonempty run identity or None"
        raise ValueError(msg)
    with committer._read_snapshot() as snapshot:
        tracker = RetainedStepTracker(committer, "")
        _, attempts = tracker._read_lifecycle(snapshot)
        states = (
            tracker._select_current_states(attempts, pipeline_run_id)
            if pipeline_run_id is not None
            else [
                state for state in attempts if state.replay_of_execution_run_id is None
            ]
        )
        members = _execution_membership(snapshot, states)
    return _failure_rows(committer, members, original_origins=pipeline_run_id is None)


def _failure_rows(
    committer: RetainedCommitter,
    members: tuple[ExecutionMembership, ...],
    *,
    original_origins: bool = False,
) -> pl.DataFrame:
    """Project already selected failures without reopening their source authority."""
    rows: list[FailureRow] = []
    origins: dict[str, int] = {}
    for member in members:
        previous_count = len(rows)
        source = member.source
        if isinstance(source, FailedScientificExecutionSource):
            diagnostic = _scientific_diagnostics(committer, source)
            envelope = (
                ArtisanErrorEnvelope.model_validate(diagnostic.error_envelope)
                if diagnostic is not None and diagnostic.error_envelope is not None
                else None
            )
            assert source.unit.failed_delivery is not None
            log = None
            if diagnostic is not None:
                relative = failure_log_relative_path(
                    source.unit.failed_delivery.delivery_id, diagnostic.timestamp_start
                )
                path = uri_join(
                    uri_parent(committer.delta_root), "logs", "failures", relative
                )
                if committer.fs.exists(path):
                    log = relative
            rows.append(
                FailureRow(
                    step=member.state.step_number,
                    operation=source.admission.recipe.definition.name,
                    execution_run_id=source.unit.failed_delivery.delivery_id,
                    timestamp_start=diagnostic.timestamp_start
                    if diagnostic is not None
                    else None,
                    code=envelope.code if envelope else None,
                    recovery_hint=envelope.recovery_hint if envelope else None,
                    field=envelope.field if envelope else None,
                    suggestions=envelope.suggestions if envelope else None,
                    error=source.unit.error,
                    log=log,
                )
            )
        elif (
            isinstance(source, CuratorExecutionSource)
            and not source.execution_record["success"]
        ):
            rows.append(_curator_failure(committer, member, source.execution_record))
        if len(rows) != previous_count:
            origin = (
                source.admission.record.step_number
                if isinstance(source, CuratorExecutionSource)
                else source.admission.step_number
            )
            origins[rows[-1].execution_run_id] = origin
    if original_origins:
        rows = list(
            {
                row.execution_run_id: row.model_copy(
                    update={"step": origins[row.execution_run_id]}
                )
                for row in rows
            }.values()
        )
    return pl.DataFrame(
        [row.model_dump() for row in rows], schema=_FAILURES_SCHEMA
    ).sort("timestamp_start", "execution_run_id", "step")


def retained_commands(
    committer: RetainedCommitter, execution_run_id: str
) -> ExecutionCommands:
    """Read original command groups without rewriting their local sequence numbers."""
    source = read_execution_source(committer, execution_run_id)
    if source is None:
        msg = f"Completed execution {execution_run_id!r} not found"
        raise FileNotFoundError(msg)
    diagnostic = (
        None
        if isinstance(source, CuratorExecutionSource)
        else _scientific_diagnostics(committer, source)
    )
    return _execution_commands(execution_run_id, source, diagnostic)


def _execution_commands(
    execution_run_id: str,
    source: ExecutionSource,
    diagnostic: DeliveryDiagnostics | None,
) -> ExecutionCommands:
    """Project already selected evidence without reopening source authority."""
    recording = None
    invocations: tuple[InvocationCommands, ...] = ()
    if isinstance(source, CuratorExecutionSource):
        recording = _curator_commands(source.execution_record["command_recording"])
        omitted: int | None = 0
    else:
        omitted = diagnostic.omitted_invocations if diagnostic is not None else None
        if diagnostic is not None:
            invocations = tuple(
                InvocationCommands(
                    invocation_id=item.invocation_id,
                    call_path=item.call_path,
                    operation_name=item.operation_name,
                    operation_version=item.operation_version,
                    recording=item.command_recording,
                )
                for item in diagnostic.invocations
            )
    return ExecutionCommands(
        execution_run_id=execution_run_id,
        status=_command_coverage(recording, invocations, omitted),
        recording=recording,
        invocations=invocations,
        omitted_invocations=omitted,
    )


def _scientific_diagnostics(
    committer: RetainedCommitter,
    source: ScientificExecutionSource | FailedScientificExecutionSource,
) -> DeliveryDiagnostics | None:
    if isinstance(source, ScientificExecutionSource):
        delivery = source.plan.receipt.delivery
        seal = source.plan.receipt.seal_digest
    else:
        assert source.unit.failed_delivery is not None
        delivery = source.unit.failed_delivery
        seal = None
    diagnostic = read_delivery_diagnostics(
        delivery,
        delta_root=committer.delta_root,
        fs=committer.fs,
        expected_seal_digest=seal,
    )
    if diagnostic is None or isinstance(source, ScientificExecutionSource):
        return diagnostic
    if (
        diagnostic.seal_digest is not None
        or diagnostic.outcome != "science"
        or diagnostic.cleanup != "closed"
    ):
        msg = "Diagnostic outcome contradicts its selected closed scientific failure"
        raise StoreIntegrityError(msg)
    return diagnostic


def _curator_commands(encoded: object) -> CommandRecording:
    if not isinstance(encoded, str):
        msg = "Curator command evidence is not encoded JSON"
        raise StoreIntegrityError(msg)
    try:
        return CommandRecording.model_validate_json(encoded)
    except ValidationError as error:
        msg = "Curator command evidence is malformed"
        raise StoreIntegrityError(msg) from error


def _curator_failure(
    committer: RetainedCommitter,
    member: ExecutionMembership,
    row: Mapping[str, object],
) -> FailureRow:
    try:
        encoded = row["error_envelope"]
        if encoded is not None and not isinstance(encoded, str):
            msg = "Curator failure envelope is not encoded JSON"
            raise StoreIntegrityError(msg)
        envelope = (
            ArtisanErrorEnvelope.model_validate_json(encoded)
            if encoded is not None
            else None
        )
        result = FailureRow.model_validate(
            {
                "step": member.state.step_number,
                "operation": row["operation_name"],
                "execution_run_id": row["execution_run_id"],
                "timestamp_start": row["timestamp_start"],
                "code": envelope.code if envelope else None,
                "recovery_hint": envelope.recovery_hint if envelope else None,
                "field": envelope.field if envelope else None,
                "suggestions": envelope.suggestions if envelope else None,
                "error": row["error"],
                "log": None,
            }
        )
    except ValidationError as error:
        msg = "Curator failure diagnostics are malformed"
        raise StoreIntegrityError(msg) from error
    if result.timestamp_start is not None:
        relative = failure_log_relative_path(
            result.execution_run_id, result.timestamp_start
        )
        path = uri_join(uri_parent(committer.delta_root), "logs", "failures", relative)
        if committer.fs.exists(path):
            result.log = relative
    return result
