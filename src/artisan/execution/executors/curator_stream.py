"""Consume curator artifact streams inside their execution and recording scope."""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from typing import Any, cast

from artisan.execution.exceptions import ArtifactValidationError
from artisan.execution.lineage.builder import build_edges
from artisan.execution.lineage.enrich import (
    build_artifact_edges_from_types,
    require_artifact_type,
)
from artisan.execution.lineage.validation import (
    validate_artifacts_match_specs,
    validate_lineage_completeness,
    validate_lineage_integrity,
)
from artisan.execution.recording.commands import command_snapshot, sanitize_diagnostic
from artisan.execution.recording.parquet_writer import (
    StagingResult,
    _write_execution_record,
)
from artisan.execution.recording.recorder import (
    error_envelope_dict,
    execution_is_sealed,
    record_execution_failure,
)
from artisan.execution.recording.replay_snapshot import replay_recording_fields
from artisan.execution.recording.stream_recorder import StreamRecorder
from artisan.schemas.execution.curator_result import (
    ArtifactEmission,
    ArtifactResult,
    ArtifactStreamResult,
)
from artisan.schemas.execution.execution_context import ExecutionContext
from artisan.utils.command_supervisor import check_command_scope, close_command_scope
from artisan.utils.json import artisan_json_default
from artisan.utils.traceback import format_error

logger = logging.getLogger(__name__)
_RESERVED_METADATA = {"timings", "skip_reason", "status", "cancelled", "skipped"}


def _snapshot_metadata(result: ArtifactStreamResult) -> dict[str, Any]:
    if _RESERVED_METADATA.intersection(result.metadata):
        msg = "Curator metadata uses reserved framework keys"
        raise ValueError(msg)
    return cast(
        dict[str, Any],
        json.loads(
            json.dumps(result.metadata, default=artisan_json_default, allow_nan=False)
        ),
    )


def _validate_contract(context: ExecutionContext) -> None:
    outputs = context.operation.outputs
    if not outputs:
        msg = "Artifact streams require static output roles"
        raise ArtifactValidationError(msg)
    for role, spec in outputs.items():
        if spec.derives_from is None or "inputs" not in spec.derives_from:
            msg = f"Stream output {role!r} requires explicit input-parent lineage"
            raise ArtifactValidationError(msg)


def _stage_emission(
    value: object,
    session: StreamRecorder,
    inputs: dict[str, list[str]],
    input_sets: dict[str, set[str]],
    artifact_types: dict[str, str],
) -> None:
    if not isinstance(value, ArtifactEmission):
        msg = "Artifact streams must yield ArtifactEmission values"
        raise TypeError(msg)
    emission = ArtifactEmission.model_validate(value)
    if emission.artifact.artifact_id is not None:
        msg = "Artifact streams must emit draft artifacts"
        raise ArtifactValidationError(msg)
    result = ArtifactResult()
    result.add_artifact(
        emission.role,
        emission.artifact,
        sources={role: list(ids) for role, ids in emission.sources.items()},
    )
    specs = session.context.operation.outputs
    validate_artifacts_match_specs(result.artifacts, specs, check_required_roles=False)
    validate_lineage_integrity(
        result.lineage, inputs, result.artifacts, specs, prepared_input_ids=input_sets
    )
    validate_lineage_completeness(result.artifacts, specs, result.lineage)
    for ids in emission.sources.values():
        for source_id in ids:
            require_artifact_type(source_id, artifact_types)
    finalized = emission.artifact.finalize()
    assert finalized.artifact_id is not None
    previous_type = artifact_types.get(finalized.artifact_id)
    try:
        artifact_types[finalized.artifact_id] = finalized.artifact_type
        pairs = build_edges(
            result.lineage, {emission.role: [finalized]}, artifact_types
        )
        edges = build_artifact_edges_from_types(
            pairs, session.context.execution_run_id, artifact_types
        )
    finally:
        if previous_type is None:
            artifact_types.pop(finalized.artifact_id)
        else:
            artifact_types[finalized.artifact_id] = previous_type
    session.append(emission.role, finalized, edges)


def _close_iterator(result: ArtifactStreamResult) -> None:
    close = getattr(result.items, "close", None)
    if close is not None:
        close()


def handle_artifact_stream(
    result: ArtifactStreamResult,
    context: ExecutionContext,
    inputs: dict[str, list[str]],
    timings: dict[str, Any],
    execute_start: float,
    total_start: float,
    params: dict[str, Any],
    user_overrides: dict[str, Any] | None,
    failure_logs_root: str | None,
) -> StagingResult:
    """Consume and seal one complete stream, containing every failure fallback."""
    session = None
    iterator_closed = False
    try:
        session = StreamRecorder(context, inputs)
        metadata = _snapshot_metadata(result)
        _validate_contract(context)
        input_sets = {role: set(ids) for role, ids in inputs.items()}
        source_ids = sorted({aid for ids in input_sets.values() for aid in ids})
        types = (
            context.artifact_store.provenance.load_type_map(source_ids)
            if source_ids
            else {}
        )
        while True:
            check_command_scope()
            try:
                emission = next(result.items)
            except StopIteration:
                break
            _stage_emission(emission, session, inputs, input_sets, types)
            del emission
        iterator_closed = True
        _close_iterator(result)
        for role, spec in context.operation.outputs.items():
            if spec.required and not session.output_ids.get(role):
                msg = f"Missing required output role: {role}"
                raise ArtifactValidationError(msg)
        close_command_scope()
        timings["execute"] = round(time.perf_counter() - execute_start, 4)
        record_start = time.perf_counter()
        session.prepare()
        session.promote()
        timings["record"] = round(time.perf_counter() - record_start, 4)
        timings["total"] = round(time.perf_counter() - total_start, 4)
        _write_execution_record(
            **replay_recording_fields(),
            command_recording=command_snapshot(),
            execution_run_id=context.execution_run_id,
            execution_spec_id=context.execution_spec_id,
            operation_name=context.operation_name,
            operation=context.operation,
            step_number=context.step_number,
            success=True,
            error=None,
            timestamp_start=context.timestamp_start,
            timestamp_end=datetime.now(UTC),
            worker_id=context.worker_id,
            staging_path=session.staging_path,
            fs=context.fs,
            params=params,
            compute_backend=context.compute_backend,
            result_metadata={**metadata, "timings": dict(timings)},
            user_overrides=user_overrides,
            step_run_id=context.step_run_id,
        )
        if not execution_is_sealed(context):
            msg = "Stream success seal was not confirmed"
            raise RuntimeError(msg)
    except Exception as exc:
        error = format_error(exc)
        if not iterator_closed:
            try:
                _close_iterator(result)
            except Exception as cleanup:
                error += f" | Iterator close failed: {format_error(cleanup)}"
        try:
            close_command_scope(exc)
        except Exception as cleanup:
            error += f" | Command scope close failed: {format_error(cleanup)}"
        if session is not None and not session.promotion_started:
            try:
                session.close()
            except Exception as cleanup:
                error += f" | Stream staging close failed: {format_error(cleanup)}"
            try:
                failure = record_execution_failure(
                    **replay_recording_fields(),
                    command_recording=command_snapshot(),
                    execution_context=context,
                    error=error,
                    inputs=inputs,
                    timestamp_end=datetime.now(UTC),
                    params=params,
                    user_overrides=user_overrides,
                    failure_logs_root=failure_logs_root,
                    error_envelope=error_envelope_dict(exc),
                )
                if failure.staging_path and execution_is_sealed(context):
                    return failure
                error = failure.error or error
            except Exception as cleanup:
                error += f" | Failure recording failed: {format_error(cleanup)}"
        error = sanitize_diagnostic(
            f"{error} | Original stream shard: {context.execution_run_id}"
        )
        return StagingResult(success=False, error=error, execution_run_id=None)
    try:
        session.cleanup()
    except Exception:
        logger.debug("Stream temporary cleanup failed after sealing", exc_info=True)
    return StagingResult(
        success=True,
        staging_path=session.staging_path,
        execution_run_id=context.execution_run_id,
        artifact_ids=[aid for ids in session.output_ids.values() for aid in ids],
    )
