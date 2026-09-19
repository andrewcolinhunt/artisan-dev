"""Preflight and coordinate one recorded unit through the ordinary lifecycle."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

import polars as pl
from pydantic import ValidationError

from artisan.errors import (
    ArtifactIntegrityError,
    ArtisanError,
    CommitError,
    ErrorCode,
    StoreIntegrityError,
)
from artisan.execution.executors.curator import is_curator_operation
from artisan.execution.inputs.instantiation import instantiate_inputs
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording.commands import CommandRecorder
from artisan.execution.recording.replay_snapshot import (
    build_replay_snapshot,
    operation_behavior,
    runtime_roots,
    set_pointer,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.pipeline_manager import PipelineManager
from artisan.orchestration.runners import resolve_runner
from artisan.orchestration.runners.base import RunnerBase
from artisan.registry.resolve import operation_identity, resolve_operation
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.replay import ReplayDiagnostic, ReplaySnapshot
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.schemas.orchestration.replay_result import ReplayResult
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.committed_scan import read_committed
from artisan.utils.hashing import (
    compute_execution_spec_id,
    effective_config_payload,
    serialize_params,
)
from artisan.utils.path import uri_join


def _error(code: str, message: str) -> ArtisanError:
    return ArtisanError(
        code, message, error_type="validation", recovery_hint="CHECK_INPUT"
    )


def _read_executions(runtime: RuntimeEnvironment) -> pl.DataFrame:
    return read_committed(
        runtime.delta_root,
        TablePath.EXECUTIONS,
        fs=runtime.storage.filesystem(),
        storage_options=runtime.storage.delta_storage_options(),
    )


def _source_snapshot(
    execution_run_id: str, runtime: RuntimeEnvironment
) -> ReplaySnapshot:
    rows = _read_executions(runtime).filter(
        pl.col("execution_run_id") == execution_run_id
    )
    if rows.is_empty():
        steps = read_committed(
            runtime.delta_root,
            TablePath.STEPS,
            fs=runtime.storage.filesystem(),
            storage_options=runtime.storage.delta_storage_options(),
        )
        wrong_identifier = steps.filter(
            (pl.col("step_run_id") == execution_run_id)
            | (pl.col("pipeline_run_id") == execution_run_id)
        )
        message = (
            "A step or pipeline ID is not an execution-unit ID"
            if not wrong_identifier.is_empty()
            else "Unknown committed execution ID"
        )
        raise _error(ErrorCode.REPLAY_EXECUTION_NOT_FOUND, message)
    if rows.height != 1:
        msg = "Duplicate committed execution ID"
        raise StoreIntegrityError(msg)
    try:
        snapshot = ReplaySnapshot.model_validate_json(rows["replay_snapshot"][0])
    except (ValueError, TypeError):
        msg = "Missing or malformed required replay snapshot"
        raise StoreIntegrityError(msg) from None
    if snapshot.status == "unavailable":
        raise _error(
            ErrorCode.REPLAY_EVIDENCE_UNAVAILABLE,
            f"Replay evidence unavailable: {snapshot.unavailable_reason}",
        )
    if (
        snapshot.source is None
        or snapshot.source.execution_spec_id != rows["execution_spec_id"][0]
    ):
        msg = "Replay source spec disagrees with execution row"
        raise StoreIntegrityError(msg)
    row = rows.row(0, named=True)
    if (
        snapshot.source.step_number != row["origin_step_number"]
        or snapshot.source.step_run_id != row["step_run_id"]
        or snapshot.source.runner != row["compute_backend"]
        or snapshot.operation is None
        or snapshot.operation.identity.name != row["operation_name"]
    ):
        msg = "Replay source owner disagrees with execution row"
        raise StoreIntegrityError(msg)
    return snapshot


def _restore_operation(
    snapshot: ReplaySnapshot,
    operation_class: type[OperationDefinition] | None,
    replacement_env: dict[str, str],
    allow_code_change: bool,
) -> tuple[OperationDefinition, CommandRecorder]:
    assert snapshot.operation is not None
    evidence = snapshot.operation
    source = evidence.identity
    if operation_class is None:
        try:
            operation_class = resolve_operation(f"{source.module}:{source.qualname}")
        except (ImportError, AttributeError, TypeError, ValueError):
            raise _error(
                ErrorCode.REPLAY_DEPENDENCY_UNAVAILABLE,
                "Operation class cannot be imported; install it or supply operation_class",
            ) from None
    operation_value: object = operation_class
    if not isinstance(operation_value, type) or not issubclass(
        operation_value, OperationDefinition
    ):
        raise _error(
            ErrorCode.REPLAY_DEPENDENCY_UNAVAILABLE,
            "operation_class must be an OperationDefinition subclass",
        )
    selected = operation_identity(operation_class)
    if (selected.name, selected.version) != (source.name, source.version):
        raise _error(
            ErrorCode.REPLAY_CODE_CHANGED, "Operation name/version differs from source"
        )
    if (selected != source or source.module_digest is None) and not allow_code_change:
        raise _error(
            ErrorCode.REPLAY_CODE_CHANGED,
            "Operation code changed or cannot be verified; explicitly allow_code_change",
        )
    required = {slot.pointer for slot in snapshot.required_replacements}
    if set(replacement_env) != required:
        raise _error(
            ErrorCode.REPLAY_REPLACEMENT_REQUIRED,
            f"Supply exactly the recorded replacement pointers: {sorted(required)}",
        )
    payload = json.loads(json.dumps(evidence.configuration))
    redactor = CommandRecorder()
    for pointer, variable in replacement_env.items():
        if variable not in os.environ:
            raise _error(
                ErrorCode.REPLAY_REPLACEMENT_REQUIRED,
                f"Replacement environment variable is unset: {variable}",
            )
        raw = os.environ[variable]
        redactor.add_environment({variable: raw})
        try:
            value = json.loads(raw)
        except ValueError:
            value = raw
        redactor.add_sensitive_data(value)
        set_pointer(payload, pointer, value)
    try:
        operation = operation_class.model_validate(payload)
    except (ValidationError, TypeError, ValueError):
        raise _error(
            ErrorCode.REPLAY_CONFIGURATION_INVALID,
            "Restored configuration does not validate against the concrete operation",
        ) from None
    if not _same_structure(operation.model_dump(mode="json", round_trip=True), payload):
        raise _error(
            ErrorCode.REPLAY_CONFIGURATION_INVALID,
            "Concrete configuration fields changed; defaults cannot be silently added",
        )
    if operation_behavior(operation) != evidence.behavior and not allow_code_change:
        raise _error(
            ErrorCode.REPLAY_CODE_CHANGED,
            "Operation input/output specifications or class behavior changed",
        )
    redactor.add_operation(operation)
    return operation, redactor


def _same_structure(current: Any, recorded: Any) -> bool:
    """Reject new nested defaults and removed fields before creating an attempt."""
    if isinstance(current, dict) and isinstance(recorded, dict):
        return current.keys() == recorded.keys() and all(
            _same_structure(current[key], recorded[key]) for key in current
        )
    if isinstance(current, list) and isinstance(recorded, list):
        return len(current) == len(recorded) and all(
            _same_structure(a, b) for a, b in zip(current, recorded, strict=True)
        )
    return not isinstance(current, (dict, list)) and not isinstance(
        recorded, (dict, list)
    )


def _verify_artifacts(
    snapshot: ReplaySnapshot,
    operation: OperationDefinition,
    runtime: RuntimeEnvironment,
) -> None:
    assert snapshot.source is not None
    store = ArtifactStore(
        runtime.delta_root,
        fs=runtime.storage.filesystem(),
        storage_options=runtime.storage.delta_storage_options(),
        files_root=snapshot.source.roots.get("files_root"),
    )
    if (
        any(spec.with_associated for spec in operation.inputs.values())
        and not snapshot.associated_complete
    ):
        raise _error(
            ErrorCode.REPLAY_EVIDENCE_UNAVAILABLE,
            "Associated input capture did not complete",
        )
    identities = [
        (entry.artifact_id, entry.artifact_type)
        for entries in snapshot.inputs.values()
        for entry in entries
    ]
    identities.extend(
        (aid, item.artifact_type)
        for item in snapshot.associated
        for aid in item.artifact_ids
    )
    checked: set[str] = set()
    for artifact_id, artifact_type in identities:
        artifact = store.get_artifact(artifact_id, artifact_type=artifact_type)
        if artifact is None:
            msg = f"Recorded artifact {artifact_id} is missing"
            raise ArtifactIntegrityError(msg)
        if isinstance(artifact, ExecutionConfigArtifact):
            for reference in artifact.get_artifact_references():
                if reference not in checked and store.get_artifact(reference) is None:
                    msg = f"Configuration reference {reference} is missing"
                    raise ArtifactIntegrityError(msg)
                checked.add(reference)
        checked.add(artifact_id)
    instantiate_inputs(
        {
            role: [entry.artifact_id for entry in entries]
            for role, entries in snapshot.inputs.items()
        },
        store,
        operation.inputs,
        operation.hydrate_inputs,
        recorded_associated=snapshot.associated,
    )


def _diagnostic_runtime(
    runtime: RuntimeEnvironment, snapshot: ReplaySnapshot
) -> RuntimeEnvironment:
    assert snapshot.source is not None
    if (
        runtime.working_root is None
        or runtime.failure_logs_root is None
        or runtime.files_root is None
    ):
        raise _error(
            ErrorCode.REPLAY_CONFIGURATION_INVALID,
            "Replay requires working, failure_logs, staging and files roots",
        )
    roots = runtime_roots(runtime)
    for key in ("working_root", "failure_logs_root"):
        if "://" in (roots[key] or ""):
            raise _error(
                ErrorCode.REPLAY_CONFIGURATION_INVALID, f"{key} must be a local path"
            )
    for key in ("delta_root", "staging_root", "files_root"):
        protocol = urlsplit(roots[key]).scheme or "file"
        if protocol != runtime.storage.protocol:
            raise _error(
                ErrorCode.REPLAY_CONFIGURATION_INVALID,
                f"{key} must use the store storage protocol",
            )
    child = uuid4().hex
    selected: dict[str, str] = {}
    forbidden = [runtime.delta_root]
    original_sandbox = snapshot.source.roots.get("sandbox_path")
    if original_sandbox:
        forbidden.append(original_sandbox)
    for key, root in roots.items():
        if key == "delta_root" or root is None:
            continue
        target = uri_join(root, child)
        canonical = (
            str(Path(target).resolve()) if "://" not in target else target.rstrip("/")
        )
        for original in forbidden:
            source = (
                str(Path(original).resolve())
                if "://" not in original
                else original.rstrip("/")
            )
            if (
                canonical == source
                or canonical.startswith(source + "/")
                or source.startswith(canonical + "/")
            ):
                raise _error(
                    ErrorCode.REPLAY_CONFIGURATION_INVALID,
                    "Diagnostic roots overlap source storage or working roots",
                )
        selected[key] = target
    return runtime.model_copy(
        update={**selected, "preserve_working": True, "preserve_staging": True}
    )


def replay_execution(
    execution_run_id: str,
    *,
    runtime: RuntimeEnvironment,
    step_runner: str | RunnerBase | None = None,
    operation_class: type[OperationDefinition] | None = None,
    replacement_env: dict[str, str] | None = None,
    allow_code_change: bool = False,
) -> ReplayResult:
    """Replay exactly one committed unit with fresh IDs and preserved diagnostics.

    Args:
        execution_run_id: Opaque committed execution identifier.
        runtime: Source store and fresh diagnostic destination roots.
        step_runner: Configured lifecycle runner, or explicit local override.
        operation_class: Explicit class for notebook/local definitions.
        replacement_env: Recorded JSON pointers mapped to caller environment names.
        allow_code_change: Permit the detected driver/source code difference.

    Returns:
        Actual committed outcome, nullable execution identity, and evidence paths.
    """
    snapshot = _source_snapshot(execution_run_id, runtime)
    operation, redactor = _restore_operation(
        snapshot, operation_class, replacement_env or {}, allow_code_change
    )
    _verify_artifacts(snapshot, operation, runtime)
    assert snapshot.source is not None
    assert snapshot.operation is not None
    if step_runner is None and snapshot.source.runner != "local":
        raise _error(
            ErrorCode.REPLAY_DEPENDENCY_UNAVAILABLE,
            "Supply the original configured external runner or explicitly select local",
        )
    try:
        runner = resolve_runner(step_runner or "local")
        if is_curator_operation(operation) and runner.name != "local":
            msg = "Curators require the local lifecycle runner"
            raise ValueError(msg)
        runner.validate_operation(operation)
    except (TypeError, ValueError, RuntimeError) as exc:
        raise _error(
            ErrorCode.REPLAY_DEPENDENCY_UNAVAILABLE, redactor.sanitize(str(exc))
        ) from None
    diagnostic_runtime = _diagnostic_runtime(runtime, snapshot).model_copy(
        update={
            "compute_backend_name": runner.name,
            "worker_id_env_var": runner.worker_traits.worker_id_env_var,
            "shared_filesystem": runner.worker_traits.shared_filesystem,
        }
    )
    spec = compute_execution_spec_id(
        operation.name,
        snapshot.inputs,
        serialize_params(operation),
        effective_config_payload(operation),
    )
    if (
        not replacement_env
        and not allow_code_change
        and spec != snapshot.source.execution_spec_id
    ):
        msg = "Restored operation does not reproduce the recorded execution spec"
        raise StoreIntegrityError(msg)
    notes = [
        "Explicit inputs and framework associations are frozen; operation reads see current committed store, files, environment and network state."
    ]
    if runner.name != snapshot.source.runner:
        notes.append(
            f"Lifecycle runner changed from {snapshot.source.runner} to {runner.name}."
        )
    diagnostic = ReplayDiagnostic(
        source_execution_run_id=execution_run_id,
        source_execution_spec_id=snapshot.source.execution_spec_id,
        source_identity=snapshot.operation.identity,
        selected_identity=operation_identity(type(operation)),
        source_remote_identity=snapshot.remote_identity,
        allow_code_change=allow_code_change,
        runner=runner.name,
        roots=runtime_roots(diagnostic_runtime),
        reproducibility_notes=notes,
    )
    unit = ExecutionUnit(
        operation=operation,
        inputs={
            role: [entry.artifact_id for entry in entries]
            for role, entries in snapshot.inputs.items()
        },
        execution_spec_id=spec,
        step_number=snapshot.source.step_number,
        group_ids=snapshot.group_ids,
        replay_of_execution_run_id=execution_run_id,
        replay_sensitive_values=redactor.sensitive_values,
    )
    fresh = build_replay_snapshot(unit, diagnostic_runtime, snapshot.inputs)
    unit.replay_snapshot = fresh.model_copy(
        update={
            "associated": snapshot.associated,
            "associated_complete": snapshot.associated_complete,
            "diagnostic": diagnostic,
        }
    )
    config = PipelineConfig(
        name=f"replay-{execution_run_id[:12]}",
        pipeline_run_id=f"replay-{uuid4().hex}",
        delta_root=runtime.delta_root,
        staging_root=diagnostic_runtime.staging_root,
        working_root=diagnostic_runtime.working_root or "",
        files_root=diagnostic_runtime.files_root,
        storage=runtime.storage,
        default_step_runner=runner.name,
        skip_cache=True,
        preserve_staging=True,
        preserve_working=True,
    )
    manager = PipelineManager(config, default_step_runner=runner)
    try:
        with manager._log_context():
            logging.getLogger(__name__).info("Replay read semantics: %s", notes[0])
        result = manager._run_prepared_unit(unit, diagnostic_runtime, redactor=redactor)
    except ArtisanError as exc:
        clean = redactor.sanitize(str(exc))
        exc.args = (clean,)
        exc.envelope = exc.envelope.model_copy(
            update={
                "message": clean,
                "hint": f"Diagnostic run {config.pipeline_run_id}; roots: {redactor.sanitize_data(runtime_roots(diagnostic_runtime))}",
            }
        )
        if isinstance(exc, CommitError):
            exc.plan_key = redactor.sanitize(exc.plan_key)
            exc.staging_objects = tuple(
                redactor.sanitize(path) for path in exc.staging_objects
            )
        exc.__cause__ = None
        exc.__suppress_context__ = True
        raise
    finally:
        manager.finalize()
    rows = _read_executions(runtime).filter(pl.col("step_run_id") == result.step_run_id)
    committed_id = None
    if rows.height > 1:
        msg = "Single-unit replay committed multiple execution rows"
        raise StoreIntegrityError(msg)
    if rows.height:
        committed_id = rows["execution_run_id"][0]
        committed = ReplaySnapshot.model_validate_json(rows["replay_snapshot"][0])
        if committed.diagnostic is None:
            msg = "Diagnostic execution lost its replay context"
            raise StoreIntegrityError(msg)
        diagnostic = committed.diagnostic
    assert result.step_run_id is not None
    return ReplayResult(
        source_execution_run_id=execution_run_id,
        pipeline_run_id=config.pipeline_run_id,
        step_run_id=result.step_run_id,
        execution_run_id=committed_id,
        step_result=result,
        diagnostic_roots=diagnostic.roots,
        diagnostic_status=diagnostic.status if committed_id else "unavailable",
        diagnostic_errors=diagnostic.errors,
        reproducibility_notes=notes,
    )
