"""Replay a recorded outer boundary through the ordinary pipeline controller."""

from __future__ import annotations

import posixpath
from collections.abc import Mapping, Sequence
from pathlib import Path
from tempfile import gettempdir
from typing import Literal
from uuid import uuid4

from fsspec.utils import get_protocol
from pydantic import JsonValue

from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.errors import ArtisanError, ErrorCode, StoreIntegrityError
from artisan.execution.assignment_transport import AssignmentDispatch
from artisan.execution.recording.retained_replay import read_replay_diagnostic
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.curator_replay import (
    curator_replay_record,
    prepare_curator_replay,
)
from artisan.orchestration.engine.retained_curator_executor import (
    RetainedCuratorExecution,
)
from artisan.orchestration.engine.retained_execution_query import (
    read_retained_execution_membership,
)
from artisan.orchestration.engine.retained_executor import RetainedStepExecution
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.pipeline_manager import PipelineManager, PipelineRunner
from artisan.orchestration.retained_replay import Definition, prepare_retained_replay
from artisan.orchestration.runners.local import LocalRunner
from artisan.schemas.artifact.external import validate_persistable_uri
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.replay_result import ReplayResult
from artisan.storage.core.execution_source import (
    CuratorExecutionSource,
    ExecutionSource,
    FailedScientificExecutionSource,
    ScientificExecutionSource,
    read_execution_source,
)
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_store import open_configured_store
from artisan.utils.path import uri_join, uri_parent

_unresolved_replays: list[PipelineManager] = []


def replay_execution(
    execution_run_id: str,
    *,
    delta_root: str,
    operation: Definition,
    definitions: Sequence[Definition] = (),
    application_modules: Sequence[str] = (),
    files_root: str | None = None,
    storage: StorageConfig | None = None,
    step_runner: PipelineRunner | None = None,
    runtime_replacements: Mapping[str, str] | None = None,
    configuration_replacements: Mapping[str, JsonValue] | None = None,
    allow_code_change: bool = False,
    working_root: Path | None = None,
    staging_root: str | None = None,
    runner_resources: RunnerResources | None = None,
) -> ReplayResult:
    """Rerun one captured operation or composite with fresh diagnostic ownership.

    Explicit inputs and their associations are frozen. Authored reads of stores,
    files, environment and network observe their current state. Replay bypasses
    ordinary caches and its outputs never become ordinary cache sources.

    Args:
        execution_run_id: Original successful or closed failed execution ID.
        delta_root: Existing supported store, opened without recovery.
        operation: Already imported root operation or composite class.
        definitions: Already imported nested classes used by the captured composite.
        application_modules: Explicit trusted modules that register application
            artifact types on the controller and workers.
        files_root: Managed carrier root; required for nonlocal or custom layouts.
        storage: Storage configuration; defaults to local files.
        step_runner: Configured provider. Omission selects local only for a source
            whose recorded lifecycle provider was local.
        runtime_replacements: Values for exactly the captured runtime keys.
        configuration_replacements: Curator configuration values keyed by the
            captured JSON pointers. Scientific parameters come from the recipe.
        allow_code_change: Explicitly allow different code or resolved software.
        working_root: Absolute parent for fresh, preserved worker diagnostics.
        staging_root: Parent for fresh, preserved staging on the store backend.
        runner_resources: Resources for this diagnostic execution.

    Returns:
        The actual terminal result and persisted diagnostic coverage. An execution
        ID remains available when optional diagnostic evidence is missing.

    Raises:
        ArtisanError: Source, trusted definitions or replacements fail preflight.
        StoreIntegrityError: Required store evidence is inconsistent.
        ValueError: Root configuration or definition selection is invalid.
    """
    from artisan.registry.discovery import bootstrap_applications
    from artisan.utils.application_modules import validate_application_modules

    registered = _registered_definitions(operation, definitions)
    modules = validate_application_modules(application_modules)
    bootstrap_applications(modules, purpose="replay")
    storage = StorageConfig() if storage is None else storage
    committer = open_configured_store(
        delta_root, files_root=files_root, storage=storage
    )
    source = read_execution_source(committer, execution_run_id)
    if source is None:
        raise _error(
            ErrorCode.REPLAY_EXECUTION_NOT_FOUND, "Unknown completed execution ID"
        )
    runner = _select_runner(committer, source, step_runner)
    dispatch = _diagnostic_dispatch(
        committer,
        storage,
        runtime_replacements or {},
        working_root,
        staging_root,
        modules,
    )
    resources = runner_resources or RunnerResources.model_validate({})
    if isinstance(source, CuratorExecutionSource):
        if definitions or not issubclass(operation, OperationDefinition):
            raise _error(
                ErrorCode.REPLAY_DEPENDENCY_UNAVAILABLE,
                "Curator replay requires one explicit operation class",
            )
        replay = prepare_curator_replay(
            committer,
            execution_run_id,
            operation_class=operation,
            configuration_replacements=configuration_replacements or {},
            runtime_replacements=dispatch.runtime_env,
            allow_code_change=allow_code_change,
        )
        execution: RetainedStepExecution | RetainedCuratorExecution = (
            RetainedCuratorExecution(
                curator_replay_record(replay, runner_name=runner.name),
                operation=replay.operation,
                resolve_inputs=None,
                tracker=RetainedStepTracker(committer, f"replay-{uuid4().hex}"),
                runner=runner,
                dispatch=dispatch,
                runner_resources=resources,
                replay=replay,
            )
        )
    else:
        if configuration_replacements:
            raise _error(
                ErrorCode.REPLAY_CONFIGURATION_INVALID,
                "Scientific replay restores parameters from its captured recipe",
            )
        captured = prepare_retained_replay(
            committer,
            execution_run_id,
            registered_definitions=registered,
            runtime_replacements=dispatch.runtime_env,
            allow_code_change=allow_code_change,
        )
        if captured.plan.definition is not operation:
            raise _error(
                ErrorCode.REPLAY_DEPENDENCY_UNAVAILABLE,
                "Supplied root operation differs from the captured root",
            )
        execution = RetainedStepExecution.from_replay(
            captured,
            committer=committer,
            runner=runner,
            dispatch=dispatch,
            runner_resources=resources,
        )
    manager = PipelineManager(
        committer,
        execution.dispatch,
        pipeline_run_id=execution.tracker._pipeline_run_id,
        default_step_runner=runner,
        skip_cache=True,
    )
    try:
        with manager:
            result = manager._submit_execution(execution).result()
    finally:
        if manager.ownership_state() != OwnershipState.CLOSED:
            _unresolved_replays.append(manager)
    members = read_retained_execution_membership(
        committer, pipeline_run_id=manager.config.pipeline_run_id
    )
    if len(members) > 1:
        msg = "Single-boundary replay selected multiple executions"
        raise StoreIntegrityError(msg)
    selected = members[0].source if members else None
    status, errors = _diagnostic_coverage(committer, selected)
    return ReplayResult(
        source_execution_run_id=execution_run_id,
        pipeline_run_id=manager.config.pipeline_run_id,
        step_run_id=execution.record.step_run_id,
        execution_run_id=_execution_id(selected),
        step_result=result,
        diagnostic_roots={
            "delta_root": dispatch.delta_root,
            "files_root": dispatch.files_root,
            "staging_root": dispatch.staging_root,
            "working_root": str(dispatch.working_root),
        },
        diagnostic_status=status,
        diagnostic_errors=errors,
        reproducibility_notes=[
            "Explicit inputs and associations are frozen; authored reads observe "
            "current store, files, environment and network state.",
            "The selected provider and software are resolved for this diagnostic run.",
        ],
    )


def _registered_definitions(
    operation: Definition, definitions: Sequence[Definition]
) -> dict[tuple[str, str], Definition]:
    registered: dict[tuple[str, str], Definition] = {}
    for value in (operation, *definitions):
        definition: object = value
        if not isinstance(definition, type) or not issubclass(
            definition, (OperationDefinition, CompositeDefinition)
        ):
            raise _error(
                ErrorCode.REPLAY_DEPENDENCY_UNAVAILABLE,
                "Replay requires already imported operation or composite classes",
            )
        key = definition.__module__, definition.__qualname__
        if key in registered and registered[key] is not definition:
            msg = "Conflicting replay definition identities"
            raise ValueError(msg)
        registered[key] = definition
    return registered


def _select_runner(
    committer: RetainedCommitter,
    source: ExecutionSource,
    supplied: PipelineRunner | None,
) -> PipelineRunner:
    if supplied is not None:
        if isinstance(supplied, str) or not supplied.name.strip():
            msg = "Replay providers require an explicit configured instance"
            raise ValueError(msg)
        return supplied
    name: str | None
    if isinstance(source, CuratorExecutionSource):
        name = source.admission.record.compute_backend
    else:
        step_id = (
            source.plan.receipt.delivery.attempt.assignment.step_run_id
            if isinstance(source, ScientificExecutionSource)
            else source.plan.step_run_id
        )
        tracker = RetainedStepTracker(committer, "replay-source-inspection")
        with committer._read_snapshot() as snapshot:
            _, attempts = tracker._read_lifecycle(snapshot)
        names = [
            item.compute_backend for item in attempts if item.step_run_id == step_id
        ]
        name = names[0] if len(names) == 1 else None
    if name != "local":
        raise _error(
            ErrorCode.REPLAY_DEPENDENCY_UNAVAILABLE,
            "Supply a configured provider or explicitly select LocalRunner()",
        )
    return LocalRunner()


def _diagnostic_dispatch(
    committer: RetainedCommitter,
    storage: StorageConfig,
    runtime_env: Mapping[str, str],
    working_root: Path | None,
    staging_root: str | None,
    application_modules: tuple[str, ...],
) -> AssignmentDispatch:
    identity = f"replay-{uuid4().hex}"
    parent = (
        working_root if working_root is not None else Path(gettempdir()) / "artisan"
    )
    if not parent.is_absolute():
        msg = "Replay working_root must be absolute"
        raise ValueError(msg)
    working = parent / identity
    staging = uri_join(
        staging_root
        if staging_root is not None
        else uri_join(uri_parent(committer.delta_root), "staging"),
        identity,
    )
    validate_persistable_uri(staging)
    protocols = committer.fs.protocol
    protocols = (protocols,) if isinstance(protocols, str) else protocols
    if get_protocol(staging) not in protocols:
        msg = "Replay staging protocol differs from the configured backend"
        raise ValueError(msg)
    protected = (committer.delta_root, committer.files_root)
    _require_separate_root(staging, protected, committer)
    if storage.is_local:
        _require_separate_root(str(working), (*protected, staging), committer)
    if working.exists() or committer.fs.exists(staging):
        msg = "Replay diagnostics require fresh scratch and staging paths"
        raise ValueError(msg)
    return AssignmentDispatch(
        committer.delta_root,
        committer.files_root,
        staging,
        storage,
        working,
        runtime_env=runtime_env,
        application_modules=application_modules,
        preserve_working=True,
        preserve_staging=True,
    )


def _require_separate_root(
    root: str, protected: Sequence[str], committer: RetainedCommitter
) -> None:
    def normalize(value: str) -> str:
        path = committer.fs._strip_protocol(value)
        return (
            str(Path(path).resolve())
            if get_protocol(value) in {"file", "local"}
            else posixpath.normpath(path)
        ).rstrip("/")

    path = normalize(root)
    for other in protected:
        source = normalize(other)
        if (
            path == source
            or path.startswith(source + "/")
            or source.startswith(path + "/")
        ):
            msg = "Replay scratch and staging must be separate from store roots"
            raise ValueError(msg)


def _execution_id(source: ExecutionSource | None) -> str | None:
    if isinstance(source, ScientificExecutionSource):
        return source.plan.receipt.delivery.delivery_id
    if isinstance(source, FailedScientificExecutionSource):
        assert source.unit.failed_delivery is not None
        return source.unit.failed_delivery.delivery_id
    if isinstance(source, CuratorExecutionSource):
        value = source.execution_record["execution_run_id"]
        if not isinstance(value, str):
            msg = "Curator execution identity is invalid"
            raise StoreIntegrityError(msg)
        return value
    return None


def _diagnostic_coverage(
    committer: RetainedCommitter, source: ExecutionSource | None
) -> tuple[Literal["complete", "incomplete", "unavailable"], list[str]]:
    try:
        if isinstance(
            source, (ScientificExecutionSource, FailedScientificExecutionSource)
        ):
            receipt = (
                source.plan.receipt
                if isinstance(source, ScientificExecutionSource)
                else source.unit.failed_delivery
            )
            assert receipt is not None
            diagnostic = read_replay_diagnostic(
                receipt, delta_root=committer.delta_root, fs=committer.fs
            )
            if diagnostic is not None:
                return "complete", []
        elif isinstance(source, CuratorExecutionSource):
            encoded = source.execution_record.get("replay_snapshot")
            if isinstance(encoded, str):
                snapshot = ReplaySnapshot.model_validate_json(encoded)
                if snapshot.diagnostic is not None:
                    if (
                        snapshot.diagnostic.source_execution_run_id
                        != source.admission.record.replay_of_execution_run_id
                        or snapshot.diagnostic.selected_software_digest
                        != source.admission.software_digest
                    ):
                        msg = "Replay diagnostic differs from its curator owner"
                        raise StoreIntegrityError(msg)
                    return snapshot.diagnostic.status, snapshot.diagnostic.errors
    except (OSError, ValueError, StoreIntegrityError):
        return "incomplete", ["Persisted replay diagnostics could not be verified"]
    return "unavailable", ["No persisted replay diagnostic is available"]


def _error(code: str, message: str) -> ArtisanError:
    return ArtisanError(
        code, message, error_type="validation", recovery_hint="CHECK_INPUT"
    )
