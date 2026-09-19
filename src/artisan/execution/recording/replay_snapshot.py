"""Per-unit replay capture, including exact occurrences and worker observations."""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from importlib.metadata import version
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from pydantic import BaseModel, SecretBytes, SecretStr

from artisan.errors import ArtisanError, ErrorCode
from artisan.execution.recording.commands import CommandRecorder, credential_name
from artisan.registry.resolve import operation_identity, resolve_operation
from artisan.schemas.execution.replay import (
    RemoteObservation,
    ReplayAssociation,
    ReplayOperation,
    ReplayReplacement,
    ReplaySnapshot,
    ReplaySource,
)
from artisan.utils.hashing import (
    CacheInputIdentity,
    compute_execution_spec_id,
    effective_config_payload,
    serialize_params,
)

if TYPE_CHECKING:
    from artisan.execution.models.execution_unit import ExecutionUnit
    from artisan.operations.base.operation_definition import OperationDefinition
    from artisan.schemas.artifact.base import Artifact
    from artisan.schemas.execution.runtime_environment import RuntimeEnvironment

_current: ContextVar[ReplayBuilder | None] = ContextVar("replay_builder", default=None)


def operation_behavior(operation: OperationDefinition) -> dict[str, Any]:
    """Serialize execution-relevant class declarations outside model fields."""
    return {
        "inputs": {
            key: spec.model_dump(mode="json") for key, spec in operation.inputs.items()
        },
        "outputs": {
            key: spec.model_dump(mode="json") for key, spec in operation.outputs.items()
        },
        **{
            key: getattr(operation, key)
            for key in (
                "runtime_defined_inputs",
                "independent_input_streams",
                "hydrate_inputs",
                "per_artifact_dispatch",
                "execute_as_tool",
            )
        },
    }


def runtime_roots(runtime: RuntimeEnvironment) -> dict[str, str | None]:
    """Return only root context, never storage credentials or worker environment."""
    return {
        key: getattr(runtime, key)
        for key in (
            "delta_root",
            "working_root",
            "staging_root",
            "files_root",
            "failure_logs_root",
        )
    }


def _pointer(parent: str, key: str | int) -> str:
    return parent + "/" + str(key).replace("~", "~0").replace("/", "~1")


def set_pointer(payload: dict[str, Any], pointer: str, value: Any) -> None:
    """Replace one existing JSON value, including a complete credential URL."""
    keys = [key.replace("~1", "/").replace("~0", "~") for key in pointer.split("/")[1:]]
    target: Any = payload
    for key in keys[:-1]:
        target = target[int(key)] if isinstance(target, list) else target[key]
    key = keys[-1]
    target[int(key) if isinstance(target, list) else key] = value


def _configuration(
    operation: OperationDefinition,
) -> tuple[dict[str, Any], list[ReplayReplacement]]:
    payload = operation.model_dump(mode="json", round_trip=True)
    slots: list[ReplayReplacement] = []
    values: dict[str, Any] = {}

    def visit(value: Any, path: str, key: str = "", environment: bool = False) -> None:
        secret = isinstance(value, (SecretStr, SecretBytes))
        raw = value.get_secret_value() if secret else value
        credential_url = False
        if isinstance(raw, str) and "://" in raw:
            parsed = urlsplit(raw)
            credential_url = bool(parsed.username or parsed.query or parsed.fragment)
        provider_reference = path.startswith("/compute_provider/") and key in {
            "auth_secret",
            "secrets",
            "image_registry_secret",
        }
        if (
            path
            and raw is not None
            and (
                secret
                or environment
                or (credential_name(key) and not provider_reference)
                or credential_url
            )
        ):
            slots.append(
                ReplayReplacement(
                    pointer=path, suggested_environment=key if environment else None
                )
            )
            values[path] = raw.decode() if isinstance(raw, bytes) else raw
        elif isinstance(value, BaseModel):
            for name in type(value).model_fields:
                visit(getattr(value, name), _pointer(path, name), name)
        elif isinstance(value, dict):
            for name, item in value.items():
                visit(item, _pointer(path, name), str(name), key == "env")
        elif isinstance(value, (list, tuple)):
            for index, item in enumerate(value):
                visit(item, _pointer(path, index))

    visit(operation, "")
    restored = json.loads(json.dumps(payload, allow_nan=False))
    for pointer, value in values.items():
        set_pointer(restored, pointer, value)
    if type(operation).model_validate(restored) != operation:
        msg = "Concrete operation does not roundtrip through JSON"
        raise ValueError(msg)
    redactor = CommandRecorder(operation)
    redactor.add_sensitive_data(values)

    def redact_references(value: Any, path: str) -> None:
        if path in values:
            return
        if (
            isinstance(value, (str, int, float, bool))
            and redactor.sanitize_data(value) != value
        ):
            slots.append(ReplayReplacement(pointer=path))
        elif isinstance(value, dict):
            for key, item in value.items():
                if redactor.sanitize(str(key)) != str(key):
                    msg = "Sensitive configuration key cannot be restored safely"
                    raise ValueError(msg)
                redact_references(item, _pointer(path, key))
        elif isinstance(value, list):
            for index, item in enumerate(value):
                redact_references(item, _pointer(path, index))

    redact_references(payload, "")
    for slot in slots:
        set_pointer(payload, slot.pointer, None)
    return payload, slots


def build_replay_snapshot(
    unit: ExecutionUnit,
    runtime: RuntimeEnvironment,
    cache_inputs: dict[str, list[CacheInputIdentity]],
) -> ReplaySnapshot:
    """Verify the actual hash preimage and capture the concrete operation safely."""
    try:
        if set(cache_inputs) != set(unit.inputs) or any(
            [entry.artifact_id for entry in cache_inputs[role]] != ids
            for role, ids in unit.inputs.items()
        ):
            msg = "Input occurrences do not match the unit"
            raise ValueError(msg)
        computed = compute_execution_spec_id(
            unit.operation.name,
            cache_inputs,
            serialize_params(unit.operation),
            effective_config_payload(unit.operation),
        )
        if computed != unit.execution_spec_id:
            msg = "Unverified execution cache preimage"
            raise ValueError(msg)
        configuration, replacements = _configuration(unit.operation)
        identity = operation_identity(type(unit.operation))
        importable = False
        with suppress(ImportError, AttributeError, ValueError, TypeError):
            importable = resolve_operation(
                f"{identity.module}:{identity.qualname}"
            ) is type(unit.operation)
        redactor = CommandRecorder(unit.operation)
        return ReplaySnapshot(
            status="ready" if importable else "requires_operation_class",
            unavailable_reason=None,
            operation=ReplayOperation(
                identity=identity,
                configuration=configuration,
                behavior=operation_behavior(unit.operation),
                artisan_version=version("dexterity-artisan"),
            ),
            inputs=cache_inputs,
            group_ids=unit.group_ids,
            associated=[],
            associated_complete=not any(
                spec.with_associated for spec in unit.operation.inputs.values()
            ),
            source=ReplaySource(
                step_number=unit.step_number,
                step_run_id=unit.step_run_id,
                runner=runtime.compute_backend_name,
                execution_spec_id=unit.execution_spec_id,
                roots=redactor.sanitize_data(runtime_roots(runtime)),
            ),
            required_replacements=replacements,
            remote_identity=[],
            diagnostic=None,
        )
    except (TypeError, ValueError, OverflowError) as exc:
        return ReplaySnapshot.unavailable(
            f"configuration_or_occurrence_unavailable:{type(exc).__name__}"
        )


class ReplayBuilder:
    """Mutable evidence confined to one dispatched unit, with thread-safe slots."""

    def __init__(self, snapshot: ReplaySnapshot) -> None:
        self._snapshot = snapshot
        self._lock = threading.Lock()

    @property
    def snapshot(self) -> ReplaySnapshot:
        """Emit independently validated evidence at the recording boundary."""
        with self._lock:
            return ReplaySnapshot.model_validate_json(self._snapshot.model_dump_json())

    def record_associated(
        self, associated: dict[tuple[str, str], list[Artifact]]
    ) -> None:
        """Freeze the actual hydrated list, preserving its order and empty pairs."""
        records = [
            ReplayAssociation(
                primary_id=primary,
                artifact_type=kind,
                artifact_ids=[
                    a.artifact_id for a in artifacts if a.artifact_id is not None
                ],
            )
            for (primary, kind), artifacts in associated.items()
        ]
        with self._lock:
            self._snapshot = self._snapshot.model_copy(
                update={"associated": records, "associated_complete": True}
            )

    def record_sandbox(self, sandbox_path: str) -> None:
        """Remember the original sandbox boundary for safe diagnostic roots."""
        with self._lock:
            source = self._snapshot.source
            if source is not None:
                source = source.model_copy(
                    update={"roots": {**source.roots, "sandbox_path": sandbox_path}}
                )
                self._snapshot = self._snapshot.model_copy(update={"source": source})

    def record_remote(self, observation: RemoteObservation) -> None:
        """Retain actual worker identity and diagnostic delivery separately."""
        from artisan.execution.recording.commands import sanitize_diagnostic

        observation = observation.model_copy(
            update={
                "diagnostic_error": sanitize_diagnostic(observation.diagnostic_error),
            }
        )
        with self._lock:
            slots = {
                item.dispatch_index: item for item in self._snapshot.remote_identity
            }
            slots[observation.dispatch_index] = observation
            diagnostic = self._snapshot.diagnostic
            if diagnostic is not None and observation.diagnostic_status in {
                "incomplete",
                "unavailable",
            }:
                diagnostic = diagnostic.model_copy(
                    update={
                        "status": "incomplete",
                        "errors": [
                            *diagnostic.errors,
                            *(
                                [observation.diagnostic_error]
                                if observation.diagnostic_error
                                else []
                            ),
                        ],
                    }
                )
            self._snapshot = self._snapshot.model_copy(
                update={
                    "remote_identity": [slots[i] for i in sorted(slots)],
                    "diagnostic": diagnostic,
                }
            )


def current_replay_builder() -> ReplayBuilder | None:
    """Return the builder propagated through the current lifecycle context."""
    return _current.get()


def replay_snapshot() -> ReplaySnapshot:
    """Require an active execution capture scope for durable recording."""
    builder = current_replay_builder()
    if builder is None:
        msg = "Replay evidence requires an active capture scope"
        raise RuntimeError(msg)
    return builder.snapshot


def replay_recording_fields() -> dict[str, Any]:
    """Read the immutable snapshot once at a durable recorder boundary."""
    snapshot = replay_snapshot()
    return {
        "replay_snapshot": snapshot,
        "replay_of_execution_run_id": (
            snapshot.diagnostic.source_execution_run_id if snapshot.diagnostic else None
        ),
    }


@contextmanager
def capture_replay(
    unit: ExecutionUnit, runtime: RuntimeEnvironment
) -> Iterator[ReplayBuilder]:
    """Install one builder; direct facade calls must verify their hash preimage."""
    snapshot = unit.replay_snapshot
    if snapshot is None:
        from artisan.storage.core.artifact_store import ArtifactStore

        try:
            store = ArtifactStore(
                runtime.delta_root,
                fs=runtime.storage.filesystem(),
                storage_options=runtime.storage.delta_storage_options(),
                files_root=runtime.files_root,
            )
            types = store.provenance.load_type_map(
                [aid for ids in unit.inputs.values() for aid in ids]
            )
            occurrences = {
                role: [
                    CacheInputIdentity(
                        role,
                        unit.group_ids[i] if unit.group_ids else None,
                        i,
                        types[aid],
                        aid,
                    )
                    for i, aid in enumerate(ids)
                ]
                for role, ids in unit.inputs.items()
            }
            snapshot = build_replay_snapshot(unit, runtime, occurrences)
        except (KeyError, ValueError, OSError, ArtisanError):
            snapshot = ReplaySnapshot.unavailable("unverified_input_occurrences")
    builder = ReplayBuilder(snapshot)
    builder.record_remote(
        RemoteObservation(
            dispatch_index=0,
            status="not_applicable"
            if unit.operation.compute_provider.active == "local"
            else "not_started",
        )
    )
    token = _current.set(builder)
    try:
        yield builder
    finally:
        _current.reset(token)


def verify_replay_worker(unit: ExecutionUnit) -> None:
    """Check selected code inside the normal worker failure-recording boundary."""
    snapshot = unit.replay_snapshot
    if snapshot is None or snapshot.diagnostic is None:
        return
    if (
        operation_identity(type(unit.operation))
        != snapshot.diagnostic.selected_identity
    ):
        msg = "Replay worker operation code differs from selected driver code"
        raise ArtisanError(
            code=ErrorCode.REPLAY_CODE_CHANGED,
            message=msg,
            error_type="config",
            recovery_hint="CHECK_INPUT",
        )
    logging.getLogger("artisan").setLevel(logging.DEBUG)
