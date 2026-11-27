"""Per-unit replay capture, including exact occurrences and worker observations."""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, SecretBytes, SecretStr

from artisan.errors import ArtisanError, ErrorCode
from artisan.execution.recording.commands import CommandRecorder
from artisan.registry.resolve import operation_identity
from artisan.schemas.execution.replay import (
    ReplayAssociation,
    ReplayReplacement,
    ReplaySnapshot,
)
from artisan.utils.configuration_secrets import (
    credential_name,
    credential_url,
)

if TYPE_CHECKING:
    from artisan.execution.models.execution_unit import ExecutionUnit
    from artisan.operations.base.operation_definition import OperationDefinition
    from artisan.schemas.artifact.base import Artifact
    from artisan.schemas.execution.worker_config import WorkerConfig

_current: ContextVar[ReplayBuilder | None] = ContextVar("replay_builder", default=None)


def operation_behavior(operation: OperationDefinition) -> dict[str, Any]:
    """Serialize execution-relevant class declarations outside model fields."""
    return {
        "inputs": {
            key: spec.model_dump(mode="json") for key, spec in operation.inputs.items()
        },
        **(
            {"shared_input_roles": list(operation.shared_input_roles)}
            if operation.shared_input_roles
            else {}
        ),
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


def runtime_roots(worker_config: WorkerConfig) -> dict[str, str | None]:
    """Return only root context, never storage credentials or worker environment."""
    return {
        key: getattr(worker_config, key)
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


def same_configuration_structure(current: Any, recorded: Any) -> bool:
    """Reject new nested defaults and removed fields during reconstruction."""
    if isinstance(current, dict) and isinstance(recorded, dict):
        return current.keys() == recorded.keys() and all(
            same_configuration_structure(current[key], recorded[key]) for key in current
        )
    if isinstance(current, list) and isinstance(recorded, list):
        return len(current) == len(recorded) and all(
            same_configuration_structure(a, b)
            for a, b in zip(current, recorded, strict=True)
        )
    return not isinstance(current, (dict, list)) and not isinstance(
        recorded, (dict, list)
    )


def _configuration(
    operation: OperationDefinition,
    sensitive_values: tuple[str, ...] = (),
) -> tuple[dict[str, Any], list[ReplayReplacement]]:
    slots: list[ReplayReplacement] = []
    values: dict[str, Any] = {}

    def visit(
        value: Any,
        path: str,
        key: str = "",
    ) -> None:
        secret = isinstance(value, (SecretStr, SecretBytes))
        raw = value.get_secret_value() if secret else value
        if (
            path
            and raw is not None
            and (secret or credential_name(key) or credential_url(raw))
        ):
            slots.append(ReplayReplacement(pointer=path))
            values[path] = raw.decode() if isinstance(raw, bytes) else raw
        elif isinstance(value, BaseModel):
            for name in type(value).model_fields:
                visit(getattr(value, name), _pointer(path, name), name)
        elif isinstance(value, dict):
            for name, item in value.items():
                visit(item, _pointer(path, name), str(name))
        elif isinstance(value, (list, tuple)):
            for index, item in enumerate(value):
                visit(item, _pointer(path, index))

    visit(operation, "")
    payload = operation.model_dump(mode="json", round_trip=True)
    restored = json.loads(json.dumps(payload, allow_nan=False))
    for pointer, value in values.items():
        set_pointer(restored, pointer, value)
    if type(operation).model_validate(restored, by_name=True) != operation:
        msg = "Concrete operation does not roundtrip through JSON"
        raise ValueError(msg)
    redactor = CommandRecorder(operation)
    redactor.add_sensitive_data(sensitive_values)
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
        """Verify hydrated membership and retain canonical association evidence."""
        records = [
            ReplayAssociation(
                primary_id=primary,
                artifact_type=kind,
                artifact_ids=[
                    a.artifact_id for a in artifacts if a.artifact_id is not None
                ],
            )
            for (primary, kind), artifacts in sorted(associated.items())
        ]
        with self._lock:
            if self._snapshot.associated_complete and records != sorted(
                self._snapshot.associated,
                key=lambda item: (item.primary_id, item.artifact_type),
            ):
                msg = "Hydrated associations differ from the frozen input snapshot"
                raise ValueError(msg)
            self._snapshot = self._snapshot.model_copy(
                update={"associated": records, "associated_complete": True}
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
def capture_replay(unit: ExecutionUnit) -> Iterator[ReplayBuilder]:
    """Record only evidence already captured against a required curator admission."""
    snapshot = unit.replay_snapshot or ReplaySnapshot.unavailable(
        "required_admission_unavailable"
    )
    if snapshot.associated_complete and sorted(
        unit.associated_inputs, key=lambda item: (item.primary_id, item.artifact_type)
    ) != sorted(
        snapshot.associated, key=lambda item: (item.primary_id, item.artifact_type)
    ):
        msg = "Runtime associations differ from the frozen replay snapshot"
        raise ValueError(msg)
    builder = ReplayBuilder(snapshot)
    token = _current.set(builder)
    try:
        yield builder
    finally:
        _current.reset(token)


def verify_replay_worker(unit: ExecutionUnit) -> None:
    """Verify selected replay code, then enable DEBUG logging in its worker."""
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
