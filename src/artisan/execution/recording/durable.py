"""Apply the command secret policy to copies of durable user diagnostics."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel
from pydantic_core import PydanticUndefined

from artisan.execution.recording.commands import CommandRecorder, current_recorder
from artisan.schemas.execution.replay import ReplaySnapshot


def configuration_recorder(
    operation: Any = None,
    *,
    params: dict[str, Any] | None = None,
    tool: Any = None,
    recorder: CommandRecorder | None = None,
) -> CommandRecorder:
    """Learn declared and supplied config without instantiation or credential I/O."""
    recorder = recorder or current_recorder() or CommandRecorder()
    recorder._learn_model_fields(operation, set())
    operation_class = operation if isinstance(operation, type) else type(operation)
    fields = getattr(operation_class, "model_fields", {})
    for name, field in fields.items():
        value = (
            getattr(operation, name)
            if isinstance(operation, BaseModel)
            else field.default
        )
        if value is not PydanticUndefined:
            recorder._learn_json({name: value})
            recorder.add_schema_data(field.annotation, value)
        if name == "params":
            recorder.add_schema_data(field.annotation, params or {})
    recorder._learn_json(params)
    recorder._learn_json(tool)
    return recorder


def sanitize_metadata(recorder: CommandRecorder, metadata: Any) -> dict[str, Any]:
    """Redact authored metadata before restoring framework lifecycle evidence."""
    framework = {"timings", "skip_reason", "status", "cancelled", "skipped"}
    user = {
        key: value for key, value in (metadata or {}).items() if key not in framework
    }
    recorder._learn_json(user)
    recorder._learn_json(recorder.json_data(user))
    return {
        **recorder.sanitize_json(user),
        **{key: value for key, value in (metadata or {}).items() if key in framework},
    }


def sanitize_replay_snapshot(
    recorder: CommandRecorder, snapshot: ReplaySnapshot
) -> ReplaySnapshot:
    """Refuse earlier public evidence containing sensitivity discovered at seal."""
    user_data: list[object] = [
        snapshot.operation.configuration if snapshot.operation is not None else None,
        snapshot.source.roots if snapshot.source is not None else None,
    ]
    if snapshot.diagnostic is not None:
        user_data.extend(
            [
                snapshot.diagnostic.roots,
                snapshot.diagnostic.errors,
                snapshot.diagnostic.reproducibility_notes,
            ]
        )
    if recorder.sanitize_json(user_data) != user_data:
        return ReplaySnapshot.unavailable("sensitive_evidence_discovered_at_seal")
    return snapshot


def sanitize_error_envelope(
    recorder: CommandRecorder, envelope: dict[str, Any] | None
) -> dict[str, Any] | None:
    """Preserve machine-readable error and repair identities around diagnostics."""
    if envelope is None:
        return None
    framework = {
        "error_type",
        "code",
        "recovery_hint",
        "operation_name",
        "step_name",
        "logical_commit_id",
        "table",
        "plan_key",
        "verified_tables",
        "staging_objects",
    }
    diagnostics = {"message", "field", "hint", "suggestions", "doc_uri", "traceback"}
    clean = {}
    for key, value in envelope.items():
        if key in framework:
            clean[key] = value
        elif key == "cause" and isinstance(value, dict):
            clean[key] = {
                name: item if name == "type" else recorder.sanitize_json(item)
                for name, item in value.items()
            }
        elif key in diagnostics:
            clean[key] = recorder.sanitize_json(value)
        else:
            clean.update(recorder.sanitize_json({key: value}))
    return clean
