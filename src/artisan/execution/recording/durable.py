"""Apply the command secret policy to copies of durable user diagnostics."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel
from pydantic_core import PydanticUndefined

from artisan.execution.recording.commands import CommandRecorder, current_recorder
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.utils.configuration_secrets import sensitive_environment


def configuration_recorder(
    operation: Any = None,
    *,
    params: dict[str, Any] | None = None,
    environment: Any = None,
    compute_provider: Any = None,
    tool: Any = None,
    recorder: CommandRecorder | None = None,
) -> CommandRecorder:
    """Learn declared and supplied config without instantiation or credential I/O."""
    recorder = recorder or current_recorder() or CommandRecorder()
    recorder._learn_model_fields(operation, {"environments", "compute_provider"})
    operation_class = operation if isinstance(operation, type) else type(operation)
    fields = getattr(operation_class, "model_fields", {})
    for name, field in fields.items():
        value = (
            getattr(operation, name)
            if isinstance(operation, BaseModel)
            else field.default
        )
        if name in {"environments", "compute_provider"}:
            override = environment if name == "environments" else compute_provider
            _learn_environments(
                recorder, value, override, provider=name == "compute_provider"
            )
        else:
            if value is not PydanticUndefined:
                recorder._learn_json({name: value})
                recorder.add_schema_data(field.annotation, value)
            if name == "params":
                recorder.add_schema_data(field.annotation, params or {})
    recorder._learn_json(params)
    recorder._learn_json(tool)
    return recorder


def _learn_environments(
    recorder: CommandRecorder, defaults: Any, patch: Any, *, provider: bool
) -> None:
    """Classify every persisted environment, retaining exact provider references."""
    base = recorder.json_data(defaults) if isinstance(defaults, BaseModel) else {}
    changes = recorder.json_data(patch) if isinstance(patch, dict) else {}
    for name in base.keys() | changes.keys():
        original, update = base.get(name), changes.get(name)
        if not isinstance(original, dict) and not isinstance(update, dict):
            recorder._learn_json(update)
            continue
        original = original if isinstance(original, dict) else {}
        update = update if isinstance(update, dict) else {}
        values = {**original, **update}
        old_env, new_env = original.get("env", {}), values.get("env", {})
        if not isinstance(old_env, dict) or not isinstance(new_env, dict):
            recorder._learn_json(new_env, credential=True)
            continue
        env = {**old_env, **new_env}
        try:
            sensitive = sensitive_environment(env, values.get("public_env", ()))
        except (TypeError, ValueError):
            # Classification precedes the owner's validation and pending write.
            sensitive = env
        recorder._learn_json(sensitive, credential=True)
        excluded = {"env", "public_env"}
        if provider and name == "modal":
            excluded |= {"auth_secret", "secrets", "image_registry_secret"}
        recorder._learn_model_fields(getattr(defaults, name, None), excluded)
        recorder._learn_model_fields(
            (patch or {}).get(name) if isinstance(patch, dict) else None, excluded
        )
        recorder._learn_json(
            {key: value for key, value in values.items() if key not in excluded}
        )


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
    user_data = [
        snapshot.operation.configuration if snapshot.operation is not None else None,
        snapshot.source.roots if snapshot.source is not None else None,
        [observation.diagnostic_error for observation in snapshot.remote_identity],
    ]
    if snapshot.diagnostic is not None:
        user_data.extend(
            [
                snapshot.diagnostic.roots,
                snapshot.diagnostic.errors,
                snapshot.diagnostic.reproducibility_notes,
                [
                    observation.diagnostic_error
                    for observation in snapshot.diagnostic.source_remote_identity
                ],
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
