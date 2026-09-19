"""Driver preflight rejects incomplete evidence before creating an attempt."""

from __future__ import annotations

import importlib
import json
from unittest.mock import Mock

import polars as pl
import pytest
from pydantic import ValidationError

from artisan.errors import ArtisanError, StoreIntegrityError
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording.replay_snapshot import build_replay_snapshot
from artisan.operations.examples.data_generator import DataGenerator
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.schemas.operation_config.environments import Environments
from artisan.utils.hashing import (
    compute_execution_spec_id,
    effective_config_payload,
    serialize_params,
)

replay = importlib.import_module("artisan.orchestration.replay")


def _snapshot(tmp_path, operation=None):
    operation = operation or DataGenerator()
    runtime = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "debug" / "staging"),
        working_root=str(tmp_path / "debug" / "work"),
        files_root=str(tmp_path / "debug" / "files"),
        failure_logs_root=str(tmp_path / "debug" / "logs"),
    )
    spec = compute_execution_spec_id(
        operation.name,
        {},
        serialize_params(operation),
        effective_config_payload(operation),
    )
    unit = ExecutionUnit(operation=operation, execution_spec_id=spec, step_number=7)
    return build_replay_snapshot(unit, runtime, {}), runtime


@pytest.mark.parametrize("bad", [None, "{}", "not-json"])
def test_malformed_snapshot_integrity_error_has_no_unsafe_cause(
    tmp_path, monkeypatch, bad
):
    snapshot, runtime = _snapshot(tmp_path)
    monkeypatch.setattr(
        replay,
        "_read_executions",
        lambda runtime: pl.DataFrame(
            {
                "execution_run_id": ["id"],
                "execution_spec_id": [snapshot.source.execution_spec_id],
                "replay_snapshot": [bad],
            }
        ),
    )
    with pytest.raises(StoreIntegrityError) as caught:
        replay.replay_execution("id", runtime=runtime)
    assert caught.value.__cause__ is None
    assert not (tmp_path / "debug").exists()


def test_unavailable_manual_snapshot_rejected_before_manager(tmp_path, monkeypatch):
    _, runtime = _snapshot(tmp_path)
    monkeypatch.setattr(
        replay,
        "_read_executions",
        lambda runtime: pl.DataFrame(
            {
                "execution_run_id": ["id"],
                "execution_spec_id": ["spec"],
                "replay_snapshot": [
                    ReplaySnapshot.unavailable(
                        "manual_interactive_commit"
                    ).model_dump_json()
                ],
            }
        ),
    )
    manager = Mock(side_effect=AssertionError("preflight created an attempt"))
    monkeypatch.setattr(replay, "PipelineManager", manager)
    with pytest.raises(ArtisanError, match="manual_interactive_commit") as caught:
        replay.replay_execution("id", runtime=runtime)
    assert caught.value.code == "replay_evidence_unavailable"
    manager.assert_not_called()


def test_duplicate_execution_id_rejected(tmp_path, monkeypatch):
    snapshot, runtime = _snapshot(tmp_path)
    monkeypatch.setattr(
        replay,
        "_read_executions",
        lambda runtime: pl.DataFrame(
            {
                "execution_run_id": ["id", "id"],
                "replay_snapshot": [snapshot.model_dump_json()] * 2,
            }
        ),
    )
    with pytest.raises(StoreIntegrityError, match="Duplicate"):
        replay._source_snapshot("id", runtime)


def test_explicit_environment_replacements_decode_before_concrete_validation(
    tmp_path, monkeypatch
):
    operation = DataGenerator(
        environments=Environments(
            local=LocalEnvironmentSpec(env={"TOKEN": "old-token"})
        )
    )
    snapshot, _ = _snapshot(tmp_path, operation)
    pointer = "/environments/local/env/TOKEN"
    with pytest.raises(ArtisanError) as caught:
        replay._restore_operation(snapshot, None, {}, False)
    assert caught.value.code == "replay_replacement_required"
    monkeypatch.setenv("FRESH_TOKEN", '"null"')
    restored, redactor = replay._restore_operation(
        snapshot, None, {pointer: "FRESH_TOKEN"}, False
    )
    assert restored.environments.local.env["TOKEN"] == "null"
    assert "null" not in redactor.sanitize("received null")
    monkeypatch.setenv("FRESH_TOKEN", "42")
    with pytest.raises(ArtisanError) as caught:
        replay._restore_operation(snapshot, None, {pointer: "FRESH_TOKEN"}, False)
    assert caught.value.code == "replay_configuration_invalid"
    assert "42" not in json.dumps(caught.value.to_dict())


def test_nested_defaults_never_silently_added(tmp_path):
    snapshot, _ = _snapshot(tmp_path)
    payload = snapshot.model_dump(mode="json")
    del payload["operation"]["configuration"]["params"]["rows_per_file"]
    changed = ReplaySnapshot.model_validate(payload)
    with pytest.raises(ArtisanError, match="defaults"):
        replay._restore_operation(changed, None, {}, True)


def test_changed_code_requires_explicit_allowance(tmp_path):
    snapshot, _ = _snapshot(tmp_path)
    payload = snapshot.model_dump(mode="json")
    payload["operation"]["identity"]["module_digest"] = "old"
    changed = ReplaySnapshot.model_validate(payload)
    with pytest.raises(ArtisanError) as caught:
        replay._restore_operation(changed, None, {}, False)
    assert caught.value.code == "replay_code_changed"
    restored, _ = replay._restore_operation(changed, None, {}, True)
    assert restored == DataGenerator()


def test_diagnostic_roots_fresh_and_caller_failure_root_retained(tmp_path):
    snapshot, runtime = _snapshot(tmp_path)
    one = replay._diagnostic_runtime(runtime, snapshot)
    two = replay._diagnostic_runtime(runtime, snapshot)
    assert one.failure_logs_root.startswith(runtime.failure_logs_root + "/")
    assert one.working_root != two.working_root
    assert one.preserve_working
    assert one.preserve_staging
    with pytest.raises(ArtisanError, match="overlap"):
        replay._diagnostic_runtime(
            runtime.model_copy(update={"working_root": runtime.delta_root}), snapshot
        )


def test_malformed_group_length_is_validation_error(tmp_path):
    snapshot, _ = _snapshot(tmp_path)
    payload = snapshot.model_dump(mode="json")
    payload["inputs"] = {
        "data": [
            {
                "role": "data",
                "group_id": "group",
                "position": 0,
                "artifact_type": "data",
                "artifact_id": "a" * 32,
            }
        ]
    }
    payload["group_ids"] = []
    with pytest.raises(ValidationError, match="group IDs"):
        ReplaySnapshot.model_validate(payload)


def test_structured_and_scalar_replacement_values_are_redacted():
    from artisan.execution.recording.commands import CommandRecorder

    redactor = CommandRecorder()
    redactor.add_sensitive_data(
        {"key": "fresh-object-secret", "codes": [784219, False]}
    )
    assert redactor.sanitize_data(
        {"credentials": {"key": "fresh-object-secret", "codes": [784219, False]}}
    ) == {"credentials": {"key": "<redacted>", "codes": ["<redacted>", "<redacted>"]}}


def test_snapshot_owner_must_match_physical_execution_row(tmp_path, monkeypatch):
    snapshot, runtime = _snapshot(tmp_path)
    row = {
        "execution_run_id": "id",
        "execution_spec_id": snapshot.source.execution_spec_id,
        "replay_snapshot": snapshot.model_dump_json(),
        "origin_step_number": 99,
        "step_run_id": "other-step",
        "compute_backend": "local",
        "operation_name": "data_generator",
    }
    monkeypatch.setattr(replay, "_read_executions", lambda runtime: pl.DataFrame([row]))
    with pytest.raises(StoreIntegrityError, match="owner"):
        replay._source_snapshot("id", runtime)


@pytest.mark.parametrize(
    "pointer", ["/params/missing/value", "/params/count", "/params/~2invalid"]
)
def test_snapshot_replacement_must_own_redacted_value(tmp_path, pointer):
    snapshot, _ = _snapshot(tmp_path)
    payload = snapshot.model_dump(mode="json")
    payload["required_replacements"] = [{"pointer": pointer}]
    with pytest.raises(ValidationError, match="redacted"):
        ReplaySnapshot.model_validate(payload)


def test_unknown_and_wrong_identifier_are_structured(tmp_path, monkeypatch):
    _, runtime = _snapshot(tmp_path)
    monkeypatch.setattr(
        replay,
        "_read_executions",
        lambda runtime: pl.DataFrame(schema={"execution_run_id": pl.String}),
    )
    monkeypatch.setattr(
        replay,
        "read_committed",
        lambda *args, **kwargs: pl.DataFrame(
            {"step_run_id": ["step-id"], "pipeline_run_id": ["run-id"]}
        ),
    )
    for identifier in ("unknown", "step-id", "run-id"):
        with pytest.raises(ArtisanError) as caught:
            replay._source_snapshot(identifier, runtime)
        assert caught.value.code == "replay_execution_not_found"
        if identifier != "unknown":
            assert "not an execution-unit" in str(caught.value)


def test_missing_operation_module_is_dependency_error(tmp_path, monkeypatch):
    snapshot, _ = _snapshot(tmp_path)
    monkeypatch.setattr(
        replay,
        "resolve_operation",
        Mock(side_effect=ImportError("private import detail")),
    )
    with pytest.raises(ArtisanError) as caught:
        replay._restore_operation(snapshot, None, {}, False)
    assert caught.value.code == "replay_dependency_unavailable"
    assert "private import detail" not in json.dumps(caught.value.to_dict())


def test_runner_rejection_precedes_attempt_and_diagnostic_roots(tmp_path, monkeypatch):
    snapshot, runtime = _snapshot(tmp_path)
    monkeypatch.setattr(replay, "_source_snapshot", lambda *args: snapshot)
    monkeypatch.setattr(replay, "_verify_artifacts", lambda *args: None)
    runner = Mock(name="runner")
    runner.validate_operation.side_effect = ValueError("Unsupported operation")
    monkeypatch.setattr(replay, "resolve_runner", lambda value: runner)
    manager = Mock(side_effect=AssertionError("preflight created an attempt"))
    monkeypatch.setattr(replay, "PipelineManager", manager)
    with pytest.raises(ArtisanError) as caught:
        replay.replay_execution("id", runtime=runtime)
    assert caught.value.code == "replay_dependency_unavailable"
    runner.validate_operation.assert_called_once()
    manager.assert_not_called()
    assert not (tmp_path / "debug").exists()


def test_complete_association_evidence_requires_explicit_empty_owner(tmp_path):
    snapshot, _ = _snapshot(tmp_path)
    payload = snapshot.model_dump(mode="json")
    payload["operation"]["behavior"]["inputs"] = {
        "dataset": {"with_associated": ["metric"]}
    }
    payload["inputs"] = {
        "dataset": [
            {
                "role": "dataset",
                "group_id": None,
                "position": 0,
                "artifact_type": "data",
                "artifact_id": "a" * 32,
            }
        ]
    }
    with pytest.raises(ValidationError, match="every declared primary/type"):
        ReplaySnapshot.model_validate(payload)
    payload["associated"] = [
        {"primary_id": "a" * 32, "artifact_type": "metric", "artifact_ids": []}
    ]
    assert ReplaySnapshot.model_validate(payload).associated_complete
