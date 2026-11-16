"""Durable configuration copies use the scoped command redaction policy."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest
from fixtures.commands import IdentityCommandEnvironment
from fsspec.implementations.local import LocalFileSystem
from pydantic import BaseModel, Field, SecretBytes, SecretStr, field_validator

from artisan.execution.recording.commands import CommandRecorder
from artisan.execution.recording.durable import (
    configuration_recorder,
    sanitize_metadata,
)
from artisan.execution.recording.parquet_writer import _write_execution_record
from artisan.operations.examples.data_generator import DataGenerator
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.utils.external_tools import redact_values


class NestedValues(BaseModel):
    value: SecretStr = Field(validation_alias="supplied")
    pair: tuple[SecretBytes, str]


class DurableSecretGenerator(DataGenerator):
    name = "durable_secret_generator"

    class Params(DataGenerator.Params):
        nested: NestedValues = Field(description="Nested typed credential input.")
        token: int = Field(default=47, description="Numeric credential.")
        password: bool = Field(default=True, description="Boolean credential.")
        tokenizer_path: Path = Field(
            default=Path("/private/default"), description="Credential path."
        )
        label: str = Field(default="public label", description="Ordinary public value.")

    params: Params


def test_schema_learning_preserves_public_tuple_and_skips_user_code():
    calls = []

    class Values(BaseModel):
        nested: NestedValues
        generated: str = Field(
            default_factory=lambda: calls.append("factory") or "value"
        )

        @field_validator("nested")
        @classmethod
        def validate_nested(cls, value):
            calls.append("validator")
            return value

    supplied = {
        "nested": {"supplied": "nested-raw-secret", "pair": [b"byte-secret", "visible"]}
    }
    recorder = CommandRecorder()
    recorder.add_schema_data(Values, supplied)
    clean = recorder.sanitize_json(supplied)
    assert clean == {
        "nested": {"supplied": "<redacted>", "pair": ["<redacted>", "visible"]}
    }
    assert calls == []
    assert supplied["nested"]["supplied"] == "nested-raw-secret"


def test_configuration_classifies_defaults_and_environment_aliases():
    params = {
        "nested": {"supplied": "nested-raw-secret", "pair": [b"byte-secret", "visible"]}
    }
    environment = IdentityCommandEnvironment(
        env={"PUBLIC": "/public", "ALIAS": "/private/default", "HIDDEN": "opaque-env"},
        public_env=("PUBLIC", "ALIAS"),
    )
    recorder = configuration_recorder(DurableSecretGenerator, params=params)
    recorder.prepare(environment, ["tool"], ())
    assert recorder.sanitize_json(environment.env) == {
        "PUBLIC": "/public",
        "ALIAS": "<redacted>",
        "HIDDEN": "<redacted>",
    }
    assert recorder.sanitize_json({"value": 47, "flag": True}) == {
        "value": "<redacted>",
        "flag": "<redacted>",
    }


@pytest.mark.parametrize("success", [True, False])
def test_ordinary_seal_sanitizes_json_copies_without_capture(tmp_path, success):
    raw = "nested-raw-secret"
    overrides = {"nested": {"supplied": raw, "pair": [b"byte-secret", "visible"]}}
    operation = DurableSecretGenerator(params=overrides)
    metadata = {
        "alias": (raw, Path("/private/default")),
        raw: "named key",
        "public": "visible",
        "timings": {"count": 47, "valid": True},
    }
    staging = tmp_path / "seal"
    staging.mkdir()
    now = datetime.now(UTC)
    _write_execution_record(
        execution_run_id="e" * 32,
        execution_spec_id="s" * 32,
        operation_name="generator",
        step_number=47,
        success=success,
        error=None if success else f"failed {raw}",
        timestamp_start=now,
        timestamp_end=now,
        worker_id=47,
        staging_path=str(staging),
        fs=LocalFileSystem(),
        command_recording=CommandRecording.empty(),
        replay_snapshot=ReplaySnapshot.unavailable("test"),
        replay_of_execution_run_id=None,
        params=operation.params.model_dump(mode="json"),
        user_overrides=overrides,
        result_metadata=metadata,
        operation=operation,
        error_envelope=None if success else {"message": raw},
    )
    row = pl.read_parquet(staging / "executions.parquet").row(0, named=True)
    assert raw not in str(row)
    assert "byte-secret" not in str(row)
    assert "/private/default" not in str(row)
    assert json.loads(row["metadata"])["timings"] == metadata["timings"]
    assert row["source_worker"] == row["origin_step_number"] == 47
    assert row["success"] is success
    assert json.loads(row["params"])["label"] == "public label"
    assert json.loads(row["user_overrides"])["nested"]["pair"][1] == "visible"
    assert operation.params.nested.value.get_secret_value() == raw
    assert overrides["nested"]["supplied"] == raw
    assert metadata["alias"][0] == raw


@pytest.mark.parametrize("value", ["red", "<", "acted", "<redacted>", "<redacted>tail"])
def test_repeated_redaction_preserves_markers_and_remaining_raw_values(value):
    once = redact_values(f"prefix {value} suffix", (value,))
    assert once == "prefix <redacted> suffix"
    assert redact_values(once, (value,)) == once
    recorder = CommandRecorder()
    recorder.add_sensitive_data(value)
    metadata = sanitize_metadata(recorder, {"alias": value, "timings": {"total": 1}})
    assert sanitize_metadata(recorder, metadata) == metadata


def test_marker_composition_is_bounded_and_idempotent():
    values = ("b", "a<redacted>")
    assert redact_values("ab", values) == "<redacted>"
    assert redact_values("a" * 100_000 + "b", values) == "<redacted>"
    assert redact_values("<redacted>", values) == "<redacted>"


def test_error_envelope_preserves_machine_fields_under_secret_collisions():
    from artisan.errors import ArtisanErrorEnvelope
    from artisan.execution.recording.durable import sanitize_error_envelope

    recorder = CommandRecorder()
    recorder.add_sensitive_data(["io", "message"])
    original = {
        "error_type": "io",
        "code": "io",
        "message": "message",
        "recovery_hint": "RETRY_LATER",
        "cause": {"type": "RuntimeError", "message": "message"},
    }
    clean = sanitize_error_envelope(recorder, original)
    assert ArtisanErrorEnvelope.model_validate(clean).error_type == "io"
    assert clean["code"] == "io"
    assert clean["message"] == "<redacted>"
    assert clean["cause"] == {"type": "RuntimeError", "message": "<redacted>"}
    assert original["message"] == "message"


def test_raw_override_alias_is_removed_from_initial_configuration():
    from artisan.execution.recording.replay_snapshot import _configuration

    class NormalizedGenerator(DataGenerator):
        name = "durable_normalized_generator"

        class Params(DataGenerator.Params):
            token: str = Field(description="Normalized credential.")
            alias: str = Field(description="Ordinary alias of raw input.")
            public: str = Field(default="/public/root", description="Public value.")

            @field_validator("token")
            @classmethod
            def normalize_token(cls, value: str) -> str:
                return value.upper()

        params: Params

    raw = "raw-credential-before-validation"
    operation = NormalizedGenerator(params={"token": raw, "alias": raw})
    configuration, replacements = _configuration(operation, (raw,))
    assert raw not in str(configuration)
    assert "/params/alias" in {slot.pointer for slot in replacements}
    assert configuration["params"]["public"] == "/public/root"
    assert operation.params.token == raw.upper()
    assert operation.params.alias == raw


def test_seal_late_metadata_classification_protects_copied_evidence(tmp_path):
    from artisan.execution.recording.commands import capture_commands
    from artisan.execution.recording.replay_snapshot import (
        _configuration,
        operation_behavior,
    )
    from artisan.registry.resolve import operation_identity
    from artisan.schemas.execution.replay import ReplayOperation
    from artisan.utils.hashing import (
        compute_execution_spec_id,
        serialize_params,
    )

    class LabeledGenerator(DataGenerator):
        name = "durable_late_labeled_generator"

        class Params(DataGenerator.Params):
            label: str = Field(description="Initially ordinary parameter.")

        params: Params

    raw = "late-secret-authored-metadata"
    operation = LabeledGenerator(params={"label": raw})
    identity = compute_execution_spec_id(
        operation.name,
        {},
        serialize_params(operation),
    )
    configuration, replacements = _configuration(operation)
    snapshot = ReplaySnapshot.unavailable("configuration_only_fixture").model_copy(
        update={
            "operation": ReplayOperation(
                identity=operation_identity(type(operation)),
                configuration=configuration,
                behavior=operation_behavior(operation),
                artisan_version="0.0.0",
            ),
            "required_replacements": replacements,
        }
    )
    assert raw in snapshot.model_dump_json()
    staging = tmp_path / "seal"
    staging.mkdir()
    with capture_commands(operation) as recorder:
        attempt = recorder.begin(
            IdentityCommandEnvironment(), ["tool", raw], ["tool", raw], "/public", ()
        )
        attempt.finish("succeeded", 0)
        recording = recorder.snapshot()
        now = datetime.now(UTC)
        _write_execution_record(
            execution_run_id="e" * 32,
            execution_spec_id=identity,
            operation_name=operation.name,
            step_number=0,
            success=True,
            error=None,
            timestamp_start=now,
            timestamp_end=now,
            worker_id=0,
            staging_path=str(staging),
            fs=LocalFileSystem(),
            command_recording=recording,
            replay_snapshot=snapshot,
            replay_of_execution_run_id=None,
            params=serialize_params(operation),
            result_metadata={"password": raw, "public": "visible"},
            operation=operation,
        )
    row = pl.read_parquet(staging / "executions.parquet").row(0, named=True)
    assert raw not in str(row)
    final = CommandRecording.model_validate_json(row["command_recording"])
    assert final.commands[0].argv == ["tool", "<redacted>"]
    assert final.commands[0].invocation == recording.commands[0].invocation
    assert final.commands[0].outcome == "succeeded"
    assert (
        json.loads(row["replay_snapshot"])["unavailable_reason"]
        == "sensitive_evidence_discovered_at_seal"
    )
    assert operation.params.label == raw
    assert raw in snapshot.model_dump_json()


def test_unparameterized_collections_keep_value_based_classification():
    class Values(BaseModel):
        mapping: dict
        sequence: list
        choices: set
        pair: tuple

    raw = {
        "mapping": {"token": "bare-container-secret", "public": "visible"},
        "sequence": ["bare-container-secret", "visible"],
        "choices": {"bare-container-secret", "visible"},
        "pair": ("bare-container-secret", "visible"),
    }
    Values.model_validate(raw)
    recorder = CommandRecorder()
    recorder.add_schema_data(Values, raw)
    clean = recorder.sanitize_json(raw)
    assert "bare-container-secret" not in str(clean)
    assert clean["mapping"]["public"] == "visible"
    assert clean["sequence"][1] == clean["pair"][1] == "visible"


@pytest.mark.parametrize(
    "location",
    ["source_roots", "diagnostic_roots", "diagnostic_errors", "diagnostic_notes"],
)
def test_late_secret_removes_unsafe_runtime_evidence_at_seal(location):
    from artisan.execution.recording.durable import sanitize_replay_snapshot
    from artisan.schemas.execution.replay import (
        OperationIdentity,
        ReplayDiagnostic,
        ReplaySource,
    )

    private = "synthetic-late-runtime-secret-73291"
    snapshot = ReplaySnapshot.unavailable("unverified_input_occurrences")
    if location == "source_roots":
        snapshot = snapshot.model_copy(
            update={
                "source": ReplaySource(
                    step_number=0,
                    step_run_id=None,
                    runner="local",
                    execution_spec_id="source-spec",
                    roots={"working_root": private},
                )
            }
        )
    else:
        operation = OperationIdentity(
            module="example.ops",
            qualname="Tool",
            name="tool",
            version="1",
            module_digest="b" * 64,
        )
        diagnostic = ReplayDiagnostic(
            source_execution_run_id="source-run",
            source_execution_spec_id="source-spec",
            source_identity=operation,
            selected_identity=operation,
            allow_code_change=False,
            runner="local",
            roots={"working_root": private} if location == "diagnostic_roots" else {},
            errors=[private] if location == "diagnostic_errors" else [],
            reproducibility_notes=[private] if location == "diagnostic_notes" else [],
        )
        snapshot = snapshot.model_copy(update={"diagnostic": diagnostic})
    recorder = CommandRecorder()
    assert sanitize_replay_snapshot(recorder, snapshot) == snapshot
    recorder.add_sensitive_data(private)
    sealed = sanitize_replay_snapshot(recorder, snapshot)
    assert sealed.status == "unavailable"
    assert sealed.unavailable_reason == "sensitive_evidence_discovered_at_seal"
    assert private not in sealed.model_dump_json()
