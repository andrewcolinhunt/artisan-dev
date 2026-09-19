"""Exact input and concrete configuration evidence for single-unit replay."""

from __future__ import annotations

from pydantic import SecretStr

from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording.replay_snapshot import (
    _configuration,
    build_replay_snapshot,
)
from artisan.operations.examples.data_generator import DataGenerator
from artisan.operations.examples.data_transformer import DataTransformer
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.schemas.operation_config.environments import Environments
from artisan.utils.hashing import (
    CacheInputIdentity,
    compute_execution_spec_id,
    effective_config_payload,
    serialize_params,
)


def _runtime(tmp_path):
    return RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    )


def test_capture_exact_duplicates_and_groups(tmp_path):
    operation = DataTransformer(params=DataTransformer.Params(seed=7))
    ids = ["a" * 32, "a" * 32, "b" * 32]
    groups = ["g1", "g1", "g2"]
    occurrences = {
        "dataset": [
            CacheInputIdentity("dataset", groups[i], i, "data", aid)
            for i, aid in enumerate(ids)
        ]
    }
    spec = compute_execution_spec_id(
        operation.name,
        occurrences,
        serialize_params(operation),
        effective_config_payload(operation),
    )
    unit = ExecutionUnit(
        operation=operation,
        inputs={"dataset": ids},
        group_ids=groups,
        execution_spec_id=spec,
    )
    snapshot = build_replay_snapshot(unit, _runtime(tmp_path), occurrences)
    assert snapshot.status == "ready"
    assert snapshot.inputs == occurrences
    assert snapshot.group_ids == groups
    assert snapshot.operation.configuration["params"]["seed"] == 7
    assert ReplaySnapshot.model_validate_json(snapshot.model_dump_json()) == snapshot


def test_capture_rejects_unverified_identity(tmp_path):
    snapshot = build_replay_snapshot(
        ExecutionUnit(operation=DataGenerator(), execution_spec_id="wrong"),
        _runtime(tmp_path),
        {},
    )
    assert snapshot.status == "unavailable"


def test_concrete_configuration_preserves_defaults_and_redacts_all_env_values():
    operation = DataGenerator(
        params=DataGenerator.Params(count=3, seed=7),
        environments=Environments(
            local=LocalEnvironmentSpec(
                env={"MODE": "private-mode", "TOKEN": "private-token"}
            )
        ),
    )
    payload, slots = _configuration(operation)
    assert payload["params"] == {"count": 3, "rows_per_file": 10, "seed": 7}
    assert payload["environments"]["local"]["env"] == {"MODE": None, "TOKEN": None}
    assert {slot.pointer for slot in slots} == {
        "/environments/local/env/MODE",
        "/environments/local/env/TOKEN",
    }
    assert "private" not in str(payload)


def test_pydantic_secret_is_slot_before_masking():
    from pydantic import BaseModel, Field

    class SecretGenerator(DataGenerator):
        name = "replay_secret_generator"

        class Params(BaseModel):
            secret: SecretStr = Field(description="Required secret for test.")

        params: Params

    operation = SecretGenerator(params={"secret": "hidden-value"})
    payload, slots = _configuration(operation)
    assert payload["params"]["secret"] is None
    assert [slot.pointer for slot in slots] == ["/params/secret"]
