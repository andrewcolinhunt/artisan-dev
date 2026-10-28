"""Exact input and concrete configuration evidence for single-unit replay."""

from __future__ import annotations

import pytest
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
            CacheInputIdentity("dataset", groups[i], i, "csv", aid)
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


def _public_operation():
    from artisan.schemas.operation_config.compute import (
        ComputeProvider,
        ModalComputeConfig,
    )
    from artisan.schemas.operation_config.environment_spec import DockerEnvironmentSpec
    from artisan.schemas.operation_config.tool_spec import ToolSpec

    return DataGenerator(
        params={"count": 3, "seed": 7},
        tool=ToolSpec(executable="/opt/application/tool"),
        environments=Environments(
            local=LocalEnvironmentSpec(
                env={"ROOT": "/opt/application"}, public_env=("ROOT",)
            ),
            docker=DockerEnvironmentSpec(
                image="application:fixed",
                env={"ROOT": "/opt/application"},
                public_env=("ROOT",),
            ),
        ),
        compute_provider=ComputeProvider(
            modal=ModalComputeConfig(
                image="application:fixed",
                env={"ROOT": "/opt/application"},
                public_env=("ROOT",),
                volumes={"/opt/application": "application-data"},
                secrets=["provider-secret-name"],
                image_registry_secret="registry-reference",
                auth_secret="AUTH_PREFIX",
            )
        ),
    )


def test_public_path_roundtrips_complete_inactive_configuration_and_keys():
    operation = _public_operation()
    payload, slots = _configuration(operation)
    assert slots == []
    assert DataGenerator.model_validate(payload) == operation
    assert payload["compute_provider"]["modal"]["volumes"] == {
        "/opt/application": "application-data"
    }
    assert payload["environments"]["docker"]["env"]["ROOT"] == "/opt/application"
    assert payload["tool"]["executable"] == "/opt/application/tool"


def test_secret_reference_exception_applies_only_to_provider_fields():
    operation = _public_operation()
    operation.compute_provider.modal.volumes["auth_secret"] = "private-volume-name"
    payload, slots = _configuration(operation)
    assert [slot.pointer for slot in slots] == [
        "/compute_provider/modal/volumes/auth_secret"
    ]
    assert payload["compute_provider"]["modal"]["auth_secret"] == "AUTH_PREFIX"
    assert payload["compute_provider"]["modal"]["secrets"] == ["provider-secret-name"]


def test_public_alias_of_later_secret_is_redacted_regardless_of_env_order():
    import pytest

    for env in (
        {"ROOT": "known-secret", "TOKEN": "known-secret"},
        {"TOKEN": "known-secret", "ROOT": "known-secret"},
    ):
        operation = DataGenerator(
            environments=Environments(
                local=LocalEnvironmentSpec(env=env, public_env=("ROOT",))
            )
        )
        payload, slots = _configuration(operation)
        assert {slot.pointer for slot in slots} == {
            "/environments/local/env/ROOT",
            "/environments/local/env/TOKEN",
        }
        assert "known-secret" not in str(payload)
        operation = _public_operation()
        operation.environments.local.env["TOKEN"] = "/opt/application"
        with pytest.raises(ValueError, match="Sensitive configuration key"):
            _configuration(operation)


def test_fresh_snapshot_classifies_transient_replacement_values_before_capture(
    tmp_path,
):
    operation = DataGenerator(
        environments=Environments(
            local=LocalEnvironmentSpec(
                env={"ROOT": "fresh-replacement-value"}, public_env=("ROOT",)
            )
        )
    )
    unit = ExecutionUnit(
        operation=operation,
        execution_spec_id=compute_execution_spec_id(
            operation.name,
            {},
            serialize_params(operation),
            effective_config_payload(operation),
        ),
        replay_sensitive_values=("fresh-replacement-value",),
    )
    snapshot = build_replay_snapshot(unit, _runtime(tmp_path), {})
    assert snapshot.status == "ready"
    assert [slot.pointer for slot in snapshot.required_replacements] == [
        "/environments/local/env/ROOT"
    ]
    assert "fresh-replacement-value" not in snapshot.model_dump_json()
    assert "replay_sensitive_values" not in unit.model_dump()


def test_fresh_snapshot_rejects_sensitive_replacement_alias_in_key(tmp_path):
    operation = _public_operation()
    unit = ExecutionUnit(
        operation=operation,
        execution_spec_id=compute_execution_spec_id(
            operation.name,
            {},
            serialize_params(operation),
            effective_config_payload(operation),
        ),
        replay_sensitive_values=("/opt/application",),
    )
    snapshot = build_replay_snapshot(unit, _runtime(tmp_path), {})
    assert snapshot.status == "unavailable"
    assert "/opt/application" not in snapshot.model_dump_json()


@pytest.mark.parametrize(
    "update",
    [
        {"env": {"ROOT": "https://user:copy-sensitive@host/path"}},
        {"public_env": "copy-sensitive"},
    ],
)
def test_invalid_copied_inactive_annotation_makes_capture_safely_unavailable(
    tmp_path, update
):
    import warnings

    operation = _public_operation()
    operation.compute_provider.modal = operation.compute_provider.modal.model_copy(
        update=update
    )
    unit = ExecutionUnit(
        operation=operation,
        execution_spec_id=compute_execution_spec_id(
            operation.name,
            {},
            serialize_params(operation),
            effective_config_payload(operation),
        ),
    )
    with warnings.catch_warnings(record=True) as caught:
        snapshot = build_replay_snapshot(unit, _runtime(tmp_path), {})
    assert not caught
    assert snapshot.status == "unavailable"
    assert "copy-sensitive" not in snapshot.model_dump_json()


def test_typed_secret_alias_cannot_be_made_public():
    from pydantic import BaseModel, Field

    class TypedSecretAlias(DataGenerator):
        name = "replay_typed_secret_alias"

        class Params(BaseModel):
            value: SecretStr = Field(
                description="Typed secret with an ordinary field name."
            )

        params: Params

    operation = TypedSecretAlias(
        params={"value": "typed-sensitive-alias"},
        environments=Environments(
            local=LocalEnvironmentSpec(
                env={"ROOT": "typed-sensitive-alias"}, public_env=("ROOT",)
            )
        ),
    )
    payload, slots = _configuration(operation)
    assert {slot.pointer for slot in slots} == {
        "/params/value",
        "/environments/local/env/ROOT",
    }
    assert "typed-sensitive-alias" not in str(payload)
