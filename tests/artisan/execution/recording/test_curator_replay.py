"""Qualified curator capture keeps exact inputs and omits private configuration."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import ClassVar

import pytest
from fixtures.curator_cache import DynamicCurator
from fixtures.retained_acceptance import open_committer
from fixtures.retained_curators import admit_curator
from fsspec.implementations.local import LocalFileSystem

from artisan.execution.curator_definition import capture_curator_definition
from artisan.execution.recording.curator_replay import capture_curator_replay
from artisan.schemas.execution.curator_admission import (
    CuratorAdmission,
    CuratorInputAssociation,
    CuratorInputMember,
    CuratorInputRole,
)
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.specs.input_spec import InputSpec


class AssociatedCurator(DynamicCurator):
    name = "replay_context_curator"

    class InputRole(StrEnum):
        source = "source"

    inputs: ClassVar = {
        "source": InputSpec(artifact_type="csv", with_associated=("metric",))
    }


class PrivateCurator(DynamicCurator):
    name = "replay_private_curator"

    class Params(DynamicCurator.Params):
        """Configuration containing synthetic private values.

        Attributes:
            empty: Emit no populated output role.
            fail: Return a scientific failure.
            password: Credential that must be replaced for replay.
            endpoint: Credential-bearing locator that must be replaced.
            literal: Parameter also present in private runtime injections.
        """

        password: str = "synthetic-password"
        endpoint: str = "https://synthetic-user:synthetic-password@example.invalid/data"
        literal: str = "private runtime value"

    params: Params = Params()


@pytest.fixture
def runtime(tmp_path: Path) -> RuntimeEnvironment:
    return RuntimeEnvironment(
        delta_root=str(tmp_path / "store"),
        staging_root=str(tmp_path / "staging"),
        files_root=str(tmp_path / "files"),
    )


def _admission(
    runtime: RuntimeEnvironment, operation: DynamicCurator
) -> CuratorAdmission:
    committer = open_committer(
        runtime.delta_root, LocalFileSystem(), StorageConfig(), initialize=True
    )
    _, admission, _ = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, software_digest="b" * 64
    )
    return admission


def test_capture_preserves_repeated_inputs_and_empty_association_membership(
    runtime: RuntimeEnvironment, monkeypatch: pytest.MonkeyPatch
) -> None:
    operation = AssociatedCurator()
    admission = _admission(runtime, operation)
    admission = CuratorAdmission.model_validate(
        {
            **admission.model_dump(),
            "inputs": (
                CuratorInputRole(
                    role="source",
                    members=tuple(
                        CuratorInputMember(artifact_id=identifier, artifact_type="csv")
                        for identifier in ("c" * 32, "d" * 32, "c" * 32)
                    ),
                ),
            ),
            "associations": tuple(
                CuratorInputAssociation(
                    primary_id=identifier, artifact_type="metric", artifact_ids=()
                )
                for identifier in ("c" * 32, "d" * 32)
            ),
        }
    )
    monkeypatch.setattr(
        "artisan.registry.resolve.resolve_operation",
        lambda *a, **kw: pytest.fail("Capture must not resolve stored names"),
    )
    snapshot = capture_curator_replay(operation, admission, runtime, {})
    assert snapshot.status == "requires_operation_class"
    assert snapshot.source is not None
    assert snapshot.source.execution_spec_id == admission.digest
    assert [item.artifact_id for item in snapshot.inputs["source"]] == [
        "c" * 32,
        "d" * 32,
        "c" * 32,
    ]
    assert [item.position for item in snapshot.inputs["source"]] == [0, 1, 2]
    assert [item.artifact_ids for item in snapshot.associated] == [[], []]
    assert snapshot.required_runtime_keys == ()
    assert snapshot.read_semantics == "explicit_inputs_and_current_committed_store"
    assert ReplaySnapshot.model_validate_json(snapshot.model_dump_json()) == snapshot


@pytest.mark.parametrize("replaced", [False, True])
def test_capture_redacts_configuration_and_keeps_runtime_replacement_keys(
    runtime: RuntimeEnvironment,
    replaced: bool,
) -> None:
    operation = PrivateCurator()
    admission = _admission(runtime, operation)
    snapshot = capture_curator_replay(
        operation,
        admission,
        runtime,
        {
            "SYNTHETIC_LITERAL": "different-runtime-replacement"
            if replaced
            else operation.params.literal
        },
        sensitive_values=(operation.params.literal,) if replaced else (),
    )
    assert snapshot.operation is not None
    encoded = snapshot.model_dump_json()
    assert operation.params.password not in encoded
    assert operation.params.endpoint not in encoded
    assert operation.params.literal not in encoded
    assert snapshot.required_runtime_keys == ("SYNTHETIC_LITERAL",)
    assert {slot.pointer for slot in snapshot.required_replacements} >= {
        "/params/password",
        "/params/endpoint",
        "/params/literal",
    }


def test_capture_failure_is_optional_but_definition_mismatch_is_not(
    runtime: RuntimeEnvironment, monkeypatch: pytest.MonkeyPatch
) -> None:
    operation = DynamicCurator()
    admission = _admission(runtime, operation)

    def unavailable(*args: object, **kwargs: object) -> None:
        message = "sensitive configuration failure"
        raise ValueError(message)

    monkeypatch.setattr(
        "artisan.execution.recording.curator_replay._configuration", unavailable
    )
    snapshot = capture_curator_replay(operation, admission, runtime, {})
    assert snapshot.status == "unavailable"
    assert "sensitive configuration failure" not in snapshot.model_dump_json()
    assert admission.definition == capture_curator_definition(operation)
    operation.params.empty = True
    with pytest.raises(ValueError, match="differs from its exact admitted"):
        capture_curator_replay(operation, admission, runtime, {})


@pytest.mark.parametrize("keys", [("A", "A"), ("Z", "A"), ("invalid-key",)])
def test_runtime_replacement_keys_reject_ambiguous_or_invalid_capture(
    runtime: RuntimeEnvironment, keys: tuple[str, ...]
) -> None:
    operation = DynamicCurator()
    snapshot = capture_curator_replay(
        operation, _admission(runtime, operation), runtime, {}
    )
    with pytest.raises(ValueError):
        ReplaySnapshot.model_validate(
            {**snapshot.model_dump(), "required_runtime_keys": keys}
        )
