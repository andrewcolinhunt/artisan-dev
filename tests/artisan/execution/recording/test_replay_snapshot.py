"""Curator admission capture keeps exact occurrence context and private slots."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import ClassVar

import pytest
from fixtures.curator_cache import DynamicCurator
from fixtures.retained_acceptance import open_committer
from fixtures.retained_curators import admit_curator
from pydantic import BaseModel, Field, SecretStr

from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording.curator_replay import capture_curator_replay
from artisan.execution.recording.replay_snapshot import (
    ReplayBuilder,
    _configuration,
    capture_replay,
    current_replay_builder,
)
from artisan.schemas.artifact.metric import MetricArtifact
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
from artisan.storage import ArtifactStore


class AssociatedCurator(DynamicCurator):
    name = "snapshot_associated_curator"
    runtime_defined_inputs = True

    class InputRole(StrEnum):
        data = "data"

    inputs: ClassVar = {
        "data": InputSpec(artifact_type="csv", with_associated=("metric",))
    }


class PrivateCurator(DynamicCurator):
    name = "snapshot_private_curator"

    class Params(BaseModel):
        """Private and ordinary configuration.

        Attributes:
            private: Typed credential under an ordinary name.
            alias: Ordinary scalar that may equal a private value.
            mapping: Mapping whose keys must remain public.
            count: Ordinary default preserved in capture.
        """

        private: SecretStr = Field(validation_alias="supplied")
        alias: str = "public"
        mapping: dict[str, str] = Field(default_factory=dict)
        count: int = 7

    params: Params


@pytest.fixture
def unit(tmp_path: Path) -> ExecutionUnit:
    storage = StorageConfig()
    committer = open_committer(
        str(tmp_path / "snapshot-store"), storage.filesystem(), storage, initialize=True
    )
    operation = AssociatedCurator()
    _, admission, _ = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, software_digest="b" * 64
    )
    admission = CuratorAdmission.model_validate(
        {
            **admission.model_dump(),
            "inputs": (
                CuratorInputRole(
                    role="data",
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
    runtime = RuntimeEnvironment(
        delta_root=committer.delta_root, staging_root=str(tmp_path / "staging")
    )
    snapshot = capture_curator_replay(operation, admission, runtime, {})
    return ExecutionUnit(
        operation=operation,
        inputs={"data": [item.artifact_id for item in admission.inputs[0].members]},
        execution_spec_id=admission.digest,
        replay_snapshot=snapshot,
        associated_inputs=snapshot.associated,
    )


def test_capture_preserves_supplied_admission_and_repeated_positions(
    unit: ExecutionUnit,
) -> None:
    with capture_replay(unit) as builder:
        snapshot = builder.snapshot
        assert snapshot.source.execution_spec_id == unit.execution_spec_id
        assert [item.artifact_id for item in snapshot.inputs["data"]] == unit.inputs[
            "data"
        ]
        assert [item.position for item in snapshot.inputs["data"]] == [0, 1, 2]
        assert [item.artifact_ids for item in snapshot.associated] == [[], []]
        assert snapshot.group_ids is None
        assert current_replay_builder() is builder
    assert current_replay_builder() is None


def test_runtime_associations_cannot_replace_captured_empty_membership(
    unit: ExecutionUnit,
) -> None:
    unit.associated_inputs = [
        item.model_copy(update={"artifact_ids": ["e" * 32]})
        for item in unit.associated_inputs
    ]
    with (
        pytest.raises(ValueError, match="Runtime associations differ"),
        capture_replay(unit),
    ):
        pytest.fail("changed membership entered capture")


def test_missing_admission_evidence_never_opens_an_ambient_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("capture discovered an ambient store")

    monkeypatch.setattr(ArtifactStore, "__init__", forbidden)
    with capture_replay(ExecutionUnit(operation=DynamicCurator())) as builder:
        assert builder.snapshot == ReplaySnapshot.unavailable(
            "required_admission_unavailable"
        )


def test_capture_rejects_association_changes_inside_supplied_snapshot(
    unit: ExecutionUnit,
) -> None:
    payload = unit.replay_snapshot.model_dump(mode="json")
    payload["associated"][0]["artifact_ids"] = ["e" * 32]
    with pytest.raises(ValueError, match="frozen cache inputs"):
        ReplaySnapshot.model_validate(payload)


def test_association_owner_order_does_not_change_membership() -> None:
    artifact = MetricArtifact.draft({"value": 1}, "metric.json", 0).finalize()
    captured = {("a" * 32, "metric"): [], ("b" * 32, "metric"): [artifact]}
    builder = ReplayBuilder(ReplaySnapshot.unavailable("fixture"))
    builder.record_associated(captured)
    snapshot = builder.snapshot
    prior_order = snapshot.model_copy(
        update={"associated": list(reversed(snapshot.associated))}
    )
    replay = ReplayBuilder(prior_order)
    replay.record_associated(captured)
    assert replay.snapshot.associated == snapshot.associated


def test_private_slots_precede_pydantic_masking_and_preserve_defaults() -> None:
    operation = PrivateCurator(params={"supplied": "typed-private-value"})
    payload, slots = _configuration(operation)
    assert payload["params"] == {
        "private": None,
        "alias": "public",
        "mapping": {},
        "count": 7,
    }
    assert [slot.pointer for slot in slots] == ["/params/private"]
    assert "typed-private-value" not in str(payload)
    assert "**********" not in str(payload)


def test_ordinary_alias_of_private_value_gets_its_own_replacement_slot() -> None:
    operation = PrivateCurator(
        params={"supplied": "same-private-value", "alias": "same-private-value"}
    )
    payload, slots = _configuration(operation)
    assert {slot.pointer for slot in slots} == {"/params/private", "/params/alias"}
    assert "same-private-value" not in str(payload)


def test_transient_runtime_values_redact_ordinary_configuration_aliases() -> None:
    operation = PrivateCurator(
        params={"supplied": "private-value", "alias": "runtime-value"}
    )
    payload, slots = _configuration(operation, ("runtime-value",))
    assert {slot.pointer for slot in slots} == {"/params/private", "/params/alias"}
    assert "runtime-value" not in str(payload)


def test_private_mapping_key_cannot_become_unrestorable_configuration() -> None:
    operation = PrivateCurator(
        params={"supplied": "private-value", "mapping": {"runtime-value": "entry"}}
    )
    with pytest.raises(ValueError, match="Sensitive configuration key") as caught:
        _configuration(operation, ("runtime-value",))
    assert "runtime-value" not in str(caught.value)
