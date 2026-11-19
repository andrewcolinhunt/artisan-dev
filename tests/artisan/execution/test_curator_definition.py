"""Closed curator configuration is verified before authored work, without secrets."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from enum import StrEnum
from pathlib import Path
from typing import ClassVar

import polars as pl
import pytest
from fixtures.replay_operations import Increment
from fixtures.retained_acceptance import open_committer
from fixtures.retained_curators import admit_curator
from fsspec.implementations.local import LocalFileSystem
from pydantic import BaseModel, ConfigDict, ValidationError

from artisan.execution.curator_definition import (
    capture_curator_definition,
    verify_curator_definition,
)
from artisan.execution.executors.curator import run_curator_flow
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.base import OperationDefinition
from artisan.schemas import (
    ArtifactEmission,
    ArtifactStreamResult,
    CSVArtifact,
    InputSpec,
    OutputSpec,
    PassthroughResult,
)
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.curator_admission import CuratorAdmission
from artisan.schemas.execution.curator_definition import CuratorDefinitionContract
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_config import WorkerConfig
from artisan.schemas.operation_config.environment import Environment
from artisan.storage.cache.curator_dependencies import CuratorReadRecorder
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_view import CuratorView
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


class ConfiguredCurator(OperationDefinition):
    name = "configured_curator"

    class InputRole(StrEnum):
        config = "config"

    class OutputRole(StrEnum):
        selected = "selected"

    inputs: ClassVar = {
        "config": InputSpec(artifact_type="config", materialize=False, required=False)
    }
    outputs: ClassVar = {
        "selected": OutputSpec(
            artifact_type="config", required=False, derives_from={"inputs": ["config"]}
        )
    }

    class Params(BaseModel):
        """Fixture parameters.

        Attributes:
            token: Synthetic sensitive configuration.
            tags: Unordered scalar selection.
        """

        model_config = ConfigDict(strict=True)
        token: str = "synthetic-secret-value"
        tags: frozenset[str] = frozenset({"beta", "alpha"})

    params: Params = Params()

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> PassthroughResult:
        return PassthroughResult(passthrough={"selected": []})


def test_curator_contract_hashes_canonical_parameters_without_storing_values() -> None:
    original = ConfiguredCurator()
    contract = capture_curator_definition(original)
    assert contract.kind == "curator"
    assert original.params.token not in contract.model_dump_json()
    assert "params_json" not in type(contract).model_fields
    reordered = ConfiguredCurator(
        params=ConfiguredCurator.Params(tags=frozenset({"alpha", "beta"}))
    )
    assert capture_curator_definition(reordered) == contract
    changed = ConfiguredCurator(params=ConfiguredCurator.Params(token="another-secret"))
    assert capture_curator_definition(changed).params_digest != contract.params_digest
    with pytest.raises(ValueError, match="exact admitted configuration"):
        verify_curator_definition(changed, contract)
    assert (
        CuratorDefinitionContract.model_validate_json(contract.model_dump_json())
        == contract
    )
    with pytest.raises(ValidationError, match="extra_forbidden"):
        CuratorDefinitionContract.model_validate(
            {**contract.model_dump(), "params_json": "{}"}
        )


def test_existing_curator_output_contract_bytes_and_digest_stay_unchanged() -> None:
    contract = capture_curator_definition(ConfiguredCurator())
    historical = contract.model_dump(mode="json")
    historical["outputs"] = [
        {
            "role": "selected",
            "artifact_type": "config",
            "required": False,
            "parent_kind": "inputs",
            "parent_roles": ["config"],
        }
    ]
    encoded = canonical_json_bytes(historical)
    restored = CuratorDefinitionContract.model_validate_json(encoded)
    assert canonical_json_bytes(restored.model_dump(mode="json")) == encoded
    assert (
        restored.digest
        == contract.digest
        == compute_content_digest(b"artisan.curator-definition-v1\0" + encoded)
    )


def test_curator_contract_records_both_parent_namespaces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ConfiguredCurator,
        "outputs",
        {
            "source": OutputSpec(artifact_type="config", derives_from={"inputs": []}),
            "selected": OutputSpec(
                artifact_type="config",
                required=False,
                derives_from={"inputs": ["config"], "outputs": ["source"]},
            ),
        },
    )
    operation = ConfiguredCurator()
    contract = capture_curator_definition(operation)
    assert (
        CuratorDefinitionContract.model_validate_json(contract.model_dump_json())
        == contract
    )
    for parents in ({"inputs": ["config"]}, {"outputs": ["source"]}):
        monkeypatch.setattr(
            ConfiguredCurator,
            "outputs",
            {
                "source": OutputSpec(
                    artifact_type="config", derives_from={"inputs": []}
                ),
                "selected": OutputSpec(
                    artifact_type="config", required=False, derives_from=parents
                ),
            },
        )
        with pytest.raises(ValueError, match="exact admitted configuration"):
            verify_curator_definition(operation, contract)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("artifact_type", "csv"),
        ("required", True),
        ("hydrate", False),
        ("materialize", True),
        ("materialize_as", ".json"),
        ("resolve_references", True),
        ("with_associated", ("metric",)),
    ],
)
def test_every_input_behavior_participates_in_the_contract(
    monkeypatch: pytest.MonkeyPatch, field: str, value: object
) -> None:
    operation = ConfiguredCurator()
    contract = capture_curator_definition(operation)
    monkeypatch.setattr(
        ConfiguredCurator,
        "inputs",
        {
            "config": ConfiguredCurator.inputs["config"].model_copy(
                update={field: value}
            )
        },
    )
    with pytest.raises(ValueError, match="exact admitted configuration"):
        verify_curator_definition(operation, contract)


@pytest.mark.parametrize(
    "change",
    [
        {"artifact_type": "any"},
        {"required": True},
        {"derives_from": {"outputs": ["selected"]}},
        {"derives_from": None},
    ],
)
def test_output_presence_types_and_lineage_are_bound(
    monkeypatch: pytest.MonkeyPatch, change: dict[str, object]
) -> None:
    operation = ConfiguredCurator()
    contract = capture_curator_definition(operation)
    monkeypatch.setattr(
        ConfiguredCurator,
        "outputs",
        {"selected": ConfiguredCurator.outputs["selected"].model_copy(update=change)},
    )
    with pytest.raises(ValueError, match="exact admitted configuration"):
        verify_curator_definition(operation, contract)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("name", "changed"),
        ("version", "new"),
        ("cacheable", False),
        ("runtime_defined_inputs", True),
        ("hydrate_inputs", False),
        ("independent_input_streams", True),
        ("per_artifact_dispatch", False),
    ],
)
def test_curator_behavior_and_identity_are_bound(
    monkeypatch: pytest.MonkeyPatch, field: str, value: object
) -> None:
    operation = ConfiguredCurator()
    contract = capture_curator_definition(operation)
    monkeypatch.setattr(ConfiguredCurator, field, value)
    with pytest.raises(ValueError, match="exact admitted configuration"):
        verify_curator_definition(operation, contract)


def test_invocation_policy_is_bound_but_description_is_not(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation = ConfiguredCurator()
    contract = capture_curator_definition(operation)
    monkeypatch.setattr(ConfiguredCurator, "description", "New documentation")
    monkeypatch.setattr(
        ConfiguredCurator,
        "inputs",
        {
            "config": ConfiguredCurator.inputs["config"].model_copy(
                update={"description": "New input explanation"}
            )
        },
    )
    verify_curator_definition(operation, contract)
    operation.group_by = GroupByStrategy.ZIP
    with pytest.raises(ValueError, match="exact admitted configuration"):
        verify_curator_definition(operation, contract)


def test_noncurator_and_separate_command_environment_are_rejected(
    tmp_path: Path,
) -> None:
    with pytest.raises(TypeError, match="curator definition"):
        capture_curator_definition(Increment())
    operation = ConfiguredCurator(
        environment=Environment(manifest=tmp_path / "pixi.toml")
    )
    with pytest.raises(ValueError, match="only to command"):
        capture_curator_definition(operation)


def test_entry_rejects_changed_contract_before_author_or_process_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = StorageConfig()
    committer = open_committer(
        str(tmp_path / "candidate"), LocalFileSystem(), storage, initialize=True
    )
    operation = ConfiguredCurator()
    coordinator, admission, reference = admit_curator(
        committer,
        operation,
        {},
        step_run_id="a" * 32,
        software_digest="e" * 64,
    )
    operation.params.token = "changed-secret"
    monkeypatch.setattr(
        ConfiguredCurator,
        "execute_curator",
        lambda *_: pytest.fail("Changed configuration must fail before authored entry"),
    )
    view = CuratorView(committer, admission.view)
    with pytest.raises(ValueError, match="exact admitted configuration"):
        run_curator_flow(
            ExecutionUnit(
                operation=operation,
                inputs={},
                step_number=admission.record.step_number,
                step_run_id=reference.step_run_id,
                execution_spec_id=admission.digest,
                replay_snapshot=ReplaySnapshot.unavailable("contract-fixture"),
            ),
            WorkerConfig(
                delta_root=committer.delta_root,
                staging_root=coordinator.staging.staging_dir,
                files_root=committer.files_root,
                storage=storage,
            ),
            curator_view=view,
            read_recorder=CuratorReadRecorder(view, admission),
        )


class StatefulStreamCurator(OperationDefinition):
    name = "stateful_stream_curator"

    class OutputRole(StrEnum):
        data = "data"

    outputs: ClassVar = {
        "data": OutputSpec(artifact_type="csv", derives_from={"inputs": []})
    }

    class Params(BaseModel):
        """Fixture state at invocation entry.

        Attributes:
            cursor: Initial internal stream position.
        """

        cursor: int = 0

    params: Params = Params()

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactStreamResult:
        def items() -> Iterator[ArtifactEmission]:
            for _ in range(2):
                self.params.cursor += 1
                yield ArtifactEmission(
                    role="data",
                    artifact=CSVArtifact.draft(
                        f"cursor\n{self.params.cursor}\n".encode(),
                        f"row-{self.params.cursor}.csv",
                        step_number,
                    ),
                    sources={},
                )

        return ArtifactStreamResult(items=items())


def test_contract_is_checked_once_before_mutable_stream_consumption(
    tmp_path: Path,
) -> None:
    storage = StorageConfig()
    committer = open_committer(
        str(tmp_path / "candidate"), LocalFileSystem(), storage, initialize=True
    )
    operation = StatefulStreamCurator()
    coordinator, admission, reference = admit_curator(
        committer,
        operation,
        {},
        step_run_id="a" * 32,
        software_digest="e" * 64,
    )
    view = CuratorView(committer, admission.view)
    recorder = CuratorReadRecorder(view, admission)
    staged = run_curator_flow(
        ExecutionUnit(
            operation=operation,
            inputs={},
            step_number=admission.record.step_number,
            step_run_id=reference.step_run_id,
            execution_spec_id=admission.digest,
            replay_snapshot=ReplaySnapshot.unavailable("mutable-stream-fixture"),
        ),
        WorkerConfig(
            delta_root=committer.delta_root,
            staging_root=coordinator.staging.staging_dir,
            files_root=committer.files_root,
            storage=storage,
        ),
        curator_view=view,
        read_recorder=recorder,
    )
    assert staged.success, staged.error
    assert len(staged.artifact_ids) == 2
    assert operation.params.cursor == 2
    assert capture_curator_definition(operation) != admission.definition
    assert recorder.proof.definition_digest == admission.definition_digest


@pytest.mark.parametrize(
    "change", ["identity", "intrinsic_policy", "output_type", "dynamic"]
)
def test_qualified_admission_rejects_metadata_that_contradicts_definition(
    tmp_path: Path, change: str
) -> None:
    storage = StorageConfig()
    committer = open_committer(
        str(tmp_path / "candidate"), LocalFileSystem(), storage, initialize=True
    )
    _, admission, _ = admit_curator(
        committer,
        ConfiguredCurator(),
        {},
        step_run_id="a" * 32,
        software_digest="e" * 64,
    )
    payload = admission.model_dump(mode="json")
    if change == "identity":
        payload["record"]["operation_class"] = "different.RegisteredClass"
    elif change == "intrinsic_policy":
        payload["cacheable"] = False
    elif change == "output_type":
        payload["record"]["output_types_json"] = '{"selected":"csv"}'
    else:
        payload["record"]["compute_options_json"] = '{"dynamic_output_roles":true}'
    with pytest.raises(ValidationError, match="closed definition"):
        CuratorAdmission.model_validate(payload)
    payload = admission.model_dump(mode="json")
    payload["record"]["params_json"] = '{"token":"[REDACTED]","tags":["alpha","beta"]}'
    redacted = CuratorAdmission.model_validate(payload)
    assert redacted.definition_digest == admission.definition_digest
    assert ConfiguredCurator.Params().token not in redacted.model_dump_json()
