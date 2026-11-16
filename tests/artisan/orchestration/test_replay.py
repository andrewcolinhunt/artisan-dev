"""Public replay preflight preserves trusted code, private values and source ownership."""

from __future__ import annotations

import importlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import cast
from unittest.mock import Mock

import polars as pl
import pytest
from fixtures.retained_pipeline import NativePipelineRunner
from pydantic import BaseModel, Field, SecretStr, ValidationError

from artisan.errors import ArtisanError, StoreIntegrityError
from artisan.execution.recording.commands import CommandRecorder
from artisan.operations.base import OperationDefinition
from artisan.operations.examples import DataGenerator
from artisan.orchestration import PipelineManager
from artisan.orchestration.curator_replay import prepare_curator_replay
from artisan.schemas import ArtifactResult, MetricArtifact
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.storage import ArtifactStore
from artisan.storage.core.execution_source import (
    CuratorExecutionSource,
    read_execution_source,
)
from artisan.storage.io.retained_commit import RetainedCommitter

replay = importlib.import_module("artisan.orchestration.replay")
curator_replay = importlib.import_module("artisan.orchestration.curator_replay")


class PrivateCurator(OperationDefinition):
    name = "public_replay_private_curator"

    class Params(BaseModel):
        """Private credential and ordinary configuration.

        Attributes:
            password: A caller-supplied private credential.
            count: An ordinary scientific setting.
        """

        password: SecretStr = Field(validation_alias="supplied_password")
        count: int = 3

    params: Params

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        result = ArtifactResult()
        result.add_artifact(
            "metrics",
            MetricArtifact.draft(
                {"value": self.params.count}, "metric.json", step_number
            ),
            sources={},
        )
        return result


@dataclass(frozen=True)
class Case:
    committer: RetainedCommitter
    source: CuratorExecutionSource
    execution_id: str
    pipeline_run_id: str
    root: Path

    @property
    def snapshot(self) -> ReplaySnapshot:
        return ReplaySnapshot.model_validate_json(
            cast("str", self.source.execution_record["replay_snapshot"])
        )


@pytest.fixture
def case(tmp_path: Path) -> Case:
    with PipelineManager.create(
        "replay-preflight",
        str(tmp_path / "replay-store"),
        str(tmp_path / "source-staging"),
        working_root=str(tmp_path / "source-work"),
    ) as manager:
        result = manager.run(
            PrivateCurator,
            params={"supplied_password": "source-private-value"},
            step_runner=NativePipelineRunner(manager.committer),
        )
        assert result.status == StepStatus.SUCCEEDED, result.error
        staged = manager.executions[0].handles[0].result()
        assert staged.execution_run_id is not None
        source = read_execution_source(manager.committer, staged.execution_run_id)
        assert isinstance(source, CuratorExecutionSource)
    return Case(
        manager.committer,
        source,
        staged.execution_run_id,
        manager.config.pipeline_run_id,
        tmp_path,
    )


def _prepare(case: Case) -> OperationDefinition:
    return prepare_curator_replay(
        case.committer,
        case.execution_id,
        operation_class=PrivateCurator,
        configuration_replacements={"/params/password": "replacement-private-value"},
        runtime_replacements={},
    ).operation


def _replace_optional_snapshot(
    case: Case, monkeypatch: pytest.MonkeyPatch, encoded: str | None
) -> None:
    source = replace(
        case.source,
        execution_record={**case.source.execution_record, "replay_snapshot": encoded},
    )
    monkeypatch.setattr(curator_replay, "read_execution_source", lambda *_: source)


def test_typed_private_replacement_runs_without_disclosure(case: Case) -> None:
    restored = _prepare(case)
    assert isinstance(restored, PrivateCurator)
    assert restored.params.password.get_secret_value() == "replacement-private-value"
    outcome = replay.replay_execution(
        case.execution_id,
        delta_root=case.committer.delta_root,
        operation=PrivateCurator,
        step_runner=NativePipelineRunner(case.committer),
        configuration_replacements={"/params/password": "replacement-private-value"},
        working_root=case.root / "diagnostic-work",
        staging_root=str(case.root / "diagnostic-staging"),
    )
    assert outcome.step_result.status == StepStatus.SUCCEEDED
    assert outcome.execution_run_id != case.execution_id
    assert "replacement-private-value" not in outcome.model_dump_json()
    for document in Path(case.committer.delta_root).rglob("*.json"):
        assert "replacement-private-value" not in document.read_text()


@pytest.mark.parametrize("bad", [None, "{}", "not-json"])
def test_malformed_optional_snapshot_is_sanitized_before_attempt(
    case: Case, monkeypatch: pytest.MonkeyPatch, bad: str | None
) -> None:
    _replace_optional_snapshot(case, monkeypatch, bad)
    manager = Mock(side_effect=AssertionError("preflight created an attempt"))
    monkeypatch.setattr(replay, "PipelineManager", manager)
    with pytest.raises((ArtisanError, StoreIntegrityError)) as caught:
        replay.replay_execution(
            case.execution_id,
            delta_root=case.committer.delta_root,
            operation=PrivateCurator,
            step_runner=NativePipelineRunner(case.committer),
        )
    assert caught.value.__cause__ is None
    manager.assert_not_called()


def test_unavailable_snapshot_remains_unavailable(
    case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    _replace_optional_snapshot(
        case,
        monkeypatch,
        ReplaySnapshot.unavailable("capture_failed").model_dump_json(),
    )
    with pytest.raises(ArtisanError) as caught:
        _prepare(case)
    assert caught.value.code == "replay_evidence_unavailable"


@pytest.mark.parametrize("field", ["step_run_id", "execution_spec_id"])
def test_optional_snapshot_must_match_required_source(
    case: Case, monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    payload = case.snapshot.model_dump(mode="json")
    payload["source"][field] = "f" * 32
    _replace_optional_snapshot(case, monkeypatch, json.dumps(payload))
    with pytest.raises(StoreIntegrityError, match="required source admission"):
        _prepare(case)


def test_nested_defaults_cannot_be_silently_added(
    case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = case.snapshot.model_dump(mode="json")
    del payload["operation"]["configuration"]["params"]["count"]
    _replace_optional_snapshot(case, monkeypatch, json.dumps(payload))
    with pytest.raises(ArtisanError) as caught:
        _prepare(case)
    assert caught.value.code == "replay_configuration_invalid"


@pytest.mark.parametrize(
    "pointer", ["/params/missing/value", "/params/count", "/params/~2invalid"]
)
def test_replacement_slot_requires_an_existing_redacted_value(
    case: Case, pointer: str
) -> None:
    payload = case.snapshot.model_dump(mode="json")
    payload["required_replacements"] = [{"pointer": pointer}]
    with pytest.raises(ValidationError, match="redacted"):
        ReplaySnapshot.model_validate(payload)


@pytest.mark.parametrize(
    "replacement", [{}, {"/params/password": "private", "/params/count": 8}]
)
def test_replacement_keys_are_exact_before_any_new_owner(
    case: Case, replacement: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = Mock(side_effect=AssertionError("replacement validation created owner"))
    monkeypatch.setattr(replay, "PipelineManager", manager)
    with pytest.raises(ArtisanError) as caught:
        replay.replay_execution(
            case.execution_id,
            delta_root=case.committer.delta_root,
            operation=PrivateCurator,
            step_runner=NativePipelineRunner(case.committer),
            configuration_replacements=replacement,
        )
    assert caught.value.code == "replay_replacement_required"
    manager.assert_not_called()


def test_invalid_typed_replacement_has_no_private_validation_cause(case: Case) -> None:
    value = {"private": "replacement-must-not-appear"}
    with pytest.raises(ArtisanError) as caught:
        prepare_curator_replay(
            case.committer,
            case.execution_id,
            operation_class=PrivateCurator,
            configuration_replacements={"/params/password": value},
            runtime_replacements={},
        )
    assert caught.value.code == "replay_configuration_invalid"
    assert "replacement-must-not-appear" not in json.dumps(caught.value.to_dict())
    assert caught.value.__cause__ is None


def test_explicit_live_root_does_not_import_its_recorded_module(
    case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = importlib.import_module

    def checked_import(name: str, package: str | None = None) -> object:
        assert name != PrivateCurator.__module__
        return original(name, package)

    monkeypatch.setattr(importlib, "import_module", checked_import)
    assert isinstance(_prepare(case), PrivateCurator)


@pytest.mark.parametrize("allow_code_change", [False, True])
def test_recorded_module_cannot_change_required_code_authority(
    case: Case, monkeypatch: pytest.MonkeyPatch, allow_code_change: bool
) -> None:
    payload = case.snapshot.model_dump(mode="json")
    payload["operation"]["identity"]["module"] = "untrusted.application.ops"
    _replace_optional_snapshot(case, monkeypatch, json.dumps(payload))
    with pytest.raises(StoreIntegrityError, match="required source admission"):
        prepare_curator_replay(
            case.committer,
            case.execution_id,
            operation_class=PrivateCurator,
            configuration_replacements={"/params/password": "private"},
            runtime_replacements={},
            allow_code_change=allow_code_change,
        )


@pytest.mark.parametrize("identifier", ["unknown", "step", "pipeline"])
def test_nonexecution_selectors_are_structured_not_new_attempts(
    case: Case, identifier: str
) -> None:
    identity = {
        "unknown": "missing-execution",
        "step": case.source.admission.record.step_run_id,
        "pipeline": case.pipeline_run_id,
    }[identifier]
    with pytest.raises(ArtisanError) as caught:
        replay.replay_execution(
            identity,
            delta_root=case.committer.delta_root,
            operation=PrivateCurator,
            step_runner=NativePipelineRunner(case.committer),
        )
    assert caught.value.code == "replay_execution_not_found"


def test_omitted_remote_runner_never_silently_selects_local(case: Case) -> None:
    with pytest.raises(ArtisanError, match="configured provider") as caught:
        replay.replay_execution(
            case.execution_id,
            delta_root=case.committer.delta_root,
            operation=PrivateCurator,
        )
    assert caught.value.code == "replay_dependency_unavailable"


@pytest.mark.parametrize("which", ["working", "staging"])
def test_replay_roots_cannot_overlap_managed_store(case: Case, which: str) -> None:
    arguments = (
        {"working_root": Path(case.committer.files_root)}
        if which == "working"
        else {"staging_root": case.committer.delta_root}
    )
    with pytest.raises(ValueError, match="separate"):
        replay.replay_execution(
            case.execution_id,
            delta_root=case.committer.delta_root,
            operation=PrivateCurator,
            step_runner=NativePipelineRunner(case.committer),
            **arguments,
        )


def test_live_root_is_required_before_opening_store(tmp_path: Path) -> None:
    path = tmp_path / "absent-store"
    with pytest.raises(ArtisanError, match="already imported"):
        replay.replay_execution(
            "unknown", delta_root=str(path), operation="untrusted.ops.Root"
        )
    assert not path.exists()


def test_wrong_curator_root_cannot_be_replaced_by_supplied_nested_definition(
    case: Case,
) -> None:
    with pytest.raises(ArtisanError):
        replay.replay_execution(
            case.execution_id,
            delta_root=case.committer.delta_root,
            operation=DataGenerator,
            definitions=(PrivateCurator,),
            step_runner=NativePipelineRunner(case.committer),
        )


def test_structured_and_scalar_replacement_values_are_redacted() -> None:
    recorder = CommandRecorder()
    values = {"key": "fresh-object-secret", "codes": [784219, False]}
    recorder.add_sensitive_data(values)
    assert recorder.sanitize_data(values) == {
        "key": "<redacted>",
        "codes": ["<redacted>", "<redacted>"],
    }
