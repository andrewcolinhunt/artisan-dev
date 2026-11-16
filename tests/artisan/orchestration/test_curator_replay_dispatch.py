"""Diagnostic curator dispatch preserves frozen inputs and current store reads."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import ClassVar
from unittest.mock import Mock

import pytest
from fixtures.retained_acceptance import admit_case
from fixtures.retained_readers import Backend
from fsspec.implementations.local import LocalFileSystem
from pydantic import BaseModel

from artisan.errors import ArtisanError, StoreIntegrityError
from artisan.execution.assignment_transport import AssignmentDispatch
from artisan.execution.curator_worker import (
    AdmittedCurator,
    PreparedCuratorSoftware,
    execute_admitted_curator,
)
from artisan.operations.base import OperationDefinition
from artisan.orchestration.curator_replay import (
    CuratorReplayPreparation,
    curator_replay_record,
    prepare_curator_replay,
)
from artisan.orchestration.engine.retained_curator_executor import (
    RetainedCuratorExecution,
)
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas import (
    ArtifactResult,
    CuratorInput,
    InputSpec,
    LineageMapping,
    MetricArtifact,
    OutputSpec,
    RunnerResources,
)
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.execution_source import (
    CuratorExecutionSource,
    read_execution_source,
)
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_store import create_retained_store


class ReplayCurator(OperationDefinition):
    name = "current_store_replay_curator"

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        metrics = "metrics"

    class Params(BaseModel):
        """Synthetic configuration.

        Attributes:
            password: Private value required to reconstruct configuration.
            label: Ordinary field matching a private source runtime value.
            fail: Return a failed scientific result.
        """

        password: str = "original-private-value"
        label: str = "source-runtime-private"
        fail: bool = False

    params: Params = Params()
    inputs: ClassVar = {
        "source": InputSpec(
            artifact_type="csv", materialize=False, with_associated=("metric",)
        )
    }
    outputs: ClassVar = {
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["source"]}
        )
    }

    def execute_curator(
        self, inputs: CuratorInput, step_number: int, artifact_store: ArtifactStore
    ) -> ArtifactResult:
        if self.params.fail:
            return ArtifactResult(success=False, error="synthetic curator failure")
        primary = inputs["source"]["artifact_id"][0]
        content = {
            "captured": len(inputs.associated_artifacts(primary, "metric")),
            "current": len(
                artifact_store.get_associated({primary}, "metric").get(primary, [])
            ),
            "inputs": inputs["source"].height,
        }
        return ArtifactResult(
            artifacts={
                "metrics": [MetricArtifact.draft(content, "counts.json", step_number)]
            },
            lineage={
                "metrics": [
                    LineageMapping(
                        draft_index=0, source_artifact_id=primary, source_role="source"
                    )
                ]
            },
        )


class DirectRunner:
    """Exercise actual curator execution without claiming provider transport coverage."""

    name = "direct-curator-fixture"

    def __init__(self) -> None:
        self.software = Mock(spec=PreparedCuratorSoftware)
        self.software.software_digest = "e" * 64
        self.software.software_cacheable = True
        self.software.application_modules = ()
        self.launched: list[AdmittedCurator] = []

    def resolve_curator(
        self, *args: object, **kwargs: object
    ) -> PreparedCuratorSoftware:
        return self.software

    def launch_curator(
        self,
        curator: AdmittedCurator,
        *,
        software: PreparedCuratorSoftware,
        dispatch: AssignmentDispatch,
    ) -> Mock:
        self.launched.append(curator)
        staged = execute_admitted_curator(
            curator, software, dispatch, working_root=dispatch.working_root
        )
        handle = Mock()
        handle.admission = curator.admission
        handle.worker_owner = None
        handle.done.return_value = True
        handle.ownership_state.return_value = OwnershipState.CLOSED
        handle.result.return_value = staged
        return handle


@dataclass
class Case:
    committer: RetainedCommitter
    dispatch: AssignmentDispatch
    runner: DirectRunner
    source: CuratorExecutionSource
    execution_id: str


@pytest.fixture(params=["local", pytest.param("s3", marks=pytest.mark.s3)])
def backend_fs(request: pytest.FixtureRequest, tmp_path: Path) -> Backend:
    if request.param == "local":
        return LocalFileSystem(), StorageConfig(), str(tmp_path)
    return request.getfixturevalue("s3_fs")


@pytest.fixture
def case(backend_fs: Backend, tmp_path: Path) -> Case:
    fs, storage, root = backend_fs
    committer = create_retained_store(
        f"{root}/store",
        f"{root}/files",
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )
    _, receipt = admit_case(committer, tmp_path, kind="alias")
    committer.accept(receipt)
    inputs = RetainedReader(committer).read(receipt).output("data")
    dispatch = AssignmentDispatch(
        committer.delta_root,
        committer.files_root,
        f"{root}/staging",
        storage,
        tmp_path / "worker",
        {"SYNTHETIC_RUNTIME": "source-runtime-private"},
    )
    runner = DirectRunner()
    record = StepStartRecord(
        step_run_id="b" * 32,
        step_number=8,
        step_name="replayable-curator",
        operation_class=f"{ReplayCurator.__module__}.{ReplayCurator.__qualname__}",
        params_json='{"password": null, "label": null, "fail": false}',
        input_refs_json="{}",
        compute_backend=runner.name,
        compute_options_json='{"dynamic_output_roles": false}',
        output_roles_json='["metrics"]',
        output_types_json='{"metrics": "metric"}',
    )
    execution = RetainedCuratorExecution(
        record,
        operation=ReplayCurator(),
        resolve_inputs=lambda: {"source": inputs},
        tracker=RetainedStepTracker(committer, "source-run"),
        runner=runner,
        dispatch=dispatch,
        runner_resources=RunnerResources(),
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        result = execution.run(pool)
    assert result.status == StepStatus.SUCCEEDED, result.error
    staged = execution.handles[0].result()
    assert staged.execution_run_id is not None
    source = read_execution_source(committer, staged.execution_run_id)
    assert isinstance(source, CuratorExecutionSource)
    return Case(committer, dispatch, runner, source, staged.execution_run_id)


def _prepare(
    case: Case, *, allow_code_change: bool = False
) -> CuratorReplayPreparation:
    return prepare_curator_replay(
        case.committer,
        case.execution_id,
        operation_class=ReplayCurator,
        configuration_replacements={
            "/params/password": "replacement-private-value",
            "/params/label": "replacement-label-private",
        },
        runtime_replacements={"SYNTHETIC_RUNTIME": "replacement-runtime-private"},
        allow_code_change=allow_code_change,
    )


def _execute(case: Case, replay: CuratorReplayPreparation) -> RetainedCuratorExecution:
    execution = RetainedCuratorExecution(
        curator_replay_record(replay, runner_name=case.runner.name),
        operation=replay.operation,
        resolve_inputs=None,
        replay=replay,
        tracker=RetainedStepTracker(case.committer, "diagnostic-run"),
        runner=case.runner,
        dispatch=replace(case.dispatch, runtime_env=replay.runtime_env),
        runner_resources=RunnerResources(),
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        execution.run(pool)
    return execution


def test_replay_freezes_explicit_inputs_but_uses_current_store_and_new_ownership(
    case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "artisan.registry.resolve.resolve_operation",
        lambda *args, **kwargs: pytest.fail("Stored names must not import code"),
    )
    replay = _prepare(case)
    monkeypatch.setattr(
        "artisan.orchestration.engine.retained_curator_executor.lookup_curator",
        lambda *args, **kwargs: pytest.fail("Diagnostic replay must bypass cache"),
    )
    execution = _execute(case, replay)
    state = execution.tracker.current_state(execution.record.step_run_id)
    assert state.status == StepStatus.SUCCEEDED
    staged = execution.handles[0].result()
    assert staged.execution_run_id is not None
    assert staged.execution_run_id != case.execution_id
    selected = read_execution_source(case.committer, staged.execution_run_id)
    assert isinstance(selected, CuratorExecutionSource)
    assert selected.admission.inputs == case.source.admission.inputs
    assert selected.admission.associations == case.source.admission.associations
    assert selected.admission.view != case.source.admission.view
    result = CuratorReader(case.committer).read_result(
        selected.plan.step_run_id, selected.plan.plan_digest
    )
    assert json.loads(result.output("metrics")[0].artifact.content) == {
        "captured": 0,
        "current": 1,
        "inputs": 2,
    }
    snapshot = ReplaySnapshot.model_validate_json(
        selected.execution_record["replay_snapshot"]
    )
    assert snapshot.diagnostic is not None
    assert snapshot.diagnostic.source_execution_run_id == case.execution_id
    assert snapshot.diagnostic.source_execution_spec_id == case.source.admission.digest
    assert (
        snapshot.diagnostic.source_software_digest
        == snapshot.diagnostic.selected_software_digest
        == "e" * 64
    )
    assert snapshot.required_runtime_keys == ("SYNTHETIC_RUNTIME",)
    assert "replacement-private-value" not in snapshot.model_dump_json()
    assert "replacement-label-private" not in snapshot.model_dump_json()
    assert "replacement-runtime-private" not in snapshot.model_dump_json()
    assert "/params/label" in {item.pointer for item in snapshot.required_replacements}
    assert "replacement-label-private" not in json.dumps(
        dict(selected.execution_record), default=str
    )
    assert (
        lookup_curator(selected.plan.admission, reader=CuratorReader(case.committer))
        is None
    )


@pytest.mark.parametrize("missing", ["configuration", "runtime"])
def test_replay_requires_exact_private_replacements_without_admission(
    case: Case, missing: str
) -> None:
    before = set(case.committer.fs.find(case.committer.delta_root))
    with pytest.raises(ArtisanError) as error:
        prepare_curator_replay(
            case.committer,
            case.execution_id,
            operation_class=ReplayCurator,
            configuration_replacements={}
            if missing == "configuration"
            else {"/params/password": "secret"},
            runtime_replacements={}
            if missing == "runtime"
            else {"SYNTHETIC_RUNTIME": "secret"},
        )
    assert error.value.code == "replay_replacement_required"
    assert set(case.committer.fs.find(case.committer.delta_root)) == before


def test_software_change_requires_explicit_diagnostic_selection(case: Case) -> None:
    replay = _prepare(case)
    case.runner.software.software_digest = "f" * 64
    failed = _execute(case, replay)
    assert (
        failed.tracker.current_state(failed.record.step_run_id).status
        == StepStatus.FAILED
    )
    assert len(case.runner.launched) == 1
    changed = _execute(case, _prepare(case, allow_code_change=True))
    assert (
        changed.tracker.current_state(changed.record.step_run_id).status
        == StepStatus.SUCCEEDED
    )
    staged = changed.handles[0].result()
    source = read_execution_source(case.committer, staged.execution_run_id)
    assert isinstance(source, CuratorExecutionSource)
    snapshot = ReplaySnapshot.model_validate_json(
        source.execution_record["replay_snapshot"]
    )
    assert snapshot.diagnostic is not None
    assert snapshot.diagnostic.allow_code_change
    assert snapshot.diagnostic.source_software_digest == "e" * 64
    assert snapshot.diagnostic.selected_software_digest == "f" * 64


def test_optional_snapshot_cannot_change_required_input_membership(
    case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    encoded = case.source.execution_record["replay_snapshot"]
    snapshot = ReplaySnapshot.model_validate_json(encoded)
    snapshot = snapshot.model_copy(
        update={
            "inputs": {
                "source": [
                    replace(item, artifact_id="f" * 32)
                    for item in snapshot.inputs["source"]
                ]
            },
            "associated": [
                item.model_copy(update={"primary_id": "f" * 32})
                for item in snapshot.associated
            ],
        }
    )
    corrupted = replace(
        case.source,
        execution_record={
            **case.source.execution_record,
            "replay_snapshot": snapshot.model_dump_json(),
        },
    )
    monkeypatch.setattr(
        "artisan.orchestration.curator_replay.read_execution_source",
        lambda *args: corrupted,
    )
    with pytest.raises(StoreIntegrityError):
        _prepare(case)


@pytest.mark.parametrize("missing", ["configuration", "runtime_keys"])
def test_missing_optional_capture_reports_unavailable(
    case: Case, monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    record = dict(case.source.execution_record)
    if missing == "configuration":
        record.pop("replay_snapshot")
    else:
        snapshot = ReplaySnapshot.model_validate_json(record["replay_snapshot"])
        record["replay_snapshot"] = snapshot.model_copy(
            update={"required_runtime_keys": None}
        ).model_dump_json()
    monkeypatch.setattr(
        "artisan.orchestration.curator_replay.read_execution_source",
        lambda *args: replace(case.source, execution_record=record),
    )
    with pytest.raises(ArtisanError) as error:
        _prepare(case)
    assert error.value.code == "replay_evidence_unavailable"


def test_prepared_operation_mutation_fails_with_a_new_owner_before_launch(
    case: Case,
) -> None:
    replay = _prepare(case)
    replay.operation.params.fail = True
    execution = _execute(case, replay)
    state = execution.tracker.current_state(execution.record.step_run_id)
    assert state.status == StepStatus.FAILED
    assert state.step_run_id != case.source.admission.record.step_run_id
    assert len(case.runner.launched) == 1


def test_changed_curator_declaration_requires_explicit_replay(
    case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ReplayCurator, "cacheable", False)
    with pytest.raises(ArtisanError) as error:
        _prepare(case)
    assert error.value.code == "replay_code_changed"
    replay = _prepare(case, allow_code_change=True)
    assert not replay.definition.cacheable


def test_configuration_replacement_is_redacted_from_controller_failure(
    case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    replay = _prepare(case)
    monkeypatch.setattr(
        case.runner,
        "resolve_curator",
        Mock(side_effect=RuntimeError("Could not resolve replacement-label-private")),
    )
    execution = _execute(case, replay)
    state = execution.tracker.current_state(execution.record.step_run_id)
    assert state.status == StepStatus.FAILED
    assert state.error is not None
    assert "replacement-label-private" not in state.error


def test_public_replay_uses_curator_controller_and_fresh_diagnostic_owner(
    case: Case,
    tmp_path: Path,
) -> None:
    from typing import cast

    from artisan.orchestration.pipeline_manager import PipelineRunner
    from artisan.orchestration.replay import replay_execution

    result = replay_execution(
        case.execution_id,
        delta_root=case.committer.delta_root,
        files_root=case.committer.files_root,
        storage=case.dispatch.storage,
        operation=ReplayCurator,
        step_runner=cast(PipelineRunner, case.runner),
        configuration_replacements={
            "/params/password": "replacement-private-value",
            "/params/label": "replacement-label-private",
        },
        runtime_replacements={"SYNTHETIC_RUNTIME": "replacement-runtime-private"},
        working_root=tmp_path / "diagnostics",
        staging_root=case.dispatch.staging_root,
    )
    assert result.step_result.status == StepStatus.SUCCEEDED
    assert result.execution_run_id is not None
    assert result.execution_run_id != case.execution_id
    assert result.diagnostic_status == "complete"
    assert result.step_result.step_number == 8
    assert result.diagnostic_roots["files_root"] == case.committer.files_root
    assert "replacement-private" not in result.model_dump_json()
    selected = read_execution_source(case.committer, result.execution_run_id)
    assert isinstance(selected, CuratorExecutionSource)
    assert selected.admission.inputs == case.source.admission.inputs
    assert selected.admission.associations == case.source.admission.associations
    assert selected.admission.record.replay_of_execution_run_id == case.execution_id
