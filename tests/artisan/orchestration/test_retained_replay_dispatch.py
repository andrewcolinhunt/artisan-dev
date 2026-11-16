"""Scientific replay uses required source authority and the ordinary controller."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar, cast

import pytest
from fixtures.retained_execution import NativeHandle, NativeRunner, NativeSoftware
from fixtures.retained_readers import Backend
from fsspec.implementations.local import LocalFileSystem

from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.compiler import compile_definition
from artisan.errors import ArtisanError
from artisan.execution.admitted_worker import execute_admitted_assignment
from artisan.execution.assignment import Assignment, PreparedAssignmentSoftware
from artisan.execution.assignment_reply import AssignmentReply
from artisan.execution.assignment_transport import (
    AdmittedAssignment,
    AssignmentDispatch,
)
from artisan.execution.composition import ExecuteAdapter
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.exceptions import AssignmentExecutionError
from artisan.execution.recording.retained_replay import (
    read_replay_diagnostic,
    record_selected_failed_replay_diagnostic,
)
from artisan.operations.base import OperationDefinition
from artisan.orchestration.engine.retained_executor import RetainedStepExecution
from artisan.orchestration.engine.retained_inputs import prepare_step_inputs
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.pipeline_manager import PipelineRunner
from artisan.orchestration.replay import replay_execution
from artisan.orchestration.retained_replay import (
    RetainedReplayPreparation,
    prepare_retained_replay,
)
from artisan.orchestration.runners.assignment import (
    AssignmentLaunchError,
    AssignmentSoftware,
)
from artisan.schemas import (
    ArtifactResult,
    FileRefArtifact,
    InputSpec,
    MetricArtifact,
    OutputSpec,
)
from artisan.schemas.execution.assignment import DeliveryIdentity, SealedDelivery
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.storage.cache.retained_lookup import lookup_assignment
from artisan.storage.core.execution_source import (
    FailedScientificExecutionSource,
    read_execution_source,
)
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.admission import admission_directory
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_store import create_retained_store
from artisan.utils.hashing import compute_content_digest


@pytest.fixture(params=["local", pytest.param("s3", marks=pytest.mark.s3)])
def backend_fs(request: pytest.FixtureRequest, tmp_path: Path) -> Backend:
    if request.param == "local":
        return LocalFileSystem(), StorageConfig(), str(tmp_path)
    return request.getfixturevalue("s3_fs")


class _RuntimeCheckedFile(OperationDefinition):
    name = "replay_runtime_checked_file"
    per_artifact_dispatch = False

    class InputRole(StrEnum):
        items = "items"

    class OutputRole(StrEnum):
        metrics = "metrics"

    inputs: ClassVar = {"items": InputSpec(artifact_type="file_ref", materialize=False)}
    outputs: ClassVar = {
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["items"]}
        )
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        artifacts = inputs.input_artifacts["items"]
        values = [
            Path(cast(FileRefArtifact, artifact).path).read_bytes()
            for artifact in artifacts
        ]
        if os.environ.get("REPLAY_TOKEN") != "replacement-value":
            msg = "Runtime credential rejected"
            raise ValueError(msg)
        return {"size": sum(map(len, values)), "positions": len(values)}

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        return dict(inputs.inputs)

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        result.add_artifact(
            "metrics",
            MetricArtifact.draft(
                {
                    "size": inputs.memory_outputs["size"][0],
                    "positions": inputs.memory_outputs["positions"][0],
                },
                "summary.json",
            ),
            sources={
                "items": [
                    inputs.ref("items", index=index)
                    for index in range(len(inputs.input_artifacts["items"]))
                ]
            },
        )
        return result


@dataclass(frozen=True)
class _Software(NativeSoftware):
    """Exercise real worker code without claiming installed-software qualification."""

    def verify(self, plan: CompiledPlan) -> None:
        plan.instantiate()

    def create_adapter(
        self,
        assignment: Assignment,
        *,
        working_root: Path,
        runtime_env: Mapping[str, str],
    ) -> ExecuteAdapter:
        return lambda _operation, _path: LocalExecuteRouter()


class _WorkerRunner(NativeRunner):
    def __init__(self, committer: RetainedCommitter, digest: str = "c" * 64) -> None:
        super().__init__(committer)
        self.software = _Software(software_digest=digest)
        self.expected_pending: str | None = None

    def resolve(
        self,
        plan: CompiledPlan,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> _Software:
        if self.expected_pending is not None:
            state = RetainedStepTracker(
                self.committer, "diagnostic-tests"
            ).current_state(self.expected_pending)
            assert state.status == StepStatus.PENDING
        super().resolve(plan, runner_resources, application_modules=application_modules)
        return self.software

    def launch(
        self,
        assignments: Sequence[AdmittedAssignment],
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> tuple[NativeHandle, ...]:
        assert software is self.software
        return super().launch(assignments, software=software, dispatch=dispatch)

    def _invoke(
        self, assignment: AdmittedAssignment, dispatch: AssignmentDispatch
    ) -> SealedDelivery:
        try:
            return execute_admitted_assignment(
                assignment,
                cast(PreparedAssignmentSoftware, self.software),
                dispatch,
                working_root=dispatch.working_root,
            )
        except AssignmentExecutionError as error:
            reply = AssignmentReply.failed(assignment.attempt, error)
            return AssignmentReply.model_validate_json(reply.model_dump_json()).result(
                assignment.attempt
            )


def _failed_source(
    backend: Backend, scratch: Path
) -> tuple[RetainedStepExecution, DeliveryIdentity, Path]:
    fs, storage, root = backend
    committer = create_retained_store(
        f"{root}/replay",
        f"{root}/files",
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )
    path = scratch / "original.bin"
    path.write_bytes(b"frozen-input")
    artifact = FileRefArtifact.draft(
        str(path), compute_content_digest(path.read_bytes()), path.stat().st_size
    ).finalize()
    plan = compile_definition(
        _RuntimeCheckedFile, batch_strategy={"artifacts_per_unit": 2}
    )
    record = StepStartRecord(
        step_run_id="a" * 32,
        step_number=0,
        step_name=plan.recipe.definition.name,
        operation_class=f"{_RuntimeCheckedFile.__module__}.{_RuntimeCheckedFile.__qualname__}",
        params_json=plan.recipe.params_json,
        input_refs_json="{}",
        compute_backend="native-test",
        compute_options_json="{}",
        output_roles_json='["metrics"]',
        output_types_json='{"metrics":"metric"}',
    )
    dispatch = AssignmentDispatch(
        committer.delta_root,
        committer.files_root,
        f"{root}/staging",
        storage,
        scratch / "worker",
        runtime_env={"REPLAY_TOKEN": "original-private-value"},
    )
    runner = _WorkerRunner(committer)
    runner.expected_pending = record.step_run_id
    execution = RetainedStepExecution(
        record,
        resolve_plan=lambda: plan,
        resolve_inputs=lambda current: prepare_step_inputs(
            current.recipe,
            {"items": [artifact, artifact]},
            step_run_id=record.step_run_id,
            reader=RetainedReader(committer),
        ),
        tracker=RetainedStepTracker(committer, "diagnostic-tests"),
        runner=runner,
        dispatch=dispatch,
        runner_resources=RunnerResources(),
        batch_strategy=BatchStrategy(),
    )
    with ThreadPoolExecutor() as pool:
        assert execution.run(pool).status == StepStatus.FAILED
    with pytest.raises(AssignmentExecutionError) as observed:
        execution.handles[0].result()
    assert observed.value.delivery is not None
    return execution, observed.value.delivery, path


def _prepare(
    first: RetainedStepExecution,
    source: DeliveryIdentity,
    *,
    token: str = "replacement-value",
    allow_code_change: bool = False,
) -> RetainedReplayPreparation:
    return prepare_retained_replay(
        first.tracker.committer,
        source.delivery_id,
        registered_definitions={
            (
                _RuntimeCheckedFile.__module__,
                _RuntimeCheckedFile.__qualname__,
            ): _RuntimeCheckedFile
        },
        runtime_replacements={"REPLAY_TOKEN": token},
        allow_code_change=allow_code_change,
    )


@pytest.mark.parametrize("succeeds", [False, True])
def test_failed_source_dispatch_uses_frozen_carrier_and_new_diagnostic_owner(
    backend_fs: Backend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    succeeds: bool,
) -> None:
    first, source, path = _failed_source(backend_fs, tmp_path)
    original_states = first.tracker.load_current_states("diagnostic-tests")
    original_outputs = first.results.outputs(first.record.step_run_id)
    path.unlink()
    replay = _prepare(
        first,
        source,
        token="replacement-value" if succeeds else "another-private-value",
    )
    runner = _WorkerRunner(first.tracker.committer)
    runner.expected_pending = replay.identity.step_run_id
    execution = RetainedStepExecution.from_replay(
        replay,
        committer=first.tracker.committer,
        runner=runner,
        dispatch=first.dispatch,
    )

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Diagnostic execution attempted ordinary cache lookup")

    monkeypatch.setattr(execution.results, "lookup", forbidden)
    monkeypatch.setattr(
        "artisan.orchestration.engine.retained_executor._cache_hits", forbidden
    )
    with ThreadPoolExecutor() as pool:
        result = execution.run(pool)
    assert runner.resolutions == 1
    assert runner.packs == [(0,)]
    assert execution.ownership_state() == OwnershipState.CLOSED
    assert result.status == (StepStatus.SUCCEEDED if succeeds else StepStatus.FAILED)
    assert first.tracker.load_current_states("diagnostic-tests") == original_states
    assert first.results.outputs(first.record.step_run_id) == original_outputs
    diagnostic_state = execution.tracker.current_state(replay.identity.step_run_id)
    assert diagnostic_state.pipeline_run_id != original_states[0].pipeline_run_id
    assert diagnostic_state.step_number == original_states[0].step_number
    assert execution.replay_diagnostic is not None
    assert execution.replay_diagnostic.source_seal_digest is None
    assert execution.replay_diagnostic.source_delivery == source
    assert replay.identity != source.attempt.assignment
    admitted = first.tracker.read_admission(replay.identity.step_run_id).assignments[0]
    assert admitted.attempt.replay_of_execution_run_id == source.delivery_id
    assert lookup_assignment(admitted, reader=execution.reader) is None
    assert first.reader.completed_plans() == ()
    if succeeds:
        receipt = execution.handles[0].result()
        assert receipt.delivery.delivery_id != source.delivery_id
        assert (
            receipt.delivery.attempt.software_digest == runner.software.software_digest
        )
        accepted = execution.reader.read(receipt)
        assert (
            read_replay_diagnostic(
                receipt,
                delta_root=first.tracker.committer.delta_root,
                fs=first.tracker.committer.fs,
            )
            == execution.replay_diagnostic
        )
        assert accepted.output("metrics")[0].artifact.values == {
            "size": 24,
            "positions": 2,
        }
    else:
        with pytest.raises(AssignmentExecutionError) as observed:
            execution.handles[0].result()
        failed = observed.value.delivery
        assert failed is not None
        assert failed.delivery_id != source.delivery_id
        selected = read_execution_source(first.tracker.committer, failed.delivery_id)
        assert isinstance(selected, FailedScientificExecutionSource)
        assert not selected.plan.selected_receipts
        assert (
            read_replay_diagnostic(
                failed,
                delta_root=first.tracker.committer.delta_root,
                fs=first.tracker.committer.fs,
            )
            == execution.replay_diagnostic
        )
        from artisan.orchestration.replay import _diagnostic_coverage

        assert _diagnostic_coverage(first.tracker.committer, selected) == (
            "complete",
            [],
        )
    for record in (
        execution.record.model_dump_json(),
        execution.replay_diagnostic.model_dump_json(),
    ):
        assert "original-private-value" not in record
        assert "replacement-value" not in record
        assert "another-private-value" not in record


@pytest.mark.parametrize("logger_fails", [False, True])
@pytest.mark.parametrize("succeeds", [False, True])
def test_optional_diagnostic_failure_cannot_replace_accepted_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, logger_fails: bool, succeeds: bool
) -> None:
    first, source, _ = _failed_source(
        (LocalFileSystem(), StorageConfig(), str(tmp_path)), tmp_path
    )
    replay = _prepare(
        first, source, token="replacement-value" if succeeds else "still-invalid"
    )
    execution = RetainedStepExecution.from_replay(
        replay,
        committer=first.tracker.committer,
        runner=_WorkerRunner(first.tracker.committer),
        dispatch=first.dispatch,
    )

    def unavailable(*args: object, **kwargs: object) -> None:
        message = "Optional recording is unavailable"
        raise OSError(message)

    module = "artisan.execution.recording.retained_replay"
    monkeypatch.setattr(f"{module}.publish_replay_diagnostic", unavailable)
    if logger_fails:
        monkeypatch.setattr(f"{module}._LOGGER.warning", unavailable)
    with ThreadPoolExecutor() as pool:
        result = execution.run(pool)
    assert result.status == (StepStatus.SUCCEEDED if succeeds else StepStatus.FAILED)
    if succeeds:
        receipt = execution.handles[0].result()
        accepted = execution.reader.read(receipt)
        assert accepted.output("metrics")[0].artifact.values["positions"] == 2
    else:
        with pytest.raises(AssignmentExecutionError) as observed:
            execution.handles[0].result()
        receipt = observed.value.delivery
        assert receipt is not None
        assert isinstance(
            read_execution_source(first.tracker.committer, receipt.delivery_id),
            FailedScientificExecutionSource,
        )
    assert (
        read_replay_diagnostic(
            receipt,
            delta_root=first.tracker.committer.delta_root,
            fs=first.tracker.committer.fs,
        )
        is None
    )


@pytest.mark.parametrize("allow", [False, True])
def test_actual_resolved_software_is_checked_once_before_diagnostic_admission(
    tmp_path: Path, allow: bool
) -> None:
    first, source, _ = _failed_source(
        (LocalFileSystem(), StorageConfig(), str(tmp_path)), tmp_path
    )
    replay = _prepare(first, source, allow_code_change=allow)
    runner = _WorkerRunner(first.tracker.committer, "d" * 64)
    runner.expected_pending = replay.identity.step_run_id
    execution = RetainedStepExecution.from_replay(
        replay,
        committer=first.tracker.committer,
        runner=runner,
        dispatch=first.dispatch,
    )
    with ThreadPoolExecutor() as pool:
        result = execution.run(pool)
    assert runner.resolutions == 1
    if allow:
        assert result.status == StepStatus.SUCCEEDED
        assert execution.handles[0].attempt.software_digest == "d" * 64
        assert execution.replay_diagnostic is not None
        assert execution.replay_diagnostic.selected_software_digest == "d" * 64
    else:
        assert result.status == StepStatus.FAILED
        assert "software changed" in (result.error or "")
        assert not runner.packs
        assert first.tracker.committer.journal.state(replay.identity) is None


@pytest.mark.parametrize("missing", ["configuration", "carrier"])
def test_failed_replay_requires_optional_key_evidence_and_complete_frozen_inputs(
    tmp_path: Path, missing: str
) -> None:
    first, source, _ = _failed_source(
        (LocalFileSystem(), StorageConfig(), str(tmp_path)), tmp_path
    )
    committer = first.tracker.committer
    selected = read_execution_source(committer, source.delivery_id)
    assert isinstance(selected, FailedScientificExecutionSource)
    if missing == "configuration":
        path = f"{committer.delta_root}/_artisan/replay/{source.delivery_id}/configuration.json"
    else:
        path = f"{committer.files_root}/{selected.admission.carriers[0].relative_path}"
    committer.fs.rm(path)
    with pytest.raises(
        ArtisanError if missing == "configuration" else FileNotFoundError
    ):
        _prepare(first, source)


@pytest.mark.parametrize("missing", ["admission", "configuration"])
def test_replay_rechecks_original_authority_after_preparation_before_publication(
    tmp_path: Path, missing: str
) -> None:
    first, source, _ = _failed_source(
        (LocalFileSystem(), StorageConfig(), str(tmp_path)), tmp_path
    )
    replay = _prepare(first, source)
    committer = first.tracker.committer
    path = (
        admission_directory(committer.delta_root, source.attempt) + "/admission.json"
        if missing == "admission"
        else f"{committer.delta_root}/_artisan/replay/{source.delivery_id}/configuration.json"
    )
    committer.fs.rm(path)
    runner = _WorkerRunner(committer)
    execution = RetainedStepExecution.from_replay(
        replay,
        committer=first.tracker.committer,
        runner=runner,
        dispatch=first.dispatch,
    )
    with ThreadPoolExecutor() as pool:
        result = execution.run(pool)
    assert result.status == StepStatus.FAILED
    assert runner.resolutions == 1
    assert not execution.handles
    assert committer.journal.state(replay.identity) is None


class _UnknownOwner:
    def __init__(self) -> None:
        self.requested = False

    def ownership_state(self) -> OwnershipState:
        return OwnershipState.UNKNOWN

    def request_close(self, deadline: float) -> None:
        self.requested = True


def test_replay_resolution_failure_keeps_pending_owner_and_unknown_cleanup(
    tmp_path: Path,
) -> None:
    first, source, _ = _failed_source(
        (LocalFileSystem(), StorageConfig(), str(tmp_path)), tmp_path
    )
    replay = _prepare(first, source)
    owner = _UnknownOwner()

    class FailedResolver(_WorkerRunner):
        def resolve(
            self,
            plan: CompiledPlan,
            runner_resources: RunnerResources,
            *,
            application_modules: tuple[str, ...] = (),
        ) -> _Software:
            super().resolve(
                plan, runner_resources, application_modules=application_modules
            )
            message = "Resolution ownership unresolved"
            raise AssignmentLaunchError(
                message,
                handles=(),
                worker_owners=(owner,),
                cancellation=CancellationAcknowledgement(CancellationStatus.UNKNOWN),
            )

    runner = FailedResolver(first.tracker.committer)
    runner.expected_pending = replay.identity.step_run_id
    execution = RetainedStepExecution.from_replay(
        replay,
        committer=first.tracker.committer,
        runner=runner,
        dispatch=first.dispatch,
    )
    with ThreadPoolExecutor() as pool:
        result = execution.run(pool)
    assert result.cancellation_status == CancellationStatus.UNKNOWN
    assert execution.worker_owners == (owner,)
    assert owner.requested
    assert not execution.handles
    assert first.tracker.committer.journal.state(replay.identity) is None


@pytest.mark.parametrize("missing_diagnostic", [False, True])
def test_public_replay_keeps_frozen_inputs_and_reports_persisted_coverage(
    backend_fs: Backend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    missing_diagnostic: bool,
) -> None:
    first, source, original = _failed_source(backend_fs, tmp_path)
    original.unlink()
    committer = first.tracker.committer
    if missing_diagnostic:
        monkeypatch.setattr(
            "artisan.orchestration.engine.retained_executor.record_replay_diagnostic",
            lambda *args, **kwargs: None,
        )
    result = replay_execution(
        source.delivery_id,
        delta_root=committer.delta_root,
        files_root=committer.files_root,
        storage=backend_fs[1],
        operation=_RuntimeCheckedFile,
        step_runner=cast(PipelineRunner, _WorkerRunner(committer)),
        runtime_replacements={"REPLAY_TOKEN": "replacement-value"},
        working_root=tmp_path / "diagnostics",
        staging_root=first.dispatch.staging_root,
    )
    assert result.step_result.status == StepStatus.SUCCEEDED
    assert result.execution_run_id is not None
    assert result.execution_run_id != source.delivery_id
    assert result.pipeline_run_id != "diagnostic-tests"
    assert result.diagnostic_status == (
        "unavailable" if missing_diagnostic else "complete"
    )
    assert bool(result.diagnostic_errors) == missing_diagnostic
    assert result.diagnostic_roots["files_root"] == committer.files_root
    assert (
        first.tracker.current_state(first.record.step_run_id).status
        == StepStatus.FAILED
    )
    assert "replacement-value" not in result.model_dump_json()


@pytest.mark.parametrize("unknown", [False, True])
def test_failed_replay_diagnostic_requires_selected_closed_delivery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unknown: bool,
) -> None:
    first, source, _ = _failed_source(
        (LocalFileSystem(), StorageConfig(), str(tmp_path)), tmp_path
    )
    replay = _prepare(first, source, token="still-invalid")
    execution = RetainedStepExecution.from_replay(
        replay,
        committer=first.tracker.committer,
        runner=_WorkerRunner(first.tracker.committer),
        dispatch=first.dispatch,
    )
    if unknown:
        monkeypatch.setattr(
            NativeHandle, "ownership_state", lambda self: OwnershipState.UNKNOWN
        )
    with ThreadPoolExecutor() as pool:
        result = execution.run(pool)
    assert result.status == StepStatus.FAILED
    with pytest.raises(AssignmentExecutionError) as observed:
        execution.handles[0].result()
    actual = observed.value.delivery
    assert actual is not None
    delivery = actual if unknown else DeliveryIdentity.new(actual.attempt)
    assert execution.replay_diagnostic is not None
    record_selected_failed_replay_diagnostic(
        delivery, execution.replay_diagnostic, committer=first.tracker.committer
    )
    assert read_execution_source(first.tracker.committer, delivery.delivery_id) is None
    assert (
        read_replay_diagnostic(
            delivery,
            delta_root=first.tracker.committer.delta_root,
            fs=first.tracker.committer.fs,
        )
        is None
    )
    assert execution.ownership_state() == (
        OwnershipState.UNKNOWN if unknown else OwnershipState.CLOSED
    )
