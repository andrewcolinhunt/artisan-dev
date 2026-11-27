"""Keep phase and publication barriers after real invocation-owner loss."""

from __future__ import annotations

import sys
from collections.abc import Callable
from concurrent.futures import CancelledError, ThreadPoolExecutor
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest
from fixtures.commands import IdentityCommandEnvironment
from fsspec.implementations.local import LocalFileSystem

from artisan.composites.base.compiler import compile_definition
from artisan.execution import invocation
from artisan.execution.composition import PlanExecution, execute_compiled_plan
from artisan.execution.compute.base import ExecuteRouter
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.executor import Executor
from artisan.execution.executors import curator
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording import delivery_diagnostics
from artisan.execution.recording.delivery_diagnostics import DeliveryRecorder
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.execution.curator_result import ArtifactResult, PassthroughResult
from artisan.schemas.execution.worker_config import WorkerConfig
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.io.retained_store import create_retained_store
from artisan.utils import command_supervisor
from artisan.utils.external_tools import run_command


class _OwnedCreator(OperationDefinition):
    name = "owned_phase_creator"

    class OutputRole(StrEnum):
        metrics = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "metrics": OutputSpec(artifact_type="metric", derives_from={"inputs": []})
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {}

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        Path(inputs.execute_dir, "retained.txt").write_text("diagnostic evidence")
        return {}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        result.add_artifact(
            "metrics", MetricArtifact.draft({"value": 1}, "metric", 1), sources={}
        )
        return result


class _OwnedCurator(OperationDefinition):
    name = "owned_phase_curator"

    class OutputRole(StrEnum):
        metrics = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "metrics": OutputSpec(artifact_type="metric", derives_from={"inputs": []})
    }

    def execute_curator(self, **kwargs: Any) -> ArtifactResult:
        result = ArtifactResult()
        result.add_artifact(
            "metrics", MetricArtifact.draft({"value": 1}, "metric", 1), sources={}
        )
        return result


@pytest.fixture
def runtime(tmp_path: Path) -> WorkerConfig:
    delta = tmp_path / "owner-store"
    files = tmp_path / "files"
    create_retained_store(str(delta), str(files), fs=LocalFileSystem())
    return WorkerConfig(
        delta_root=str(delta),
        files_root=str(files),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
        failure_logs_root=str(tmp_path / "failures"),
    )


def _lose_owner(executor: Executor | None = None) -> None:
    """Destroy this test's exact supervisor while it has no active commands."""
    scope = (
        executor._invocation if executor is not None else command_supervisor._current
    )
    assert scope is not None
    assert scope.owner is not None
    assert scope.owner.process.poll() is None
    scope.owner.process.kill()
    scope.owner.process.wait(timeout=5)
    with pytest.raises(command_supervisor.CommandOwnershipError):
        command_supervisor.check_command_scope()


def _forbid_publication(*args: Any, **kwargs: Any) -> None:
    pytest.fail("uncertain commands reached output collection or publication")


def _creator(executor: Executor, router: ExecuteRouter | None = None) -> PlanExecution:
    return execute_compiled_plan(
        compile_definition(_OwnedCreator),
        {},
        executor,
        invocation_id="owned-scientific-unit",
        execute_adapter=lambda _operation, _path: (
            LocalExecuteRouter() if router is None else router
        ),
        step_number=1,
    )


@pytest.mark.parametrize(
    "failed_phase", ["entry", "preprocess", "execute", "postprocess"]
)
def test_creator_owner_loss_forbids_later_phases_and_finalization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failed_phase: str
) -> None:
    visited: list[str] = []
    executor = Executor(working_root=tmp_path / "owned-work")
    phases = {
        "preprocess": "preprocess",
        "execute": "execute_function",
        "postprocess": "postprocess",
    }
    for phase, method in phases.items():
        original = getattr(_OwnedCreator, method)

        def run(
            self: _OwnedCreator,
            inputs: Any,
            *,
            phase: str = phase,
            original: Callable[..., Any] = original,
        ) -> Any:
            visited.append(phase)
            value = original(self, inputs)
            if phase == failed_phase:
                _lose_owner(executor)
            return value

        monkeypatch.setattr(_OwnedCreator, method, run)
    monkeypatch.setattr(invocation, "finalize_artifacts", _forbid_publication)
    monkeypatch.setattr(invocation, "_read_tool_output", _forbid_publication)

    with pytest.raises(command_supervisor.CommandOwnershipError), executor:
        if failed_phase == "entry":
            _lose_owner(executor)
        _creator(executor)

    expected = (
        []
        if failed_phase == "entry"
        else list(phases)[: list(phases).index(failed_phase) + 1]
    )
    assert visited == expected
    assert not executor.cleanup_complete
    assert executor._root is not None
    assert executor._root.is_dir()
    with pytest.raises(RuntimeError, match="unresolved cleanup"):
        executor.__enter__()


@pytest.mark.parametrize(
    "failed_phase", ["preprocess", "execute_function", "postprocess"]
)
def test_creator_preserves_primary_error_without_reading_unclosed_logs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failed_phase: str
) -> None:
    diagnostics = DeliveryRecorder(sensitive_values=())
    executor = Executor(working_root=tmp_path / "owned-work", diagnostics=diagnostics)

    def fail(self: _OwnedCreator, inputs: Any) -> None:
        _lose_owner(executor)
        message = "primary user failure"
        raise ValueError(message)

    monkeypatch.setattr(_OwnedCreator, failed_phase, fail)
    monkeypatch.setattr(invocation, "_read_tool_output", _forbid_publication)
    monkeypatch.setattr(delivery_diagnostics, "_read_tool_output", _forbid_publication)
    error_type = (
        invocation.ExecuteFailure if failed_phase == "execute_function" else ValueError
    )
    with pytest.raises(error_type, match="primary user failure") as caught, executor:
        _creator(executor)

    assert "cleanup could not be confirmed" in " ".join(caught.value.__notes__)
    assert not executor.cleanup_complete
    assert diagnostics.cleanup == "unknown"
    assert diagnostics.invocations[0].cleanup == "unknown"
    assert executor._root is not None
    assert executor._root.is_dir()


@pytest.mark.parametrize(
    "result_kind", ["bootstrap", "artifact", "passthrough", "exception"]
)
def test_curator_owner_loss_forbids_success_handlers(
    runtime: WorkerConfig, monkeypatch: pytest.MonkeyPatch, result_kind: str
) -> None:
    original = _OwnedCurator.execute_curator

    def execute(
        self: _OwnedCurator, **kwargs: Any
    ) -> ArtifactResult | PassthroughResult:
        assert result_kind != "bootstrap"
        _lose_owner()
        if result_kind == "exception":
            message = "primary curator failure"
            raise ValueError(message)
        if result_kind == "passthrough":
            return PassthroughResult(passthrough={"metrics": ["a" * 32]})
        return original(self, **kwargs)

    monkeypatch.setattr(_OwnedCurator, "execute_curator", execute)
    if result_kind == "bootstrap":
        monkeypatch.setattr(
            curator, "bootstrap_applications", lambda *a, **k: _lose_owner()
        )
    monkeypatch.setattr(curator, "_handle_artifact_result", _forbid_publication)
    monkeypatch.setattr(curator, "_handle_passthrough_result", _forbid_publication)
    with pytest.raises(command_supervisor.CommandOwnershipError):
        curator.run_curator_flow(
            ExecutionUnit(operation=_OwnedCurator(), inputs={}, step_number=1), runtime
        )
    records = list(Path(runtime.staging_root).rglob("executions.parquet"))
    if result_kind == "bootstrap":
        assert not records
    else:
        assert len(records) == 1
        row = pl.read_parquet(records[0]).row(0, named=True)
        assert row["success"] is False
        assert "cleanup could not be confirmed" in row["error"]
        if result_kind == "exception":
            assert "primary curator failure" in row["error"]


def _curator_result_hook(monkeypatch: pytest.MonkeyPatch, result_kind: str) -> str:
    if result_kind == "passthrough":
        monkeypatch.setattr(
            _OwnedCurator,
            "execute_curator",
            lambda *a, **k: PassthroughResult(passthrough={"metrics": ["a" * 32]}),
        )
    return (
        "finalize_artifacts"
        if result_kind == "artifact"
        else "validate_passthrough_result"
    )


@pytest.mark.parametrize("result_kind", ["artifact", "passthrough"])
def test_curator_finishes_owned_supervisor_before_result_processing(
    runtime: WorkerConfig, monkeypatch: pytest.MonkeyPatch, result_kind: str
) -> None:
    hook_name = _curator_result_hook(monkeypatch, result_kind)
    original = getattr(curator, hook_name)

    def check_owner_before_result_processing(*args: Any, **kwargs: Any) -> Any:
        scope = command_supervisor._current
        assert scope is not None
        assert scope.closed
        assert not scope.uncertain
        assert scope.owner is not None
        assert scope.owner.process.poll() == 0
        assert scope.owner.poll().state == "closed"
        return original(*args, **kwargs)

    monkeypatch.setattr(curator, hook_name, check_owner_before_result_processing)
    result = curator.run_curator_flow(
        ExecutionUnit(operation=_OwnedCurator(), inputs={}, step_number=1), runtime
    )
    assert result.success
    records = list(Path(runtime.staging_root).rglob("executions.parquet"))
    assert len(records) == 1
    assert pl.read_parquet(records[0])["success"].to_list() == [True]


@pytest.mark.parametrize("result_kind", ["artifact", "passthrough"])
def test_curator_closed_scope_does_not_prove_shared_owner_closure(
    runtime: WorkerConfig, monkeypatch: pytest.MonkeyPatch, result_kind: str
) -> None:
    hook_name = _curator_result_hook(monkeypatch, result_kind)
    original = getattr(curator, hook_name)

    def lose_owner_during_result_processing(*args: Any, **kwargs: Any) -> Any:
        result = original(*args, **kwargs)
        scope = command_supervisor._current
        assert scope is not None
        assert scope.closed
        assert not scope.uncertain
        assert scope.owner is None
        assert outer.owner is not None
        assert outer.owner.process.poll() is None
        outer.owner.process.kill()
        outer.owner.process.wait(timeout=5)
        return result

    monkeypatch.setattr(curator, hook_name, lose_owner_during_result_processing)
    with (
        pytest.raises(command_supervisor.CommandOwnershipError),
        command_supervisor.managed_invocation() as outer,
    ):
        result = curator.run_curator_flow(
            ExecutionUnit(operation=_OwnedCurator(), inputs={}, step_number=1), runtime
        )
        assert result.success
    assert outer.uncertain
    records = list(Path(runtime.staging_root).rglob("executions.parquet"))
    assert len(records) == 1
    assert pl.read_parquet(records[0])["success"].to_list() == [True]


@pytest.mark.parametrize("joined_threads", [False, True])
def test_creator_phases_share_supervisor_and_record_subtree_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, joined_threads: bool
) -> None:
    diagnostics = DeliveryRecorder(sensitive_values=())
    executor = Executor(working_root=tmp_path / "owned-work", diagnostics=diagnostics)
    attachments: list[str] = []
    for method in ("preprocess", "execute_function", "postprocess"):
        original = getattr(_OwnedCreator, method)

        def run(
            self: _OwnedCreator, inputs: Any, *, original: Callable[..., Any] = original
        ) -> Any:
            scope = command_supervisor._current
            assert scope is not None
            attachments.append(scope.attachment["socket"])

            def command() -> None:
                result = run_command(
                    IdentityCommandEnvironment(),
                    [sys.executable, "-c", "print('ready')"],
                )
                assert result.stdout.strip() == "ready"

            if joined_threads:
                with ThreadPoolExecutor(max_workers=2) as pool:
                    futures = [pool.submit(command) for _ in range(2)]
                    for future in futures:
                        future.result()
            else:
                command()
            return original(self, inputs)

        monkeypatch.setattr(_OwnedCreator, method, run)
    with executor:
        result = _creator(executor)
        assert len(result.result.artifacts["metrics"]) == 1
    assert executor.cleanup_complete
    assert len(attachments) == 3
    assert len(set(attachments)) == 1
    recording = diagnostics.invocations[0].command_recording
    assert len(recording.commands) == (6 if joined_threads else 3)
    assert all(command.outcome == "succeeded" for command in recording.commands)
    assert not list((tmp_path / "owned-work").iterdir())


@pytest.mark.parametrize("lose_local_owner", [False, True])
def test_cancelled_execution_preserves_actual_local_cleanup(
    tmp_path: Path, lose_local_owner: bool
) -> None:
    executor = Executor(working_root=tmp_path / "owned-work")

    class CancelledRouter(ExecuteRouter):
        def route_execute(
            self, operation: Any, execute_inputs: list[ExecuteInput], sandbox_root: str
        ) -> list[Any]:
            if lose_local_owner:
                _lose_owner(executor)
            return [CancelledError()]

    with pytest.raises(CancelledError) as caught, executor:
        _creator(executor, CancelledRouter())
    assert executor.cleanup_complete is not lose_local_owner
    if lose_local_owner:
        assert "cleanup could not be confirmed" in " ".join(caught.value.__notes__)
        assert executor._root is not None
        assert executor._root.is_dir()
    else:
        assert not list((tmp_path / "owned-work").iterdir())
