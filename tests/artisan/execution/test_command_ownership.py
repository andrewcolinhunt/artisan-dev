"""Check publication barriers after real owner loss in every user phase."""

from __future__ import annotations

import sys
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest

from artisan.execution.compute.base import ExecuteRouter
from artisan.execution.executors import creator, creator_phases, curator
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.tool_endpoint.client import EndpointCancellationError
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.curator_result import ArtifactResult, PassthroughResult
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
)
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.output_spec import OutputSpec
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
        return ArtifactResult(
            artifacts={"metrics": [MetricArtifact.draft({"value": 1}, "metric", 1)]},
            lineage={"metrics": []},
        )


class _OwnedCurator(OperationDefinition):
    name = "owned_phase_curator"

    class OutputRole(StrEnum):
        metrics = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "metrics": OutputSpec(artifact_type="metric", derives_from={"inputs": []})
    }

    def execute_curator(self, **kwargs: Any) -> ArtifactResult:
        return ArtifactResult(
            artifacts={"metrics": [MetricArtifact.draft({"value": 1}, "metric", 1)]},
            lineage={"metrics": []},
        )


@pytest.fixture
def runtime(tmp_path: Path) -> RuntimeEnvironment:
    return RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
        failure_logs_root=str(tmp_path / "failures"),
    )


def _lose_owner() -> None:
    """Destroy only this invocation's exact owner; it has no live commands."""
    invocation = command_supervisor._current
    assert invocation is not None
    assert invocation.owner is not None
    invocation.owner.process.kill()
    invocation.owner.process.wait(timeout=5)
    with pytest.raises(command_supervisor.CommandOwnershipError):
        command_supervisor.check_command_scope()


def _forbid_publication(*args: Any, **kwargs: Any) -> None:
    pytest.fail("uncertain commands reached output collection or publication")


def _assert_failed_record(result: Any) -> dict[str, Any]:
    assert not result.success
    assert result.artifact_ids == []
    row = pl.read_parquet(Path(result.staging_path) / "executions.parquet").row(
        0, named=True
    )
    assert row["success"] is False
    return row


@pytest.mark.parametrize(
    "failed_phase", ["bootstrap", "preprocess", "execute", "postprocess"]
)
def test_creator_caught_owner_loss_forbids_later_phases_and_success(
    runtime: RuntimeEnvironment, monkeypatch: pytest.MonkeyPatch, failed_phase: str
) -> None:
    visited: list[str] = []
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
                _lose_owner()
            return value

        monkeypatch.setattr(_OwnedCreator, method, run)
    if failed_phase == "bootstrap":
        monkeypatch.setattr(
            creator, "bootstrap_applications", lambda *a, **k: _lose_owner()
        )
    for name in ("finalize_artifacts", "_upload_files_to_root", "_read_tool_output"):
        monkeypatch.setattr(creator_phases, name, _forbid_publication)
    monkeypatch.setattr(creator, "_read_tool_output", _forbid_publication)

    result = creator.run_creator_flow(
        ExecutionUnit(operation=_OwnedCreator(), inputs={}, step_number=1), runtime
    )

    _assert_failed_record(result)
    expected = (
        []
        if failed_phase == "bootstrap"
        else list(phases)[: list(phases).index(failed_phase) + 1]
    )
    assert visited == expected
    if failed_phase != "bootstrap":
        assert list(Path(runtime.working_root).rglob("execute"))


@pytest.mark.parametrize(
    "failed_phase", ["preprocess", "execute_function", "postprocess"]
)
def test_creator_preserves_primary_error_and_skips_log_reads_when_owner_is_lost(
    runtime: RuntimeEnvironment, monkeypatch: pytest.MonkeyPatch, failed_phase: str
) -> None:
    def fail(self: _OwnedCreator, inputs: Any) -> None:
        _lose_owner()
        message = "primary user failure"
        raise ValueError(message)

    monkeypatch.setattr(_OwnedCreator, failed_phase, fail)
    monkeypatch.setattr(creator, "_read_tool_output", _forbid_publication)
    monkeypatch.setattr(creator_phases, "_read_tool_output", _forbid_publication)
    result = creator.run_creator_flow(
        ExecutionUnit(operation=_OwnedCreator(), inputs={}, step_number=1), runtime
    )

    row = _assert_failed_record(result)
    assert "primary user failure" in row["error"]
    assert "cleanup could not be confirmed" in row["error"]
    assert list(Path(runtime.working_root).rglob("execute"))


@pytest.mark.parametrize(
    "result_kind", ["bootstrap", "artifact", "passthrough", "exception"]
)
def test_curator_caught_owner_loss_forbids_both_success_handlers(
    runtime: RuntimeEnvironment, monkeypatch: pytest.MonkeyPatch, result_kind: str
) -> None:
    original = _OwnedCurator.execute_curator

    def execute(
        self: _OwnedCurator, **kwargs: Any
    ) -> ArtifactResult | PassthroughResult:
        assert result_kind != "bootstrap", "curator ran after bootstrap owner loss"
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
    result = curator.run_curator_flow(
        ExecutionUnit(operation=_OwnedCurator(), inputs={}, step_number=1), runtime
    )

    if result_kind == "bootstrap":
        assert not result.success
        assert result.artifact_ids == []
        assert result.error
        assert "cleanup could not be confirmed" in result.error
        return
    row = _assert_failed_record(result)
    if result_kind == "exception":
        assert "primary curator failure" in row["error"]
    assert "cleanup could not be confirmed" in row["error"]


@pytest.mark.parametrize("result_kind", ["artifact", "passthrough"])
def test_curator_rechecks_ownership_immediately_before_success_record(
    runtime: RuntimeEnvironment, monkeypatch: pytest.MonkeyPatch, result_kind: str
) -> None:
    if result_kind == "passthrough":
        monkeypatch.setattr(
            _OwnedCurator,
            "execute_curator",
            lambda *a, **k: PassthroughResult(passthrough={"metrics": ["a" * 32]}),
        )
    hook_name = (
        "finalize_artifacts"
        if result_kind == "artifact"
        else "validate_passthrough_result"
    )
    original = getattr(curator, hook_name)

    def lose_owner_during_result_processing(*args: Any, **kwargs: Any) -> Any:
        result = original(*args, **kwargs)
        _lose_owner()
        return result

    monkeypatch.setattr(curator, hook_name, lose_owner_during_result_processing)
    monkeypatch.setattr(curator, "record_execution_success", _forbid_publication)
    monkeypatch.setattr(curator, "record_passthrough", _forbid_publication)

    result = curator.run_curator_flow(
        ExecutionUnit(operation=_OwnedCurator(), inputs={}, step_number=1), runtime
    )

    row = _assert_failed_record(result)
    assert "cleanup could not be confirmed" in row["error"]


@pytest.mark.parametrize("joined_threads", [False, True])
def test_creator_all_phases_launch_from_one_owner_and_record_success(
    runtime: RuntimeEnvironment,
    monkeypatch: pytest.MonkeyPatch,
    joined_threads: bool,
) -> None:
    owner_pids: list[int] = []
    for method in ("preprocess", "execute_function", "postprocess"):
        original = getattr(_OwnedCreator, method)

        def run(
            self: _OwnedCreator,
            inputs: Any,
            *,
            original: Callable[..., Any] = original,
        ) -> Any:
            invocation = command_supervisor._current
            assert invocation is not None
            assert invocation.owner is not None

            def command() -> int:
                result = run_command(
                    LocalEnvironmentSpec(),
                    [sys.executable, "-c", "import os; print(os.getppid())"],
                )
                return int(result.stdout.strip())

            if joined_threads:
                with ThreadPoolExecutor(max_workers=2) as pool:
                    futures = [pool.submit(command) for _ in range(2)]
                    pids = [future.result() for future in futures]
            else:
                pids = [command()]
            owner_pids.extend(pids)
            assert all(pid == invocation.owner.process.pid for pid in pids)
            return original(self, inputs)

        monkeypatch.setattr(_OwnedCreator, method, run)

    result = creator.run_creator_flow(
        ExecutionUnit(operation=_OwnedCreator(), inputs={}, step_number=1), runtime
    )

    assert result.success, result.error
    assert len(owner_pids) == (6 if joined_threads else 3)
    assert len(set(owner_pids)) == 1
    row = pl.read_parquet(Path(result.staging_path) / "executions.parquet").row(
        0, named=True
    )
    recording = CommandRecording.model_validate_json(row["command_recording"])
    assert len(recording.commands) == len(owner_pids)
    assert all(command.outcome == "succeeded" for command in recording.commands)
    assert not list(Path(runtime.working_root).rglob("execute"))


@pytest.mark.parametrize("lose_local_owner", [False, True])
def test_remote_cancellation_requires_confirmed_local_command_closure(
    runtime: RuntimeEnvironment, lose_local_owner: bool
) -> None:
    class CancelledRouter(ExecuteRouter):
        def route_execute(
            self,
            operation: Any,
            execute_inputs: list[ExecuteInput],
            sandbox_root: str,
        ) -> list[Any]:
            if lose_local_owner:
                _lose_owner()
            return [
                EndpointCancellationError(
                    CancellationAcknowledgement(
                        CancellationStatus.CONFIRMED, "Remote commands stopped"
                    )
                )
            ]

    result = creator.run_creator_flow(
        ExecutionUnit(operation=_OwnedCreator(), inputs={}, step_number=1),
        runtime,
        execute_router=CancelledRouter(),
    )

    assert not result.success
    assert result.artifact_ids == []
    assert result.cancellation_acknowledgement is not None
    expected = (
        CancellationStatus.UNKNOWN if lose_local_owner else CancellationStatus.CONFIRMED
    )
    assert result.cancellation_acknowledgement.status == expected
    assert list(Path(runtime.working_root).rglob("execute"))
