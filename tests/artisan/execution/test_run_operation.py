"""Direct calls preserve operation contracts without an executor lifetime."""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from concurrent.futures import CancelledError, ThreadPoolExecutor
from enum import StrEnum
from pathlib import Path
from threading import Event
from typing import Any, ClassVar

import pytest
from fixtures.commands import IdentityCommandEnvironment
from fixtures.composition_operations import (
    Measure,
    PredictMeasure,
    SummarizeMetrics,
    Transform,
    coordinate,
    structure,
)

from artisan.composites import CompositeContext
from artisan.composites.base.compiler import Definition
from artisan.execution import invocation, local_execution, run_operation
from artisan.execution.invocation import ExecuteFailure
from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas import ArtifactResult, Environment, InputSpec, OutputSpec
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.utils import command_supervisor
from artisan.utils.configuration_secrets import COMMAND_ATTACHMENT_ENV
from artisan.utils.external_tools import run_command
from artisan.utils.hashing import compute_content_digest


class _FilelessTransform(Transform):
    name = "direct_fileless_transform"
    batch_strategy: BatchStrategy = BatchStrategy(artifacts_per_unit=2)
    processes: ClassVar[list[int]] = []

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        assert inputs.preprocess_dir is None
        return super().preprocess(inputs)

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        assert inputs.execute_dir is None
        assert inputs.files_dir is None
        assert inputs.log_path is None
        self.processes.append(os.getpid())
        return super().execute_function(inputs)

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        assert inputs.postprocess_dir is None
        assert inputs.file_outputs == []
        return super().postprocess(inputs)


class _FilelessComposite(PredictMeasure):
    name = "direct_fileless_composite"
    inputs: ClassVar = Transform.inputs

    def compose(self, ctx: CompositeContext) -> None:
        transformed = ctx.call(
            _FilelessTransform, inputs={"structure": ctx.input("structure")}
        )
        measured = ctx.call(
            Measure, inputs={"structure": transformed.output("structure")}
        )
        ctx.output("structure", transformed.output("structure"))
        ctx.output("metrics", measured.output("metrics"))


class _Generate(OperationDefinition):
    name = "direct_generate"

    class OutputRole(StrEnum):
        metrics = "metrics"

    outputs: ClassVar = {
        "metrics": OutputSpec(artifact_type="metric", derives_from={"inputs": []})
    }

    def execute_function(self, inputs: ExecuteInput) -> dict[str, list[int]]:
        return {"values": [7]}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        result.add_artifact(
            "metrics",
            MetricArtifact.draft(
                {"value": inputs.memory_outputs["values"][0]}, "value.json"
            ),
            sources={},
        )
        return result


class _WriteFile(_Generate):
    name = "direct_write_file"

    class OutputRole(StrEnum):
        files = "files"

    outputs: ClassVar = {
        "files": OutputSpec(artifact_type="file_ref", derives_from={"inputs": []})
    }

    def execute_function(self, inputs: ExecuteInput) -> dict[str, list[str]]:
        assert inputs.files_dir is not None
        path = Path(inputs.files_dir) / "result.txt"
        path.write_text("result")
        return {"paths": [str(path)]}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        path = Path(inputs.memory_outputs["paths"][0])
        content = path.read_bytes()
        artifact = FileRefArtifact.draft(
            str(path), compute_content_digest(content), len(content)
        )
        return ArtifactResult(artifacts={"files": [artifact]}, lineage={"files": []})


class _Associated(Measure):
    name = "direct_associated"
    inputs: ClassVar = {
        "structure": InputSpec(
            artifact_type="csv", materialize=False, with_associated=["metric"]
        )
    }
    phases: ClassVar[list[str]] = []

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        self.phases.append("preprocess")
        values = [
            sum(
                item.values["score"]
                for item in inputs.associated_artifacts(
                    inputs.ref("structure", index=index), "metric"
                )
            )
            for index in range(len(inputs.input_artifacts["structure"]))
        ]
        return {"values": PerArtifact(values)}


class _Config(Transform):
    name = "direct_config"

    class OutputRole(StrEnum):
        config = "config"

    outputs: ClassVar = {
        "config": OutputSpec(
            artifact_type="config", derives_from={"inputs": ["structure"]}
        )
    }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        for index, artifact in enumerate(inputs.input_artifacts["structure"]):
            result.add_artifact(
                "config",
                ExecutionConfigArtifact.draft(
                    {"structure": {"$artifact": artifact.artifact_id}}, "config.json"
                ),
                sources={"structure": [inputs.ref("structure", index=index)]},
            )
        return result


class _ConfigOnly(_FilelessComposite):
    name = "direct_config_only"
    OutputRole: ClassVar = _Config.OutputRole
    outputs: ClassVar = {"config": OutputSpec(artifact_type="config")}

    def compose(self, ctx: CompositeContext) -> None:
        transformed = ctx.call(Transform, inputs={"structure": ctx.input("structure")})
        config = ctx.call(
            _Config, inputs={"structure": transformed.output("structure")}
        )
        ctx.output("config", config.output("config"))


class _ReadConfig(Measure):
    name = "direct_read_config"

    class InputRole(StrEnum):
        config = "config"

    inputs: ClassVar = {
        "config": InputSpec(
            artifact_type="config", materialize=False, resolve_references=True
        )
    }
    outputs: ClassVar = {
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["config"]}
        )
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            "values": PerArtifact(
                [
                    coordinate(
                        next(
                            iter(
                                inputs.referenced_artifacts(
                                    inputs.ref("config", index=index)
                                ).values()
                            )
                        )
                    )
                    for index in range(len(inputs.input_artifacts["config"]))
                ]
            )
        }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        for index, value in enumerate(inputs.memory_outputs["values"]):
            result.add_artifact(
                "metrics",
                MetricArtifact.draft({"score": value}, "metric.json"),
                sources={"config": [inputs.ref("config", index=index)]},
            )
        return result


def _forbid_setup(*args: Any, **kwargs: Any) -> None:
    pytest.fail("Fileless native call allocated files or a command owner")


@pytest.mark.parametrize("definition", [_FilelessTransform, _FilelessComposite])
def test_run_operation_fileless_native_and_composite_have_no_runtime_setup(
    monkeypatch: pytest.MonkeyPatch,
    definition: Definition,
) -> None:
    monkeypatch.setattr(tempfile, "mkdtemp", _forbid_setup)
    monkeypatch.setattr(invocation, "create_sandbox", _forbid_setup)
    monkeypatch.setattr(command_supervisor.CommandSupervisor, "start", _forbid_setup)
    _FilelessTransform.processes.clear()
    source = structure(3)
    result = run_operation(definition, inputs={"structure": [source, source]})
    assert [coordinate(item) for item in result.artifacts["structure"]] == [4, 4]
    assert _FilelessTransform.processes == [os.getpid(), os.getpid()]
    assert [item.source_input_index for item in result.lineage["structure"]] == [0, 1]
    assert all(
        item.is_finalized for values in result.artifacts.values() for item in values
    )
    assert not command_supervisor.has_command_scope()


def test_run_operation_batches_per_artifact_and_keeps_whole_streams() -> None:
    Transform.phase_sizes.clear()
    result = run_operation(
        _FilelessTransform, inputs={"structure": [structure(i) for i in range(3)]}
    )
    assert [coordinate(item) for item in result.artifacts["structure"]] == [1, 2, 3]
    assert Transform.phase_sizes == [
        ("preprocess", 2),
        ("execute", 1),
        ("execute", 1),
        ("postprocess", 2),
        ("preprocess", 1),
        ("execute", 1),
        ("postprocess", 1),
    ]
    summary = run_operation(
        SummarizeMetrics,
        inputs={
            "final": result.artifacts["structure"][:1],
            "history": [
                MetricArtifact.draft({"score": i}, "metric.json").finalize()
                for i in range(3)
            ],
        },
    )
    assert summary.artifacts["metrics"][0].values == {
        "selected": 1,
        "history": [0, 1, 2],
    }


def test_run_operation_generator_and_empty_streams() -> None:
    generated = run_operation(_Generate)
    assert generated.artifacts["metrics"][0].values == {"value": 7}
    assert generated.lineage == {"metrics": []}
    Transform.phase_sizes.clear()
    empty = run_operation(_FilelessTransform, inputs={"structure": []})
    assert empty.artifacts == {"structure": []}
    assert empty.lineage == {"structure": []}
    assert Transform.phase_sizes == []


def test_run_operation_does_not_enter_storage_or_software_preparation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Direct execution entered persistence, preparation, or history")

    for target in (
        "artisan.storage.core.artifact_store.ArtifactStore.__init__",
        "artisan.storage.cache.retained_lookup.lookup_assignment",
        "artisan.storage.cache.retained_lookup.lookup_step",
        "artisan.orchestration.runners.local.LocalRunner.prepare",
        "artisan.execution.executor_runtime.ExecutorRuntime.__init__",
        "artisan.execution.local_execution.observe_current_worker",
    ):
        monkeypatch.setattr(target, forbidden)
    for _ in range(2):
        assert run_operation(_Generate).artifacts["metrics"][0].values == {"value": 7}


def test_run_operation_declared_materialization_requires_directory_before_phases() -> (
    None
):
    class Materialized(Transform):
        name = "direct_materialized"
        inputs: ClassVar = {"structure": InputSpec(artifact_type="csv")}

    Transform.phase_sizes.clear()
    with pytest.raises(ValueError, match="directory|work_dir|path"):
        run_operation(Materialized, inputs={"structure": [structure(1)]})
    assert Transform.phase_sizes == []


def test_run_operation_files_belong_to_caller_after_success_and_failure(
    tmp_path: Path,
) -> None:
    class FailsAfterWriting(_WriteFile):
        name = "direct_failed_file"

        def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
            message = "postprocess failed"
            raise ValueError(message)

    root = tmp_path / "parent"
    first = run_operation(_WriteFile, work_dir=root).artifacts["files"][0]
    second = run_operation(_WriteFile, work_dir=root).artifacts["files"][0]
    with pytest.raises(ValueError, match="postprocess failed"):
        run_operation(FailsAfterWriting, work_dir=root)
    assert first.path != second.path
    assert len(list(root.rglob("result.txt"))) == 3
    assert Path(first.path).read_text() == "result"
    assert Path(second.path).read_text() == "result"
    shutil.rmtree(root)
    assert not Path(first.path).exists()


def test_run_operation_associations_bind_original_positions_without_history() -> None:
    source = structure(1)
    seven = MetricArtifact.draft({"score": 7}, "score.json").finalize()
    eleven = MetricArtifact.draft({"score": 11}, "score.json").finalize()
    _Associated.phases.clear()
    result = run_operation(
        _Associated,
        inputs={"structure": [source, source, source]},
        associations={
            ("structure", 0, "metric"): [seven],
            ("structure", 1, "metric"): [eleven],
            ("structure", 2, "metric"): [],
        },
    )
    assert [item.values["score"] for item in result.artifacts["metrics"]] == [7, 11, 0]
    assert [item.source_input_index for item in result.lineage["metrics"]] == [0, 1, 2]
    with pytest.raises(ValueError, match="captur|context|association|Association"):
        run_operation(_Associated, inputs={"structure": [source]})
    assert len(_Associated.phases) == 3


@pytest.mark.parametrize(
    "case", ["missing", "wrong_type", "wrong_position", "undeclared"]
)
def test_run_operation_invalid_associations_fail_before_phases(case: str) -> None:
    source = structure(1)
    valid = MetricArtifact.draft({"score": 2}, "score.json").finalize()
    contexts = {
        "missing": {},
        "wrong_type": {("structure", 0, "metric"): [source]},
        "wrong_position": {("structure", 1, "metric"): [valid]},
        "undeclared": {("structure", 0, "csv"): [source]},
    }
    _Associated.phases.clear()
    with pytest.raises(ValueError):
        run_operation(
            _Associated, inputs={"structure": [source]}, associations=contexts[case]
        )
    assert _Associated.phases == []


def test_run_operation_unexported_support_supplies_next_call(tmp_path: Path) -> None:
    result = run_operation(_ConfigOnly, inputs={"structure": [structure(4)]})
    assert set(result.artifacts) == {"config"}
    config = result.artifacts["config"][0]
    referents = result.referenced_artifacts("config", index=0)
    assert [coordinate(item) for item in referents.values()] == [5]
    with pytest.raises(ValueError, match="reference|referent|captur|context"):
        run_operation(_ReadConfig, inputs={"config": [config]}, work_dir=tmp_path)
    following = run_operation(
        _ReadConfig,
        inputs={"config": [config]},
        config_references={("config", 0): referents},
        work_dir=tmp_path,
    )
    assert following.artifacts["metrics"][0].values == {"score": 5}


def test_run_operation_reentrant_calls_fail_and_release_entry_guard() -> None:
    class Recursive(Transform):
        name = "direct_recursive"

        def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
            run_operation(_Generate)
            return super().execute_function(inputs)

    with pytest.raises(ExecuteFailure, match="recursive|reentrant|overlap|sequential"):
        run_operation(Recursive, inputs={"structure": [structure(1)]})
    assert run_operation(_Generate).artifacts["metrics"][0].values == {"value": 7}


def test_run_operation_overlapping_calls_fail_without_disturbing_active_call() -> None:
    entered, release = Event(), Event()

    class Waiting(_Generate):
        name = "direct_waiting"

        def execute_function(self, inputs: ExecuteInput) -> dict[str, list[int]]:
            entered.set()
            assert release.wait(10)
            return super().execute_function(inputs)

    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(run_operation, Waiting)
        try:
            assert entered.wait(10)
            with pytest.raises(
                RuntimeError, match="recursive|reentrant|overlap|sequential"
            ):
                run_operation(_Generate)
        finally:
            release.set()
        assert pending.result(timeout=10).artifacts["metrics"][0].values == {"value": 7}
    assert run_operation(_Generate).success


def test_run_operation_managed_parent_remains_usable_after_child_calls() -> None:
    class Failure(_Generate):
        name = "direct_parent_failure"

        def execute_function(self, inputs: ExecuteInput) -> dict[str, list[int]]:
            message = "child failed"
            raise ValueError(message)

    with command_supervisor.managed_invocation() as parent:
        assert run_operation(_Generate).success
        assert command_supervisor._current is parent
        with pytest.raises(ExecuteFailure, match="child failed"):
            run_operation(Failure)
        assert command_supervisor._current is parent
        command_supervisor.check_command_scope(expected=parent)
        result = run_command(
            IdentityCommandEnvironment(), [sys.executable, "-c", "print('alive')"]
        )
        assert result.stdout.strip() == "alive"


def test_run_operation_native_authored_command_manages_its_own_process() -> None:
    class NativeCommand(_Generate):
        name = "direct_native_command"

        def execute_function(self, inputs: ExecuteInput) -> dict[str, list[int]]:
            assert command_supervisor._current is None
            result = run_command(
                IdentityCommandEnvironment(), [sys.executable, "-c", "print(11)"]
            )
            assert command_supervisor._current is None
            return {"values": [int(result.stdout)]}

    result = run_operation(NativeCommand)
    assert result.artifacts["metrics"][0].values == {"value": 11}
    assert not command_supervisor.has_command_scope()


def test_run_operation_cancellation_propagates_and_releases_guard() -> None:
    class Cancelled(_Generate):
        name = "direct_cancelled"

        def execute_function(self, inputs: ExecuteInput) -> dict[str, list[int]]:
            message = "cancelled"
            raise CancelledError(message)

    with pytest.raises(CancelledError, match="cancelled"):
        run_operation(Cancelled)
    assert run_operation(_Generate).success


def test_run_operation_uncertain_cleanup_prevents_finalization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def uncertain(*args: Any, **kwargs: Any) -> bool:
        message = "cleanup uncertain"
        raise command_supervisor.CommandOwnershipError(message)

    monkeypatch.setattr(invocation, "close_command_scope", uncertain)
    monkeypatch.setattr(invocation, "finalize_artifacts", _forbid_setup)
    with pytest.raises(
        command_supervisor.CommandOwnershipError, match="cleanup uncertain"
    ):
        run_operation(_Generate)


def test_run_operation_invalid_inherited_ownership_fails_before_phases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for key in COMMAND_ATTACHMENT_ENV:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv(COMMAND_ATTACHMENT_ENV[0], "invalid-attachment")
    Transform.phase_sizes.clear()
    with pytest.raises(command_supervisor.CommandOwnershipError):
        run_operation(Transform, inputs={"structure": [structure(1)]})
    assert Transform.phase_sizes == []


def test_run_operation_empty_command_needs_no_software_or_directory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class EmptyCommand(OperationDefinition):
        name = "direct_empty_command"
        InputRole: ClassVar = Transform.InputRole
        OutputRole: ClassVar = Measure.OutputRole
        inputs: ClassVar = {"structure": InputSpec(artifact_type="csv")}
        outputs: ClassVar = Measure.outputs
        tool: ToolSpec = ToolSpec(executable="python")

        def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
            pytest.fail("Empty command reached preprocessing")

        def execute_command(self, inputs: dict[str, Any]) -> list[str]:
            pytest.fail("Empty command reached execution")

        def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
            pytest.fail("Empty command reached postprocessing")

    monkeypatch.setattr(local_execution, "observe_current_worker", _forbid_setup)
    monkeypatch.setattr(tempfile, "mkdtemp", _forbid_setup)
    monkeypatch.setattr(command_supervisor.CommandSupervisor, "start", _forbid_setup)
    result = run_operation(EmptyCommand, inputs={"structure": []})
    assert result.artifacts == {"metrics": []}
    assert result.lineage == {"metrics": []}


def test_run_operation_native_environment_declaration_is_not_silently_ignored(
    tmp_path: Path,
) -> None:
    class NativeEnvironment(Transform):
        name = "direct_native_environment"
        environment: Environment = Environment.from_pixi(tmp_path / "pixi.toml")

    Transform.phase_sizes.clear()
    with pytest.raises(ValueError, match="Native operations cannot declare"):
        run_operation(NativeEnvironment, inputs={"structure": [structure(1)]})
    assert Transform.phase_sizes == []
