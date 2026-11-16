"""Standalone roots retain exact context without worker or store admission."""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Sequence
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import pytest
from fixtures.composition_operations import (
    Iterate,
    Measure,
    PredictMeasure,
    Transform,
    coordinate,
    structure,
)

from artisan.composites import CompositeContext
from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.execution.composition import ExecuteAdapter
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.session import ExecutionSession
from artisan.execution.session_runtime import SessionRuntime
from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas import ArtifactResult, InputSpec, OutputSpec
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)


@pytest.fixture
def native_adapter(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, ...]]:
    """Keep kernel tests independent of measured software tests and source freeze."""
    calls: list[tuple[str, ...]] = []

    def software(
        self: SessionRuntime,
        plan: CompiledPlan,
        inventory: Sequence[Artifact],
        root: Path,
    ) -> ExecuteAdapter:
        def adapter(
            operation: OperationDefinition, path: tuple[str, ...]
        ) -> LocalExecuteRouter:
            calls.append(path)
            return LocalExecuteRouter()

        return adapter

    monkeypatch.setattr(SessionRuntime, "_software", software)
    return calls


def test_standalone_root_partitions_preserve_visible_lineage_and_tool_paths(
    tmp_path: Path,
    native_adapter: list[tuple[str, ...]],
) -> None:
    Transform.phase_sizes.clear()
    inputs = [structure(i) for i in range(3)]
    with ExecutionSession(working_root=tmp_path) as session:
        result = session.run(PredictMeasure, inputs={"structure": inputs})
        assert [coordinate(item) for item in result.artifacts["structure"]] == [1, 2, 3]
        assert [item.source_input_index for item in result.lineage["structure"]] == [
            0,
            1,
            2,
        ]
        assert [item.source_output_index for item in result.lineage["metrics"]] == [
            0,
            1,
            2,
        ]
        assert all(
            item.is_finalized and item.origin_step_number is None
            for values in result.artifacts.values()
            for item in values
        )
    assert native_adapter == [("predict",), ("measure",)] * 3
    assert Transform.phase_sizes == [
        (phase, 1)
        for _ in range(3)
        for phase in ("preprocess", "execute", "postprocess")
    ]


class BatchedTransform(Transform):
    name = "session_batched_transform"
    per_artifact_dispatch = False
    batch_strategy: BatchStrategy = BatchStrategy(
        artifacts_per_unit=9, max_artifacts_per_unit=2
    )


def test_creator_partitioning_and_fixed_loop_share_kernel(
    tmp_path: Path, native_adapter: list[tuple[str, ...]]
) -> None:
    Transform.phase_sizes.clear()
    with ExecutionSession(working_root=tmp_path) as session:
        result = session.run(
            BatchedTransform, inputs={"structure": [structure(i) for i in range(5)]}
        )
        assert [coordinate(item) for item in result.artifacts["structure"]] == [
            1,
            2,
            3,
            4,
            5,
        ]
        assert Transform.phase_sizes == [
            (phase, size)
            for size in (2, 2, 1)
            for phase in ("preprocess", "execute", "postprocess")
        ]
        loop = session.run(Iterate, inputs={"structure": [structure(0)]})
        assert coordinate(loop.artifacts["structure"][0]) == 14


@pytest.mark.parametrize(
    "inputs", [{"structure": []}, {"structure": [], "unknown": []}, {}]
)
def test_empty_and_invalid_inputs_never_resolve_software(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, inputs: dict[str, list[Artifact]]
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("zero or invalid work resolved software")

    monkeypatch.setattr(SessionRuntime, "_software", forbidden)
    with ExecutionSession(working_root=tmp_path) as session:
        if inputs == {"structure": []}:
            result = session.run(PredictMeasure, inputs=inputs)
            assert result.artifacts == {"structure": [], "metrics": []}
            assert result.lineage == {"structure": [], "metrics": []}
        else:
            with pytest.raises(ValueError, match="missing required or undeclared"):
                session.run(PredictMeasure, inputs=inputs)


class Associated(Measure):
    name = "session_associated"
    inputs: ClassVar = {
        "structure": InputSpec(
            artifact_type="csv", materialize=False, with_associated=["metric"]
        )
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        counts = [
            len(inputs.associated_artifacts(inputs.ref("structure", index=i), "metric"))
            for i in range(len(inputs.input_artifacts["structure"]))
        ]
        return {"values": PerArtifact(counts)}


def test_association_context_accumulates_only_retained_exact_relations(
    tmp_path: Path, native_adapter: list[tuple[str, ...]]
) -> None:
    with ExecutionSession(working_root=tmp_path) as session:
        first = session.run(Transform, inputs={"structure": [structure(0)]}).artifacts[
            "structure"
        ][0]
        empty = session.run(Associated, inputs={"structure": [first]})
        assert empty.artifacts["metrics"][0].values == {"score": 0}
        # The query's own exported metric is a genuine later direct relation too.
        session.run(Measure, inputs={"structure": [first]})
        later = session.run(Associated, inputs={"structure": [first]})
        assert later.artifacts["metrics"][0].values == {"score": 2}
        other = session.run(
            PredictMeasure, inputs={"structure": [structure(0)]}
        ).artifacts["structure"][0]
        assert first.artifact_id == other.artifact_id
        exact = session.run(Associated, inputs={"structure": [other, other]})
        assert [item.values for item in exact.artifacts["metrics"]] == [
            {"score": 1},
            {"score": 1},
        ]
        with pytest.raises(ValueError, match="no captured session producer"):
            session.run(Associated, inputs={"structure": [other.model_copy(deep=True)]})


class Passthrough(CompositeDefinition):
    name = "session_passthrough"
    InputRole: ClassVar = Transform.InputRole
    OutputRole: ClassVar = Transform.OutputRole
    inputs: ClassVar = Transform.inputs
    outputs: ClassVar = {"structure": OutputSpec(artifact_type="csv")}

    def compose(self, ctx: CompositeContext) -> None:
        ctx.output("structure", ctx.input("structure"))


def test_unscoped_passthrough_and_static_nested_query_fail_before_science(
    tmp_path: Path, native_adapter: list[tuple[str, ...]]
) -> None:
    class Query(PredictMeasure):
        name = "session_nested_query"

        def compose(self, ctx: CompositeContext) -> None:
            ctx.call(Transform, inputs={"structure": ctx.input("structure")})
            measured = ctx.call(
                Associated, inputs={"structure": ctx.input("structure")}
            )
            ctx.output("structure", ctx.input("structure"))
            ctx.output("metrics", measured.output("metrics"))

    with ExecutionSession(working_root=tmp_path) as session:
        original = structure(2)
        returned = session.run(Passthrough, inputs={"structure": [original]}).artifacts[
            "structure"
        ][0]
        assert returned is original
        with pytest.raises(ValueError, match="no captured session producer"):
            session.run(Query, inputs={"structure": [returned]})
    assert native_adapter == []


class Config(Transform):
    name = "session_config"

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


class ConfigOnly(Passthrough):
    name = "session_config_only"
    OutputRole: ClassVar = Config.OutputRole
    outputs: ClassVar = {"config": OutputSpec(artifact_type="config")}

    def compose(self, ctx: CompositeContext) -> None:
        transformed = ctx.call(Transform, inputs={"structure": ctx.input("structure")})
        config = ctx.call(Config, inputs={"structure": transformed.output("structure")})
        ctx.output("config", config.output("config"))


class ReadConfig(Measure):
    name = "session_read_config"

    class InputRole(StrEnum):
        config = "config"

    inputs: ClassVar = {"config": InputSpec(artifact_type="config")}
    outputs: ClassVar = {
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["config"]}
        )
    }
    paths: ClassVar[list[Path]] = []

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        config = inputs.input_artifacts["config"][0]
        path = Path(config.materialized_path)
        reference = Path(json.loads(path.read_text())["structure"])
        self.paths.extend([path, reference])
        return {"values": [int(reference.read_text().splitlines()[1])]}


def test_required_support_survives_later_call_and_mutation_rejects(
    tmp_path: Path, native_adapter: list[tuple[str, ...]]
) -> None:
    # Config output has hidden required CSV support; the next call need not resupply it.
    class InspectConfig(ReadConfig):
        name = "session_inspect_config"
        outputs: ClassVar = {
            "metrics": OutputSpec(
                artifact_type="metric",
                required=False,
                derives_from={"inputs": ["config"]},
            )
        }

        def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
            return ArtifactResult(artifacts={"metrics": []}, lineage={"metrics": []})

    ReadConfig.paths.clear()
    with ExecutionSession(working_root=tmp_path) as session:
        result = session.run(ConfigOnly, inputs={"structure": [structure(4)]})
        config = result.artifacts["config"][0]
        session.run(InspectConfig, inputs={"config": [config]})
        assert all(path.is_file() for path in ReadConfig.paths)
        assert ReadConfig.paths[-1].read_text() == "coordinate\n5\n"
        with pytest.raises(ValueError, match="referen|missing|Missing"):
            session.run(
                InspectConfig, inputs={"config": [config.model_copy(deep=True)]}
            )
    assert all(not path.exists() for path in ReadConfig.paths)


def test_unavailable_carrier_rejects_without_opening_source_store(
    tmp_path: Path, native_adapter: list[tuple[str, ...]]
) -> None:
    from artisan.schemas.artifact.file_ref import FileRefArtifact

    path = tmp_path / "external.txt"
    path.write_text("external")
    from artisan.utils.hashing import compute_content_digest

    artifact = FileRefArtifact.draft(
        str(path), compute_content_digest(path.read_bytes()), path.stat().st_size
    ).finalize()
    path.unlink()

    class Files(Passthrough):
        name = "session_files"
        inputs: ClassVar = {
            "structure": InputSpec(artifact_type="file_ref", materialize=False)
        }
        outputs: ClassVar = {"structure": OutputSpec(artifact_type="file_ref")}

    with (
        ExecutionSession(working_root=tmp_path) as session,
        pytest.raises(ValueError, match="available local artifact carriers"),
    ):
        session.run(Files, inputs={"structure": [artifact]})
    assert native_adapter == []


def test_lifetime_borrowed_and_recursive_entry_reject(
    tmp_path: Path, native_adapter: list[tuple[str, ...]]
) -> None:
    session = ExecutionSession(working_root=tmp_path)
    with pytest.raises(RuntimeError, match="active independently"):
        session.run(Transform)
    with session:
        with (
            ExecutionSession(working_root=tmp_path) as borrowed,
            pytest.raises(RuntimeError, match="active independently"),
        ):
            borrowed.run(Transform)
        with (
            session.invocation(),
            pytest.raises(RuntimeError, match="scope is not current"),
        ):
            session.run(Transform)
        session._run_lock.acquire()
        try:
            with pytest.raises(RuntimeError, match="sequential and nonrecursive"):
                session.run(Transform)
        finally:
            session._run_lock.release()
    with pytest.raises(RuntimeError, match="active independently"):
        session.run(Transform)


def test_empty_invalid_grouping_is_not_silently_accepted(
    tmp_path: Path, native_adapter: list[tuple[str, ...]]
) -> None:
    class Named(Transform):
        name = "session_named"
        group_by: GroupByStrategy = GroupByStrategy.NAME

    with (
        ExecutionSession(working_root=tmp_path) as session,
        pytest.raises(ValueError, match="Local input pairing"),
    ):
        session.run(Named, inputs={"structure": []})


@pytest.mark.parametrize("invoked", [False, True])
def test_unknown_closure_keeps_exact_scratch_and_prevents_reentry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    native_adapter: list[tuple[str, ...]],
    invoked: bool,
) -> None:
    session = ExecutionSession(working_root=tmp_path)
    with session:
        if invoked:
            with pytest.raises(ValueError, match="missing required"):
                session.run(Transform)
        root = session._root
        monkeypatch.setattr(
            "artisan.execution.session.close_command_scope", lambda _error: False
        )
    assert root is not None
    assert root.is_dir()
    assert session._root == root
    assert not session.cleanup_complete
    with pytest.raises(RuntimeError, match="unresolved cleanup ownership"):
        session.__enter__()


class Pair(CompositeDefinition):
    name = "session_pair"

    class InputRole(StrEnum):
        left = "left"
        right = "right"

    class OutputRole(StrEnum):
        left = "left"
        right = "right"

    inputs: ClassVar = {
        role: InputSpec(artifact_type="csv") for role in ("left", "right")
    }
    outputs: ClassVar = {
        role: OutputSpec(artifact_type="csv") for role in ("left", "right")
    }

    def compose(self, ctx: CompositeContext) -> None:
        ctx.output("left", ctx.input("left"))
        ctx.output("right", ctx.input("right"))


class PairCreator(OperationDefinition):
    name = "session_pair_creator"
    InputRole: ClassVar = Pair.InputRole
    OutputRole: ClassVar = Pair.OutputRole
    inputs: ClassVar = Pair.inputs
    outputs: ClassVar = {
        role: OutputSpec(artifact_type="csv", derives_from={"inputs": [role]})
        for role in ("left", "right")
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {}

    def execute_function(self, inputs: ExecuteInput) -> None:
        return None

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        for role, values in inputs.input_artifacts.items():
            for index, artifact in enumerate(values):
                result.add_artifact(
                    role, artifact, sources={role: [inputs.ref(role, index=index)]}
                )
        return result


@pytest.mark.parametrize("cross", [False, True])
def test_root_pairing_retains_input_positions(
    tmp_path: Path, native_adapter: list[tuple[str, ...]], cross: bool
) -> None:
    class SelectedPair(PairCreator):
        name = "session_selected_pair"
        group_by: GroupByStrategy = (
            GroupByStrategy.CROSS_PRODUCT if cross else GroupByStrategy.ZIP
        )

    with ExecutionSession(working_root=tmp_path) as session:
        result = session.run(
            SelectedPair,
            inputs={
                "left": [structure(1), structure(2)],
                "right": [structure(3), structure(4)],
            },
        )
        assert [coordinate(item) for item in result.artifacts["left"]] == (
            [1, 1, 2, 2] if cross else [1, 2]
        )
        assert [coordinate(item) for item in result.artifacts["right"]] == (
            [3, 4, 3, 4] if cross else [3, 4]
        )
        assert [item.source_input_index for item in result.lineage["left"]] == (
            [0, 0, 1, 1] if cross else [0, 1]
        )
        if cross:
            assert session.run(
                SelectedPair, inputs={"left": [structure(1)], "right": []}
            ).artifacts == {"left": [], "right": []}
        else:
            with pytest.raises(ValueError, match="equal lengths"):
                session.run(SelectedPair, inputs={"left": [structure(1)], "right": []})


def test_aliases_share_context_but_hidden_metrics_are_discarded(
    tmp_path: Path, native_adapter: list[tuple[str, ...]]
) -> None:
    class Hidden(Passthrough):
        name = "session_hidden"

        def compose(self, ctx: CompositeContext) -> None:
            made = ctx.call(Transform, inputs={"structure": ctx.input("structure")})
            ctx.call(Measure, inputs={"structure": made.output("structure")})
            ctx.output(
                "structure", [made.output("structure"), made.output("structure")]
            )

    with ExecutionSession(working_root=tmp_path) as session:
        result = session.run(Hidden, inputs={"structure": [structure(1)]})
        assert result.artifacts["structure"][0] is result.artifacts["structure"][1]
        found = session.run(
            Associated, inputs={"structure": result.artifacts["structure"]}
        )
        assert [item.values for item in found.artifacts["metrics"]] == [
            {"score": 0},
            {"score": 0},
        ]


class Generate(OperationDefinition):
    name = "session_generate"

    class OutputRole(StrEnum):
        metrics = "metrics"

    outputs: ClassVar = {
        "metrics": OutputSpec(artifact_type="metric", derives_from={"inputs": []})
    }

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        return {"pid": os.getpid(), "executable": sys.executable}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        return ArtifactResult(
            artifacts={
                "metrics": [
                    MetricArtifact.draft(
                        inputs.memory_outputs, "runtime.json", inputs.step_number
                    )
                ]
            },
            lineage={"metrics": []},
        )


def test_actual_observation_keeps_local_class_in_current_interpreter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import artisan.execution.environments.observed_worker as observed

    class LocalDefinition(Generate):
        name = "session_local_definition"

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Standalone native call launched a qualification child")

    monkeypatch.setattr(observed.subprocess, "run", forbidden)
    with ExecutionSession(working_root=tmp_path) as session:
        result = session.run(LocalDefinition)
        assert result.artifacts["metrics"][0].values == {
            "pid": [os.getpid()],
            "executable": [sys.executable],
        }
    assert session.cleanup_complete


class Command(OperationDefinition):
    name = "session_command"
    OutputRole: ClassVar = Generate.OutputRole
    outputs: ClassVar = Generate.outputs
    tool: ToolSpec = ToolSpec(executable="python")

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {}

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        assert self.tool is not None
        return [
            *self.tool.parts(),
            "-c",
            "import json,os,sys;from pathlib import Path;Path(sys.argv[1]).write_text(json.dumps({'pid':os.getpid(),'executable':sys.executable}))",
            "runtime.json",
        ]

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        path = next(
            Path(item)
            for item in inputs.file_outputs
            if Path(item).name == "runtime.json"
        )
        values = json.loads(path.read_text())
        return ArtifactResult(
            artifacts={
                "metrics": [
                    MetricArtifact.draft(values, "runtime.json", inputs.step_number)
                ]
            },
            lineage={"metrics": []},
        )


def test_actual_inherited_command_uses_verified_existing_helper(tmp_path: Path) -> None:
    cache = os.environ.get("ARTISAN_TEST_PREPARED_CACHE")
    if cache is None:
        pytest.skip("Explicit existing command-witness qualification cache required")
    with ExecutionSession(working_root=tmp_path) as session:
        runtime = SessionRuntime("pixi")
        runtime.cache_root = Path(cache)
        session._runtime = runtime
        result = session.run(Command)
        assert result.artifacts["metrics"][0].values["pid"] != os.getpid()
        assert result.artifacts["metrics"][0].values["executable"] == sys.executable
        assert session.run(Generate).artifacts["metrics"][0].values["pid"] == [
            os.getpid()
        ]
    assert session.cleanup_complete


def test_actual_prepared_cpu_tool_uses_existing_prefix(tmp_path: Path) -> None:
    from artisan.execution.environments.command_witness import BUILD_MANIFEST, COMPILER
    from artisan.execution.recording.commands import capture_commands
    from artisan.schemas.operation_config.environment import Environment

    cache = os.environ.get("ARTISAN_TEST_PREPARED_CACHE")
    if cache is None:
        pytest.skip("Explicit existing witness-builder qualification cache required")

    class Compiler(Command):
        name = "session_compiler_probe"
        environment: Environment | None = Environment.from_pixi(BUILD_MANIFEST)
        tool: ToolSpec = ToolSpec(executable=COMPILER)

        def execute_command(self, inputs: dict[str, Any]) -> list[str]:
            assert self.tool is not None
            return [*self.tool.parts(), "--version"]

        def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
            return ArtifactResult(
                artifacts={
                    "metrics": [MetricArtifact.draft({"completed": True}, "probe.json")]
                },
                lineage={"metrics": []},
            )

    with (
        ExecutionSession(working_root=tmp_path) as session,
        capture_commands() as recorder,
    ):
        runtime = SessionRuntime("pixi")
        runtime.cache_root = Path(cache)
        session._runtime = runtime
        assert session.run(Compiler).artifacts["metrics"][0].values == {
            "completed": True
        }
    commands = recorder.snapshot().commands
    assert len(commands) == 1
    assert commands[0].returncode == 0
    assert any(
        str(Path(cache) / "native") in argument and COMPILER in argument
        for argument in commands[0].argv
    )
    assert session.cleanup_complete


def test_actual_missing_target_fails_before_authored_phases(tmp_path: Path) -> None:
    from artisan.schemas.operation_config.environment import Environment

    class Missing(Command):
        name = "session_missing_target"
        environment: Environment | None = Environment.from_pixi(tmp_path / "pixi.toml")

        def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
            pytest.fail("Missing prepared target reached authored code")

    with (
        ExecutionSession(working_root=tmp_path) as session,
        pytest.raises(FileNotFoundError),
    ):
        session.run(Missing)


def test_same_object_with_two_producers_rejects_later_context(
    tmp_path: Path, native_adapter: list[tuple[str, ...]]
) -> None:
    class Shared(Generate):
        name = "session_shared"
        value: ClassVar = MetricArtifact.draft({"value": 1}, "shared.json").finalize()

        def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
            return ArtifactResult(
                artifacts={"metrics": [self.value]}, lineage={"metrics": []}
            )

    class MetricIdentity(Passthrough):
        name = "session_metric_identity"
        inputs: ClassVar = {
            "structure": InputSpec(artifact_type="metric", materialize=False)
        }
        outputs: ClassVar = {"structure": OutputSpec(artifact_type="metric")}

    with ExecutionSession(working_root=tmp_path) as session:
        first = session.run(Shared).artifacts["metrics"][0]
        assert session.run(Shared).artifacts["metrics"][0] is first
        with pytest.raises(ValueError, match="ambiguous producer"):
            session.run(MetricIdentity, inputs={"structure": [first]})


def test_whole_stream_is_not_split_and_invalid_output_does_not_gain_context(
    tmp_path: Path, native_adapter: list[tuple[str, ...]]
) -> None:
    from artisan.execution.exceptions import ArtifactValidationError

    class Whole(Transform):
        name = "session_whole"
        independent_input_streams = True
        per_artifact_dispatch = False

    class Drop(Transform):
        name = "session_drop"

        def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
            return {"values": []}

    Transform.phase_sizes.clear()
    with ExecutionSession(working_root=tmp_path) as session:
        result = session.run(
            Whole, inputs={"structure": [structure(i) for i in range(4)]}
        )
        assert len(result.artifacts["structure"]) == 4
        assert Transform.phase_sizes == [
            (phase, 4) for phase in ("preprocess", "execute", "postprocess")
        ]
        with pytest.raises(ArtifactValidationError, match="required"):
            session.run(Drop, inputs={"structure": [structure(0)]})


def test_mutated_prior_output_rejects_before_following_science(
    tmp_path: Path, native_adapter: list[tuple[str, ...]]
) -> None:
    from artisan.errors import ArtifactIntegrityError

    with ExecutionSession(working_root=tmp_path) as session:
        first = session.run(Transform, inputs={"structure": [structure(0)]}).artifacts[
            "structure"
        ][0]
        with pytest.raises(TypeError, match="finalized artifact"):
            first.content = b"coordinate\n999\n"
        object.__setattr__(first, "content", b"coordinate\n999\n")
        before = len(native_adapter)
        with pytest.raises(ArtifactIntegrityError):
            session.run(Transform, inputs={"structure": [first]})
        assert len(native_adapter) == before


def test_creator_returning_unscoped_object_cannot_capture_empty_associations(
    tmp_path: Path, native_adapter: list[tuple[str, ...]]
) -> None:
    class ReturnInput(Transform):
        name = "session_return_input"

        def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
            result = ArtifactResult()
            result.add_artifact(
                "structure",
                inputs.input_artifacts["structure"][0],
                sources={"structure": [inputs.ref("structure", index=0)]},
            )
            return result

    with ExecutionSession(working_root=tmp_path) as session:
        original = structure(0)
        result = session.run(ReturnInput, inputs={"structure": [original]})
        assert result.artifacts["structure"][0] is original
        with pytest.raises(ValueError, match="no captured session producer"):
            session.run(Associated, inputs=result.artifacts)


@pytest.mark.parametrize("body_fails", [False, True])
def test_final_supervisor_unknown_keeps_session_ownership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    body_fails: bool,
) -> None:
    from artisan.utils.command_supervisor import (
        CommandOwnershipError,
        CommandSupervisor,
        OwnershipOutcome,
    )

    original_finish = CommandSupervisor.finish

    def unknown(owner: CommandSupervisor, deadline: float) -> OwnershipOutcome:
        assert original_finish(owner, deadline).state == "closed"
        return OwnershipOutcome("unknown", False, "injected final collection loss")

    monkeypatch.setattr(CommandSupervisor, "finish", unknown)
    session = ExecutionSession(working_root=tmp_path)
    failure = ValueError("authored failure")
    with (
        pytest.raises(ValueError if body_fails else CommandOwnershipError) as raised,
        session,
    ):
        root = session._root
        if body_fails:
            raise failure
    if body_fails:
        assert raised.value is failure
        assert any(
            "cleanup could not be confirmed" in note for note in failure.__notes__
        )
    assert root is not None
    assert root.is_dir()
    assert session._root == root
    assert not session.cleanup_complete
    with pytest.raises(RuntimeError, match="unresolved cleanup ownership"):
        session.__enter__()
