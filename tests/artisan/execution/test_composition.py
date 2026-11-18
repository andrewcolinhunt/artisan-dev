"""One worker executes static scientific compositions and retains exact outputs."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import pytest
from fixtures.composition_operations import (
    Iterate as _Iterate,
)
from fixtures.composition_operations import (
    Measure as _Measure,
)
from fixtures.composition_operations import (
    PredictMeasure as _PredictMeasure,
)
from fixtures.composition_operations import (
    SummarizeMetrics as _Summary,
)
from fixtures.composition_operations import (
    Transform as _Transform,
)
from fixtures.composition_operations import (
    coordinate as _coordinate,
)
from fixtures.composition_operations import (
    structure as _structure,
)

from artisan.composites import CompositeContext
from artisan.composites.base.compiler import compile_definition
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.execution.composition import PlanExecution, execute_compiled_plan
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.exceptions import ArtifactValidationError
from artisan.execution.executor import Executor
from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas import ArtifactResult, InputSpec, OutputSpec
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.storage.core.artifact_store import ArtifactStore


def _run(
    definition: type[CompositeDefinition] | type[OperationDefinition],
    inputs: dict[str, list[Artifact]],
    tmp_path: Path,
) -> PlanExecution:
    with Executor(working_root=tmp_path / "session") as executor:
        return execute_compiled_plan(
            compile_definition(definition),
            inputs,
            executor,
            invocation_id="unit",
            execute_adapter=lambda _operation, _path: LocalExecuteRouter(),
        )


def test_prediction_and_measurement_preserve_direct_retained_relationship(
    tmp_path: Path,
) -> None:
    output = _run(_PredictMeasure, {"structure": [_structure(0)]}, tmp_path)
    structure = output.projection.export_roles()["structure"][0]
    metric = output.projection.export_roles()["metrics"][0]
    assert _coordinate(output.result.artifacts["structure"][0]) == 1
    assert output.result.artifacts["metrics"][0].values == {"score": 1}
    assert {(edge.source, edge.target) for edge in output.projection.associations} >= {
        (structure, metric)
    }
    assert len(output.retained_artifacts) == 2
    mapping = output.result.lineage["metrics"][0]
    assert (mapping.source_role, mapping.source_output_index) == ("structure", 0)


def test_fixed_iterations_retain_only_final_structure_and_ordinary_summary(
    tmp_path: Path,
) -> None:
    _Summary.observed.clear()
    output = _run(_Iterate, {"structure": [_structure(0)]}, tmp_path)
    assert _Summary.observed == [(1, 3)]
    assert _coordinate(output.result.artifacts["structure"][0]) == 14
    assert output.result.artifacts["metrics"][0].values == {
        "selected": 14,
        "history": [2, 6, 14],
    }
    assert len(output.retained_artifacts) == 2
    assert output.projection.support == ()
    structure = output.projection.export_roles()["structure"][0]
    metric = output.projection.export_roles()["metrics"][0]
    assert [(edge.source, edge.target) for edge in output.projection.associations] == [
        (structure, metric)
    ]


class _OrderedAliases(_PredictMeasure):
    name = "composition_ordered_aliases"

    def compose(self, ctx: CompositeContext) -> None:
        a = ctx.call(
            _Transform,
            name="a",
            inputs={"structure": ctx.input("structure")},
            params={"add": 0},
        )
        b = ctx.call(
            _Transform,
            name="b",
            inputs={"structure": ctx.input("structure")},
            params={"add": 0},
        )
        metric = ctx.call(_Measure, inputs={"structure": b.output("structure")})
        ctx.output(
            "structure",
            [b.output("structure"), a.output("structure"), b.output("structure")],
        )
        ctx.output("metrics", metric.output("metrics"))


def test_equal_content_branches_and_alias_export_order_remain_distinct(
    tmp_path: Path,
) -> None:
    output = _run(_OrderedAliases, {"structure": [_structure(0)]}, tmp_path)
    exports = output.projection.export_roles()["structure"]
    assert exports[0] == exports[2]
    assert exports[0] != exports[1]
    assert (
        len({artifact.artifact_id for artifact in output.result.artifacts["structure"]})
        == 1
    )
    metric = output.projection.export_roles()["metrics"][0]
    assert (exports[0], metric) in {
        (edge.source, edge.target) for edge in output.projection.associations
    }
    assert (exports[1], metric) not in {
        (edge.source, edge.target) for edge in output.projection.associations
    }
    assert output.result.lineage["metrics"][0].source_output_index == 0


def test_interpreter_never_queries_or_records_to_a_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = _structure(0)

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Composition accessed store or publication")

    monkeypatch.setattr(ArtifactStore, "get_artifact", forbidden)
    monkeypatch.setattr(ArtifactStore, "get_artifacts_by_type", forbidden)
    monkeypatch.setattr(
        "artisan.execution.recording.recorder.record_execution_success", forbidden
    )
    monkeypatch.setattr(
        "artisan.execution.recording.recorder.record_execution_failure", forbidden
    )
    output = _run(_Iterate, {"structure": [artifact]}, tmp_path)
    assert output.result.success


class _BatchedTransform(_Transform):
    name = "composition_batched_transform"
    batch_strategy: BatchStrategy = BatchStrategy(
        artifacts_per_unit=10, max_artifacts_per_unit=2
    )
    per_artifact_dispatch = False


class _BatchedComposite(CompositeDefinition):
    name = "composition_batched"

    class InputRole(StrEnum):
        structure = "structure"

    class OutputRole(StrEnum):
        structure = "structure"

    inputs: ClassVar = {"structure": InputSpec(artifact_type="csv")}
    outputs: ClassVar = {"structure": OutputSpec(artifact_type="csv")}

    def compose(self, ctx: CompositeContext) -> None:
        child = ctx.call(
            _BatchedTransform, inputs={"structure": ctx.input("structure")}
        )
        ctx.output("structure", child.output("structure"))


def test_child_lifecycle_partitions_are_distinct_from_command_splitting(
    tmp_path: Path,
) -> None:
    _BatchedTransform.phase_sizes.clear()
    output = _run(
        _BatchedComposite, {"structure": [_structure(i) for i in range(5)]}, tmp_path
    )
    assert _BatchedTransform.phase_sizes == [
        (phase, size)
        for size in (2, 2, 1)
        for phase in ("preprocess", "execute", "postprocess")
    ]
    assert [
        _coordinate(artifact) for artifact in output.result.artifacts["structure"]
    ] == [1, 2, 3, 4, 5]
    assert (
        len(
            {
                address.invocation_id
                for address in output.projection.export_roles()["structure"]
            }
        )
        == 3
    )


def test_admitted_creator_partition_is_not_split_again(tmp_path: Path) -> None:
    _BatchedTransform.phase_sizes.clear()
    output = _run(
        _BatchedTransform, {"structure": [_structure(i) for i in range(5)]}, tmp_path
    )
    assert _BatchedTransform.phase_sizes == [
        (phase, 5) for phase in ("preprocess", "execute", "postprocess")
    ]
    assert len(output.result.artifacts["structure"]) == 5


class _Drop(_Transform):
    name = "composition_drop"
    outputs: ClassVar = {
        "structure": OutputSpec(
            artifact_type="csv", required=False, derives_from={"inputs": ["structure"]}
        )
    }

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        return {"values": []}


class _GenerateMetric(OperationDefinition):
    name = "composition_generate_metric"

    class OutputRole(StrEnum):
        metrics = "metrics"

    outputs: ClassVar = {
        "metrics": OutputSpec(artifact_type="metric", derives_from={"inputs": []})
    }
    runs: ClassVar[int] = 0

    def execute_function(self, inputs: ExecuteInput) -> None:
        type(self).runs += 1

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        return ArtifactResult(
            artifacts={
                "metrics": [
                    MetricArtifact.draft(
                        {"score": 99}, "generated.json", inputs.step_number
                    )
                ]
            },
            lineage={"metrics": []},
        )


class _NestedEmpty(_PredictMeasure):
    name = "composition_nested_empty"

    def compose(self, ctx: CompositeContext) -> None:
        skipped = ctx.call(_Transform, inputs={"structure": ctx.input("structure")})
        generated = ctx.call(_GenerateMetric)
        ctx.output("structure", skipped.output("structure"))
        ctx.output("metrics", generated.output("metrics"))


class _OuterEmpty(_PredictMeasure):
    name = "composition_outer_empty"
    outputs: ClassVar = {
        "structure": OutputSpec(artifact_type="csv", required=False),
        "metrics": OutputSpec(artifact_type="metric"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        dropped = ctx.call(_Drop, inputs={"structure": ctx.input("structure")})
        nested = ctx.call(
            _NestedEmpty, inputs={"structure": dropped.output("structure")}
        )
        ctx.output("structure", nested.output("structure"))
        ctx.output("metrics", nested.output("metrics"))


def test_nested_empty_stream_skips_consumer_but_independent_generator_runs(
    tmp_path: Path,
) -> None:
    _GenerateMetric.runs = 0
    _Transform.phase_sizes.clear()
    result = _run(_OuterEmpty, {"structure": [_structure(0)]}, tmp_path)
    assert _GenerateMetric.runs == 1
    assert _Transform.phase_sizes == [("preprocess", 1), ("postprocess", 0)]
    assert result.result.artifacts["structure"] == []
    assert result.projection.export_roles()["structure"] == ()
    assert result.result.artifacts["metrics"][0].values == {"score": 99}


class _RequiredOuterEmpty(_OuterEmpty):
    name = "composition_required_outer_empty"
    outputs: ClassVar = _PredictMeasure.outputs


def test_required_submitted_output_rejects_empty_stream(tmp_path: Path) -> None:
    with pytest.raises(
        ArtifactValidationError, match="Empty artifact list for required role"
    ):
        _run(_RequiredOuterEmpty, {"structure": [_structure(0)]}, tmp_path)


class _OptionalGenerator(_GenerateMetric):
    name = "composition_optional_generator"

    class InputRole(StrEnum):
        structure = "structure"

    inputs: ClassVar = {
        "structure": InputSpec(artifact_type="csv", required=False, materialize=False)
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        assert "structure" not in inputs.input_artifacts
        return {}


class _OptionalComposition(CompositeDefinition):
    name = "composition_optional"

    class InputRole(StrEnum):
        structure = "structure"

    class OutputRole(StrEnum):
        metrics = "metrics"

    inputs: ClassVar = _OptionalGenerator.inputs
    outputs: ClassVar = {"metrics": OutputSpec(artifact_type="metric", required=False)}

    def compose(self, ctx: CompositeContext) -> None:
        call = ctx.call(
            _OptionalGenerator, inputs={"structure": ctx.input("structure")}
        )
        ctx.output("metrics", call.output("metrics"))


class _ExplicitEmpty(_OptionalComposition):
    name = "composition_explicit_empty"

    def compose(self, ctx: CompositeContext) -> None:
        call = ctx.call(_OptionalGenerator, inputs={"structure": []})
        ctx.output("metrics", call.output("metrics"))


def test_optional_omission_remains_absent_and_explicit_empty_skips(
    tmp_path: Path,
) -> None:
    _OptionalGenerator.runs = 0
    absent = _run(_OptionalComposition, {}, tmp_path)
    empty = _run(_ExplicitEmpty, {}, tmp_path)
    assert _OptionalGenerator.runs == 1
    assert len(absent.result.artifacts["metrics"]) == 1
    assert empty.result.artifacts == {"metrics": []}
    assert empty.result.lineage == {"metrics": []}


@pytest.mark.parametrize("inputs", [{}, {"structure": []}])
def test_invalid_or_empty_boundary_never_runs_children(
    tmp_path: Path, inputs: dict[str, list[Artifact]]
) -> None:
    _Transform.phase_sizes.clear()
    with pytest.raises(ValueError):
        _run(_PredictMeasure, inputs, tmp_path)
    assert _Transform.phase_sizes == []


class _FailingTransform(_Transform):
    name = "composition_failing_transform"

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        msg = "intentional scientific failure"
        raise RuntimeError(msg)


class _FailFirst(_PredictMeasure):
    name = "composition_fail_first"

    def compose(self, ctx: CompositeContext) -> None:
        first = ctx.call(
            _FailingTransform, inputs={"structure": ctx.input("structure")}
        )
        second = ctx.call(_Transform, inputs={"structure": ctx.input("structure")})
        metric = ctx.call(_Measure, inputs={"structure": second.output("structure")})
        ctx.output("structure", first.output("structure"))
        ctx.output("metrics", metric.output("metrics"))


def test_first_failure_stops_independent_later_calls(tmp_path: Path) -> None:
    from artisan.execution.invocation import ExecuteFailure

    _Transform.phase_sizes.clear()
    with pytest.raises(ExecuteFailure, match="intentional scientific failure"):
        _run(_FailFirst, {"structure": [_structure(0)]}, tmp_path)
    assert _Transform.phase_sizes == [("preprocess", 1)]


class _Configuration(OperationDefinition):
    name = "composition_configuration"

    class InputRole(StrEnum):
        structure = "structure"

    class OutputRole(StrEnum):
        config = "config"

    inputs: ClassVar = {"structure": InputSpec(artifact_type="csv", materialize=False)}
    outputs: ClassVar = {
        "config": OutputSpec(
            artifact_type="config", derives_from={"inputs": ["structure"]}
        )
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {}

    def execute_function(self, inputs: ExecuteInput) -> None:
        return None

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

        result = ArtifactResult()
        for index, artifact in enumerate(inputs.input_artifacts["structure"]):
            result.add_artifact(
                "config",
                ExecutionConfigArtifact.draft(
                    {"structure": {"$artifact": artifact.artifact_id}},
                    "config.json",
                    inputs.step_number,
                ),
                sources={"structure": [inputs.ref("structure", index=index)]},
            )
        return result


class _ConfigOnly(CompositeDefinition):
    name = "composition_config_only"

    class InputRole(StrEnum):
        structure = "structure"

    class OutputRole(StrEnum):
        config = "config"

    inputs: ClassVar = {"structure": InputSpec(artifact_type="csv")}
    outputs: ClassVar = {"config": OutputSpec(artifact_type="config")}

    def compose(self, ctx: CompositeContext) -> None:
        structure = ctx.call(_Transform, inputs={"structure": ctx.input("structure")})
        config = ctx.call(
            _Configuration, inputs={"structure": structure.output("structure")}
        )
        ctx.output("config", config.output("config"))


def test_required_reference_support_is_retained_without_becoming_an_export(
    tmp_path: Path,
) -> None:
    output = _run(_ConfigOnly, {"structure": [_structure(0)]}, tmp_path)
    assert set(output.result.artifacts) == {"config"}
    assert len(output.projection.support) == 1
    support = output.projection.support[0]
    assert _coordinate(output.retained_artifacts[support]) == 1
    assert len(output.retained_artifacts) == 2
    config = output.result.artifacts["config"][0]
    assert config.get_artifact_references() == [
        output.retained_artifacts[support].artifact_id
    ]
    assert output.result.referenced_artifacts("config", index=0) == {
        output.retained_artifacts[support].artifact_id: output.retained_artifacts[
            support
        ]
    }
    assert output.result.support is not None
    assert set(output.result.support.artifacts) == {
        occurrence.address for occurrence in output.projection.occurrences
    }
    mapping = output.result.lineage["config"][0]
    assert (mapping.source_role, mapping.source_input_index) == ("structure", 0)


class _Associated(_Measure):
    name = "composition_associated"
    inputs: ClassVar = {
        "structure": InputSpec(
            artifact_type="csv", materialize=False, with_associated=["metric"]
        )
    }
    observed: ClassVar[list[list[int]]] = []

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        memberships = [
            [
                artifact.values["score"]
                for artifact in inputs.associated_artifacts(
                    inputs.ref("structure", index=index), "metric"
                )
            ]
            for index in range(len(inputs.input_artifacts["structure"]))
        ]
        self.observed.extend(memberships)
        return {"values": PerArtifact([len(values) for values in memberships])}


class _ReadLocalAssociations(_OptionalComposition):
    name = "composition_read_local_associations"
    inputs: ClassVar = _PredictMeasure.inputs

    def compose(self, ctx: CompositeContext) -> None:
        first = ctx.call(
            _Transform, params={"add": 0}, inputs={"structure": ctx.input("structure")}
        )
        second = ctx.call(
            _Transform, params={"add": 0}, inputs={"structure": ctx.input("structure")}
        )
        ctx.call(_Measure, inputs={"structure": second.output("structure")})
        observed = ctx.call(
            _Associated,
            inputs={
                "structure": [first.output("structure"), second.output("structure")]
            },
        )
        ctx.output("metrics", observed.output("metrics"))


def test_local_association_membership_uses_original_producer_not_equal_content(
    tmp_path: Path,
) -> None:
    _Associated.observed.clear()
    output = _run(_ReadLocalAssociations, {"structure": [_structure(7)]}, tmp_path)
    assert _Associated.observed == [[], [7]]
    assert [item.values["score"] for item in output.result.artifacts["metrics"]] == [
        0,
        1,
    ]
    assert output.projection.support == ()


@pytest.mark.parametrize("capture_empty", [False, True])
def test_external_associations_require_frozen_capture_even_when_empty(
    tmp_path: Path, capture_empty: bool
) -> None:
    from artisan.schemas.provenance.occurrence import OccurrenceAddress
    from artisan.schemas.provenance.projection import (
        FrozenAssociation,
        FrozenBoundaryContext,
    )

    root = OccurrenceAddress(invocation_id="unit", role="structure", index=0)
    context = FrozenBoundaryContext(
        associations=(
            FrozenAssociation(primary=root, artifact_type="metric", members=()),
        )
        if capture_empty
        else ()
    )
    _Associated.observed.clear()
    with Executor(working_root=tmp_path) as executor:
        if not capture_empty:
            with pytest.raises(
                ValueError, match="Association membership was not captured"
            ):
                execute_compiled_plan(
                    compile_definition(_Associated),
                    {"structure": [_structure(7)]},
                    executor,
                    invocation_id="unit",
                    execute_adapter=lambda _op, _path: LocalExecuteRouter(),
                    frozen_context=context,
                )
            assert _Associated.observed == []
        else:
            output = execute_compiled_plan(
                compile_definition(_Associated),
                {"structure": [_structure(7)]},
                executor,
                invocation_id="unit",
                execute_adapter=lambda _op, _path: LocalExecuteRouter(),
                frozen_context=context,
            )
            assert _Associated.observed == [[]]
            assert output.result.artifacts["metrics"][0].values == {"score": 0}
            assert output.projection.frozen_context == context


def test_captured_membership_is_exact_and_not_extended_by_local_measurement(
    tmp_path: Path,
) -> None:
    from artisan.schemas.provenance.occurrence import OccurrenceAddress
    from artisan.schemas.provenance.projection import (
        FrozenAssociation,
        FrozenBoundaryContext,
    )

    class ReadBoundary(_OptionalComposition):
        name = "composition_read_boundary"
        inputs: ClassVar = _PredictMeasure.inputs

        def compose(self, ctx: CompositeContext) -> None:
            ctx.call(_Measure, inputs={"structure": ctx.input("structure")})
            observed = ctx.call(
                _Associated, inputs={"structure": ctx.input("structure")}
            )
            ctx.output("metrics", observed.output("metrics"))

    root = OccurrenceAddress(invocation_id="unit", role="structure", index=0)
    captured = OccurrenceAddress(invocation_id="captured", role="metrics", index=0)
    context = FrozenBoundaryContext(
        associations=(
            FrozenAssociation(
                primary=root, artifact_type="metric", members=(captured,)
            ),
        )
    )
    _Associated.observed.clear()
    with Executor(working_root=tmp_path) as executor:
        result = execute_compiled_plan(
            compile_definition(ReadBoundary),
            {"structure": [_structure(7)]},
            executor,
            invocation_id="unit",
            execute_adapter=lambda _op, _path: LocalExecuteRouter(),
            context_artifacts={
                captured: MetricArtifact.draft({"score": 8}, "score.json").finalize()
            },
            frozen_context=context,
        )
    assert _Associated.observed == [[8]]
    assert len(result.projection.occurrences) == 3
    assert result.projection.frozen_context == context


class _UseConfig(OperationDefinition):
    name = "composition_use_config"

    class InputRole(StrEnum):
        config = "config"

    class OutputRole(StrEnum):
        metrics = "metrics"

    inputs: ClassVar = {
        "config": InputSpec(
            artifact_type="config", resolve_references=True, materialize=False
        )
    }
    outputs: ClassVar = {
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["config"]}
        )
    }
    paths: ClassVar[list[Path]] = []

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        references = inputs.referenced_artifacts(inputs.ref("config", index=0))
        structure = next(iter(references.values()))
        path = Path(structure.materialized_path)
        assert path.read_bytes() == structure.content
        self.paths.append(path)
        assert all(path.exists() for path in self.paths)
        return {"value": _coordinate(structure)}

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        return {"values": [inputs.inputs["value"]]}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        result.add_artifact(
            "metrics",
            MetricArtifact.draft(
                {"score": inputs.memory_outputs["values"][0]}, "score.json"
            ),
            sources={"config": [inputs.ref("config", index=0)]},
        )
        return result


class _MaterializeConfig(_UseConfig):
    name = "composition_materialize_config"
    inputs: ClassVar = {"config": InputSpec(artifact_type="config")}

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        import json

        config = inputs.input_artifacts["config"][0]
        config_path = Path(config.materialized_path)
        resolved_path = Path(json.loads(config_path.read_text())["structure"])
        self.paths.extend([config_path, resolved_path])
        return {"value": int(resolved_path.read_text().splitlines()[1])}


class _MaterializedConfig(_OptionalComposition):
    name = "composition_materialized_config"
    inputs: ClassVar = _PredictMeasure.inputs

    def compose(self, ctx: CompositeContext) -> None:
        nested = ctx.call(_ConfigOnly, inputs={"structure": ctx.input("structure")})
        ctx.call(_MaterializeConfig, inputs={"config": nested.output("config")})
        metric = ctx.call(_UseConfig, inputs={"config": nested.output("config")})
        ctx.output("metrics", metric.output("metrics"))


def test_nested_config_references_are_materialized_without_store_and_kept_until_close(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Local configuration materialization queried the store")

    monkeypatch.setattr(ArtifactStore, "get_artifact", forbidden)
    _UseConfig.paths.clear()
    with Executor(working_root=tmp_path) as executor:
        output = execute_compiled_plan(
            compile_definition(_MaterializedConfig),
            {"structure": [_structure(3)]},
            executor,
            invocation_id="unit",
            execute_adapter=lambda _op, _path: LocalExecuteRouter(),
        )
        assert output.result.artifacts["metrics"][0].values == {"score": 4}
        assert all(path.exists() for path in _UseConfig.paths)
        assert output.projection.support == ()
    assert all(not path.exists() for path in _UseConfig.paths)


def test_independent_root_keeps_empty_role_and_exact_duplicate_input_positions(
    tmp_path: Path,
) -> None:
    class EmptyHistory(_Summary):
        name = "composition_empty_history"
        outputs: ClassVar = {
            "metrics": OutputSpec(
                artifact_type="metric", derives_from={"inputs": ["final"]}
            )
        }

    artifact = _structure(2)
    _Summary.observed.clear()
    output = _run(
        EmptyHistory, {"final": [artifact, artifact], "history": []}, tmp_path
    )
    assert _Summary.observed == [(2, 0)]
    assert output.projection.input_roles()["history"] == ()
    final = output.projection.input_roles()["final"]
    assert final[0] != final[1]
    assert [edge.source_input_index for edge in output.result.lineage["metrics"]] == [
        0,
        1,
    ]


class _PairedSummary(_Summary):
    name = "composition_paired_summary"
    independent_input_streams = False


class _CrossSummary(_PairedSummary):
    name = "composition_cross_summary"
    group_by: GroupByStrategy | None = GroupByStrategy.CROSS_PRODUCT


class _Pairing(CompositeDefinition):
    name = "composition_pairing"

    class InputRole(StrEnum):
        final = "final"
        history = "history"

    class OutputRole(StrEnum):
        metrics = "metrics"

    inputs: ClassVar = _Summary.inputs
    outputs: ClassVar = {"metrics": OutputSpec(artifact_type="metric")}
    child: ClassVar[type[OperationDefinition]] = _PairedSummary

    def compose(self, ctx: CompositeContext) -> None:
        result = ctx.call(
            self.child, inputs={role: ctx.input(role) for role in self.inputs}
        )
        ctx.output("metrics", result.output("metrics"))


class _CrossPairing(_Pairing):
    name = "composition_cross_pairing"
    child: ClassVar[type[OperationDefinition]] = _CrossSummary


@pytest.mark.parametrize(
    ("definition", "expected"),
    [
        (_Pairing, [(0, [3]), (1, [4])]),
        (_CrossPairing, [(0, [3]), (0, [4]), (1, [3]), (1, [4])]),
    ],
)
def test_child_pairing_preserves_scientific_values_and_exact_root_sources(
    tmp_path: Path,
    definition: type[CompositeDefinition],
    expected: list[tuple[int, list[int]]],
) -> None:
    output = _run(
        definition,
        {
            "final": [_structure(0), _structure(1)],
            "history": [
                MetricArtifact.draft({"score": value}, "score.json").finalize()
                for value in (3, 4)
            ],
        },
        tmp_path,
    )
    assert [
        (metric.values["selected"], metric.values["history"])
        for metric in output.result.artifacts["metrics"]
    ] == expected
    for target, (selected, history) in enumerate(expected):
        sources = {
            (edge.source_role, edge.source_input_index)
            for edge in output.result.lineage["metrics"]
            if edge.draft_index == target
        }
        assert sources == {("final", selected), ("history", history[0] - 3)}


def test_unequal_child_pairing_fails_before_scientific_phases(tmp_path: Path) -> None:
    _Summary.observed.clear()
    with pytest.raises(ValueError, match="equal lengths"):
        _run(_Pairing, {"final": [_structure(0)], "history": []}, tmp_path)
    assert _Summary.observed == []


class _FileStep(OperationDefinition):
    name = "composition_file_step"

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        files = "files"
        metrics = "metrics"

    inputs: ClassVar = {"source": InputSpec(artifact_type="file_ref", required=False)}
    outputs: ClassVar = {
        "files": OutputSpec(
            artifact_type="file_ref", derives_from={"inputs": ["source"]}
        ),
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"outputs": ["files"]}
        ),
    }
    paths: ClassVar[list[Path]] = []

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        source = inputs.input_artifacts.get("source", [])
        value = int(Path(source[0].materialized_path).read_text()) if source else 0
        return {"value": value}

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        assert inputs.files_dir is not None
        path = Path(inputs.files_dir) / "structure.txt"
        path.write_text(str(inputs.inputs["value"] + 1))
        self.paths.append(path)
        return {"paths": [str(path)]}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        from artisan.schemas.artifact.file_ref import FileRefArtifact
        from artisan.utils.hashing import compute_content_digest

        path = Path(inputs.memory_outputs["paths"][0])
        content = path.read_bytes()
        result = ArtifactResult()
        result.add_artifact(
            "files",
            FileRefArtifact.draft(
                str(path), compute_content_digest(content), len(content)
            ),
            sources={"source": [inputs.ref("source", index=0)]}
            if "source" in inputs.input_artifacts
            else {},
        )
        result.add_artifact(
            "metrics",
            MetricArtifact.draft({"score": int(content)}, "score.json"),
            sources={"files": [0]},
        )
        return result


class _FileGenerator(_FileStep):
    name = "composition_file_generator"

    class InputRole(StrEnum):
        pass

    inputs: ClassVar = {}
    outputs: ClassVar = {
        "files": OutputSpec(artifact_type="file_ref", derives_from={"inputs": []}),
        "metrics": _FileStep.outputs["metrics"],
    }


class _FileComposition(CompositeDefinition):
    name = "composition_files"

    class OutputRole(StrEnum):
        files = "files"
        metrics = "metrics"

    outputs: ClassVar = {
        "files": OutputSpec(artifact_type="file_ref"),
        "metrics": OutputSpec(artifact_type="metric"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        first = ctx.call(_FileGenerator)
        second = ctx.call(_FileStep, inputs={"source": first.output("files")})
        ctx.output("files", second.output("files"))
        ctx.output("metrics", second.output("metrics"))


def test_file_intermediates_and_exports_survive_for_retained_evidence_capture(
    tmp_path: Path,
) -> None:
    _FileStep.paths.clear()
    with Executor(working_root=tmp_path) as executor:
        output = execute_compiled_plan(
            compile_definition(_FileComposition),
            {},
            executor,
            invocation_id="unit",
            execute_adapter=lambda _op, _path: LocalExecuteRouter(),
        )
        assert len(_FileStep.paths) == 2
        assert all(path.exists() for path in _FileStep.paths)
        final = output.result.artifacts["files"][0]
        retained = output.projection.export_roles()
        assert len(output.retained_artifacts) == 2
        assert {
            (edge.source, edge.target) for edge in output.projection.associations
        } == {(retained["files"][0], retained["metrics"][0])}
        captured_bytes = Path(final.path).read_bytes()
        assert captured_bytes == b"2"
        assert output.result.artifacts["metrics"][0].values == {"score": 2}
    assert all(not path.exists() for path in _FileStep.paths)
    assert captured_bytes == b"2"


def test_frozen_child_drift_fails_before_any_scientific_phase(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = compile_definition(_PredictMeasure)
    monkeypatch.setattr(_Measure, "per_artifact_dispatch", False)
    _Transform.phase_sizes.clear()
    with (
        Executor(working_root=tmp_path) as executor,
        pytest.raises(ValueError, match="changed"),
    ):
        execute_compiled_plan(
            plan,
            {"structure": [_structure(0)]},
            executor,
            invocation_id="unit",
            execute_adapter=lambda _op, _path: LocalExecuteRouter(),
        )
    assert _Transform.phase_sizes == []


def test_equal_content_boundary_slots_keep_distinct_accepted_producers_and_context(
    tmp_path: Path,
) -> None:
    from artisan.schemas.provenance.occurrence import OccurrenceAddress
    from artisan.schemas.provenance.projection import (
        AcceptedOccurrence,
        BoundaryInputAttribution,
        FrozenAssociation,
        FrozenBoundaryContext,
    )

    slots = tuple(
        OccurrenceAddress(invocation_id="unit", role="structure", index=i)
        for i in range(2)
    )
    captured = OccurrenceAddress(invocation_id="context", role="metrics", index=0)
    attribution = tuple(
        BoundaryInputAttribution(
            boundary=slot,
            producer=AcceptedOccurrence(
                delivery_id=f"{index + 1:032x}",
                seal_digest=f"{index + 10:032x}",
                address=OccurrenceAddress(
                    invocation_id=f"producer-{index}", role=slot.role, index=0
                ),
            ),
        )
        for index, slot in enumerate((*slots, captured))
    )
    context = FrozenBoundaryContext(
        associations=(
            FrozenAssociation(primary=slots[0], artifact_type="metric", members=()),
            FrozenAssociation(
                primary=slots[1], artifact_type="metric", members=(captured,)
            ),
        )
    )
    artifact = _structure(7)
    _Associated.observed.clear()
    with Executor(working_root=tmp_path) as executor:
        output = execute_compiled_plan(
            compile_definition(_Associated),
            {"structure": [artifact, artifact]},
            executor,
            invocation_id="unit",
            execute_adapter=lambda _op, _path: LocalExecuteRouter(),
            context_artifacts={
                captured: MetricArtifact.draft({"score": 9}, "score.json").finalize()
            },
            frozen_context=context,
            input_attribution=attribution,
        )
    assert output.projection.input_attribution == attribution
    assert output.projection.input_roles()["structure"] == slots
    assert _Associated.observed == [[], [9]]
    assert [
        mapping.source_input_index for mapping in output.result.lineage["metrics"]
    ] == [0, 1]
    assert all(
        mapping.source_invocation == "unit"
        for mapping in output.result.lineage["metrics"]
    )


def test_missing_producer_attribution_membership_fails_before_science(
    tmp_path: Path,
) -> None:
    _Transform.phase_sizes.clear()
    with (
        Executor(working_root=tmp_path) as executor,
        pytest.raises(ValueError, match="attribution"),
    ):
        execute_compiled_plan(
            compile_definition(_PredictMeasure),
            {"structure": [_structure(0)]},
            executor,
            invocation_id="unit",
            execute_adapter=lambda _op, _path: LocalExecuteRouter(),
            input_attribution=(),
        )
    assert _Transform.phase_sizes == []


def test_omitted_producer_attribution_is_explicitly_unscoped(tmp_path: Path) -> None:
    output = _run(_PredictMeasure, {"structure": [_structure(0)]}, tmp_path)
    attribution = output.projection.input_attribution
    assert len(attribution) == 1
    assert attribution[0].boundary == output.projection.input_roles()["structure"][0]
    assert attribution[0].producer is None
