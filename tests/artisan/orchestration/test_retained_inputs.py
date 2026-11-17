"""Controller input capture preserves producer scope, closure and scientific batches."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.replay_operations import Increment, TwoIncrements
from fixtures.retained_acceptance import open_committer
from fixtures.retained_curators import admit_curator, stage_curator
from fixtures.retained_readers import admit_read_case
from fixtures.retained_results import RetainedCase, _derive, retained_case

from artisan.composites.base.compiler import compile_definition
from artisan.errors import ArtifactIntegrityError, StoreIntegrityError
from artisan.execution.composition import execute_compiled_plan
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.executor import Executor
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.execution.recording.retained_replay import (
    capture_replay_configuration,
    publish_replay_configuration,
)
from artisan.operations.curator.declare_lineage import DeclareLineage
from artisan.operations.curator.merge import Merge
from artisan.orchestration.engine.retained_inputs import prepare_step_inputs
from artisan.orchestration.retained_replay import prepare_retained_replay
from artisan.schemas import (
    Artifact,
    CSVArtifact,
    ExecutionConfigArtifact,
    FileRefArtifact,
    MetricArtifact,
    OutputSpec,
)
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.composites.plan import (
    ExecutionRecipe,
    InputContract,
    InvocationPolicy,
)
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.provenance.occurrence import ReferenceContext, RequiredReference
from artisan.schemas.provenance.projection import (
    AcceptedCuratorOccurrence,
    BoundaryInputAttribution,
)
from artisan.storage.core.curator_reader import AcceptedCuratorResult, CuratorReader
from artisan.storage.core.retained_reader import AcceptedArtifact, RetainedReader
from artisan.storage.io.curator_plan import read_curator_plan


@pytest.fixture
def reader(tmp_path: Path) -> RetainedReader:
    storage = StorageConfig()
    return RetainedReader(
        open_committer(
            str(tmp_path / "store"), storage.filesystem(), storage, initialize=True
        )
    )


def _csv(value: int = 1, name: str = "input.csv") -> Artifact:
    return CSVArtifact.draft(f"value\n{value}\n".encode(), name).finalize()


def _recipe(
    *roles: str,
    policy: InvocationPolicy | None = None,
    associated: tuple[str, ...] = (),
) -> ExecutionRecipe:
    graph = OccurrenceGraph()
    for role in roles:
        graph.declare_input_role(role)
    base = RetainedCase(graph.project({}, {}), {}, {}).recipe
    return base.model_copy(
        update={
            "inputs": tuple(
                InputContract(
                    role=role,
                    artifact_type="any",
                    required=True,
                    materialize=False,
                    hydrate=True,
                    materialize_as=None,
                    resolve_references=False,
                    with_associated=associated,
                )
                for role in roles
            ),
            "policy": policy or InvocationPolicy(),
        }
    )


def test_unscoped_duplicates_remain_distinct_and_partition_once(
    reader: RetainedReader,
) -> None:
    first, second = _csv(), _csv(2)
    units = prepare_step_inputs(
        _recipe(
            "data",
            policy=InvocationPolicy(
                artifacts_per_unit=5,
                max_artifacts_per_unit=2,
                per_artifact_dispatch=False,
            ),
        ),
        {"data": [first, second, first]},
        step_run_id="a" * 32,
        reader=reader,
    )
    assert [unit.assignment.unit_index for unit in units] == [0, 1]
    assert [len(unit.boundary.input_roles()["data"]) for unit in units] == [2, 1]
    assert all(
        item.producer is None
        for unit in units
        for item in unit.boundary.input_attribution
    )
    together = prepare_step_inputs(
        _recipe("data", policy=InvocationPolicy(artifacts_per_unit=3)),
        {"data": [first, second, first]},
        step_run_id="b" * 32,
        reader=reader,
    )[0]
    members = together.boundary.input_roles()["data"]
    assert len(set(members)) == 3
    assert [together.artifacts[item].artifact_id for item in members] == [
        first.artifact_id,
        second.artifact_id,
        first.artifact_id,
    ]


def test_whole_stream_empty_and_generative_inputs_preserve_presence(
    reader: RetainedReader,
) -> None:
    policy = InvocationPolicy(
        independent_input_streams=True, per_artifact_dispatch=False
    )
    units = prepare_step_inputs(
        _recipe("data", "metrics", policy=policy),
        {"data": [_csv(), _csv(2)], "metrics": []},
        step_run_id="a" * 32,
        reader=reader,
    )
    assert len(units) == 1
    assert len(units[0].boundary.input_roles()["data"]) == 2
    assert units[0].boundary.input_roles()["metrics"] == ()
    optional = _recipe("data", "metrics", policy=policy)
    optional = optional.model_copy(
        update={
            "inputs": (
                optional.inputs[0],
                optional.inputs[1].model_copy(update={"required": False}),
            )
        }
    )
    omitted = prepare_step_inputs(
        optional, {"data": [_csv(), _csv(2)]}, step_run_id="a" * 32, reader=reader
    )[0]
    assert "metrics" not in omitted.boundary.input_roles()
    assert (
        omitted.boundary.input_context_digest != units[0].boundary.input_context_digest
    )
    assert (
        prepare_step_inputs(
            _recipe("data"), {"data": []}, step_run_id="b" * 32, reader=reader
        )
        == ()
    )
    assert (
        len(prepare_step_inputs(_recipe(), {}, step_run_id="c" * 32, reader=reader))
        == 1
    )
    with pytest.raises(ValueError, match="missing required"):
        prepare_step_inputs(_recipe("data"), {}, step_run_id="d" * 32, reader=reader)


def test_pairing_rejects_unequal_zip_and_cross_product_preserves_duplicates(
    reader: RetainedReader,
) -> None:
    first, second = _csv(), _csv(2)
    inputs = {"left": [first, first], "right": [second]}
    with pytest.raises(ValueError, match="equal lengths"):
        prepare_step_inputs(
            _recipe("left", "right"), inputs, step_run_id="a" * 32, reader=reader
        )
    cross = _recipe(
        "left", "right", policy=InvocationPolicy(group_by=GroupByStrategy.CROSS_PRODUCT)
    )
    assert (
        len(prepare_step_inputs(cross, inputs, step_run_id="b" * 32, reader=reader))
        == 2
    )
    assert (
        prepare_step_inputs(
            cross, {"left": [first], "right": []}, step_run_id="c" * 32, reader=reader
        )
        == ()
    )


def test_name_pairing_uses_actual_names_and_rejects_ambiguous_stems(
    reader: RetainedReader,
) -> None:
    recipe = _recipe(
        "left", "right", policy=InvocationPolicy(group_by=GroupByStrategy.NAME)
    )
    values = {
        "left": [_csv(1, "b.csv"), _csv(2, "a.csv")],
        "right": [_csv(3, "a.csv"), _csv(4, "b.csv")],
    }
    units = prepare_step_inputs(recipe, values, step_run_id="a" * 32, reader=reader)
    assert [
        unit.artifacts[unit.boundary.input_roles()["left"][0]].original_name
        for index, unit in enumerate(units)
    ] == ["a", "b"]
    with pytest.raises(ValueError, match="duplicate original_name"):
        prepare_step_inputs(
            recipe,
            {"left": [values["left"][0]] * 2, "right": values["right"]},
            step_run_id="b" * 32,
            reader=reader,
        )


def test_unscoped_associations_and_ambiguous_references_fail(
    reader: RetainedReader,
) -> None:
    artifact = _csv()
    with pytest.raises(ValueError, match="producer scope"):
        prepare_step_inputs(
            _recipe("data", associated=("metric",)),
            {"data": [artifact]},
            step_run_id="a" * 32,
            reader=reader,
        )
    config = ExecutionConfigArtifact.draft(
        {"source": {"$artifact": artifact.artifact_id}}, "config.json"
    ).finalize()
    with pytest.raises(ValueError, match="missing or ambiguous"):
        prepare_step_inputs(
            _recipe("config"), {"config": [config]}, step_run_id="b" * 32, reader=reader
        )
    whole = InvocationPolicy(
        independent_input_streams=True, per_artifact_dispatch=False
    )
    with pytest.raises(ValueError, match="missing or ambiguous"):
        prepare_step_inputs(
            _recipe("config", "data", policy=whole),
            {"config": [config], "data": [artifact, artifact]},
            step_run_id="c" * 32,
            reader=reader,
        )
    unit = prepare_step_inputs(
        _recipe("config", "data", policy=whole),
        {"config": [config], "data": [artifact]},
        step_run_id="d" * 32,
        reader=reader,
    )[0]
    capture = unit.boundary.frozen_context.references[0]
    assert capture.members == unit.boundary.input_roles()["data"]
    assert capture.direct_parents == ()
    assert all(item.producer is None for item in unit.boundary.input_attribution)


def _seed(reader: RetainedReader) -> tuple[RetainedCase, SealedDelivery]:
    graph = OccurrenceGraph()
    artifact = _csv()
    root = graph.add_input("data", artifact)
    first = _derive(graph, "first", artifact, [root])
    other = _derive(graph, "other", artifact, [root])
    metric = MetricArtifact.draft({"score": 1}, "score.json").finalize()
    score = _derive(graph, "score", metric, [first])
    case = RetainedCase(
        graph.project(
            {"structures": [first, other], "metrics": [score]},
            {"structures": OutputSpec(), "metrics": OutputSpec()},
        ),
        {root: artifact},
        {first: artifact, other: artifact, score: metric},
    )
    _, receipt = admit_read_case(reader.committer, case, step_run_id="a" * 32)
    reader.committer.accept(receipt)
    return case, receipt


def test_scoped_associations_capture_empty_and_repeated_positions_exactly(
    reader: RetainedReader,
) -> None:
    _, receipt = _seed(reader)
    result = reader.read(receipt)
    first, other = result.output("structures")
    recipe = _recipe(
        "data", associated=("metric",), policy=InvocationPolicy(artifacts_per_unit=3)
    )
    unit = prepare_step_inputs(
        recipe, {"data": [first, other, first]}, step_run_id="b" * 32, reader=reader
    )[0]
    positions = unit.boundary.input_roles()["data"]
    frozen = unit.boundary.frozen_context
    assert len(set(positions)) == 3
    assert frozen.associated(positions[1], "metric") == ()
    assert frozen.associated(positions[0], "metric") == frozen.associated(
        positions[2], "metric"
    )
    assert len(frozen.associated(positions[0], "metric")) == 1
    assert tuple(item.producer for item in unit.boundary.input_attribution[:3]) == (
        first.reference,
        other.reference,
        first.reference,
    )
    assert not unit.boundary.parents
    assert not unit.boundary.associations


def test_lineage_pairing_uses_occurrences_not_equal_content(
    reader: RetainedReader,
) -> None:
    _, receipt = _seed(reader)
    result = reader.read(receipt)
    first, other = result.output("structures")
    score = result.output("metrics")[0]
    recipe = _recipe(
        "structure", "metric", policy=InvocationPolicy(group_by=GroupByStrategy.LINEAGE)
    )
    units = prepare_step_inputs(
        recipe,
        {"structure": [other, first, first], "metric": [score]},
        step_run_id="b" * 32,
        reader=reader,
    )
    assert len(units) == 2
    for unit in units:
        assert unit.boundary.input_attribution[0].producer == first.reference
        assert unit.boundary.input_attribution[1].producer == score.reference


def _direct_references(
    _artifact: Artifact, context: ReferenceContext
) -> tuple[RequiredReference, ...]:
    return tuple(
        RequiredReference(
            address=address,
            artifact_id=context.artifacts[address].artifact_id,
            artifact_type=context.artifacts[address].artifact_type,
        )
        for address in context.direct_parents
    )


def test_reference_closure_preserves_original_parents_without_ancestor_expansion(
    reader: RetainedReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ArtifactTypeDef.get("metric"),
        "required_references",
        staticmethod(_direct_references),
    )
    graph = OccurrenceGraph()
    root_artifact, retained = _csv(), _csv(2)
    root = graph.add_input("data", root_artifact)
    hidden = _derive(graph, "hidden", _csv(3), [root])
    structure = _derive(graph, "final", retained, [hidden])
    metric = MetricArtifact.draft({"score": 2}, "metric.json").finalize()
    annotation = _derive(graph, "annotation", metric, [structure])
    case = RetainedCase(
        graph.project({"metric": [annotation]}, {"metric": OutputSpec()}),
        {root: root_artifact},
        {structure: retained, annotation: metric},
    )
    _, receipt = admit_read_case(reader.committer, case, step_run_id="a" * 32)
    reader.committer.accept(receipt)
    selected = reader.read(receipt).output("metric")[0]
    unit = prepare_step_inputs(
        _recipe("metric"), {"metric": [selected]}, step_run_id="b" * 32, reader=reader
    )[0]
    capture = unit.boundary.frozen_context.references[0]
    assert capture.members == capture.direct_parents
    assert {artifact.artifact_id for artifact in unit.artifacts.values()} == {
        metric.artifact_id,
        retained.artifact_id,
    }
    assert not unit.boundary.parents
    assert not unit.boundary.associations
    assert len(unit.boundary.input_attribution) == 2
    passthrough = OccurrenceGraph(boundary_invocation=unit.assignment.assignment_id)
    primary = passthrough.add_input("metric", selected.artifact)
    for address, artifact in unit.artifacts.items():
        if address != primary:
            passthrough.add_context_input(address, artifact)
    projected = passthrough.project(
        {"metric": [primary]},
        {"metric": OutputSpec()},
        frozen_context=unit.boundary.frozen_context,
        input_attribution=unit.boundary.input_attribution,
    )
    _, alias = admit_read_case(
        reader.committer,
        RetainedCase(projected, unit.artifacts, {}),
        step_run_id="b" * 32,
    )
    reader.committer.accept(alias)
    again = prepare_step_inputs(
        _recipe("metric"),
        {"metric": reader.read(alias).output("metric")},
        step_run_id="c" * 32,
        reader=reader,
    )[0]
    capture = again.boundary.frozen_context.references[0]
    assert capture.members == capture.direct_parents
    assert len(again.artifacts) == 2


def test_exact_producer_aliases_share_reference_context_but_keep_consuming_slots(
    reader: RetainedReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ArtifactTypeDef.get("metric"),
        "required_references",
        staticmethod(_direct_references),
    )
    _, receipt = _seed(reader)
    result = reader.read(receipt)
    first, other = result.output("structures")
    metric = result.output("metrics")[0]
    whole = InvocationPolicy(
        independent_input_streams=True, per_artifact_dispatch=False
    )
    unit = prepare_step_inputs(
        _recipe("structure", "metric", policy=whole),
        {"structure": [first, first, other], "metric": [metric]},
        step_run_id="b" * 32,
        reader=reader,
    )[0]
    positions = unit.boundary.input_roles()["structure"]
    capture = unit.boundary.frozen_context.references[0]
    assert len(set(positions)) == 3
    assert capture.members == (positions[0],)
    assert capture.direct_parents == (positions[0],)
    assert len(unit.artifacts) == 4
    assert unit.boundary.input_attribution[0].producer == first.reference
    assert unit.boundary.input_attribution[1].producer == first.reference
    assert unit.boundary.input_attribution[2].producer == other.reference


def test_recursive_required_references_are_captured_to_fixed_point(
    reader: RetainedReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ArtifactTypeDef.get("metric"),
        "required_references",
        staticmethod(_direct_references),
    )
    graph = OccurrenceGraph()
    artifact = _csv()
    root = graph.add_input("data", artifact)
    first_artifact = MetricArtifact.draft({"score": 1}, "first.json").finalize()
    first = _derive(graph, "first", first_artifact, [root])
    last_artifact = MetricArtifact.draft({"score": 2}, "last.json").finalize()
    last = _derive(graph, "last", last_artifact, [first])
    case = RetainedCase(
        graph.project({"metric": [last]}, {"metric": OutputSpec()}),
        {root: artifact},
        {first: first_artifact, last: last_artifact},
    )
    _, receipt = admit_read_case(reader.committer, case, step_run_id="a" * 32)
    reader.committer.accept(receipt)
    unit = prepare_step_inputs(
        _recipe("metric"),
        {"metric": reader.read(receipt).output("metric")},
        step_run_id="b" * 32,
        reader=reader,
    )[0]
    assert len(unit.artifacts) == 3
    assert len(unit.boundary.frozen_context.references) == 2
    assert all(
        item.members == item.direct_parents
        for item in unit.boundary.frozen_context.references
    )


def test_later_associations_do_not_refresh_a_frozen_assignment(
    reader: RetainedReader,
) -> None:
    _, receipt = _seed(reader)
    primary = reader.read(receipt).output("structures")[0]
    recipe = _recipe("data", associated=("metric",))
    original = prepare_step_inputs(
        recipe, {"data": [primary]}, step_run_id="b" * 32, reader=reader
    )[0]
    snapshot = original.boundary.model_dump_json()
    graph = OccurrenceGraph(boundary_invocation="later")
    source = graph.add_input("data", primary.artifact)
    metric = MetricArtifact.draft({"score": 9}, "later.json").finalize()
    output = _derive(graph, "measure", metric, [source])
    case = RetainedCase(
        graph.project(
            {"metric": [output]},
            {"metric": OutputSpec()},
            input_attribution=(
                BoundaryInputAttribution(boundary=source, producer=primary.reference),
            ),
        ),
        {source: primary.artifact},
        {output: metric},
    )
    _, later = admit_read_case(reader.committer, case, step_run_id="c" * 32)
    reader.committer.accept(later)
    fresh = prepare_step_inputs(
        recipe, {"data": [primary]}, step_run_id="d" * 32, reader=reader
    )[0]
    assert original.boundary.model_dump_json() == snapshot
    assert len(original.boundary.frozen_context.associations[0].members) == 1
    assert len(fresh.boundary.frozen_context.associations[0].members) == 2


@pytest.mark.parametrize("change", ["content", "seal"])
def test_selected_producer_evidence_is_rechecked(
    reader: RetainedReader, change: str
) -> None:
    _, receipt = _seed(reader)
    selected = reader.read(receipt).output("structures")[0]
    if change == "content":
        selected = AcceptedArtifact(selected.reference, _csv(9))
    else:
        selected = AcceptedArtifact(
            selected.reference.model_copy(update={"seal_digest": "f" * 32}),
            selected.artifact,
        )
    with pytest.raises(ValueError, match="accepted producer content|producer seal"):
        prepare_step_inputs(
            _recipe("data"), {"data": [selected]}, step_run_id="b" * 32, reader=reader
        )


def test_compiled_composition_consumes_the_exact_frozen_unit(
    reader: RetainedReader,
) -> None:
    plan = compile_definition(TwoIncrements)
    artifact = MetricArtifact.draft({"value": 3}, "input.json").finalize()
    unit = prepare_step_inputs(
        plan.recipe, {"source": [artifact]}, step_run_id="a" * 32, reader=reader
    )[0]
    with Executor() as executor:
        result = execute_compiled_plan(
            plan,
            {
                role: [unit.artifacts[address] for address in addresses]
                for role, addresses in unit.boundary.input_roles().items()
            },
            executor,
            invocation_id=unit.assignment.assignment_id,
            execute_adapter=lambda *_: LocalExecuteRouter(),
            frozen_context=unit.boundary.frozen_context,
            input_attribution=unit.boundary.input_attribution,
            expected_input_context_digest=unit.boundary.input_context_digest,
        )
    assert result.result.artifacts["metrics"][0].values == {"value": 5}
    assert result.projection.input_context_digest == unit.boundary.input_context_digest


def test_lineage_pairing_rejects_distinct_equal_content_ancestors_at_same_depth(
    reader: RetainedReader,
) -> None:
    graph = OccurrenceGraph()
    artifact = _csv()
    root = graph.add_input("data", artifact)
    first = _derive(graph, "first", artifact, [root])
    second = _derive(graph, "second", artifact, [root])
    metric = MetricArtifact.draft({"score": 1}, "metric.json").finalize()
    output = _derive(graph, "measure", metric, [first, second])
    case = RetainedCase(
        graph.project(
            {"data": [first, second], "metric": [output]},
            {"data": OutputSpec(), "metric": OutputSpec()},
        ),
        {root: artifact},
        {first: artifact, second: artifact, output: metric},
    )
    _, receipt = admit_read_case(reader.committer, case, step_run_id="a" * 32)
    reader.committer.accept(receipt)
    result = reader.read(receipt)
    recipe = _recipe(
        "data", "metric", policy=InvocationPolicy(group_by=GroupByStrategy.LINEAGE)
    )
    with pytest.raises(RuntimeError, match="multiple targets"):
        prepare_step_inputs(
            recipe,
            {"data": result.output("data"), "metric": result.output("metric")},
            step_run_id="b" * 32,
            reader=reader,
        )


def _merge(
    reader: RetainedReader,
    members: tuple[AcceptedArtifact, ...],
    step_run_id: str,
) -> AcceptedCuratorResult:
    operation = Merge()
    coordinator, admission, reference = admit_curator(
        reader.committer, operation, {"data": members}, step_run_id=step_run_id
    )
    staged, terminal = stage_curator(coordinator, admission, operation)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    plan = read_curator_plan(
        reader.committer.delta_root, step_run_id, reader.committer.fs
    )
    assert plan is not None
    return CuratorReader(reader.committer).read_result(step_run_id, plan.plan_digest)


def _measure(
    reader: RetainedReader, primary: AcceptedArtifact, step_run_id: str
) -> AcceptedArtifact:
    graph = OccurrenceGraph()
    source = graph.add_input("data", primary.artifact)
    metric = MetricArtifact.draft({"score": 9}, "later.json").finalize()
    output = _derive(graph, "measure", metric, [source])
    case = RetainedCase(
        graph.project(
            {"metric": [output]},
            {"metric": OutputSpec()},
            input_attribution=(
                BoundaryInputAttribution(boundary=source, producer=primary.reference),
            ),
        ),
        {source: primary.artifact},
        {output: metric},
    )
    _, receipt = admit_read_case(reader.committer, case, step_run_id=step_run_id)
    reader.committer.accept(receipt)
    return reader.read(receipt).output("metric")[0]


def test_curator_associations_union_frozen_seed_and_exact_later_edges(
    reader: RetainedReader,
) -> None:
    _, receipt = _seed(reader)
    original = reader.read(receipt).output("structures")[0]
    first = _merge(reader, (original,), "b" * 32).output("merged")[0]
    other = _merge(reader, (original,), "c" * 32).output("merged")[0]
    assert first.artifact.artifact_id == other.artifact.artifact_id
    assert first.reference != other.reference
    recipe = _recipe("data", associated=("metric",))
    frozen = prepare_step_inputs(
        recipe, {"data": [first]}, step_run_id="d" * 32, reader=reader
    )[0]
    snapshot = frozen.boundary.model_dump_json()
    later = _measure(reader, first, "e" * 32)
    assert len(reader.associated(first.reference, "metric")) == 2
    assert len(reader.associated(other.reference, "metric")) == 1
    assert later.reference not in {
        item.reference for item in reader.associated(other.reference, "metric")
    }
    fresh = prepare_step_inputs(
        recipe.model_copy(update={"policy": InvocationPolicy(artifacts_per_unit=3)}),
        {"data": [first, other, first]},
        step_run_id="f" * 32,
        reader=reader,
    )[0]
    positions = fresh.boundary.input_roles()["data"]
    context = fresh.boundary.frozen_context
    assert len(set(positions)) == 3
    assert len(context.associated(positions[0], "metric")) == 2
    assert len(context.associated(positions[1], "metric")) == 1
    assert context.associated(positions[0], "metric") == context.associated(
        positions[2], "metric"
    )
    assert frozen.boundary.model_dump_json() == snapshot
    assert len(frozen.boundary.frozen_context.associations[0].members) == 1
    # CuratorReader deliberately exposes only the curator's original query seed.
    assert isinstance(first.reference, AcceptedCuratorOccurrence)
    assert (
        len(CuratorReader(reader.committer).associated(first.reference, "metric")) == 1
    )


def test_curator_selection_sources_are_ancestry_without_aliasing(
    reader: RetainedReader,
) -> None:
    _, receipt = _seed(reader)
    originals = reader.read(receipt).output("structures")
    merged = _merge(reader, originals, "b" * 32).output("merged")[0]
    recipe = _recipe(
        "source", "merged", policy=InvocationPolicy(group_by=GroupByStrategy.LINEAGE)
    )
    with pytest.raises(RuntimeError, match="multiple targets"):
        prepare_step_inputs(
            recipe,
            {"source": originals, "merged": [merged]},
            step_run_id="c" * 32,
            reader=reader,
        )
    for original in originals:
        unit = prepare_step_inputs(
            recipe,
            {"source": [original], "merged": [merged]},
            step_run_id="c" * 32,
            reader=reader,
        )[0]
        assert unit.boundary.input_attribution[0].producer == original.reference
        assert unit.boundary.input_attribution[1].producer == merged.reference
        assert not unit.boundary.parents
        assert not unit.boundary.associations


def test_curator_required_context_uses_direct_parents_not_selection_sources(
    reader: RetainedReader, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        ArtifactTypeDef.get("metric"),
        "required_references",
        staticmethod(_direct_references),
    )
    _, receipt = _seed(reader)
    original = reader.read(receipt).output("metrics")[0]
    merged = _merge(reader, (original,), "b" * 32).output("merged")[0]
    unit = prepare_step_inputs(
        _recipe("metric", policy=InvocationPolicy(artifacts_per_unit=2)),
        {"metric": [merged, merged]},
        step_run_id="c" * 32,
        reader=reader,
    )[0]
    positions = unit.boundary.input_roles()["metric"]
    assert positions[0] != positions[1]
    captures = unit.boundary.frozen_context.references
    assert len(captures) == 2
    assert captures[0].members == captures[1].members
    assert all(item.members == item.direct_parents for item in captures)
    assert all(
        unit.artifacts[address].artifact_type == "csv"
        for address in captures[0].members
    )
    assert len(unit.artifacts) == 3
    assert all(
        isinstance(item.producer, AcceptedCuratorOccurrence)
        for item in unit.boundary.input_attribution
    )
    assert original.reference not in {
        item.producer for item in unit.boundary.input_attribution
    }
    assert not unit.boundary.parents
    assert not unit.boundary.associations


def test_curator_config_reference_restores_sibling_without_parent_edge(
    reader: RetainedReader,
) -> None:
    graph = OccurrenceGraph()
    source = graph.add_input("data", _csv())
    payload = _csv(2, "support.csv")
    support = _derive(graph, "support", payload, [source])
    config = ExecutionConfigArtifact.draft(
        {"input": {"$artifact": payload.artifact_id}}, "config.json"
    ).finalize()
    exported = _derive(graph, "config", config, [source])
    case = RetainedCase(
        graph.project({"config": [exported]}, {"config": OutputSpec()}),
        {source: graph.nodes[source].artifact},
        {support: payload, exported: config},
    )
    _, receipt = admit_read_case(reader.committer, case, step_run_id="a" * 32)
    reader.committer.accept(receipt)
    merged = _merge(reader, reader.read(receipt).output("config"), "b" * 32)
    unit = prepare_step_inputs(
        _recipe("config", associated=("metric",)),
        {"config": merged.output("merged")},
        step_run_id="c" * 32,
        reader=reader,
    )[0]
    capture = unit.boundary.frozen_context.references[0]
    assert len(capture.members) == 1
    assert unit.artifacts[capture.members[0]].artifact_id == payload.artifact_id
    assert capture.direct_parents == ()
    assert unit.boundary.frozen_context.associations[0].members == ()
    assert not unit.boundary.parents
    assert not unit.boundary.associations
    assert len(unit.artifacts) == 2


def test_creator_replay_preserves_curator_input_authority(
    reader: RetainedReader,
) -> None:
    artifact = MetricArtifact.draft({"value": 3}, "source.json").finalize()
    graph = OccurrenceGraph()
    root = graph.add_input("source", artifact)
    case = RetainedCase(
        graph.project({"metric": [root]}, {"metric": OutputSpec()}),
        {root: artifact},
        {},
    )
    _, receipt = admit_read_case(reader.committer, case, step_run_id="a" * 32)
    reader.committer.accept(receipt)
    merged = _merge(reader, reader.read(receipt).output("metric"), "b" * 32)
    plan = compile_definition(TwoIncrements)
    unit = prepare_step_inputs(
        plan.recipe,
        {"source": merged.output("merged")},
        step_run_id="c" * 32,
        reader=reader,
    )[0]
    with Executor() as executor:
        executed = execute_compiled_plan(
            plan,
            {"source": [artifact]},
            executor,
            invocation_id=unit.assignment.assignment_id,
            execute_adapter=lambda *_: LocalExecuteRouter(),
            frozen_context=unit.boundary.frozen_context,
            input_attribution=unit.boundary.input_attribution,
            expected_input_context_digest=unit.boundary.input_context_digest,
        )
    admission, source = admit_read_case(
        reader.committer,
        RetainedCase(executed.projection, unit.artifacts, executed.retained_artifacts),
        step_run_id="c" * 32,
        recipe=plan.recipe,
    )
    reader.committer.accept(source)
    publish_replay_configuration(
        capture_replay_configuration(source, {}, admission=admission),
        delta_root=reader.committer.delta_root,
        fs=reader.committer.fs,
    )
    replay = prepare_retained_replay(
        reader.committer,
        source.delivery.delivery_id,
        registered_definitions={
            (definition.__module__, definition.__qualname__): definition
            for definition in (TwoIncrements, Increment)
        },
        runtime_replacements={},
    )
    assert (
        replay.boundary.input_attribution[0].producer
        == merged.output("merged")[0].reference
    )
    assert (
        replay.boundary.input_attribution[0].boundary
        != unit.boundary.input_attribution[0].boundary
    )
    assert (
        replay.boundary.input_attribution[0].producer
        == unit.boundary.input_attribution[0].producer
    )
    with Executor() as executor:
        result = execute_compiled_plan(
            replay.plan,
            {
                role: [replay.artifacts[address] for address in addresses]
                for role, addresses in replay.boundary.input_roles().items()
            },
            executor,
            invocation_id=replay.identity.assignment_id,
            execute_adapter=lambda *_: LocalExecuteRouter(),
            frozen_context=replay.boundary.frozen_context,
            input_attribution=replay.boundary.input_attribution,
            expected_input_context_digest=replay.boundary.input_context_digest,
        )
    assert result.result.artifacts["metrics"][0].values == {"value": 5}


def test_lineage_grouping_follows_curator_authored_edges(
    reader: RetainedReader,
) -> None:
    graph = OccurrenceGraph()
    source = graph.add_input("data", _csv())
    parent = _derive(graph, "parent", _csv(2), [source])
    child = _derive(graph, "child", _csv(3), [source])
    case = RetainedCase(
        graph.project({"data": [parent, child]}, {"data": OutputSpec()}),
        {source: graph.nodes[source].artifact},
        {address: graph.nodes[address].artifact for address in (parent, child)},
    )
    _, receipt = admit_read_case(reader.committer, case, step_run_id="a" * 32)
    reader.committer.accept(receipt)
    original_parent, original_child = reader.read(receipt).output("data")
    operation = DeclareLineage(
        params=DeclareLineage.Params(pairing=GroupByStrategy.ZIP)
    )
    coordinator, admission, reference = admit_curator(
        reader.committer,
        operation,
        {"parents": (original_parent,), "children": (original_child,)},
        step_run_id="b" * 32,
    )
    staged, terminal = stage_curator(coordinator, admission, operation)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    plan = read_curator_plan(reader.committer.delta_root, "b" * 32, reader.committer.fs)
    assert plan is not None
    declared = CuratorReader(reader.committer).read_result(
        plan.step_run_id, plan.plan_digest
    )
    assert len(declared.projection.lineage) == 1
    selected = declared.output("children")[0]
    units = prepare_step_inputs(
        _recipe(
            "parent", "child", policy=InvocationPolicy(group_by=GroupByStrategy.LINEAGE)
        ),
        {"parent": [original_parent], "child": [selected]},
        step_run_id="c" * 32,
        reader=reader,
    )
    assert len(units) == 1
    assert units[0].boundary.input_attribution[0].producer == original_parent.reference
    assert units[0].boundary.input_attribution[1].producer == selected.reference
    assert not units[0].boundary.parents
    assert not units[0].boundary.associations


def test_unrelated_missing_carriers_do_not_block_exact_association_queries(
    reader: RetainedReader, tmp_path: Path
) -> None:
    _, seed = _seed(reader)
    primary = reader.read(seed).output("structures")[0]
    case = retained_case(tmp_path / "external")
    _, receipt = admit_read_case(reader.committer, case, step_run_id="b" * 32)
    reader.committer.accept(receipt)
    original = reader.read(receipt).support[0]
    merged = _merge(reader, (original,), "c" * 32).output("merged")[0]
    _measure(reader, merged, "d" * 32)
    assert isinstance(original.artifact, FileRefArtifact)
    reader.committer.fs.rm(original.artifact.path)
    found = reader.associated(primary.reference, "metric")
    assert len(found) == 1
    assert found[0].artifact.values == {"score": 1}
    for selected, error in (
        (original, FileNotFoundError),
        (merged, ArtifactIntegrityError),
    ):
        with pytest.raises(error):
            prepare_step_inputs(
                _recipe("file"),
                {"file": [selected]},
                step_run_id="e" * 32,
                reader=reader,
            )


def test_unrelated_diagnostic_endpoints_do_not_poison_ordinary_associations(
    reader: RetainedReader,
) -> None:
    _, seed = _seed(reader)
    primary = reader.read(seed).output("structures")[0]
    graph = OccurrenceGraph()
    artifact = _csv(2)
    root = graph.add_input("data", artifact)
    case = RetainedCase(
        graph.project({"data": [root]}, {"data": OutputSpec()}),
        {root: artifact},
        {},
    )
    _, receipt = admit_read_case(
        reader.committer, case, step_run_id="b" * 32, replay_of="f" * 32
    )
    reader.committer.accept(receipt)
    diagnostic = reader.read(receipt).output("data")[0]
    _measure(reader, diagnostic, "c" * 32)
    assert len(reader.associated(primary.reference, "metric")) == 1
    with pytest.raises(StoreIntegrityError, match="Diagnostic producers"):
        reader.associated(diagnostic.reference, "metric")
    selected = prepare_step_inputs(
        _recipe("data"), {"data": [diagnostic]}, step_run_id="d" * 32, reader=reader
    )[0]
    assert selected.boundary.input_attribution[0].producer == diagnostic.reference
