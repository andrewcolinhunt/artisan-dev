"""Ordered accepted outputs and direct relationships retain exact producers."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.retained_acceptance import admit_case, open_committer
from fixtures.retained_readers import Backend, admit_read_case
from fixtures.retained_results import RetainedCase, _derive, boundary_artifact

from artisan.errors import StoreIntegrityError
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.composites.plan import ExecutionRecipe, OutputContract
from artisan.schemas.execution.assignment import AssignmentIdentity
from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.schemas.provenance.projection import (
    AcceptedOccurrence,
    BoundaryInputAttribution,
    FrozenAssociation,
    FrozenBoundaryContext,
)
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.retained_reader import (
    AcceptedArtifact,
    ResultSelection,
    RetainedReader,
)


@pytest.mark.parametrize("kind", ["file", "appendable", "alias", "empty"])
def test_retained_reader_hydrates_public_support_and_empty_membership(
    backend_fs: Backend, tmp_path: Path, kind: str
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    case, receipt = admit_case(committer, tmp_path / "worker", kind=kind)
    committer.accept(receipt)
    reader = RetainedReader(committer)
    result = reader.read(receipt)
    assert result.projection == case.projection
    assert tuple(group.role for group in result.outputs) == tuple(
        group.role for group in case.projection.exports
    )
    assert (
        tuple(item.reference.address for item in result.support)
        == case.projection.support
    )
    for group in case.projection.exports:
        assert (
            tuple(item.reference.address for item in result.output(group.role))
            == group.occurrences
        )
    if kind in {"file", "appendable"}:
        assert len(result.support) == 2
        for item in result.support:
            item.artifact.verify_external_content(fs=fs)
    with reader.capture_inputs(receipt) as captured:
        assert captured.record.recipe == case.recipe
        assert captured.record.boundary.frozen_context == case.projection.frozen_context
        assert set(captured.artifacts) == set(case.boundary)


def test_direct_associations_follow_exact_cross_assignment_alias_chains(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    reader = RetainedReader(committer)
    artifact = boundary_artifact()
    graph = OccurrenceGraph()
    source = graph.add_input("data", artifact)
    first = _derive(graph, "first", artifact, [source])
    other = _derive(graph, "other", artifact, [source])
    seed = RetainedCase(
        graph.project({"data": [first, other, first]}, {"data": OutputSpec()}),
        {source: artifact},
        {first: artifact, other: artifact},
    )
    _, seed_receipt = admit_read_case(committer, seed, step_run_id="a" * 32)
    committer.accept(seed_receipt)
    selected = reader.read(seed_receipt).output("data")
    primary, unrelated = selected[0].reference, selected[1].reference

    def add_result(
        step: str, producer: AcceptedOccurrence, score: int | None
    ) -> AcceptedArtifact:
        local = OccurrenceGraph(boundary_invocation=f"input-{step}")
        slot = local.add_input("data", artifact)
        attribution = (BoundaryInputAttribution(boundary=slot, producer=producer),)
        if score is None:
            output, artifacts = slot, {}
        else:
            metric = MetricArtifact.draft({"score": score}, "metric.json", 7).finalize()
            output = _derive(local, "measure", metric, [slot])
            artifacts = {output: metric}
        case = RetainedCase(
            local.project(
                {"result": [output]},
                {"result": OutputSpec()},
                input_attribution=attribution,
            ),
            {slot: artifact},
            artifacts,
        )
        _, receipt = admit_read_case(committer, case, step_run_id=step * 32)
        committer.accept(receipt)
        return reader.read(receipt).output("result")[0]

    metric = add_result("b", primary, 1)
    frozen_graph = OccurrenceGraph(boundary_invocation="snapshot")
    slot = frozen_graph.add_input("data", artifact)
    dependency = frozen_graph.add_context_input(
        OccurrenceAddress(invocation_id="snapshot", role="associated", index=0),
        metric.artifact,
    )
    frozen = FrozenBoundaryContext(
        associations=(
            FrozenAssociation(
                primary=slot, artifact_type="metric", members=(dependency,)
            ),
        )
    )
    snapshot = RetainedCase(
        frozen_graph.project(
            {"data": [slot]},
            {"data": OutputSpec()},
            frozen_context=frozen,
            input_attribution=(
                BoundaryInputAttribution(boundary=slot, producer=primary),
                BoundaryInputAttribution(
                    boundary=dependency, producer=metric.reference
                ),
            ),
        ),
        {slot: artifact, dependency: metric.artifact},
        {},
    )
    _, snapshot_receipt = admit_read_case(committer, snapshot, step_run_id="e" * 32)
    committer.accept(snapshot_receipt)
    passthrough = add_result("c", primary, None)
    later = add_result("d", passthrough.reference, 2)
    found = reader.associated(primary, "metric")
    assert {item.reference for item in found} == {metric.reference, later.reference}
    assert {
        item.reference for item in reader.associated(passthrough.reference, "metric")
    } == {metric.reference, later.reference}
    assert reader.associated(unrelated, "metric") == ()
    with reader.capture_inputs(snapshot_receipt) as captured:
        assert captured.record.boundary.frozen_context == frozen
        assert captured.record.boundary.frozen_context.associated(slot, "metric") == (
            dependency,
        )
        assert not captured.record.boundary.associations


def test_collapsed_derivation_shortcuts_do_not_create_associations(
    backend_fs: Backend,
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    graph = OccurrenceGraph()
    artifact = boundary_artifact()
    source = graph.add_input("data", artifact)
    hidden = _derive(graph, "hidden", artifact, [source])
    metric = MetricArtifact.draft({"score": 1}, "metric.json", 7).finalize()
    final = _derive(graph, "measure", metric, [hidden])
    case = RetainedCase(
        graph.project(
            {"data": [source], "metric": [final]},
            {"data": OutputSpec(), "metric": OutputSpec()},
        ),
        {source: artifact},
        {final: metric},
    )
    assert [(edge.source, edge.target) for edge in case.projection.parents] == [
        (source, final)
    ]
    assert not case.projection.associations
    _, receipt = admit_read_case(committer, case, step_run_id="a" * 32)
    committer.accept(receipt)
    reader = RetainedReader(committer)
    primary = reader.read(receipt).output("data")[0].reference
    assert reader.associated(primary, "metric") == ()


def _optional_case(membership: str) -> tuple[RetainedCase, ExecutionRecipe]:
    graph = OccurrenceGraph()
    artifact = boundary_artifact()
    source = graph.add_input("data", artifact)
    specs = {role: OutputSpec(required=False) for role in ("optional", "other")}
    exports = (
        {}
        if membership == "omitted"
        else {"optional": []}
        if membership == "empty"
        else {"other": [], "optional": [source, source]}
    )
    case = RetainedCase(graph.project(exports, specs), {source: artifact}, {})
    recipe = case.recipe.model_copy(
        update={
            "outputs": tuple(
                OutputContract(
                    role=role,
                    artifact_type="csv",
                    required=False,
                    parent_kind=None,
                    parent_roles=(),
                )
                for role in specs
            ),
        }
    )
    return case, recipe


@pytest.mark.parametrize("second", ["present", "empty", "omitted"])
def test_step_outputs_use_declared_role_order_and_allow_unit_omissions(
    backend_fs: Backend, second: str
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    reader = RetainedReader(committer)
    selections = []
    for index, membership in enumerate(("omitted", second)):
        case, recipe = _optional_case(membership)
        _, receipt = admit_read_case(
            committer, case, step_run_id="a" * 32, unit_index=index, recipe=recipe
        )
        committer.accept(receipt)
        selections.append(
            ResultSelection(
                AssignmentIdentity(step_run_id="b" * 32, unit_index=index), receipt
            )
        )
        result = reader.read(receipt)
        assert tuple(group.role for group in result.outputs) == tuple(
            case.projection.export_roles()
        )
    outputs = reader.step_outputs(selections)
    assert tuple(group.role for group in outputs) == ("optional", "other")
    assert len(outputs[0].occurrences) == (2 if second == "present" else 0)
    assert not outputs[1].occurrences


def test_step_outputs_reject_different_admitted_output_contracts(
    backend_fs: Backend,
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    case, recipe = _optional_case("omitted")
    selections = []
    for index in range(2):
        selected = (
            recipe
            if not index
            else recipe.model_copy(update={"outputs": tuple(reversed(recipe.outputs))})
        )
        _, receipt = admit_read_case(
            committer, case, step_run_id="a" * 32, unit_index=index, recipe=selected
        )
        committer.accept(receipt)
        selections.append(ResultSelection(receipt.delivery.attempt.assignment, receipt))
    with pytest.raises(StoreIntegrityError, match="admitted output contract"):
        RetainedReader(committer).step_outputs(selections)
