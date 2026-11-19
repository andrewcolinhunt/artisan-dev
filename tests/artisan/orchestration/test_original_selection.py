"""Anchored replay proves full selection through required admission authority."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import polars as pl
import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import PersistenceIntegrityError, StoreIntegrityError
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.orchestration.engine.retained_step_results import RetainedStepResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.original_selection import resolve_original_selection
from artisan.schemas import MetricArtifact, OutputSpec
from artisan.schemas.composites.grouping import AnchoredNameGrouping
from artisan.schemas.composites.plan import (
    DefinitionIdentity,
    ExecutionContract,
    ExecutionRecipe,
    InputContract,
    InvocationPolicy,
    OutputContract,
)
from artisan.schemas.enums import GroupByStrategy, TablePath
from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    AssignmentIdentity,
    DeliveryIdentity,
    SuccessfulDelivery,
)
from artisan.schemas.execution.step_admission import StepAdmission
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.execution_source import (
    ScientificExecutionSource,
    read_execution_source,
)
from artisan.storage.io.admission import admission_directory, publish_admission
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_seal import stage_retained_result
from artisan.storage.io.retained_store import create_retained_store
from artisan.storage.io.step_result_plan import StepUnitFailure
from artisan.utils.hashing import canonical_json_bytes

type InputRows = dict[str, list[MetricArtifact]]


def _recipe() -> ExecutionRecipe:
    return ExecutionRecipe(
        schema_version=2,
        definition=DefinitionIdentity(
            kind="creator", name="named", version="1", module=__name__, qualname="Named"
        ),
        params_json="{}",
        inputs=tuple(
            InputContract(
                role=role,
                artifact_type="metric",
                required=role in {"source", "offset"},
                materialize=False,
                hydrate=True,
                materialize_as=None,
                resolve_references=False,
                with_associated=(),
            )
            for role in ("source", "offset", "context", "other")
        ),
        outputs=(
            OutputContract(
                role="output",
                artifact_type="metric",
                required=True,
                parent_kind=None,
                parent_roles=(),
            ),
        ),
        policy=InvocationPolicy(
            group_by=AnchoredNameGrouping(
                primary_role="source", matched_roles=("context", "other")
            ),
            shared_input_roles=("offset",),
            required_nonempty_inputs=("context",),
            artifacts_per_unit=5,
            per_artifact_dispatch=False,
        ),
        execution=ExecutionContract(mode="native"),
        cacheable=True,
    )


def _metric(index: int) -> MetricArtifact:
    return MetricArtifact.draft({"value": index}, f"query-{index}.json").finalize()


def _inputs(*indices: int, context: bool = False) -> InputRows:
    return {
        "source": [_metric(index) for index in indices],
        "offset": [_metric(100)],
        "context": [_metric(indices[-1])] if context else [],
    }


def _admit_step(
    committer: RetainedCommitter,
    recipe: ExecutionRecipe,
    units: tuple[InputRows, ...],
    *,
    replay_of: str | None = None,
    first_delivery_id: str | None = None,
    failed: bool = False,
    finish: bool = True,
    retry: bool = False,
) -> tuple[StepAdmission, tuple[DeliveryIdentity, ...]]:
    step_id = uuid4().hex
    tracker = RetainedStepTracker(committer, "original-selection-tests")
    tracker.create_attempt(
        StepStartRecord(
            step_run_id=step_id,
            step_number=0,
            step_name="named",
            operation_class=f"{recipe.definition.module}.{recipe.definition.qualname}",
            params_json=recipe.params_json,
            input_refs_json="{}",
            compute_backend="native-test",
            compute_options_json="{}",
            output_roles_json='["output"]',
            output_types_json='{"output":"metric"}',
            replay_of_execution_run_id=replay_of,
        )
    )
    references = []
    graphs = []
    for index, rows in enumerate(units):
        identity = AssignmentIdentity(step_run_id=step_id, unit_index=index)
        graph = OccurrenceGraph(boundary_invocation=identity.assignment_id)
        for role, artifacts in rows.items():
            graph.declare_input_role(role)
            for artifact in artifacts:
                graph.add_input(role, artifact)
        boundary = graph.project({}, {})
        attempt = AssignmentAttempt(
            assignment=identity,
            attempt=0,
            generation=0,
            recipe_digest=recipe.digest,
            software_digest="c" * 64,
            input_context_digest=boundary.input_context_digest,
            replay_of_execution_run_id=replay_of,
        )
        reference = publish_admission(
            attempt,
            recipe,
            boundary,
            {address: node.artifact for address, node in graph.nodes.items()},
            step_number=0,
            delta_root=committer.delta_root,
            files_root=committer.files_root,
            fs=committer.fs,
        )
        committer.journal.admit(attempt, admission_digest=reference.record_digest)
        references.append(reference)
        graphs.append(graph)
    step = StepAdmission(
        step_run_id=step_id,
        step_number=0,
        recipe=recipe,
        software_digest="c" * 64,
        assignments=tuple(references),
    )
    tracker.admit(step)
    if retry:
        graph = graphs[0]
        attempt = references[0].attempt.model_copy(
            update={"attempt": 1, "generation": 1}
        )
        reference = publish_admission(
            attempt,
            recipe,
            graph.project({}, {}),
            {address: node.artifact for address, node in graph.nodes.items()},
            step_number=0,
            delta_root=committer.delta_root,
            files_root=committer.files_root,
            fs=committer.fs,
        )
        committer.journal.admit(attempt, admission_digest=reference.record_digest)
        references[0] = reference
    deliveries = []
    for index, (reference, graph) in enumerate(zip(references, graphs, strict=True)):
        delivery = DeliveryIdentity(
            attempt=reference.attempt,
            delivery_id=first_delivery_id
            if index == 0 and first_delivery_id
            else uuid4().hex,
        )
        deliveries.append(delivery)
        if failed and index == 0:
            continue
        source = graph.project({}, {}).input_roles()["source"]
        receipt = stage_retained_result(
            f"{committer.delta_root}/staging",
            SuccessfulDelivery(
                delivery=delivery, computation_complete=True, cleanup_complete=True
            ),
            graph.project(
                {"output": source}, {"output": OutputSpec(artifact_type="metric")}
            ),
            {},
            fs=committer.fs,
        )
        committer.accept(receipt)
    if finish:
        RetainedStepResults(tracker).finish(
            step_id,
            failures={
                0: StepUnitFailure(
                    error="failed native work", failed_delivery=deliveries[0]
                )
            }
            if failed
            else None,
        )
    return step, tuple(deliveries)


@pytest.fixture
def committer(tmp_path: Path) -> RetainedCommitter:
    return create_retained_store(
        str(tmp_path / "store"), str(tmp_path / "files"), fs=LocalFileSystem()
    )


@pytest.mark.parametrize("failed", [False, True])
def test_sparse_replay_of_replay_uses_ordinary_full_inventory(
    committer: RetainedCommitter,
    failed: bool,
) -> None:
    recipe = _recipe()
    units = (_inputs(0, 1, 2, 3, 4), _inputs(5, context=True))
    original, deliveries = _admit_step(committer, recipe, units, failed=failed)
    first, replay = _admit_step(
        committer, recipe, units[:1], replay_of=deliveries[0].delivery_id
    )
    second, twice = _admit_step(
        committer, recipe, units[:1], replay_of=replay[0].delivery_id
    )
    proof = resolve_original_selection(committer, twice[0].delivery_id, recipe)
    assert proof.steps == (second, first, original)
    assert proof.origin_step == original
    assert proof.selected_admissions == (
        second.assignments[0],
        first.assignments[0],
        original.assignments[0],
    )
    assert resolve_original_selection(committer, twice[0].delivery_id, recipe) == proof


def test_completed_unit_can_use_admitted_inventory_before_step_terminal(
    committer: RetainedCommitter,
) -> None:
    recipe = _recipe()
    step, deliveries = _admit_step(
        committer, recipe, (_inputs(0), _inputs(1, context=True)), finish=False
    )
    assert (
        resolve_original_selection(
            committer, deliveries[0].delivery_id, recipe
        ).origin_step
        == step
    )


def test_current_requirements_use_origin_inventory_without_hydrating_siblings(
    committer: RetainedCommitter,
) -> None:
    recipe = _recipe()
    step, deliveries = _admit_step(
        committer, recipe, (_inputs(0), _inputs(1, context=True))
    )
    sibling = read_execution_source(committer, deliveries[1].delivery_id)
    assert isinstance(sibling, ScientificExecutionSource)
    directory = admission_directory(committer.delta_root, step.assignments[1].attempt)
    for table in sibling.admission.tables:
        committer.fs.rm(f"{directory}/{table.filename}")
    assert (
        resolve_original_selection(
            committer, deliveries[0].delivery_id, recipe
        ).origin_step
        == step
    )
    changed = recipe.model_copy(
        update={
            "policy": recipe.policy.model_copy(
                update={"required_nonempty_inputs": ("other",)}
            )
        }
    )
    with pytest.raises(StoreIntegrityError, match="nonempty input role 'other'"):
        resolve_original_selection(committer, deliveries[0].delivery_id, changed)


@pytest.mark.parametrize("missing", ["step", "sibling", "lifecycle"])
def test_original_selection_recheck_fails_closed_when_origin_evidence_disappears(
    committer: RetainedCommitter,
    missing: str,
) -> None:
    recipe = _recipe()
    step, deliveries = _admit_step(
        committer, recipe, (_inputs(0), _inputs(1, context=True))
    )
    resolve_original_selection(committer, deliveries[0].delivery_id, recipe)
    if missing == "lifecycle":
        path = f"{committer.delta_root}/{TablePath.STEPS.value}"
        frame = pl.read_delta(path)
        frame.filter(pl.col("step_run_id") != step.step_run_id).write_delta(
            path, mode="overwrite"
        )
    else:
        path = (
            f"{committer.delta_root}/_artisan/step-admissions/{step.step_run_id}.json"
            if missing == "step"
            else f"{admission_directory(committer.delta_root, step.assignments[1].attempt)}/admission.json"
        )
        committer.fs.rm(path)
    with pytest.raises(
        (FileNotFoundError, StoreIntegrityError, PersistenceIntegrityError)
    ):
        resolve_original_selection(committer, deliveries[0].delivery_id, recipe)


def test_replaced_origin_inventory_does_not_authenticate_its_own_new_digest(
    committer: RetainedCommitter,
) -> None:
    recipe = _recipe()
    step, deliveries = _admit_step(
        committer, recipe, (_inputs(0), _inputs(1, context=True))
    )
    path = f"{committer.delta_root}/_artisan/step-admissions/{step.step_run_id}.json"
    replaced = step.model_copy(update={"assignments": step.assignments[:1]})
    committer.fs.pipe_file(path, canonical_json_bytes(replaced.model_dump(mode="json")))
    with pytest.raises(StoreIntegrityError, match="controller-bound identity"):
        resolve_original_selection(committer, deliveries[0].delivery_id, recipe)


@pytest.mark.parametrize("change", ["order", "empty_role", "artifact"])
def test_replay_link_requires_exact_owner_independent_input_membership(
    committer: RetainedCommitter,
    change: str,
) -> None:
    recipe = _recipe()
    _, deliveries = _admit_step(
        committer, recipe, (_inputs(0, 1), _inputs(2, context=True))
    )
    altered = _inputs(0, 1)
    if change == "order":
        altered["source"].reverse()
    elif change == "empty_role":
        del altered["context"]
    else:
        altered["source"][0] = _metric(9)
    _, replay = _admit_step(
        committer, recipe, (altered,), replay_of=deliveries[0].delivery_id
    )
    with pytest.raises(StoreIntegrityError, match="exact admitted input semantics"):
        resolve_original_selection(committer, replay[0].delivery_id, recipe)


def test_replay_chain_without_ordinary_origin_fails_on_cycle(
    committer: RetainedCommitter,
) -> None:
    recipe = _recipe()
    first_id, second_id = uuid4().hex, uuid4().hex
    _admit_step(
        committer,
        recipe,
        (_inputs(0),),
        replay_of=second_id,
        first_delivery_id=first_id,
    )
    _admit_step(
        committer,
        recipe,
        (_inputs(0),),
        replay_of=first_id,
        first_delivery_id=second_id,
    )
    with pytest.raises(StoreIntegrityError, match="cycle"):
        resolve_original_selection(committer, first_id, recipe)


def test_replay_chain_with_unavailable_source_fails_closed(
    committer: RetainedCommitter,
) -> None:
    recipe = _recipe()
    _, deliveries = _admit_step(committer, recipe, (_inputs(0),), replay_of=uuid4().hex)
    with pytest.raises(
        StoreIntegrityError, match="completed scientific replay sources"
    ):
        resolve_original_selection(committer, deliveries[0].delivery_id, recipe)


@pytest.mark.parametrize("change", ["primary", "matched", "shared", "legacy"])
def test_replay_rejects_changed_grouping_shape_even_when_unit_would_fit(
    committer: RetainedCommitter,
    change: str,
) -> None:
    recipe = _recipe()
    _, deliveries = _admit_step(committer, recipe, (_inputs(0, context=True),))
    policy = recipe.policy
    if change == "shared":
        policy = policy.model_copy(update={"shared_input_roles": ()})
    elif change == "legacy":
        policy = policy.model_copy(
            update={"group_by": GroupByStrategy.ZIP, "required_nonempty_inputs": ()}
        )
    else:
        grouping = AnchoredNameGrouping(
            primary_role="context" if change == "primary" else "source",
            matched_roles=("source", "other")
            if change == "primary"
            else ("other", "context"),
        )
        policy = policy.model_copy(update={"group_by": grouping})
    changed = recipe.model_copy(update={"policy": policy})
    with pytest.raises((StoreIntegrityError, ValueError), match="grouping|shared"):
        resolve_original_selection(committer, deliveries[0].delivery_id, changed)


@pytest.mark.parametrize("finish", [False, True])
def test_original_selection_keeps_initial_inventory_for_journaled_retry(
    committer: RetainedCommitter,
    finish: bool,
) -> None:
    recipe = _recipe()
    step, deliveries = _admit_step(
        committer,
        recipe,
        (_inputs(0), _inputs(1, context=True)),
        retry=True,
        finish=finish,
    )
    proof = resolve_original_selection(committer, deliveries[0].delivery_id, recipe)
    assert proof.origin_step == step
    assert proof.selected_admissions[0].attempt.generation == 1
    assert step.assignments[0].attempt.generation == 0


def test_legacy_origin_cannot_gain_anchored_selection_proof(
    committer: RetainedCommitter,
) -> None:
    recipe = _recipe()
    legacy = recipe.model_copy(
        update={
            "policy": recipe.policy.model_copy(
                update={
                    "group_by": GroupByStrategy.ZIP,
                    "required_nonempty_inputs": (),
                }
            ),
        }
    )
    _, deliveries = _admit_step(committer, legacy, (_inputs(0, context=True),))
    with pytest.raises(StoreIntegrityError, match="grouping or shared input roles"):
        resolve_original_selection(committer, deliveries[0].delivery_id, recipe)
