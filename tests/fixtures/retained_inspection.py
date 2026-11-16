"""Real retained attempts shared by public inspection and analysis tests."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from functools import partial
from pathlib import Path
from uuid import uuid4

from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.orchestration.engine.retained_step_results import RetainedStepResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas import Artifact, ArtifactResult, InputReference, OutputSpec
from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    AssignmentIdentity,
    DeliveryIdentity,
    SealedDelivery,
    SuccessfulDelivery,
)
from artisan.schemas.execution.step_admission import StepAdmission
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
)
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.cache.retained_reuse import verify_cache_source
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.admission import capture_admission, publish_admission
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_seal import stage_retained_result
from artisan.storage.io.retained_store import create_retained_store
from artisan.storage.io.step_result_plan import StepUnitFailure
from fixtures.retained_acceptance import boundary_projection
from fixtures.retained_results import RetainedCase


def inspection_store(root: str | Path) -> RetainedCommitter:
    """Open or initialize this test's store with the ordinary sibling files root."""
    root = Path(root)
    return create_retained_store(
        str(root), str(root.parent / "files"), fs=StorageConfig().filesystem()
    )


def commit_inspection_attempt(
    root: str | Path,
    *,
    run_id: str = "source-run",
    number: int = 0,
    name: str | None = None,
    outputs: Mapping[str, Sequence[Artifact]] | None = None,
    output_types: Mapping[str, str | None] | None = None,
    status: str = "succeeded",
    step_run_id: str | None = None,
    reused_execution_id: str | None = None,
    input_refs: Mapping[str, object] | None = None,
    inputs: Mapping[str, Sequence[Artifact]] | None = None,
    error: str = "fixture failure",
    duration_seconds: float | None = 1.0,
    failed_delivery: bool = True,
    replay_of: str | None = None,
    recorded_pipeline_defaults: bool = False,
    committer: RetainedCommitter | None = None,
) -> tuple[str, str | None]:
    """Publish a real finite attempt, preserving original producers for reuse."""
    if committer is None:
        committer = inspection_store(root)
    elif committer.delta_root != str(root):
        msg = "Supplied committer belongs to another Delta root"
        raise ValueError(msg)
    step_run_id = step_run_id or uuid4().hex
    outputs = {"output": ()} if outputs is None else outputs
    case = _case(outputs, inputs or {})
    recipe = case.recipe
    recipe = recipe.model_copy(
        update={
            "definition": recipe.definition.model_copy(
                update={"name": name or f"step-{number}"}
            )
        }
    )
    recipe = recipe.model_copy(
        update={
            "outputs": tuple(
                item.model_copy(update={"artifact_type": "any"})
                if len({artifact.artifact_type for artifact in outputs[item.role]}) > 1
                else item
                for item in recipe.outputs
            )
        }
    )
    if output_types is not None:
        recipe = recipe.model_copy(
            update={
                "outputs": tuple(
                    item.model_copy(update={"artifact_type": output_types[item.role]})
                    for item in recipe.outputs
                )
            }
        )
    source = None
    boundary = boundary_projection(case)
    boundary_artifacts = case.boundary
    if reused_execution_id is not None:
        source_plan = next(
            plan
            for plan in RetainedReader(committer).completed_plans()
            if plan.receipt.delivery.delivery_id == reused_execution_id
        )
        source = source_plan.receipt
        with capture_admission(
            source_plan.admission,
            delta_root=committer.delta_root,
            files_root=committer.files_root,
            fs=committer.fs,
            verify_producer=committer._verify_producer,
        ) as captured:
            recipe, boundary = captured.record.recipe, captured.record.boundary
            boundary_artifacts = dict(captured.artifacts)
    tracker = RetainedStepTracker(committer, run_id)
    tracker.create_attempt(
        StepStartRecord(
            step_run_id=step_run_id,
            step_number=number,
            step_name=name or f"step-{number}",
            operation_class=f"{recipe.definition.module}.{recipe.definition.qualname}",
            params_json=recipe.params_json,
            input_refs_json=json.dumps(input_refs or {}),
            compute_backend="local",
            compute_options_json=json.dumps(
                {
                    "pipeline_name": run_id,
                    "pipeline_default_step_runner": "local",
                    "pipeline_default_runner_requires_instance": False,
                    "application_modules": [],
                }
            )
            if recorded_pipeline_defaults
            else "{}",
            output_roles_json=json.dumps(sorted(item.role for item in recipe.outputs)),
            output_types_json=json.dumps(
                {item.role: item.artifact_type for item in recipe.outputs}
            ),
            replay_of_execution_run_id=replay_of,
        )
    )
    if status == "pending":
        return step_run_id, None
    if status == "skipped":
        tracker.skip_pending(step_run_id, reason="empty_inputs")
        return step_run_id, None
    references = []
    for index in range(2 if status == "partial" else 1):
        attempt = AssignmentAttempt(
            assignment=AssignmentIdentity(step_run_id=step_run_id, unit_index=index),
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
            boundary_artifacts,
            step_number=number,
            delta_root=committer.delta_root,
            files_root=committer.files_root,
            fs=committer.fs,
            verify_producer=committer._verify_producer,
        )
        committer.journal.admit(attempt, admission_digest=reference.record_digest)
        references.append(reference)
    tracker.admit(
        StepAdmission(
            step_run_id=step_run_id,
            step_number=number,
            recipe=recipe,
            software_digest="c" * 64,
            assignments=tuple(references),
        )
    )
    if status == "running":
        return step_run_id, None
    coordinator = RetainedStepResults(tracker)
    receipt: SealedDelivery | None = None
    if status in {"succeeded", "partial"}:
        if source is not None:
            committer.journal.reuse(
                references[0],
                source,
                verify_source=partial(
                    verify_cache_source, reader=RetainedReader(committer)
                ),
            )
            receipt = source
        else:
            receipt = stage_retained_result(
                f"{committer.delta_root}/staging",
                SuccessfulDelivery(
                    delivery=DeliveryIdentity.new(references[0].attempt),
                    computation_complete=True,
                    cleanup_complete=True,
                ),
                case.projection,
                case.artifacts,
                fs=committer.fs,
                artifact_fs=committer.fs,
            )
            committer.accept(receipt)
    if status == "cancelled":
        for cancellation in (
            CancellationStatus.REQUESTED,
            CancellationStatus.CONFIRMED,
        ):
            coordinator.record_cancellation(
                step_run_id, CancellationAcknowledgement(cancellation)
            )
    failures = {
        index: StepUnitFailure(
            error=error,
            failed_delivery=DeliveryIdentity.new(references[index].attempt)
            if failed_delivery
            else None,
        )
        for index in range(len(references))
        if index != 0 or receipt is None
    }
    coordinator.finish(
        step_run_id, failures=failures, duration_seconds=duration_seconds
    )
    return step_run_id, (
        receipt.delivery.delivery_id
        if receipt is not None
        else failures[0].failed_delivery.delivery_id
        if failures and failures[0].failed_delivery is not None
        else None
    )


def _case(
    outputs: Mapping[str, Sequence[Artifact]],
    inputs: Mapping[str, Sequence[Artifact]],
) -> RetainedCase:
    graph = OccurrenceGraph()
    boundary = {
        role: tuple(graph.add_input(role, artifact) for artifact in items)
        for role, items in inputs.items()
    }
    result = ArtifactResult(
        artifacts={role: [] for role in outputs}, lineage={role: [] for role in outputs}
    )
    for role, items in outputs.items():
        for artifact in items:
            result.add_artifact(
                role,
                artifact,
                sources={
                    source_role: [
                        InputReference(
                            invocation_id="produce",
                            role=source_role,
                            index=index,
                            artifact_id=graph.nodes[address].artifact.artifact_id,
                        )
                        for index, address in enumerate(addresses)
                    ]
                    for source_role, addresses in boundary.items()
                    if addresses
                },
            )
    specs = {
        role: OutputSpec(required=False, derives_from={"inputs": list(inputs)})
        for role in outputs
    }
    addresses = graph.record_result("produce", boundary, result, specs)
    projection = graph.project(addresses, specs)
    return RetainedCase(
        projection,
        {
            address: graph.nodes[address].artifact
            for items in boundary.values()
            for address in items
        },
        {
            address: graph.nodes[address].artifact
            for items in addresses.values()
            for address in items
        },
    )
