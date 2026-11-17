"""Real finite scientific sources for interactive filtering scenarios."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from artisan.execution.assignment_transport import AssignmentDispatch
from artisan.execution.curator_worker import AdmittedCurator
from artisan.execution.executors.curator import run_curator_flow
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.engine.retained_step_results import RetainedStepResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.runners.assignment import AssignmentSoftware
from artisan.orchestration.runners.local import LocalRunner
from artisan.schemas import Artifact, ArtifactResult, InputReference, OutputSpec
from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    AssignmentIdentity,
    DeliveryIdentity,
    SuccessfulDelivery,
)
from artisan.schemas.execution.curator_admission import CuratorAdmissionReference
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.step_admission import StepAdmission
from artisan.schemas.execution.worker_config import WorkerConfig
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
)
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.cache.curator_dependencies import CuratorReadRecorder
from artisan.storage.core.curator_view import CuratorView
from artisan.storage.io.admission import publish_admission
from artisan.storage.io.curator_admission import read_curator_admission
from artisan.storage.io.retained_seal import stage_retained_result
from fixtures.retained_acceptance import boundary_projection
from fixtures.retained_inspection import inspection_store
from fixtures.retained_results import RetainedCase


def commit_interactive_case(
    root: Path,
    outputs: Sequence[Artifact],
    parents: Mapping[str, Sequence[Artifact]],
    *,
    run: str,
    number: int,
    name: str,
) -> str:
    graph = OccurrenceGraph()
    output_ids = {artifact.artifact_id for artifact in outputs}
    parents = {key: value for key, value in parents.items() if key in output_ids}
    sources = {
        artifact.artifact_id: artifact
        for items in parents.values()
        for artifact in items
    }
    boundary = {
        artifact_id: graph.add_input("source", artifact)
        for artifact_id, artifact in sources.items()
    }
    input_indices = {artifact_id: index for index, artifact_id in enumerate(boundary)}
    result = ArtifactResult(artifacts={"output": []}, lineage={"output": []})
    for artifact in outputs:
        selected = parents.get(artifact.artifact_id, ())
        result.add_artifact(
            "output",
            artifact,
            sources={
                "source": [
                    InputReference(
                        invocation_id="produce",
                        role="source",
                        index=input_indices[parent.artifact_id],
                        artifact_id=parent.artifact_id,
                    )
                    for parent in selected
                ]
            }
            if selected
            else {},
        )
    specs = {
        "output": OutputSpec(
            required=False, derives_from={"inputs": ["source"] if boundary else []}
        )
    }
    emitted = graph.record_result(
        "produce",
        {"source": tuple(boundary.values())} if boundary else {},
        result,
        specs,
    )
    projection = graph.project(emitted, specs)
    case = RetainedCase(
        projection,
        {address: graph.nodes[address].artifact for address in boundary.values()},
        {
            address: graph.nodes[address].artifact
            for addresses in emitted.values()
            for address in addresses
        },
    )
    recipe = case.recipe.model_copy(
        update={"definition": case.recipe.definition.model_copy(update={"name": name})}
    )
    committer = inspection_store(root)
    tracker = RetainedStepTracker(committer, run)
    step_run_id = uuid4().hex
    tracker.create_attempt(
        StepStartRecord(
            step_run_id=step_run_id,
            step_number=number,
            step_name=name,
            operation_class=f"{recipe.definition.module}.{recipe.definition.qualname}",
            params_json="{}",
            input_refs_json="{}",
            compute_backend="local",
            compute_options_json=json.dumps(
                {
                    "pipeline_name": run,
                    "pipeline_default_step_runner": "local",
                    "pipeline_default_runner_requires_instance": False,
                    "application_modules": [],
                }
            ),
            output_roles_json='["output"]',
            output_types_json=json.dumps({"output": recipe.outputs[0].artifact_type}),
        )
    )
    attempt = AssignmentAttempt(
        assignment=AssignmentIdentity(step_run_id=step_run_id, unit_index=0),
        attempt=0,
        generation=0,
        recipe_digest=recipe.digest,
        software_digest="c" * 64,
        input_context_digest=case.projection.input_context_digest,
    )
    reference = publish_admission(
        attempt,
        recipe,
        boundary_projection(case),
        case.boundary,
        step_number=number,
        delta_root=committer.delta_root,
        files_root=committer.files_root,
        fs=committer.fs,
        verify_producer=committer._verify_producer,
    )
    committer.journal.admit(attempt, admission_digest=reference.record_digest)
    tracker.admit(
        StepAdmission(
            step_run_id=step_run_id,
            step_number=number,
            recipe=recipe,
            software_digest="c" * 64,
            assignments=(reference,),
        )
    )
    receipt = stage_retained_result(
        f"{committer.delta_root}/staging",
        SuccessfulDelivery(
            delivery=DeliveryIdentity.new(attempt),
            computation_complete=True,
            cleanup_complete=True,
        ),
        case.projection,
        case.artifacts,
        fs=committer.fs,
    )
    committer.accept(receipt)
    RetainedStepResults(tracker).finish(step_run_id)
    return step_run_id


@dataclass(frozen=True)
class TestSoftware:
    """No transport qualification is claimed by the synchronous UI test runner."""

    software_digest: str = "e" * 64
    software_cacheable: bool = False


@dataclass(frozen=True)
class ClosedCuratorHandle:
    admission: CuratorAdmissionReference
    staged: StagingResult
    worker_owner: None = None

    def result(self, timeout: float | None = None) -> StagingResult:
        return self.staged

    def done(self) -> bool:
        return True

    def ownership_state(self) -> OwnershipState:
        return OwnershipState.CLOSED

    def cancel(self) -> CancellationAcknowledgement:
        return CancellationAcknowledgement(CancellationStatus.REJECTED)


def resolve_test_curator(
    runner: LocalRunner,
    operation: OperationDefinition,
    runner_resources: RunnerResources,
    *,
    application_modules: tuple[str, ...] = (),
) -> TestSoftware:
    return TestSoftware()


def launch_test_curator(
    runner: LocalRunner,
    curator: AdmittedCurator,
    *,
    software: AssignmentSoftware,
    dispatch: AssignmentDispatch,
) -> ClosedCuratorHandle:
    committer = inspection_store(dispatch.delta_root)
    admission = read_curator_admission(
        curator.admission, delta_root=dispatch.delta_root, fs=committer.fs
    )
    view = CuratorView(committer, admission.view)
    staged = run_curator_flow(
        ExecutionUnit(
            operation=curator.operation,
            inputs={
                role.role: [item.artifact_id for item in role.members]
                for role in admission.inputs
            },
            step_number=admission.record.step_number,
            step_run_id=admission.record.step_run_id,
            execution_spec_id=admission.digest,
            replay_snapshot=ReplaySnapshot.unavailable(
                "synchronous-interactive-fixture"
            ),
            replay_sensitive_values=curator.sensitive_values,
        ),
        WorkerConfig(
            delta_root=dispatch.delta_root,
            staging_root=dispatch.staging_root,
            files_root=dispatch.files_root,
            working_root=str(dispatch.working_root),
            storage=dispatch.storage,
        ),
        curator_view=view,
        read_recorder=CuratorReadRecorder(view, admission),
    )
    return ClosedCuratorHandle(curator.admission, staged)
