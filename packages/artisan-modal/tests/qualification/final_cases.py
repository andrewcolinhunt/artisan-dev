"""Exact public-pipeline assertions for the installed final provider image."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

from artisan_modal import ModalRunner
from artisan_modal.pack_owner import ModalPackOwner
from artisan_modal.software import ModalSoftware
from final_control import ControlledRunner
from modal_qualification_app import (
    CommandProbe,
    GPUCurator,
    NativeProbe,
    Seeds,
    TwoTools,
)

from artisan.orchestration import PipelineManager
from artisan.orchestration.runner_api import (
    AdmittedAssignment,
    AssignmentDispatch,
    AssignmentHandle,
    AssignmentSoftware,
    CompiledPlan,
    CuratorHandle,
    LocalRunner,
    OwnershipState,
    RunnerResources,
    SealedDelivery,
)
from artisan.schemas import MetricArtifact
from artisan.schemas.enums import FailurePolicy
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.storage import ArtifactStore
from artisan.storage.core.artifact_query import query_artifacts

APPLICATIONS = ("modal_qualification_app",)
BATCH = BatchStrategy(artifacts_per_unit=1, units_per_worker=2, max_workers=1)


def metrics(pipeline: PipelineManager, step_number: int) -> list[dict[str, Any]]:
    dispatch = pipeline.dispatch
    refs = query_artifacts(
        dispatch.delta_root,
        artifact_type="metric",
        pipeline_run_id=pipeline.config.pipeline_run_id,
        storage=dispatch.storage,
        files_root=dispatch.files_root,
    )
    store = ArtifactStore(
        dispatch.delta_root,
        files_root=dispatch.files_root,
        fs=dispatch.storage.filesystem(),
        storage_options=dispatch.storage.delta_storage_options(),
    )
    values = []
    for item in refs:
        if item.current_step_number == step_number:
            artifact = store.get_artifact(item.artifact_id, "metric")
            assert isinstance(artifact, MetricArtifact)
            values.append(artifact.values)
    return values


def seed(pipeline: PipelineManager) -> OutputReference:
    result = pipeline.run(Seeds, step_runner=LocalRunner(), skip_cache=True)
    assert result.status == StepStatus.SUCCEEDED
    assert sorted(item["input"] for item in metrics(pipeline, result.step_number)) == [
        0,
        1,
    ]
    return result.output("values")


def pack_evidence(
    handles: Sequence[AssignmentHandle | CuratorHandle],
) -> dict[str, object]:
    owner = handles[0].worker_owner
    assert isinstance(owner, ModalPackOwner)
    assert all(handle.worker_owner is owner for handle in handles)
    progress = [owner.outcome(index) for index in range(len(handles))]
    assert all(item is not None for item in progress)
    entries = [item for item in progress if item is not None]
    assert len({item.entry_id for item in entries}) == 1
    outcomes = [item.outcome for item in entries if item.outcome is not None]
    assert len(outcomes) == len(handles)
    assert len({item.entry_id for item in outcomes}) == len(outcomes)
    return {
        "owner_id": str(owner.observations[0].owner_id),
        "pack_id": str(entries[0].pack_id),
        "pack_entry": str(entries[0].entry_id),
        "core_entries": [str(item.entry_id) for item in outcomes],
        "ownership": owner.ownership_state().value,
    }


def nominal(
    pipeline: PipelineManager, manifest: Path, resources: RunnerResources
) -> dict[str, object]:
    source = seed(pipeline)
    result = pipeline.run(
        TwoTools,
        inputs={"source": source},
        params={"manifest": str(manifest), "gpu": resources.gpus > 0},
        runner_resources=resources,
        batch_strategy=BATCH,
        skip_cache=True,
    )
    assert result.status == StepStatus.SUCCEEDED, result.error
    values = sorted(
        metrics(pipeline, result.step_number), key=lambda value: value["input"]
    )
    assert [value["input"] for value in values] == [0, 1]
    assert len({value["worker_pid"] for value in values}) == 1
    assert [value["value"] for value in values] == (
        [42, 42] if resources.gpus else [2, 4]
    )
    for value in values:
        assert [tool["role"] for tool in value["tools"]] == ["a", "b"]
        assert [tool["activation"] for tool in value["tools"]] == ["alpha", "beta"]
        assert [tool["packaging"] for tool in value["tools"]] == ["24.2", "25.0"]
        assert (
            len({value["worker_python"], *(tool["python"] for tool in value["tools"])})
            == 3
        )
        if resources.gpus:
            assert value["native_gpu"]["output"] == 42
            assert [gpu["output"] for gpu in value["gpu"]] == [42, 42]
    handles = pipeline.executions[-1].handles
    assert len(handles) == 2
    assert handles[0].worker_owner is handles[1].worker_owner
    assert all(handle.ownership_state() == OwnershipState.CLOSED for handle in handles)
    receipts = [handle.result() for handle in handles]
    assert all(isinstance(receipt, SealedDelivery) for receipt in receipts)
    assert (
        len(
            {
                receipt.delivery.delivery_id
                for receipt in receipts
                if isinstance(receipt, SealedDelivery)
            }
        )
        == 2
    )
    return {
        "status": result.status.value,
        "values": values,
        "step_number": result.step_number,
        "shared_owner": True,
        "entries": pack_evidence(handles),
    }


def nonzero(
    pipeline: PipelineManager, manifest: Path, resources: RunnerResources
) -> dict[str, object]:
    source = seed(pipeline)
    result = pipeline.run(
        CommandProbe,
        inputs={"source": source},
        params={"manifest": str(manifest), "role": "a", "mode": "fail"},
        runner_resources=resources,
        batch_strategy=BATCH,
        failure_policy=FailurePolicy.CONTINUE,
        skip_cache=True,
    )
    assert result.status == StepStatus.PARTIAL, result.error
    values = metrics(pipeline, result.step_number)
    assert len(values) == 1
    assert values[0]["input"] == 0
    assert all(
        handle.ownership_state() == OwnershipState.CLOSED
        for handle in pipeline.executions[-1].handles
    )
    return {
        "status": result.status.value,
        "accepted_sibling": values[0],
        "entries": pack_evidence(pipeline.executions[-1].handles),
    }


def cancellation(
    pipeline: PipelineManager,
    runner: ModalRunner,
    manifest: Path,
    resources: RunnerResources,
    *,
    unknown: bool,
) -> dict[str, object]:
    source = seed(pipeline)
    controlled = ControlledRunner(runner, "unknown" if unknown else "sibling")
    operation = NativeProbe if unknown else CommandProbe
    params: dict[str, object] = (
        {"hold": True}
        if unknown
        else {"manifest": str(manifest), "role": "a", "mode": "hold"}
    )
    failure = None
    try:
        result = pipeline.run(
            operation,
            inputs={"source": source},
            params=params,
            step_runner=controlled,
            runner_resources=resources,
            batch_strategy=BATCH,
            skip_cache=True,
        )
    except Exception as error:
        failure = type(error).__name__
    else:
        assert result.status in {StepStatus.FAILED, StepStatus.CANCELLED}
    handles = controlled.evidence.pop("handles", ())
    assert isinstance(handles, tuple)
    assert len(handles) == 2
    if unknown:
        assert controlled.evidence["unknown_retained"]
        assert pipeline.ownership_state() == OwnershipState.UNKNOWN
    else:
        assert controlled.evidence["owners_closed"]
        assert pipeline.ownership_state() == OwnershipState.CLOSED
    return {
        **controlled.evidence,
        "controller_failure": failure,
        "scope": "provider handle/closure/seal; no controller cancellation acceptance claim",
    }


def curator_gpu(
    pipeline: PipelineManager, resources: RunnerResources
) -> dict[str, object]:
    result = pipeline.run(GPUCurator, runner_resources=resources, skip_cache=True)
    assert result.status == StepStatus.SUCCEEDED, result.error
    values = metrics(pipeline, result.step_number)
    assert len(values) == 1
    assert values[0]["output"] == 42
    return {"status": result.status.value, "value": values[0]}


class StaleGPU(ControlledRunner):
    """Alter only the resolved measurement to prove worker-side drift rejection."""

    name = "modal"

    def __init__(self, runner: ModalRunner) -> None:
        super().__init__(runner, "unknown")

    def resolve(
        self,
        plan: CompiledPlan,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> ModalSoftware:
        selected = self.delegate.resolve(
            plan, runner_resources, application_modules=application_modules
        )
        assert isinstance(selected, ModalSoftware)
        assert selected.gpu is not None
        return replace(
            selected, gpu=selected.gpu.model_copy(update={"shell_sha256": "0" * 64})
        )

    def launch(
        self,
        assignments: Sequence[AdmittedAssignment],
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> tuple[AssignmentHandle, ...]:
        return self.delegate.launch(assignments, software=software, dispatch=dispatch)


def stale_gpu(
    pipeline: PipelineManager, runner: ModalRunner, resources: RunnerResources
) -> dict[str, object]:
    source = seed(pipeline)
    selected = StaleGPU(runner)
    failure = None
    try:
        result = pipeline.run(
            NativeProbe,
            inputs={"source": source},
            params={"gpu": True},
            step_runner=selected,
            runner_resources=resources,
            batch_strategy=BATCH,
            skip_cache=True,
        )
    except Exception as error:
        failure = type(error).__name__
    else:
        assert result.status == StepStatus.FAILED
    handles = pipeline.executions[-1].handles
    assert len(handles) == 2
    owner = handles[0].worker_owner
    assert isinstance(owner, ModalPackOwner)
    assert owner.ownership_state() == OwnershipState.UNKNOWN
    assert all(owner.outcome(index) is None for index in range(2))
    diagnostics = " ".join(event.message or "" for event in owner.observations)
    assert "Modal GPU software changed after current qualification" in diagnostics
    return {
        "worker_rejected_before_inert_entry": True,
        "controller_failure": failure,
        "unknown_retained": True,
    }
