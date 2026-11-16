"""Pipeline coordination shares admission, exact outputs, and retained ownership."""

from __future__ import annotations

import json
import threading
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import pytest
from fixtures.composition_operations import (
    Measure,
    PredictMeasure,
    SummarizeMetrics,
    Transform,
    coordinate,
    structure,
)
from fixtures.curator_cache import DynamicCurator, MeasureCSV
from fixtures.retained_acceptance import open_committer
from fixtures.retained_pipeline import NativeCuratorSoftware, NativePipelineRunner
from pydantic import Field

from artisan.composites import CompositeContext
from artisan.errors import PersistenceIntegrityError
from artisan.execution.assignment_transport import (
    AdmittedAssignment,
    AssignmentDispatch,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.curator.ingest_csv import IngestCSV
from artisan.operations.curator.merge import Merge
from artisan.operations.examples.wait_tool import WaitTool
from artisan.orchestration import pipeline_manager as pipeline_module
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.orchestration.engine.pipeline_inputs import (
    FrozenPipelineInputs,
    PipelineInputs,
)
from artisan.orchestration.engine.pipeline_submission import freeze_submission
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.pipeline_manager import PipelineManager
from artisan.orchestration.runners.assignment import (
    AssignmentHandle,
    AssignmentLaunchError,
    AssignmentSoftware,
)
from artisan.orchestration.runners.curator import CuratorLaunchError
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.enums import CachePolicy, GroupByStrategy
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.specs.input_models import PostprocessInput, PreprocessInput
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.retained_reader import AcceptedArtifact, AcceptedRole
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.utils.command_supervisor import CommandOwnershipError


def _pipeline(tmp_path: Path) -> tuple[PipelineManager, NativePipelineRunner]:
    storage = StorageConfig()
    committer = open_committer(
        str(tmp_path / "store"), storage.filesystem(), storage, initialize=True
    )
    runner = NativePipelineRunner(committer)
    CuratorResults(
        RetainedStepTracker(committer, "synthetic-pipeline"), str(tmp_path / "staging")
    ).initialize()
    pipeline = PipelineManager(
        committer,
        AssignmentDispatch(
            committer.delta_root,
            committer.files_root,
            str(tmp_path / "staging"),
            storage,
            tmp_path / "work",
        ),
        pipeline_run_id="synthetic-pipeline",
        default_step_runner=runner,
    )
    return pipeline, runner


def test_creator_composite_curator_chain_and_cache_keep_exact_sources(
    tmp_path: Path,
) -> None:
    pipeline, runner = _pipeline(tmp_path)
    with pipeline:
        first = pipeline.run(PredictMeasure, inputs={"structure": [structure(0)]})
        cached = pipeline.run(PredictMeasure, inputs={"structure": [structure(0)]})
        assert first.status == cached.status == StepStatus.SUCCEEDED
        assert cached.disposition == StepDisposition.CACHE_HIT
        selected = pipeline.run(
            Merge, inputs=[first.output("structure"), cached.output("structure")]
        )
        measured = pipeline.run(
            MeasureCSV, inputs={"source": selected.output("merged")}
        )
        assert selected.status == measured.status == StepStatus.SUCCEEDED
        original = read_current_step_outputs(pipeline.committer, first.step_run_id)
        reused = read_current_step_outputs(pipeline.committer, cached.step_run_id)
        assert original == reused
        merged = read_current_step_outputs(pipeline.committer, selected.step_run_id)
        assert len(merged[0].occurrences) == 1
        assert runner.curator_launches == 1
    assert pipeline.ownership_state() == OwnershipState.CLOSED


def test_async_submission_copies_inputs_and_parameters_before_pending(
    tmp_path: Path,
) -> None:
    pipeline, runner = _pipeline(tmp_path)
    runner.release.clear()
    params = {"add": 3}
    values = [structure(0)]
    with pipeline:
        first = pipeline.submit(Transform, inputs={"structure": values}, params=params)
        assert runner.entered.wait(5)
        second = pipeline.submit(
            Transform, inputs={"structure": first.output("structure")}
        )
        assert first.status == second.status == StepStatus.PENDING
        params["add"] = 100
        values.clear()
        runner.release.set()
        result = second.result()
        assert result.status == StepStatus.SUCCEEDED
        outputs = read_current_step_outputs(pipeline.committer, result.step_run_id)
        assert coordinate(outputs[0].occurrences[0].artifact) == 4


def test_raw_file_ingestion_and_dynamic_curator_share_pipeline_entry(
    tmp_path: Path,
) -> None:
    pipeline, _ = _pipeline(tmp_path)
    source = tmp_path / "input.csv"
    source.write_bytes(b"value\n3\n")
    with pipeline:
        ingested = pipeline.run(IngestCSV, inputs=[str(source)])
        assert ingested.status == StepStatus.SUCCEEDED
        source.unlink()
        assert read_current_step_outputs(pipeline.committer, ingested.step_run_id)
        dynamic = pipeline.run(DynamicCurator)
        measured = pipeline.run(MeasureCSV, inputs={"source": dynamic.output("found")})
        assert dynamic.status == measured.status == StepStatus.SUCCEEDED


@pytest.mark.parametrize("empty", [False, True])
def test_cancel_settles_queued_pending_without_cancelling_controller_future(
    tmp_path: Path,
    empty: bool,
) -> None:
    pipeline, runner = _pipeline(tmp_path)
    runner.release.clear()
    with pipeline:
        first = pipeline.submit(Transform, inputs={"structure": [structure(0)]})
        assert runner.entered.wait(5)
        pending = (
            pipeline.submit(Merge, inputs=[])
            if empty
            else pipeline.submit(DynamicCurator)
        )
        pipeline.cancel()
        runner.release.set()
        assert first.result().status == pending.result().status == StepStatus.CANCELLED
        assert not pending.cancelled_before_start
        assert runner.curator_launches == 0
        assert runner.curator_resolutions == 0


def test_submission_patches_keep_explicit_null_and_exact_field_intent() -> None:
    patch: dict[str, object] = {"interpreter": None}
    submission = freeze_submission(
        WaitTool, tool=patch, batch_strategy={"max_artifacts_per_unit": None}
    )
    patch["interpreter"] = "unexpected"
    plan = submission.compile()
    assert plan.recipe.execution.tool is not None
    assert plan.recipe.execution.tool.interpreter is None
    assert submission.batch_patch is not None
    assert submission.batch_patch.model_fields_set == {"max_artifacts_per_unit"}


@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("phase", ["launch", "resolve", "resolve_curator"])
def test_unknown_startup_owner_is_retained_after_future_and_close(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, empty: bool, phase: str
) -> None:
    pipeline, runner = _pipeline(tmp_path)

    class UnknownOwner:
        def ownership_state(self) -> OwnershipState:
            return OwnershipState.UNKNOWN

        def request_close(self, deadline: float) -> None:
            pass

    owner = UnknownOwner()

    def fail_startup(
        assignments: Sequence[AdmittedAssignment],
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> tuple[AssignmentHandle, ...]:
        message = "startup cannot establish closure"
        raise AssignmentLaunchError(
            message,
            handles=(),
            worker_owners=(owner,),
            cancellation=CancellationAcknowledgement(CancellationStatus.UNKNOWN),
        )

    def fail_resolution(*args: object, **kwargs: object) -> AssignmentSoftware:
        error = (
            CuratorLaunchError if phase == "resolve_curator" else AssignmentLaunchError
        )
        message = "software measurement cannot establish closure"
        raise error(
            message,
            handles=(),
            worker_owners=(owner,),
            cancellation=CancellationAcknowledgement(CancellationStatus.UNKNOWN),
        )

    monkeypatch.setattr(
        runner, phase, fail_startup if phase == "launch" else fail_resolution
    )
    first = (
        pipeline.run(DynamicCurator)
        if phase == "resolve_curator"
        else pipeline.run(Transform, inputs={"structure": [structure(0)]})
    )
    assert first.status == StepStatus.FAILED
    assert first.cancellation_status == CancellationStatus.UNKNOWN
    if phase != "launch":
        assert not pipeline.committer.journal.all_states()
    assert pipeline.ownership_state() == OwnershipState.UNKNOWN
    later = pipeline.run(Merge, inputs=[]) if empty else pipeline.run(DynamicCurator)
    assert later.status == StepStatus.FAILED
    assert runner.curator_launches == 0
    assert runner.curator_resolutions == 0
    with pytest.raises(CommandOwnershipError, match="ownership remains retained"):
        pipeline.close()
    assert pipeline.executions[0].worker_owners == (owner,)
    later_record = pipeline.executions[0].record.model_copy(
        update={"step_run_id": "f" * 32}
    )
    pipeline.tracker.create_attempt(later_record)
    pipeline.tracker.transition(
        later_record.step_run_id, StepStatus.PENDING, StepStatus.RUNNING
    )
    pipeline.tracker.transition(
        later_record.step_run_id,
        StepStatus.RUNNING,
        StepStatus.FAILED,
        result=StepResult(
            step_run_id=later_record.step_run_id,
            step_number=later_record.step_number,
            step_name=later_record.step_name,
            status=StepStatus.FAILED,
            error="a later closed failure",
        ),
    )
    latest = pipeline.tracker.load_current_states("synthetic-pipeline")
    assert latest[0].step_run_id == later_record.step_run_id
    with pytest.raises(
        PersistenceIntegrityError, match="unfinished pipeline ownership"
    ):
        PipelineManager(
            pipeline.committer,
            pipeline.dispatch,
            pipeline_run_id="synthetic-pipeline",
            default_step_runner=runner,
        )


def test_context_close_runs_when_cancellation_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline, _ = _pipeline(tmp_path)

    def fail_cancel() -> tuple[CancellationAcknowledgement, ...]:
        msg = "cancellation failed"
        raise RuntimeError(msg)

    monkeypatch.setattr(pipeline, "cancel", fail_cancel)
    with pytest.raises(ExceptionGroup) as caught, pipeline:
        msg = "body failed"
        raise ValueError(msg)
    assert [type(error) for error in caught.value.exceptions] == [
        ValueError,
        RuntimeError,
    ]
    with pytest.raises(RuntimeError, match="closed"):
        pipeline.submit(DynamicCurator)


def test_complete_recipe_rejects_credentials_before_publication() -> None:
    submission = freeze_submission(
        WaitTool,
        tool={
            "subcommand": "https://example.invalid/api?token=synthetic-private-token"
        },
    )
    with pytest.raises(ValueError, match="Scientific recipe overlaps"):
        submission.compile()


def test_nested_quoted_parameter_credentials_are_rejected() -> None:
    class SecretTransform(Transform):
        name = "pipeline_private_child"

        class Params(Transform.Params):
            api_key: str = Field(
                default='synthetic"quoted\\secret', description="Synthetic credential."
            )

        params: Params = Params()

    class SecretComposite(PredictMeasure):
        name = "pipeline_private_parent"

        def compose(self, ctx: CompositeContext) -> None:
            prediction = ctx.call(
                SecretTransform,
                name="predict",
                inputs={"structure": ctx.input("structure")},
            )
            metric = ctx.call(
                Measure,
                name="measure",
                inputs={"structure": prediction.output("structure")},
            )
            ctx.output("structure", prediction.output("structure"))
            ctx.output("metrics", metric.output("metrics"))

    with pytest.raises(ValueError, match="recipe parameters contain private"):
        freeze_submission(SecretComposite).compile()


def test_pre_worker_errors_use_the_submission_credential_recorder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline, runner = _pipeline(tmp_path)
    token = "synthetic-private-storage-token"
    pipeline.dispatch = AssignmentDispatch(
        pipeline.dispatch.delta_root,
        pipeline.dispatch.files_root,
        pipeline.dispatch.staging_root,
        StorageConfig(options={"token": token}),
        pipeline.dispatch.working_root,
    )

    def fail_lookup(
        operation: OperationDefinition, runner_resources: RunnerResources
    ) -> NativeCuratorSoftware:
        msg = f"provider lookup failed with {token}"
        raise RuntimeError(msg)

    monkeypatch.setattr(runner, "resolve_curator", fail_lookup)
    with pipeline:
        result = pipeline.run(DynamicCurator)
        assert result.status == StepStatus.FAILED
        state = pipeline.tracker.current_state(result.step_run_id)
        assert token not in state.model_dump_json()


@pytest.mark.parametrize("curator", [False, True])
@pytest.mark.parametrize("literal", [False, True])
def test_supplied_empty_inputs_skip_before_software_and_stop_later_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, curator: bool, literal: bool
) -> None:
    pipeline, runner = _pipeline(tmp_path)
    operation = Merge if curator else Transform
    inputs: PipelineInputs = [] if literal else {"structure": []}
    monkeypatch.setattr(
        "artisan.orchestration.engine.pipeline_inputs.register_raw_inputs",
        lambda *args, **kwargs: pytest.fail("Empty input registered files"),
    )
    with pipeline:
        result = pipeline.run(operation, inputs=inputs)
        assert result.status == StepStatus.SKIPPED
        assert result.metadata == {"skip_reason": "empty_inputs"}
        assert not read_current_step_outputs(pipeline.committer, result.step_run_id)
        later = pipeline.run(DynamicCurator)
        assert later.status == StepStatus.SKIPPED
        assert later.metadata == {"skip_reason": "pipeline_stopped"}
        assert runner.resolutions == runner.curator_resolutions == 0
        assert not runner.packs
        assert runner.curator_launches == 0
        assert all(not execution.handles for execution in pipeline.executions)


@pytest.mark.parametrize(
    "inputs",
    [None, {}, {"unknown": []}, {"structure": [], "unknown": []}],
)
def test_invalid_empty_input_contract_fails_instead_of_stopping_pipeline(
    tmp_path: Path, inputs: PipelineInputs
) -> None:
    pipeline, runner = _pipeline(tmp_path)
    with pipeline:
        with pytest.raises(ValueError, match="missing required or undeclared roles"):
            pipeline.run(Transform, inputs=inputs)
        assert not pipeline.tracker.load_current_states(pipeline.config.pipeline_run_id)
        assert runner.resolutions == 0
        assert pipeline.run(DynamicCurator).status == StepStatus.SUCCEEDED


class _PartialTransform(Transform):
    name = "pipeline_partial_transform"

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        if coordinate(inputs.input_artifacts["structure"][0]) == 1:
            msg = "synthetic partial failure"
            raise ValueError(msg)
        return super().preprocess(inputs)


def test_pipeline_whole_step_policy_run_submit_override_and_resume_audit(
    tmp_path: Path,
) -> None:
    pipeline, runner = _pipeline(tmp_path)
    inputs = {"structure": [structure(0), structure(1)]}
    packing = {"units_per_worker": 2, "max_workers": 1}
    with pipeline:
        original = pipeline.run(
            _PartialTransform, inputs=inputs, batch_strategy=packing
        )
        assert original.status == StepStatus.PARTIAL, original.error
        assert (
            pipeline.run(
                _PartialTransform, inputs=inputs, batch_strategy=packing
            ).status
            == StepStatus.PARTIAL
        )
    pack_count = len(runner.packs)
    with PipelineManager(
        pipeline.committer,
        pipeline.dispatch,
        pipeline_run_id="reused",
        default_step_runner=runner,
        cache_policy=CachePolicy.STEP_COMPLETED,
    ) as reused:
        hit = reused.submit(_PartialTransform, inputs=inputs).result()
        assert hit.status == StepStatus.PARTIAL
        assert hit.disposition == StepDisposition.CACHE_HIT
        assert hit.error == original.error
        assert len(runner.packs) == pack_count
        assert read_current_step_outputs(
            reused.committer, hit.step_run_id
        ) == read_current_step_outputs(pipeline.committer, original.step_run_id)
    with PipelineManager(
        pipeline.committer,
        pipeline.dispatch,
        pipeline_run_id="reused",
        default_step_runner=runner,
        cache_policy=CachePolicy.ALL_SUCCEEDED,
    ) as resumed:
        retry = resumed.run(_PartialTransform, inputs=inputs)
        assert retry.status == StepStatus.PARTIAL
        assert retry.disposition == StepDisposition.EXECUTED
        assert runner.packs[-1] == (1,)
        states = resumed.tracker.load_current_states("reused")
        assert [
            json.loads(state.compute_options_json)["cache_policy"] for state in states
        ] == ["step_completed", "all_succeeded"]
    with PipelineManager(
        pipeline.committer,
        pipeline.dispatch,
        pipeline_run_id="override",
        default_step_runner=runner,
        cache_policy=CachePolicy.ALL_SUCCEEDED,
    ) as override:
        hit = override.run(
            _PartialTransform, inputs=inputs, cache_policy=CachePolicy.STEP_COMPLETED
        )
        assert hit.disposition == StepDisposition.CACHE_HIT
        assert (
            json.loads(
                override.tracker.current_state(hit.step_run_id).compute_options_json
            )["cache_policy"]
            == "step_completed"
        )


class _EmptyStructure(Transform):
    name = "pipeline_empty_structure"
    outputs: ClassVar = {
        "structure": OutputSpec(
            artifact_type="csv", required=False, derives_from={"inputs": ["structure"]}
        )
    }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        return ArtifactResult(artifacts={"structure": []}, lineage={"structure": []})


class _EmptyMetric(Measure):
    name = "pipeline_empty_metric"
    outputs: ClassVar = {
        "metrics": OutputSpec(
            artifact_type="metric",
            required=False,
            derives_from={"inputs": ["structure"]},
        )
    }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        return ArtifactResult(artifacts={"metrics": []}, lineage={"metrics": []})


def test_optional_empty_output_does_not_stop_until_exact_consumer_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline, runner = _pipeline(tmp_path)
    with pipeline:
        source = pipeline.run(_EmptyStructure, inputs={"structure": [structure(0)]})
        assert source.status == StepStatus.SUCCEEDED
        assert source.succeeded_count == 1
        assert pipeline.run(DynamicCurator).status == StepStatus.SUCCEEDED
        calls = []

        def resolve(
            committer: RetainedCommitter, step_run_id: str
        ) -> tuple[AcceptedRole, ...]:
            calls.append(step_run_id)
            return read_current_step_outputs(committer, step_run_id)

        monkeypatch.setattr(
            "artisan.orchestration.engine.pipeline_inputs.read_current_step_outputs",
            resolve,
        )
        result = pipeline.run(
            Merge, inputs=[source.output("structure"), source.output("structure")]
        )
        assert result.status == StepStatus.SKIPPED
        assert calls == [source.step_run_id]
        assert runner.curator_resolutions == runner.curator_launches == 1


@pytest.mark.parametrize("declared", ["metric", "any"])
def test_empty_upstream_wrong_type_fails_before_empty_skip(
    tmp_path: Path, declared: str
) -> None:
    pipeline, runner = _pipeline(tmp_path)
    with pipeline:
        source = pipeline.run(_EmptyMetric, inputs={"structure": [structure(0)]})
        assert source.status == StepStatus.SUCCEEDED
        inputs = {
            "structure": OutputReference(
                source_step=source.step_number,
                role="metrics",
                artifact_type=declared,
            )
        }
        if declared == "metric":
            with pytest.raises(ValueError, match="incompatible artifact type"):
                pipeline.run(Transform, inputs=inputs)
            assert len(pipeline) == 1
        else:
            result = pipeline.run(Transform, inputs=inputs)
            assert result.status == StepStatus.FAILED
            assert "incompatible artifact type" in (result.error or "")
        assert runner.resolutions == 1
        assert pipeline.run(DynamicCurator).status == StepStatus.SUCCEEDED


class _PairedInputs(Transform):
    name = "pipeline_paired_inputs"

    class InputRole(StrEnum):
        structure = "structure"
        other = "other"

    inputs: ClassVar = {
        "structure": InputSpec(artifact_type="csv", materialize=False),
        "other": InputSpec(artifact_type="csv", materialize=False),
    }


class _OptionalHistory(SummarizeMetrics):
    name = "pipeline_optional_history"
    outputs: ClassVar = {
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["final"]}
        )
    }


def test_zero_groups_skip_without_stopping_and_mixed_whole_stream_runs(
    tmp_path: Path,
) -> None:
    pipeline, runner = _pipeline(tmp_path)
    with pipeline:
        skipped = pipeline.run(
            _PairedInputs,
            inputs={"structure": [structure(0)], "other": []},
            group_by=GroupByStrategy.CROSS_PRODUCT,
        )
        assert skipped.status == StepStatus.SKIPPED
        assert skipped.metadata["skip_reason"] == "no_outer_work"
        assert runner.resolutions == 0
        mixed = pipeline.run(
            _OptionalHistory, inputs={"final": [structure(1)], "history": []}
        )
        assert mixed.status == StepStatus.SUCCEEDED, mixed.error
        output = read_current_step_outputs(pipeline.committer, mixed.step_run_id)
        artifact = output[0].occurrences[0].artifact
        assert isinstance(artifact, MetricArtifact)
        assert artifact.values["history"] == []
        assert pipeline.run(DynamicCurator).status == StepStatus.SUCCEEDED


def test_all_empty_whole_stream_skips_before_scientific_invocation(
    tmp_path: Path,
) -> None:
    pipeline, runner = _pipeline(tmp_path)
    observed = len(SummarizeMetrics.observed)
    with pipeline:
        result = pipeline.run(SummarizeMetrics, inputs={"final": [], "history": []})
        assert result.status == StepStatus.SKIPPED
        assert result.metadata["skip_reason"] == "empty_inputs"
        assert len(SummarizeMetrics.observed) == observed
        assert runner.resolutions == 0
        assert (
            pipeline.run(DynamicCurator).metadata["skip_reason"] == "pipeline_stopped"
        )


def test_empty_curator_source_reference_type_is_verified_before_skip(
    tmp_path: Path,
) -> None:
    pipeline, runner = _pipeline(tmp_path)
    with pipeline:
        source = pipeline.run(_EmptyStructure, inputs={"structure": [structure(0)]})
        result = pipeline.run(
            Merge,
            inputs=[
                OutputReference(
                    source_step=source.step_number,
                    role="structure",
                    artifact_type="metric",
                )
            ],
        )
        assert result.status == StepStatus.FAILED
        assert "reference type differs" in (result.error or "")
        assert runner.curator_resolutions == 0
        assert pipeline.run(DynamicCurator).status == StepStatus.SUCCEEDED


def test_curator_without_supplied_boundary_still_executes_empty_contract(
    tmp_path: Path,
) -> None:
    pipeline, runner = _pipeline(tmp_path)
    with pipeline:
        result = pipeline.run(Merge)
        assert result.status == StepStatus.FAILED
        assert result.error is not None
        assert runner.curator_resolutions == runner.curator_launches == 1
        assert pipeline.run(DynamicCurator).status == StepStatus.SUCCEEDED


def test_empty_consumer_stops_already_queued_independent_work(tmp_path: Path) -> None:
    pipeline, runner = _pipeline(tmp_path)
    runner.release.clear()
    with pipeline:
        source = pipeline.submit(_EmptyStructure, inputs={"structure": [structure(0)]})
        assert runner.entered.wait(5)
        consumer = pipeline.submit(
            Transform, inputs={"structure": source.output("structure")}
        )
        later = pipeline.submit(DynamicCurator)
        assert consumer.status == later.status == StepStatus.PENDING
        runner.release.set()
        assert source.result().status == StepStatus.SUCCEEDED
        assert consumer.result().metadata["skip_reason"] == "empty_inputs"
        assert later.result().metadata["skip_reason"] == "pipeline_stopped"
        assert runner.resolutions == 1
        assert runner.curator_resolutions == 0


def test_pipeline_cancel_before_empty_skip_wins_while_prior_cancel_is_blocked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline, _ = _pipeline(tmp_path)
    resolved, release_resolution = threading.Event(), threading.Event()
    cancelling, release_cancel = threading.Event(), threading.Event()
    original_resolve = pipeline_module.resolve_curator_inputs

    def pause_resolved(
        inputs: FrozenPipelineInputs, **kwargs: Any
    ) -> dict[str, tuple[AcceptedArtifact | str, ...]]:
        values = original_resolve(inputs, **kwargs)
        resolved.set()
        assert release_resolution.wait(10)
        return values

    with pipeline:
        source = pipeline.run(_EmptyStructure, inputs={"structure": [structure(0)]})
        original_cancel = pipeline.executions[0].cancel

        def pause_cancel() -> CancellationAcknowledgement:
            cancelling.set()
            assert release_cancel.wait(10)
            return original_cancel()

        monkeypatch.setattr(pipeline.executions[0], "cancel", pause_cancel)
        monkeypatch.setattr(pipeline_module, "resolve_curator_inputs", pause_resolved)
        consumer = pipeline.submit(Merge, inputs=[source.output("structure")])
        assert resolved.wait(5)
        with ThreadPoolExecutor() as pool:
            cancellation = pool.submit(pipeline.cancel)
            try:
                assert cancelling.wait(5)
                release_resolution.set()
                result = consumer.result(timeout=10)
                assert result.status == StepStatus.CANCELLED
                assert "skip_reason" not in result.metadata
            finally:
                release_resolution.set()
                release_cancel.set()
            cancellation.result(timeout=10)
