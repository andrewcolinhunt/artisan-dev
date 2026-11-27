"""Step handles retain exact accepted sources across runs in one store."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from fixtures.composition_operations import (
    PredictMeasure,
    Transform,
    coordinate,
    structure,
)
from fixtures.retained_pipeline import NativePipelineRunner

from artisan.composites.base.compiler import Definition
from artisan.orchestration import PipelineManager, StepStatus
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepDisposition,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.utils.command_supervisor import CommandOwnershipError


def _pipeline(root: Path, name: str) -> PipelineManager:
    pipeline = PipelineManager.create(
        name,
        str(root / "delta"),
        str(root / "staging"),
        working_root=str(root / "work"),
    )
    pipeline.runner = NativePipelineRunner(pipeline.committer)
    return pipeline


def _coordinates(pipeline: PipelineManager, result: StepResult) -> list[int]:
    assert result.status is StepStatus.SUCCEEDED, result.error
    assert result.step_run_id is not None
    return [
        coordinate(item.artifact)
        for role in read_current_step_outputs(pipeline.committer, result.step_run_id)
        if role.role == "structure"
        for item in role.occurrences
    ]


@pytest.mark.parametrize("definition", [Transform, PredictMeasure])
def test_cross_run_reference_selects_its_source_and_preserves_duplicates(
    tmp_path: Path, definition: Definition
) -> None:
    with (
        _pipeline(tmp_path, "producer") as producer,
        _pipeline(tmp_path, "consumer") as consumer,
    ):
        future = producer.submit(definition, inputs={"structure": [structure(10)]})
        reference = future.output("structure")
        first = future.result()
        other = consumer.run(Transform, inputs={"structure": [structure(100)]})
        assert first.step_number == other.step_number == 0
        assert reference == first.output("structure")
        assert reference.source.step_run_id != other.step_run_id
        restored = OutputReference.model_validate_json(reference.model_dump_json())
        selected = consumer.run(
            Transform,
            inputs={"structure": [restored, restored]},
            batch_strategy={"units_per_worker": 2},
        )
        assert _coordinates(consumer, selected) == [12, 12]
        assert producer[0].output("structure") == reference
        assert (
            consumer.get_step(
                0, pipeline_run_id=producer.config.pipeline_run_id
            ).output("structure")
            == reference
        )


def test_saved_result_lookup_uses_closed_run_and_current_cache_attempt(
    tmp_path: Path,
) -> None:
    with _pipeline(tmp_path, "producer") as producer:
        original = producer.run(Transform, inputs={"structure": [structure(10)]})
        reused = producer.run(Transform, inputs={"structure": [structure(10)]})
        assert reused.disposition is StepDisposition.CACHE_HIT
        assert reused.output("structure").source.step_run_id == reused.step_run_id
        assert reused.step_run_id != original.step_run_id
        run_id = producer.config.pipeline_run_id
    with _pipeline(tmp_path, "consumer") as consumer:
        saved = consumer.get_step(1, pipeline_run_id=run_id)
        assert saved.output("structure") == reused.output("structure")
        result = consumer.run(
            Transform, inputs={"structure": saved.output("structure")}
        )
        assert _coordinates(consumer, result) == [12]


def test_later_failed_attempt_does_not_retarget_an_existing_reference(
    tmp_path: Path,
) -> None:
    with (
        _pipeline(tmp_path, "producer") as producer,
        _pipeline(tmp_path, "consumer") as consumer,
    ):
        original = producer.run(Transform, inputs={"structure": [structure(10)]})
        reference = producer.output(Transform.name, "structure")
        replacement = producer.executions[0].record.model_copy(
            update={"step_run_id": uuid4().hex}
        )
        producer.tracker.create_attempt(replacement)
        with pytest.raises(ValueError, match="terminal|ready"):
            consumer.get_step(0, pipeline_run_id=producer.config.pipeline_run_id)
        failed = StepResult(
            step_run_id=replacement.step_run_id,
            step_number=0,
            step_name=replacement.step_name,
            status=StepStatus.FAILED,
            error="failed retry",
        )
        producer.tracker.transition(
            replacement.step_run_id, StepStatus.PENDING, StepStatus.RUNNING
        )
        producer.tracker.transition(
            replacement.step_run_id,
            StepStatus.RUNNING,
            StepStatus.FAILED,
            result=failed,
        )
        latest = consumer.get_step(0, pipeline_run_id=producer.config.pipeline_run_id)
        assert latest.status is StepStatus.FAILED
        assert latest.step_run_id == replacement.step_run_id
        with pytest.raises(ValueError, match="Output role"):
            latest.output("structure")
        assert reference.source.step_run_id == original.step_run_id
        result = consumer.run(Transform, inputs={"structure": reference})
        assert _coordinates(consumer, result) == [12]


def test_foreign_pending_source_rejects_before_submission_then_can_be_reused(
    tmp_path: Path,
) -> None:
    with (
        _pipeline(tmp_path, "producer") as producer,
        _pipeline(tmp_path, "consumer") as consumer,
    ):
        runner = producer.runner
        assert isinstance(runner, NativePipelineRunner)
        runner.release.clear()
        try:
            future = producer.submit(Transform, inputs={"structure": [structure(10)]})
            assert runner.entered.wait(5)
            reference = future.output("structure")
            with pytest.raises(ValueError, match="pending|ready|terminal"):
                consumer.submit(Transform, inputs={"structure": reference})
            assert consumer.current_step == 0
            assert not consumer.executions
        finally:
            runner.release.set()
        assert future.result(10).status is StepStatus.SUCCEEDED
        result = consumer.run(Transform, inputs={"structure": reference})
        assert _coordinates(consumer, result) == [12]


def test_different_store_rejects_reference_before_submission(tmp_path: Path) -> None:
    with (
        _pipeline(tmp_path / "one", "same-name") as producer,
        _pipeline(tmp_path / "two", "same-name") as consumer,
    ):
        source = producer.run(Transform, inputs={"structure": [structure(10)]})
        with pytest.raises(ValueError, match="store"):
            consumer.submit(Transform, inputs={"structure": source.output("structure")})
        assert consumer.current_step == 0
        assert not consumer.executions


def test_unknown_foreign_cleanup_cannot_become_empty_inputs(tmp_path: Path) -> None:
    with (
        _pipeline(tmp_path, "producer") as producer,
        _pipeline(tmp_path, "consumer") as consumer,
    ):
        producer.run(Transform, inputs={"structure": [structure(10)]})
        record = producer.executions[0].record.model_copy(
            update={"step_run_id": uuid4().hex, "step_number": 1}
        )
        producer.tracker.create_attempt(record)
        reference = producer.output(Transform.name, "structure", step_number=1)
        producer.tracker.transition(
            record.step_run_id, StepStatus.PENDING, StepStatus.RUNNING
        )
        for status in (CancellationStatus.REQUESTED, CancellationStatus.UNKNOWN):
            producer.tracker.record_cancellation(
                record.step_run_id,
                StepStatus.RUNNING,
                CancellationAcknowledgement(status),
            )
        producer.tracker.transition(
            record.step_run_id,
            StepStatus.RUNNING,
            StepStatus.FAILED,
            result=StepResult(
                step_run_id=record.step_run_id,
                step_name=record.step_name,
                step_number=1,
                status=StepStatus.FAILED,
                cancellation_status=CancellationStatus.UNKNOWN,
                error="worker closure unknown",
            ),
        )
        with pytest.raises(CommandOwnershipError):
            consumer.submit(Transform, inputs={"structure": reference})
        assert consumer.current_step == 0


def test_saved_lookup_rejects_invalid_and_missing_selections(tmp_path: Path) -> None:
    with _pipeline(tmp_path, "consumer") as consumer:
        for number, run_id in ((-1, None), (0, ""), (0, "missing")):
            with pytest.raises(ValueError):
                consumer.get_step(number, pipeline_run_id=run_id)
        assert consumer.current_step == 0


def test_local_store_alias_can_reuse_a_reference(tmp_path: Path) -> None:
    root = tmp_path / "original"
    root.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(root, target_is_directory=True)
    with _pipeline(root, "producer") as producer:
        source = producer.run(Transform, inputs={"structure": [structure(10)]})
        reference = source.output("structure")
    with _pipeline(alias, "consumer") as consumer:
        assert consumer.committer.store_identity == reference.source.store
        result = consumer.run(Transform, inputs={"structure": reference})
        assert _coordinates(consumer, result) == [12]
