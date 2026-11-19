"""Explicit contracts survive admission, native execution, cache and replay."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.input_contract_operations import (
    AnchoredOffset,
    OffsetGenerator,
    SharedOffset,
    metric,
    named_metric,
)

from artisan.orchestration import replay_execution
from artisan.orchestration.engine.retained_execution_query import (
    read_retained_execution_membership,
)
from artisan.orchestration.retained_replay import RetainedReplayPreparation
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.storage.core.execution_source import (
    ScientificExecutionSource,
    read_execution_source,
)
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.retained_commit import RetainedCommitter

from .conftest import _SerialNativePipelineRunner, create_native_pipeline

pytestmark = pytest.mark.integration


def test_shared_input_and_optional_parents_survive_retained_cache_and_replay(
    tmp_path: Path,
) -> None:
    with create_native_pipeline(
        name="contracts",
        delta_root=tmp_path / "delta",
        staging_root=tmp_path / "staging",
        working_root=tmp_path / "work",
    ) as pipeline:
        generated = pipeline.submit(OffsetGenerator)
        inputs = {
            "source": [metric(0), metric(1), metric(0)],
            "offset": generated.output("offset"),
            "context": [metric(20), metric(21), metric(22)],
        }
        result = pipeline.run(SharedOffset, inputs=inputs)
        assert result.status == StepStatus.SUCCEEDED, result.error
        assert result.total_count == 2
        sources = [
            item.source
            for item in read_retained_execution_membership(
                pipeline.committer, pipeline_run_id=pipeline.config.pipeline_run_id
            )
            if item.state.step_run_id == result.step_run_id
        ]
        assert len(sources) == 2
        assert all(isinstance(source, ScientificExecutionSource) for source in sources)
        original = sources[0]
        assert isinstance(original, ScientificExecutionSource)
        reader = RetainedReader(pipeline.committer)
        for source in sources:
            assert isinstance(source, ScientificExecutionSource)
            assert source.admission.recipe.schema_version == 2
            assert len(source.admission.boundary.input_roles()["offset"]) == 1
            accepted = reader.read(source.plan.receipt)
            for item in accepted.output("metrics"):
                parents = [
                    edge.source
                    for edge in accepted.projection.parents
                    if edge.target == item.reference.address
                ]
                assert [
                    parent.index for parent in parents if parent.role == "offset"
                ] == [0]
        cached = pipeline.run(SharedOffset, inputs=inputs)
        assert cached.status == StepStatus.SUCCEEDED, cached.error
        assert cached.disposition == StepDisposition.CACHE_HIT
        changed = pipeline.run(SharedOffset, inputs={**inputs, "offset": [metric(11)]})
        assert changed.status == StepStatus.SUCCEEDED, changed.error
        assert changed.disposition != StepDisposition.CACHE_HIT
    diagnostic = replay_execution(
        original.plan.receipt.delivery.delivery_id,
        delta_root=pipeline.committer.delta_root,
        operation=SharedOffset,
        step_runner=_SerialNativePipelineRunner(pipeline.committer),
        working_root=tmp_path / "replay-work",
        staging_root=str(tmp_path / "replay-staging"),
    )
    assert diagnostic.step_result.status == StepStatus.SUCCEEDED, (
        diagnostic.step_result.error
    )
    selected = read_execution_source(pipeline.committer, diagnostic.execution_run_id)
    assert isinstance(selected, ScientificExecutionSource)
    assert selected.admission.recipe == original.admission.recipe
    assert len(selected.admission.boundary.input_roles()["offset"]) == 1
    assert [
        item.artifact.artifact_id
        for item in reader.read(selected.plan.receipt).output("metrics")
    ] == [
        item.artifact.artifact_id
        for item in reader.read(original.plan.receipt).output("metrics")
    ]


def test_output_reference_shared_cardinality_fails_before_dispatch(
    tmp_path: Path,
) -> None:
    with create_native_pipeline(
        name="cardinality",
        delta_root=tmp_path / "delta",
        staging_root=tmp_path / "staging",
        working_root=tmp_path / "work",
    ) as pipeline:
        # A selected result reference may resolve to several ordinary occurrences.
        produced = pipeline.run(
            SharedOffset,
            inputs={"source": [metric(0), metric(1)], "offset": [metric(10)]},
        )
        assert produced.status == StepStatus.SUCCEEDED, produced.error
        rejected = pipeline.run(
            SharedOffset, inputs={"source": [], "offset": produced.output("metrics")}
        )
        assert rejected.status == StepStatus.FAILED
        assert "exactly one occurrence" in str(rejected.error)
        skipped = pipeline.run(
            SharedOffset, inputs={"source": [], "offset": [metric(10)]}
        )
        assert skipped.status == StepStatus.SKIPPED
        assert skipped.total_count == 0


def test_six_query_sparse_unit_keeps_original_selection_across_cache_and_replays(
    tmp_path: Path,
) -> None:
    with create_native_pipeline(
        name="anchored-contracts",
        delta_root=tmp_path / "delta",
        staging_root=tmp_path / "staging",
        working_root=tmp_path / "work",
    ) as pipeline:
        inputs = {
            "source": [
                named_metric(name, index) for index, name in enumerate("ABCDEF")
            ],
            "offset": [metric(10)],
            "context": [named_metric("F", 20)],
        }
        result = pipeline.run(AnchoredOffset, inputs=inputs)
        assert result.status == StepStatus.SUCCEEDED, result.error
        assert result.total_count == 2
        sources = [
            item.source
            for item in read_retained_execution_membership(
                pipeline.committer, pipeline_run_id=pipeline.config.pipeline_run_id
            )
            if item.state.step_run_id == result.step_run_id
        ]
        original = next(
            source
            for source in sources
            if isinstance(source, ScientificExecutionSource)
            and source.plan.admission.attempt.assignment.unit_index == 0
        )
        assert isinstance(original, ScientificExecutionSource)
        assert original.admission.boundary.input_roles()["context"] == ()
        assert len(original.admission.boundary.input_roles()["source"]) == 5
        cached = pipeline.run(AnchoredOffset, inputs=inputs)
        assert cached.status == StepStatus.SUCCEEDED, cached.error
        assert cached.disposition == StepDisposition.CACHE_HIT
        rejected = pipeline.run(AnchoredOffset, inputs={**inputs, "context": []})
        assert rejected.status == StepStatus.FAILED
        assert "Complete selection" in str(rejected.error)
        assert rejected.disposition != StepDisposition.CACHE_HIT
    previous = original.plan.receipt.delivery.delivery_id
    from artisan.composites.base.compiler import compile_definition
    from artisan.orchestration.original_selection import resolve_original_selection

    for index in range(2):
        diagnostic = replay_execution(
            previous,
            delta_root=pipeline.committer.delta_root,
            operation=AnchoredOffset,
            step_runner=_SerialNativePipelineRunner(pipeline.committer),
            working_root=tmp_path / f"replay-work-{index}",
            staging_root=str(tmp_path / f"replay-staging-{index}"),
        )
        assert diagnostic.step_result.status == StepStatus.SUCCEEDED, (
            diagnostic.step_result.error
        )
        selected = read_execution_source(
            pipeline.committer, diagnostic.execution_run_id
        )
        assert isinstance(selected, ScientificExecutionSource)
        assert selected.admission.boundary.input_roles()["context"] == ()
        assert selected.plan.admission.attempt.replay_of_execution_run_id == previous
        proof = resolve_original_selection(
            pipeline.committer,
            diagnostic.execution_run_id,
            compile_definition(AnchoredOffset).recipe,
        )
        assert proof.origin_step.step_run_id == result.step_run_id
        assert len(proof.origin_step.assignments) == 2
        assert len(proof.selected_admissions) == index + 2
        previous = diagnostic.execution_run_id


def _prepare_anchored_replay(
    committer: RetainedCommitter,
    source: ScientificExecutionSource,
    definition: type[AnchoredOffset] = AnchoredOffset,
    *,
    allow_code_change: bool = False,
) -> RetainedReplayPreparation:
    from artisan.orchestration.retained_replay import prepare_retained_replay

    return prepare_retained_replay(
        committer,
        source.plan.receipt.delivery.delivery_id,
        registered_definitions={
            (AnchoredOffset.__module__, AnchoredOffset.__qualname__): definition
        },
        runtime_replacements={},
        allow_code_change=allow_code_change,
    )


def test_changed_code_rechecks_global_requirements_and_selected_group_bound(
    tmp_path: Path,
) -> None:
    from pydantic import model_validator

    from artisan.errors import StoreIntegrityError
    from artisan.schemas import AnchoredNameGrouping, BatchStrategy

    class RequiresContext(AnchoredOffset):
        name = "contract_updated_context_requirement"

        @model_validator(mode="after")
        def _require_current_context(self) -> RequiresContext:
            self.required_nonempty_inputs = ("context",)
            return self

    class SmallerBound(AnchoredOffset):
        name = "contract_smaller_group_bound"
        batch_strategy: BatchStrategy = BatchStrategy(
            artifacts_per_unit=2, max_artifacts_per_unit=2
        )

    class ChangedAnchor(AnchoredOffset):
        name = "contract_changed_anchor"
        group_by: AnchoredNameGrouping = AnchoredNameGrouping(
            primary_role="source", matched_roles=("offset", "context")
        )
        shared_input_roles = ()

    with create_native_pipeline(
        name="changed-anchored-contracts",
        delta_root=tmp_path / "delta",
        staging_root=tmp_path / "staging",
        working_root=tmp_path / "work",
    ) as pipeline:
        inputs = {
            "source": [
                named_metric(name, index) for index, name in enumerate("ABCDEF")
            ],
            "offset": [metric(10)],
        }
        without = pipeline.run(
            AnchoredOffset, params={"require_context": False}, inputs=inputs
        )
        assert without.status == StepStatus.SUCCEEDED, without.error
        with_context = pipeline.run(
            AnchoredOffset,
            params={"require_context": False},
            inputs={**inputs, "context": [named_metric("F", 20)]},
        )
        assert with_context.status == StepStatus.SUCCEEDED, with_context.error
        sources = {
            item.state.step_run_id: item.source
            for item in read_retained_execution_membership(
                pipeline.committer, pipeline_run_id=pipeline.config.pipeline_run_id
            )
            if isinstance(item.source, ScientificExecutionSource)
            and item.source.plan.admission.attempt.assignment.unit_index == 0
        }
        missing = sources[without.step_run_id]
        present = sources[with_context.step_run_id]
        with pytest.raises(StoreIntegrityError, match="Original complete selection"):
            _prepare_anchored_replay(
                pipeline.committer, missing, RequiresContext, allow_code_change=True
            )
        accepted = _prepare_anchored_replay(
            pipeline.committer, present, RequiresContext, allow_code_change=True
        )
        assert accepted.plan.recipe.policy.required_nonempty_inputs == ("context",)
        assert accepted.boundary.input_roles()["context"] == ()
        with pytest.raises(StoreIntegrityError, match="group-count bound"):
            _prepare_anchored_replay(
                pipeline.committer, present, SmallerBound, allow_code_change=True
            )
        with pytest.raises(StoreIntegrityError, match="grouping or shared"):
            _prepare_anchored_replay(
                pipeline.committer, present, ChangedAnchor, allow_code_change=True
            )


def test_replay_rechecks_sibling_origin_admission_before_publication(
    tmp_path: Path,
) -> None:
    from artisan.errors import StoreIntegrityError
    from artisan.storage.io.admission import admission_directory

    with create_native_pipeline(
        name="origin-recheck",
        delta_root=tmp_path / "delta",
        staging_root=tmp_path / "staging",
        working_root=tmp_path / "work",
    ) as pipeline:
        result = pipeline.run(
            AnchoredOffset,
            inputs={
                "source": [
                    named_metric(name, index) for index, name in enumerate("ABCDEF")
                ],
                "offset": [metric(10)],
                "context": [named_metric("F", 20)],
            },
        )
        assert result.status == StepStatus.SUCCEEDED, result.error
        sources = [
            item.source
            for item in read_retained_execution_membership(
                pipeline.committer, pipeline_run_id=pipeline.config.pipeline_run_id
            )
            if item.state.step_run_id == result.step_run_id
        ]
        original = next(
            source
            for source in sources
            if isinstance(source, ScientificExecutionSource)
            and source.plan.admission.attempt.assignment.unit_index == 0
        )
        prepared = _prepare_anchored_replay(pipeline.committer, original)
        assert prepared.original_selection is not None
        sibling = prepared.original_selection.origin_step.assignments[1]
        path = (
            admission_directory(pipeline.committer.delta_root, sibling.attempt)
            + "/admission.json"
        )
        with pipeline.committer.fs.open(path, "ab") as stream:
            stream.write(b" ")
        with pytest.raises(StoreIntegrityError, match="digest changed"):
            prepared.validate_selection(
                pipeline.committer, prepared.delivery.attempt.software_digest
            )
        assert pipeline.committer.journal.state(prepared.identity) is None


def test_invalid_output_keeps_anchored_unit_atomic_and_failed_source_replayable(
    tmp_path: Path,
) -> None:
    from fixtures.input_contract_operations import InvalidAnchoredOffset

    from artisan.orchestration.retained_replay import prepare_retained_replay
    from artisan.storage.core.execution_source import FailedScientificExecutionSource

    with create_native_pipeline(
        name="invalid-anchored-output",
        delta_root=tmp_path / "delta",
        staging_root=tmp_path / "staging",
        working_root=tmp_path / "work",
    ) as pipeline:
        result = pipeline.run(
            InvalidAnchoredOffset,
            inputs={
                "source": [named_metric("A", 0), named_metric("B", 1)],
                "offset": [metric(10)],
                "context": [named_metric("B", 20)],
            },
        )
        assert result.status == StepStatus.FAILED
        assert result.failed_count == 1
        assert RetainedReader(pipeline.committer).completed_plans() == ()
        members = read_retained_execution_membership(
            pipeline.committer, pipeline_run_id=pipeline.config.pipeline_run_id
        )
        assert len(members) == 1
        source = members[0].source
        assert isinstance(source, FailedScientificExecutionSource)
        assert source.unit.failed_delivery is not None
        prepared = prepare_retained_replay(
            pipeline.committer,
            source.unit.failed_delivery.delivery_id,
            registered_definitions={
                (
                    InvalidAnchoredOffset.__module__,
                    InvalidAnchoredOffset.__qualname__,
                ): InvalidAnchoredOffset
            },
            runtime_replacements={},
        )
        assert prepared.original_selection is not None
        assert prepared.original_selection.origin_step.step_run_id == result.step_run_id
        assert len(prepared.boundary.input_roles()["source"]) == 2


def test_cancellation_before_selection_cannot_publish_part_of_an_anchored_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from artisan.schemas.execution.assignment import SealedDelivery
    from artisan.storage.io.retained_plan import RetainedCommitPlan

    with create_native_pipeline(
        name="cancel-anchored-output",
        delta_root=tmp_path / "delta",
        staging_root=tmp_path / "staging",
        working_root=tmp_path / "work",
    ) as pipeline:
        accepted = pipeline.committer.accept
        receipts: list[SealedDelivery] = []

        def cancel(
            receipt: SealedDelivery, *, preserve_staging: bool = False
        ) -> RetainedCommitPlan:
            receipts.append(receipt)
            pipeline.cancel()
            return accepted(receipt, preserve_staging=preserve_staging)

        monkeypatch.setattr(pipeline.committer, "accept", cancel)
        result = pipeline.run(
            AnchoredOffset,
            inputs={
                "source": [named_metric("A", 0), named_metric("A", 1)],
                "offset": [metric(10)],
                "context": [named_metric("A", 20)],
            },
        )
        assert result.status == StepStatus.CANCELLED
        assert len(receipts) == 1
        assert RetainedReader(pipeline.committer).completed_plans() == ()
        decision = pipeline.committer.journal.state(
            receipts[0].delivery.attempt.assignment
        )
        assert decision is not None
        assert decision.cancelled
        assert decision.winner is None
        assert (
            read_execution_source(pipeline.committer, receipts[0].delivery.delivery_id)
            is None
        )
