"""Public submission retains one controller under its exact manager ownership."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.composition_operations import Transform, structure
from fixtures.retained_pipeline import NativePipelineRunner

from artisan.orchestration import PipelineManager
from artisan.schemas.orchestration.step_lifecycle import StepStatus


def _pipeline(root: Path) -> PipelineManager:
    return PipelineManager.create(
        name="public-submission",
        delta_root=str(root / "submission-delta"),
        staging_root=str(root / "staging"),
        working_root=str(root / "work"),
    )


def test_definition_keyword_and_positional_submit_share_one_controller(
    tmp_path: Path,
) -> None:
    with _pipeline(tmp_path) as pipeline:
        runner = NativePipelineRunner(pipeline.committer)
        first = pipeline.run(
            definition=Transform,
            inputs={"structure": [structure(0)]},
            step_runner=runner,
            name="first",
        )
        second = pipeline.submit(
            Transform,
            inputs={"structure": first.output("structure")},
            step_runner=runner,
            name="second",
        ).result()
        assert first.status == second.status == StepStatus.SUCCEEDED
        assert len(pipeline.executions) == len(pipeline) == 2
        assert pipeline.output("second", "structure") == second.output("structure")


def test_submission_helper_rejects_duplicate_or_foreign_controller(
    tmp_path: Path,
) -> None:
    with (
        _pipeline(tmp_path / "first") as first,
        _pipeline(tmp_path / "second") as second,
    ):
        first.run(
            Transform,
            inputs={"structure": [structure(0)]},
            step_runner=NativePipelineRunner(first.committer),
        )
        execution = first.executions[0]
        with pytest.raises(ValueError, match="already has"):
            first._submit_execution(execution)
        with pytest.raises(ValueError, match="another pipeline owner"):
            second._submit_execution(execution)
        assert len(first.executions) == 1
        assert not second.executions
        assert second.current_step == 0
