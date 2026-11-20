"""Submission freezes child defaults before exposing its asynchronous handle."""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

import pytest
from fixtures.fixed_default_operations import DefaultOffset, NestedDefaultPair
from fixtures.input_contract_operations import metric
from fixtures.retained_pipeline import NativePipelineRunner

from artisan.composites import CompositeContext
from artisan.orchestration import PipelineManager
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.schemas import ArtifactResult, InputSpec, OutputSpec
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.schemas.specs.input_models import PostprocessInput


class _EmptyOffset(DefaultOffset):
    name = "empty_fixed_offset"
    outputs: ClassVar = {
        "metrics": OutputSpec(
            artifact_type="metric", required=False, derives_from={"inputs": ["source"]}
        )
    }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        return ArtifactResult(artifacts={"metrics": []}, lineage={"metrics": []})


def test_submit_captures_child_defaults_before_deferred_software_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with PipelineManager.create(
        "fixed-inputs",
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    ) as pipeline:
        runner = NativePipelineRunner(pipeline.committer)
        runner.release.clear()
        future = pipeline.submit(
            NestedDefaultPair, inputs={"source": [metric(1)]}, step_runner=runner
        )
        try:
            assert runner.entered.wait(5)
            monkeypatch.setattr(
                DefaultOffset,
                "inputs",
                {
                    **DefaultOffset.inputs,
                    "offset": InputSpec(
                        artifact_type="metric", materialize=False, default=(metric(90),)
                    ),
                },
            )
        finally:
            runner.release.set()
        result = future.result(timeout=30)
        assert result.status == StepStatus.SUCCEEDED, result.error
        outputs = read_current_step_outputs(pipeline.committer, result.step_run_id)
        assert [item.artifact.values["value"] for item in outputs[0].occurrences] == [
            11,
            21,
            11,
        ]


def test_pipeline_explicit_empty_suppresses_default_and_top_level_empty_skips(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with PipelineManager.create(
        "empty-fixed-inputs",
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    ) as pipeline:
        runner = NativePipelineRunner(pipeline.committer)
        result = pipeline.run(
            DefaultOffset,
            inputs={"source": [metric(1)], "offset": []},
            step_runner=runner,
        )
        assert result.status == StepStatus.FAILED
        assert "exactly one occurrence" in str(result.error)

        def forbidden_compose(self: NestedDefaultPair, ctx: CompositeContext) -> None:
            pytest.fail("Explicit no-work must not compile child defaults")

        monkeypatch.setattr(NestedDefaultPair, "compose", forbidden_compose)
        empty = pipeline.run(NestedDefaultPair, inputs=[], step_runner=runner)
        assert empty.status == StepStatus.SKIPPED
        assert empty.total_count == 0


def test_empty_output_reference_suppresses_default_before_dispatch(
    tmp_path: Path,
) -> None:
    with PipelineManager.create(
        "empty-reference-default",
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    ) as pipeline:
        runner = NativePipelineRunner(pipeline.committer)
        produced = pipeline.run(
            _EmptyOffset, inputs={"source": [metric(1)]}, step_runner=runner
        )
        assert produced.status == StepStatus.SUCCEEDED, produced.error
        resolutions = runner.resolutions
        rejected = pipeline.run(
            DefaultOffset,
            inputs={"source": [metric(1)], "offset": produced.output("metrics")},
            step_runner=runner,
        )
        assert rejected.status == StepStatus.FAILED
        assert "exactly one occurrence" in str(rejected.error)
        assert runner.resolutions == resolutions
