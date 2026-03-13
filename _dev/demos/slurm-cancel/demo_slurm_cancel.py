#!/usr/bin/env python
"""Demo: SLURM array job cancellation.

Submits a pipeline with a SLURM array job (multiple tasks in one sbatch)
and lets you observe what happens when you press Ctrl+C.

Usage:
    ~/.pixi/bin/pixi run python _dev/demos/slurm-cancel/demo_slurm_cancel.py

What happens:
    - Step 0 generates 8 datasets locally (fast, ~1s)
    - Step 1 transforms all 8 via SLURM as a single array job (8 tasks)
      Each task sleeps 60s to simulate slow computation
    - Press Ctrl+C while the array job is running
    - Observe: escalating signal handler, BrokenProcessPool handling,
      and how the pipeline reports results

What to look for:
    - `squeue -u $USER` shows the array job (e.g. 12345_[0-7])
    - First Ctrl+C: "cancelling" message, cancel event set
    - The pipeline waits for the current flow to finish collecting results
    - Since we don't auto-scancel yet, SLURM tasks keep running
    - Run `scancel <job_id>` manually to see fast cleanup via parallel
      result collection
    - Second Ctrl+C: "restoring default handlers" warning
    - Third Ctrl+C: force-kills the process

Requires: SLURM cluster access.
"""

from __future__ import annotations

import time
from enum import StrEnum, auto
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.examples import DataGenerator
from artisan.orchestration import Backend, PipelineManager
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.utils import tutorial_setup


class SlowTransform(OperationDefinition):
    """Transform that sleeps to simulate slow computation.

    Each execution unit (one per input artifact) sleeps for `delay` seconds,
    then copies the input file to the output directory. This creates one
    SLURM array task per input artifact.
    """

    name = "slow_transform"
    description = "Sleep then copy input — simulates slow SLURM work"

    class InputRole(StrEnum):
        dataset = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.dataset: InputSpec(
            artifact_type="data",
            required=True,
        ),
    }

    class OutputRole(StrEnum):
        dataset = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.dataset: OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": ["dataset"]},
        ),
    }

    class Params(BaseModel):
        delay: float = Field(
            default=60.0,
            ge=0.0,
            description="Seconds to sleep per task (simulates slow work)",
        )

    params: Params = Params()
    resources: ResourceConfig = ResourceConfig(time_limit="00:10:00")
    execution: ExecutionConfig = ExecutionConfig(job_name="slow_transform")

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        import shutil
        from pathlib import Path

        output_dir = inputs.execute_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        # Sleep to give time for cancellation testing
        time.sleep(self.params.delay)

        # Copy input files to output
        dataset_input = inputs.inputs.get("dataset")
        if dataset_input is None:
            raise ValueError("No dataset input")
        input_files = (
            [Path(dataset_input)]
            if isinstance(dataset_input, (str, Path))
            else [Path(f) for f in dataset_input]
        )
        for src in input_files:
            shutil.copy2(src, output_dir / src.name)

        return {}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts: list[DataArtifact] = []
        for file_path in inputs.file_outputs:
            if file_path.suffix == ".csv":
                drafts.append(
                    DataArtifact.draft(
                        content=file_path.read_bytes(),
                        original_name=file_path.name,
                        step_number=inputs.step_number,
                    )
                )
        return ArtifactResult(
            success=True,
            artifacts={"dataset": drafts},
            metadata={"operation": "slow_transform"},
        )


def main() -> None:
    env = tutorial_setup("slurm_cancel_demo")

    pipeline = PipelineManager.create(
        name="slurm_cancel_demo",
        delta_root=env.delta_root,
        staging_root=env.staging_root,
        working_root=env.working_root,
    )

    print()
    print("=" * 60)
    print("SLURM Array Job Cancellation Demo")
    print("=" * 60)
    print()

    # Step 0: Generate 8 datasets locally (fast)
    print("Step 0: Generating 8 datasets locally...")
    step0 = pipeline.run(
        DataGenerator,
        params={"count": 8, "rows_per_file": 5, "seed": 42},
        name="generate",
    )
    print(f"  -> {step0.succeeded_count} datasets created\n")

    # Step 1: Transform all 8 via SLURM array job (slow)
    # Each dataset becomes one array task, sleeping 60s
    print("Step 1: Submitting 8-task SLURM array job (60s sleep each)...")
    print("  -> Press Ctrl+C to test cancellation")
    print("  -> Run `squeue -u $USER` in another terminal to see the array job")
    print()

    step1 = pipeline.run(
        SlowTransform,
        inputs={"dataset": step0.output("datasets")},
        params={"delay": 60.0},
        backend=Backend.SLURM,
        name="slow_transform",
    )

    # Show results
    print()
    print("=" * 60)
    print("Results")
    print("=" * 60)
    print()

    for step_result in pipeline:
        cancelled = step_result.metadata.get("cancelled", False)
        skipped = step_result.metadata.get("skipped", False)
        skip_reason = step_result.metadata.get("skip_reason", "")

        if cancelled:
            tag = "CANCELLED"
        elif skipped:
            tag = f"SKIPPED ({skip_reason})"
        else:
            tag = (
                f"{step_result.succeeded_count}/{step_result.total_count} succeeded, "
                f"{step_result.failed_count} failed"
            )

        print(f"  Step {step_result.step_number} ({step_result.step_name}): {tag}")

        # Show timing if available
        timings = step_result.metadata.get("timings", {})
        if timings.get("total"):
            print(f"    Total time: {timings['total']:.1f}s")

    print()


if __name__ == "__main__":
    main()
