#!/usr/bin/env python
"""SLURM intra-allocation demo.

Run this script inside an salloc session to see Backend.SLURM_INTRA
dispatch work via srun across your allocated resources.

Usage:
    salloc --cpus-per-node=8 --time=00:30:00
    pixi run python docs/tutorials/execution/demo_slurm_intra.py

What happens:
    - Step 0 generates 8 small datasets locally (fast, no srun overhead)
    - Step 1 transforms each dataset via srun (one srun step per unit)
    - Step 2 computes metrics via srun
    - All srun steps dispatch instantly with no queue wait

Requires: Active SLURM allocation (salloc or sbatch).
"""

from __future__ import annotations

from artisan.operations.examples import (
    DataGenerator,
    DataTransformer,
    MetricCalculator,
)
from artisan.orchestration import Backend, PipelineManager
from artisan.utils import tutorial_setup
from artisan.visualization import inspect_pipeline


def main() -> None:
    env = tutorial_setup("demo_slurm_intra")

    pipeline = PipelineManager.create(
        name="slurm_intra_demo",
        delta_root=env.delta_root,
        staging_root=env.staging_root,
        working_root=env.working_root,
    )
    output = pipeline.output

    # Step 0: Generate data locally (fast)
    print("\n--- Step 0: Generating data locally ---")
    step0 = pipeline.run(
        operation=DataGenerator,
        name="generate",
        params={"count": 8, "seed": 42},
        backend=Backend.LOCAL,
    )
    print(f"  Generated {step0.succeeded_count} datasets")

    # Step 1: Transform via srun (dispatched across allocated nodes)
    print("\n--- Step 1: Transforming via srun ---")
    step1 = pipeline.run(
        operation=DataTransformer,
        name="transform",
        inputs={"dataset": output("generate", "datasets")},
        params={"scale_factor": 0.5, "variants": 2, "seed": 100},
        backend=Backend.SLURM_INTRA,
        resources={"cpus": 2, "memory_gb": 2},
    )
    print(f"  Transformed {step1.succeeded_count} datasets via srun")

    # Step 2: Compute metrics via srun
    print("\n--- Step 2: Computing metrics via srun ---")
    step2 = pipeline.run(
        operation=MetricCalculator,
        name="metrics",
        inputs={"dataset": output("transform", "dataset")},
        backend=Backend.SLURM_INTRA,
        resources={"cpus": 1, "memory_gb": 1},
    )
    print(f"  Computed metrics for {step2.succeeded_count} datasets via srun")

    # Finalize and inspect
    summary = pipeline.finalize()
    print(
        f"\n--- Pipeline complete: {summary['total_steps']} steps, "
        f"success={summary['overall_success']} ---\n"
    )

    inspect_pipeline(env.delta_root)


if __name__ == "__main__":
    main()
