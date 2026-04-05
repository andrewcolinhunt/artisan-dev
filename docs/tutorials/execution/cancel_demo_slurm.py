#!/usr/bin/env python
"""SLURM pipeline cancellation demo.

Run this script on a SLURM cluster and press Ctrl+C to cancel the
pipeline. In-flight SLURM jobs are automatically cancelled via
``scancel --name``.

Usage:
    pixi run python docs/tutorials/execution/cancel_demo_slurm.py

What happens:
    - The pipeline submits Wait steps to SLURM, each sleeping for a
      configured duration
    - Press Ctrl+C while jobs are running on the cluster
    - The signal handler fires and sets the cancel event
    - The dispatch handle calls ``scancel --name`` to kill in-flight jobs
    - Remaining steps are skipped immediately
    - finalize() returns a clean summary

Signal escalation:
    - First Ctrl+C: graceful cancellation (auto-scancel + skip remaining)
    - Second Ctrl+C: restore default signal handlers
    - Third Ctrl+C: force kill (KeyboardInterrupt)

For array job cancellation testing, see
_dev/demos/slurm-cancel/demo_slurm_cancel.py which submits a single
step with 8 array tasks.

Requires: SLURM cluster access.
"""

from __future__ import annotations

from artisan.operations.examples import Wait
from artisan.orchestration import Backend, PipelineManager
from artisan.utils import tutorial_setup


def main() -> None:
    env = tutorial_setup("cancel_demo_slurm")

    pipeline = PipelineManager.create(
        name="cancel_demo_slurm",
        delta_root=env.delta_root,
        staging_root=env.staging_root,
        working_root=env.working_root,
    )

    print("\n--- Submitting SLURM pipeline (press Ctrl+C to cancel) ---\n")

    # Submit Wait steps to SLURM
    for i in range(6):
        pipeline.submit(
            operation=Wait,
            name=f"wait_{i}",
            params={"duration": 60},
            backend=Backend.SLURM,
        )

    # finalize() blocks — Ctrl+C triggers cancel(), skipping remaining steps
    result = pipeline.finalize()

    print(f"\n--- Pipeline finished: {result['total_steps']} steps ---\n")

    for step_result in pipeline:
        skipped = step_result.metadata.get("skipped", False)
        cancelled = step_result.metadata.get("cancelled", False)

        if cancelled:
            tag = "CANCELLED (mid-execution)"
        elif skipped and step_result.metadata.get("skip_reason") == "cancelled":
            tag = "SKIPPED (cancel propagated)"
        else:
            tag = f"{step_result.succeeded_count}/{step_result.total_count} succeeded"

        print(f"  Step {step_result.step_number} ({step_result.step_name}): {tag}")

    print(f"\n  Overall success: {result['overall_success']}")


if __name__ == "__main__":
    main()
