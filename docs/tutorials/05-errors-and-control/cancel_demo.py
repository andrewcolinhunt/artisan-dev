#!/usr/bin/env python
"""Interactive Ctrl+C cancellation demo.

Run this script and press Ctrl+C while it's executing to see pipeline cancellation and its reported outcome.

Usage:
    pixi run python docs/tutorials/05-errors-and-control/cancel_demo.py

What happens:
    - The pipeline submits 10 Wait steps, each sleeping for 30 seconds
    - Press Ctrl+C at any point during execution
    - The signal handler fires and sets the cancel event
    - Affected attempts record requested and confirmed cancellation
    - All remaining steps become cancelled without executing
    - finalize() returns a clean summary

Signal escalation:
    - First Ctrl+C: request cancellation from the active runner
    - Second Ctrl+C: restore the previous signal handlers
    - Later signals: follow those handlers (usually KeyboardInterrupt for Ctrl+C)

The local runner allows a brief grace period and can terminate owned workers.
An in-flight execute phase is not guaranteed to finish. Accepted outputs depend
on the confirmed lifecycle outcome; arbitrary tool side effects may remain.

Try pressing Ctrl+C at different points to see how the cancellation
window affects which steps succeed vs cancel.
"""

from __future__ import annotations

from artisan.operations.examples import Wait
from artisan.orchestration import PipelineManager, StepStatus
from artisan.utils import tutorial_setup


def main() -> None:
    env = tutorial_setup("cancel_demo")

    pipeline = PipelineManager.create(
        name="cancel_demo",
        delta_root=env.delta_root,
        staging_root=env.staging_root,
        working_root=env.working_root,
    )

    # Submit many steps so there's time to press Ctrl+C.
    # Each Wait step sleeps for 30 seconds.
    print("\n--- Submitting 10 steps (press Ctrl+C to cancel) ---\n")

    for i in range(10):
        pipeline.submit(
            operation=Wait,
            name=f"wait_{i}",
            params={"duration": 30.0},
        )

    # finalize() blocks until all steps complete (or cancel).
    # SIGINT/SIGTERM triggers pipeline.cancel() via the signal handler.
    result = pipeline.finalize()

    # Show what happened
    print(f"\n--- Pipeline finished: {result['total_steps']} steps ---\n")

    for step_result in pipeline:
        if step_result.status is StepStatus.CANCELLED:
            tag = f"CANCELLED ({step_result.cancellation_status.value})"
        else:
            tag = (
                f"{step_result.status.value}: "
                f"{step_result.succeeded_count}/{step_result.total_count} succeeded"
            )

        print(f"  Step {step_result.step_number} ({step_result.step_name}): {tag}")

    print(f"\n  Overall success: {result['overall_success']}")


if __name__ == "__main__":
    main()
