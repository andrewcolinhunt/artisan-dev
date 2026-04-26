#!/usr/bin/env python
"""Interactive Ctrl+C cancellation demo.

Run this script and press Ctrl+C while it's executing to see graceful
pipeline cancellation in action.

Usage:
    pixi run python docs/tutorials/execution/cancel_demo.py

What happens:
    - The pipeline submits 10 Wait steps, each sleeping for 30 seconds
    - Press Ctrl+C at any point during execution
    - The signal handler fires and sets the cancel event
    - The current step finishes its active phase, then stops
    - All remaining steps are skipped immediately
    - finalize() returns a clean summary

Signal escalation:
    - First Ctrl+C: graceful cancellation (drain current step, skip rest)
    - Second Ctrl+C: restore default signal handlers
    - Third Ctrl+C: force kill (KeyboardInterrupt)

If the graceful shutdown feels slow, spam Ctrl+C to force exit.

Try pressing Ctrl+C at different points to see how the cancellation
window affects which steps complete vs skip.
"""

from __future__ import annotations

from artisan.operations.examples import Wait
from artisan.orchestration import PipelineManager
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
