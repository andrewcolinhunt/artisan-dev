"""Validate execute_unit_task runs correctly inside a Docker container.

Constructs execution objects directly (bypassing the orchestrator),
mirroring what a cloud dispatch handle does when calling workers.
"""

from __future__ import annotations

import os
import sys

# Trigger PREFECT_LOGGING_LEVEL=CRITICAL setdefault before Prefect imports.
# Belt-and-suspenders: the Dockerfile ENV already sets this, but importing
# the orchestration package ensures it even when run outside Docker.
import artisan.orchestration  # noqa: F401

from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.examples import DataGenerator
from artisan.orchestration.engine.dispatch import execute_unit_task
from artisan.schemas.execution import RuntimeEnvironment

DEMO_ROOT = "/tmp/artisan-demo"


def main() -> None:
    op = DataGenerator(params=DataGenerator.Params(count=3, rows_per_file=5))

    unit = ExecutionUnit(operation=op)

    runtime_env = RuntimeEnvironment(
        delta_root=os.path.join(DEMO_ROOT, "delta"),
        staging_root=os.path.join(DEMO_ROOT, "staging"),
        working_root=os.path.join(DEMO_ROOT, "working"),
    )

    print("--- execute_unit_task ---")
    result = execute_unit_task(unit, runtime_env)

    print(f"success:           {result.success}")
    print(f"error:             {result.error}")
    print(f"item_count:        {result.item_count}")
    print(f"execution_run_ids: {result.execution_run_ids}")

    # List staged output files
    staging_dir = os.path.join(DEMO_ROOT, "staging")
    if os.path.isdir(staging_dir):
        print("\n--- staged files ---")
        for dirpath, _dirnames, filenames in os.walk(staging_dir):
            for fname in sorted(filenames):
                rel = os.path.relpath(os.path.join(dirpath, fname), staging_dir)
                print(f"  {rel}")
    else:
        print("\nWARNING: staging directory not found")

    if not result.success:
        print(f"\nFAILED: {result.error}", file=sys.stderr)
        sys.exit(1)

    print("\nSUCCESS")


if __name__ == "__main__":
    main()
