"""Measure thread count at each stage of a pipeline lifecycle.

Run with: ~/.pixi/bin/pixi run python _dev/troubleshooting/measure_threads.py
"""

from __future__ import annotations

import threading
import time
from pathlib import Path


def snapshot(label: str) -> None:
    """Print thread count and names."""
    threads = threading.enumerate()
    print(f"\n{'=' * 60}")
    print(f"[{label}] Thread count: {len(threads)}")
    print(f"{'=' * 60}")
    # Group by prefix for readability
    names = sorted(t.name for t in threads)
    for name in names:
        daemon = next(t for t in threads if t.name == name).daemon
        print(f"  {'D' if daemon else ' '} {name}")


# -- Stage 0: Baseline before any imports --
snapshot("0. Baseline (no imports)")

# -- Stage 1: After importing artisan --
from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager
from artisan.utils import tutorial_setup

snapshot("1. After artisan imports")

# -- Stage 2: After tutorial_setup --
env = tutorial_setup("thread_measurement", base_dir=Path(__file__).parent)

snapshot("2. After tutorial_setup")

# -- Stage 3: After PipelineManager.create --
pipeline = PipelineManager.create(
    name="thread_measurement",
    delta_root=env.delta_root,
    staging_root=env.staging_root,
    working_root=env.working_root,
)
output = pipeline.output

snapshot("3. After PipelineManager.create")

# -- Stage 4: After first pipeline.run (source step) --
pipeline.run(
    operation=DataGenerator,
    name="generate",
    params={"count": 3, "seed": 42},
)

snapshot("4. After first pipeline.run (source)")

# -- Stage 5: After second pipeline.run (transform step) --
pipeline.run(
    operation=DataTransformer,
    name="transform",
    inputs={"dataset": output("generate", "datasets")},
    params={"scale_factor": 2.0, "variants": 1, "seed": 100},
)

snapshot("5. After second pipeline.run (transform)")

# -- Stage 6: After finalize --
result = pipeline.finalize()
print(f"\nPipeline result: {result['total_steps']} steps, success={result['overall_success']}")

snapshot("6. After finalize")

# -- Stage 7: Wait for idle thread cleanup (anyio has 10s timeout) --
print("\nWaiting 12s for anyio idle thread pruning...")
time.sleep(12)

snapshot("7. After 12s idle wait")

# -- Stage 8: Create a SECOND pipeline WITHOUT finalizing (simulate leak) --
pipeline2 = PipelineManager.create(
    name="thread_measurement_leaked",
    delta_root=env.delta_root,
    staging_root=env.staging_root,
    working_root=env.working_root,
)
pipeline2.run(
    operation=DataGenerator,
    name="generate2",
    params={"count": 2, "seed": 99},
)
# Deliberately NOT calling pipeline2.finalize()

snapshot("8. After leaked pipeline (no finalize)")

# -- Stage 9: Create a THIRD pipeline, also not finalized --
pipeline3 = PipelineManager.create(
    name="thread_measurement_leaked2",
    delta_root=env.delta_root,
    staging_root=env.staging_root,
    working_root=env.working_root,
)
pipeline3.run(
    operation=DataGenerator,
    name="generate3",
    params={"count": 2, "seed": 77},
)
# Deliberately NOT calling pipeline3.finalize()

snapshot("9. After second leaked pipeline")

# -- Summary --
print("\n" + "=" * 60)
print("DONE — review thread counts above to build budget.")
print("=" * 60)
