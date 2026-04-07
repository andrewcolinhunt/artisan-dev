"""Measure thread count across a full tutorial-like session.

Simulates running multiple pipelines as notebooks do, with periodic
thread sampling during flow execution.

Run with: ~/.pixi/bin/pixi run python _dev/troubleshooting/measure_threads_full.py
"""

from __future__ import annotations

import threading
import time
from pathlib import Path


def snapshot(label: str) -> dict[str, list[str]]:
    """Print thread count, grouped by prefix, return name->daemon map."""
    threads = threading.enumerate()
    print(f"\n{'=' * 60}")
    print(f"[{label}] Thread count: {len(threads)}")
    print(f"{'=' * 60}")

    # Group by prefix
    groups: dict[str, int] = {}
    for t in threads:
        # Use first word as group key
        prefix = t.name.split("_")[0].split("-")[0]
        groups[prefix] = groups.get(prefix, 0) + 1

    for name in sorted(t.name for t in threads):
        daemon = next(t for t in threads if t.name == name).daemon
        print(f"  {'D' if daemon else ' '} {name}")

    return {t.name: t.daemon for t in threads}


# -- Baseline --
snapshot("0. Baseline")

# -- Imports --
from artisan.operations.examples import (
    DataGenerator,
    DataTransformer,
    MetricCalculator,
)
from artisan.orchestration import PipelineManager
from artisan.utils import tutorial_setup

snapshot("1. After imports")

# -- Setup --
env = tutorial_setup("thread_full", base_dir=Path(__file__).parent)

# ============================================================
# Pipeline 1: Full lifecycle (generate -> transform -> metrics -> finalize)
# Mimics a typical getting-started tutorial
# ============================================================
print("\n>>> Pipeline 1: Full lifecycle")
p1 = PipelineManager.create(
    name="full_lifecycle",
    delta_root=env.delta_root,
    staging_root=env.staging_root,
    working_root=env.working_root,
)
out = p1.output

snapshot("2. After p1.create")

p1.run(operation=DataGenerator, name="gen", params={"count": 5, "seed": 42})
snapshot("3. After p1.run(gen) — 5 items")

p1.run(
    operation=DataTransformer,
    name="transform",
    inputs={"dataset": out("gen", "datasets")},
    params={"scale_factor": 2.0, "variants": 2, "seed": 100},
)
snapshot("4. After p1.run(transform) — 10 items out")

p1.run(
    operation=MetricCalculator,
    name="metrics",
    inputs={"dataset": out("transform", "dataset")},
)
snapshot("5. After p1.run(metrics)")

p1.finalize()
snapshot("6. After p1.finalize")

# ============================================================
# Pipeline 2: Error demo (create, run, fail, NO finalize)
# Mimics error-visibility tutorial leak
# ============================================================
print("\n>>> Pipeline 2: Error demo (leaked)")
env2 = tutorial_setup("thread_full_err", base_dir=Path(__file__).parent)
p2 = PipelineManager.create(
    name="error_demo",
    delta_root=env2.delta_root,
    staging_root=env2.staging_root,
    working_root=env2.working_root,
)
p2.run(operation=DataGenerator, name="gen_err", params={"count": 2, "seed": 1})
# Deliberately NOT finalizing
snapshot("7. After leaked p2")

# ============================================================
# Pipeline 3: Another leaked pipeline
# Mimics config-demo cells in SLURM tutorials
# ============================================================
print("\n>>> Pipeline 3: Config demo (leaked)")
env3 = tutorial_setup("thread_full_cfg", base_dir=Path(__file__).parent)
p3 = PipelineManager.create(
    name="config_demo",
    delta_root=env3.delta_root,
    staging_root=env3.staging_root,
    working_root=env3.working_root,
)
# No run, no finalize — just created
snapshot("8. After leaked p3 (no run)")

# ============================================================
# Pipeline 4: Full lifecycle with many steps
# Stress test: 10 steps to see thread growth
# ============================================================
print("\n>>> Pipeline 4: Many-step pipeline")
env4 = tutorial_setup("thread_full_many", base_dir=Path(__file__).parent)
p4 = PipelineManager.create(
    name="many_steps",
    delta_root=env4.delta_root,
    staging_root=env4.staging_root,
    working_root=env4.working_root,
)
out4 = p4.output

p4.run(operation=DataGenerator, name="gen0", params={"count": 5, "seed": 10})
for i in range(1, 6):
    p4.run(
        operation=DataTransformer,
        name=f"step{i}",
        inputs={"dataset": out4(f"gen0" if i == 1 else f"step{i-1}", "datasets" if i == 1 else "dataset")},
        params={"scale_factor": 1.1, "variants": 1, "seed": i * 10},
    )

snapshot("9. After p4 with 6 steps (before finalize)")
p4.finalize()
snapshot("10. After p4.finalize")

# ============================================================
# Final state and cleanup timing
# ============================================================
print("\nWaiting 12s for idle thread pruning...")
time.sleep(12)
snapshot("11. After 12s idle (final steady state)")

# Summary
print("\n" + "=" * 60)
all_threads = threading.enumerate()
leaked = [t for t in all_threads if t.name.startswith("pipeline-step")]
print(f"SUMMARY: {len(all_threads)} total threads, {len(leaked)} leaked pipeline-step threads")
print(f"Leaked pipelines: p2 (error_demo), p3 (config_demo)")
print("=" * 60)
