# Quick Start

This page gets you from installed to running in 5 minutes. You'll create a
pipeline that generates synthetic data and transforms it — two steps, visible
output, minimal concepts.

**Prerequisites:** Artisan installed (`pixi install`), Prefect server running
(`pixi run prefect-start`)

---

## Create a script

Create a file called `my_first_pipeline.py` in the repo root:

```python
from pathlib import Path
import shutil

from artisan.orchestration import PipelineManager
from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.visualization import inspect_pipeline, render_macro_graph

# Clean start — remove any previous run
runs_dir = Path("quick_start")
if runs_dir.exists():
    shutil.rmtree(runs_dir)

delta_root = runs_dir / "delta"       # Persistent storage (Delta Lake tables)
staging_root = runs_dir / "staging"   # Temporary staging for worker outputs
for d in [delta_root, staging_root]:
    d.mkdir(parents=True)

# Create the pipeline
pipeline = PipelineManager.create(
    name="quick_start",
    delta_root=delta_root,
    staging_root=staging_root,
)
output = pipeline.output  # Helper for wiring steps together

# Step 1: Generate 3 synthetic CSV datasets
pipeline.run(
    operation=DataGenerator,
    name="generate",
    params={"count": 3, "seed": 42},
)

# Step 2: Transform each dataset (scale numeric columns by 2x)
pipeline.run(
    operation=DataTransformer,
    name="transform",
    inputs={"dataset": output("generate", "datasets")},
    params={"scale_factor": 2.0, "variants": 1, "seed": 100},
)

# Finalize — wait for all steps and print summary
result = pipeline.finalize()
print(f"Pipeline complete: {result['total_steps']} steps, success={result['overall_success']}")

# Inspect what happened
print(inspect_pipeline(delta_root))

# Save a provenance graph
render_macro_graph(delta_root, Path("quick_start_graph"))
print("Graph saved to: quick_start_graph.svg")
```

:::{important}
On macOS, wrap the pipeline code in `if __name__ == "__main__":` to avoid
multiprocessing errors. The snippet above works on Linux. On macOS, put
everything after the imports inside a `main()` function and call it from the
guard.
:::

---

## Run it

```bash
pixi run python my_first_pipeline.py
```

You should see output like this:

```
Pipeline 'quick_start' initialized (run_id=quick_start_20260314_...)
  delta_root: quick_start/delta
  staging_root: quick_start/staging
Step 0 (generate) starting... [backend=local]
Step 0 (generate) completed in 1.1s [1/1 succeeded]
Step 1 (transform) starting... [backend=local]
Step 1 (transform) completed in 1.3s [3/3 succeeded]
Pipeline 'quick_start' complete: 2 steps, all succeeded
  Step 0: generate         1.1s  [1/1]
  Step 1: transform        1.3s  [3/3]
  Total: 2.5s
Pipeline complete: 2 steps, success=True
```

---

## See what happened

The script prints a pipeline summary table:

```
shape: (2, 5)
┌──────┬───────────┬────────┬──────────┬──────────┐
│ step ┆ operation ┆ status ┆ produced ┆ duration │
│ ---  ┆ ---       ┆ ---    ┆ ---      ┆ ---      │
│ i64  ┆ str       ┆ str    ┆ str      ┆ str      │
╞══════╪═══════════╪════════╪══════════╪══════════╡
│ 0    ┆ generate  ┆ ok     ┆ 3 data   ┆ 1.1s     │
│ 1    ┆ transform ┆ ok     ┆ 3 data   ┆ 1.3s     │
└──────┴───────────┴────────┴──────────┴──────────┘
```

`generate` created 3 data artifacts from nothing. `transform` read those 3
artifacts and produced 3 transformed versions. Every artifact is tracked. Every
step is recorded. The framework did this automatically.

The script also saved a provenance graph to `quick_start_graph.svg` — open it
to see the pipeline's topology.

---

## What's next

- [Your First Pipeline](first-pipeline.md) — Add metrics, filtering, and
  refinement to build a 5-step pipeline
- [Orientation](orientation.md) — Understand the mental model behind artifacts,
  operations, provenance, and storage
- [Exploring Results](../tutorials/getting-started/02-exploring-results.ipynb) —
  Dig into the stored data and query lineage
