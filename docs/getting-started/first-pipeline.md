# Your First Pipeline

TODO: this page seems unecessary. we should just point to the notebook. does it add anything at all beyond the notebook?

:::{note}
This tutorial is also available as an [interactive notebook](../tutorials/getting-started/01-first-pipeline.ipynb).
The notebook version may differ slightly from this page.
:::

This walkthrough builds a pipeline that generates synthetic datasets, transforms
them, computes metrics, filters by score, and refines the survivors. Every
artifact is tracked with full provenance.

**Estimated time:** 15 minutes \
**Prerequisites:** Artisan installed (`pixi install`), Prefect server running
(`pixi run prefect-start`). If you haven't run a pipeline before, start with
the [Quick Start](quick-start.md).

---

## Set up paths

Every pipeline needs three directories:

| Directory | Purpose |
|-----------|---------|
| `delta_root` | Delta Lake tables (persistent storage) |
| `staging_root` | Temporary staging for worker outputs |
| `working_root` | Sandbox for file operations during execution |

:::{note}
We set `working_root` explicitly here so you can inspect sandbox files during
the tutorial. In production SLURM runs, you should omit it — the default uses
`$TMPDIR`, which on most clusters points to fast node-local scratch. This keeps
transient I/O off the shared filesystem, which matters at scale. See
[Configuring Execution](../how-to-guides/configuring-execution.md)
for details.
:::

```python
from pathlib import Path
import shutil

# Clean start
RUNS_DIR = Path("my_first_pipeline")
if RUNS_DIR.exists():
    shutil.rmtree(RUNS_DIR)

delta_root = RUNS_DIR / "delta"
staging_root = RUNS_DIR / "staging"
working_root = RUNS_DIR / "working"

for d in [delta_root, staging_root, working_root]:
    d.mkdir(parents=True)
```

## Create the pipeline

`PipelineManager.create()` initializes a pipeline with a name and directory
paths. It connects to the running Prefect server automatically.

```python
from artisan.orchestration import PipelineManager

pipeline = PipelineManager.create(
    name="first_pipeline",
    delta_root=delta_root,
    staging_root=staging_root,
    working_root=working_root,
)
output = pipeline.output
```

The `output` shorthand is a callable that creates lazy references to a
step's outputs. You will use it to wire steps together.

## Generate synthetic data

`DataGenerator` is a generative operation — it takes no inputs and produces
CSV dataset files. The `count` parameter controls how many datasets to create.

```python
from artisan.operations.examples import DataGenerator

pipeline.run(
    operation=DataGenerator,
    name="generate",
    params={"count": 5, "seed": 42},
)
```

`pipeline.run()` executes the operation and returns a `StepResult`. The `name`
parameter gives the step a label you can reference later when wiring inputs.

This step created 5 dataset artifacts from nothing — each a CSV with columns
`id`, `x`, `y`, `z`, `score`.

## Transform datasets

`DataTransformer` reads each input CSV, scales numeric columns, and writes the
result. This is where **output references** come in: `output(step_name, role)`
creates a lazy reference to a previous step's outputs.

```python
from artisan.operations.examples import DataTransformer

pipeline.run(
    operation=DataTransformer,
    name="transform",
    inputs={"dataset": output("generate", "datasets")},
    params={"scale_factor": 0.5, "variants": 1, "seed": 100},
)
```

`output("generate", "datasets")` says "give me the artifacts from the `generate`
step with role `datasets`." The transformer's input role is `"dataset"`
(singular) — this maps each generated dataset to a transformation job.

## Compute metrics

`MetricCalculator` computes distribution statistics (min, max, median, range)
and summary statistics (CV, row count) from the `score` column of each
dataset. It produces one metric artifact per input.

```python
from artisan.operations.examples import MetricCalculator

pipeline.run(
    operation=MetricCalculator,
    name="metrics",
    inputs={"dataset": output("transform", "dataset")},
)
```

Each input dataset now has associated metrics that downstream steps can query.

## Filter by score

`Filter` is a curator operation that selects artifacts based on metric values.
It takes a `passthrough` input and auto-discovers associated metrics via
provenance — no need to wire metric inputs explicitly.

```python
from artisan.operations.curator import Filter

pipeline.run(
    operation=Filter,
    name="filter",
    inputs={"passthrough": output("transform", "dataset")},
    params={
        "criteria": [
            {"metric": "distribution.median", "operator": "gt", "value": 0.15},
        ]
    },
)
```

Filter discovers metrics via forward provenance walk from the passthrough
artifacts. Criteria use dot-separated field names — `"distribution.median"`
references the nested `median` key inside the `distribution` group of the
metric content.

When metrics come from multiple sources and field names collide, use `step` or
`step_number` on criteria to disambiguate — see [Metrics and Filtering](../tutorials/pipeline-design/03-metrics-and-filtering.ipynb).

## Transform filtered results

Run one more transformation on the filtered artifacts. This step only
processes artifacts that passed the filter.

```python
pipeline.run(
    operation=DataTransformer,
    name="refine",
    inputs={"dataset": output("filter", "passthrough")},
    params={"scale_factor": 0.1, "variants": 1, "seed": 101},
)
```

## Finalize

`finalize()` waits for any pending steps and returns a summary.

```python
result = pipeline.finalize()
print(f"Pipeline complete: {result['total_steps']} steps, success={result['overall_success']}")
```

Expected output:

```
Pipeline 'first_pipeline' complete: 5 steps, all succeeded
  Step 0: generate         1.1s  [1/1]
  Step 1: transform        1.4s  [5/5]
  Step 2: metrics          1.1s  [5/5]
  Step 3: filter           0.9s  [5/5]
  Step 4: refine           1.4s  [5/5]
  Total: 5.9s
Pipeline complete: 5 steps, success=True
```

---

## Inspect results

### Pipeline summary

`inspect_pipeline` shows one row per step: the operation, status, what it
produced, and how long it took.

```python
from artisan.visualization import inspect_pipeline

inspect_pipeline(delta_root)
```

Expected output:

```
shape: (5, 5)
┌──────┬───────────┬────────┬──────────┬──────────┐
│ step ┆ operation ┆ status ┆ produced ┆ duration │
│ ---  ┆ ---       ┆ ---    ┆ ---      ┆ ---      │
│ i64  ┆ str       ┆ str    ┆ str      ┆ str      │
╞══════╪═══════════╪════════╪══════════╪══════════╡
│ 0    ┆ generate  ┆ ok     ┆ 5 data   ┆ 1.1s     │
│ 1    ┆ transform ┆ ok     ┆ 5 data   ┆ 1.4s     │
│ 2    ┆ metrics   ┆ ok     ┆ 5 metric ┆ 1.1s     │
│ 3    ┆ filter    ┆ ok     ┆ 5 passed ┆ 0.9s     │
│ 4    ┆ refine    ┆ ok     ┆ 5 data   ┆ 1.4s     │
└──────┴───────────┴────────┴──────────┴──────────┘
```

A few things to notice:

- **Filter** shows how many artifacts passed rather than artifact counts.
  It works by selecting, not creating.
- **Refine** only processes the artifacts that survived the filter.

### Metric values

`inspect_metrics` parses metric artifacts into a readable table.

```python
from artisan.visualization import inspect_metrics

inspect_metrics(delta_root, step_number=2)
```

### Provenance graph

The macro graph shows the pipeline's topology — steps as boxes, artifact
outputs grouped by type, and arrows showing data flow.

```python
from artisan.visualization import build_macro_graph

build_macro_graph(delta_root)
```

---

## What you've seen

You built a pipeline that generates, transforms, scores, filters, and refines
data — with automatic content addressing, provenance tracking, and deterministic
caching. The key patterns:

| Concept | What it does |
|---|---|
| `PipelineManager.create()` | Creates a pipeline with storage paths |
| `pipeline.run(operation=Op, ...)` | Runs an operation as the next step |
| `output = pipeline.output` | Binds the output reference helper |
| `output(name, role)` | References a previous step's output by name and role |
| `pipeline.finalize()` | Closes the pipeline and returns a summary |
| `name="..."` on `run()` | Labels a step for later reference |
| `params={...}` on `run()` | Passes parameters to the operation |

The operations fall into two patterns:

- **Producing operations** (`DataGenerator`, `DataTransformer`,
  `MetricCalculator`) create new artifacts.
- **Passthrough operations** (`Filter`) route or select existing
  artifacts without creating new ones.

To understand how these mechanisms work, see
[Architecture Overview](../concepts/architecture-overview.md) and
[Execution Flow](../concepts/execution-flow.md).

---

## Next steps

- [Orientation](orientation.md) — Understand the mental model behind
  artifacts, operations, provenance, and storage
- [Exploring Results](../tutorials/getting-started/02-exploring-results.ipynb) — Dig
  into Delta tables, query lineage, and inspect individual artifacts
- [Building a Pipeline](../how-to-guides/building-a-pipeline.md) — Detailed
  guide to `PipelineManager`, input patterns, and step configuration
- [Pipeline Patterns](../tutorials/pipeline-design/01-sources-and-sequencing.ipynb) — Reusable
  patterns with provenance graphs
