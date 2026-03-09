# Your First Pipeline

:::{note}
This tutorial is also available as an [interactive notebook](../tutorials/getting-started/01-first-pipeline.ipynb).
:::

This tutorial walks you through building and running a complete pipeline using
only example operations — no GPU or external tools needed.
By the end you will have generated datasets, computed metrics,
filtered results, and inspected provenance.

**Estimated time:** 15 minutes
**Prerequisites:** Artisan installed (`pixi install`), Prefect server running (`pixi run prefect-start`)

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

`PipelineManager.create()` initializes a pipeline with a name and the three
directory paths.

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

## Generate datasets

`DataGenerator` is a generative operation — it takes no inputs and produces
CSV dataset files. The `count` parameter controls how many datasets to
generate.

```python
from artisan.operations.examples import DataGenerator

pipeline.run(
    operation=DataGenerator,
    name="generate",
    params={"count": 4, "seed": 42},
)
```

Use `output("step_name", "role")` to create an `OutputReference` that
downstream steps can consume:

```python
# This is a lazy reference — no data is copied
output("generate", "datasets")
```

## Transform datasets

`DataTransformer` takes datasets as input and applies scaling and noise to
numeric columns.

```python
from artisan.operations.examples import DataTransformer

pipeline.run(
    operation=DataTransformer,
    name="transform",
    inputs={"dataset": output("generate", "datasets")},
    params={"scale_factor": 2.0, "variants": 1, "seed": 100},
)
```

Notice the input wiring: `output("generate", "datasets")` connects the
DataGenerator outputs to DataTransformer's `"dataset"` input role.

## Compute metrics

`MetricCalculator` computes statistics for each dataset — distribution metrics
(min, max, median, range) and summary metrics (CV, row count) — and produces
METRIC artifacts.

```python
from artisan.operations.examples import MetricCalculator

pipeline.run(
    operation=MetricCalculator,
    name="metrics",
    inputs={"dataset": output("transform", "dataset")},
)
```

## Filter by score

`Filter` is a curator operation that selects datasets based on metric values.
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
            {"metric": "distribution.median", "operator": "gt", "value": 0.5},
        ]
    },
)
```

Only datasets whose associated `distribution.median` exceeds 0.5 will pass through.
For multi-source filtering or disambiguation, you can still use explicit
`"role.field"` syntax — see [Metrics and Filtering](../tutorials/pipeline-patterns/03-metrics-and-filtering.ipynb).

## Finalize

`finalize()` waits for any pending steps and returns a summary.

```python
result = pipeline.finalize()
print(result)
```

Output:
```
{'pipeline_name': 'first_pipeline', 'total_steps': 4, 'overall_success': True, ...}
```

## Inspect results

### Read Delta tables

All pipeline data is stored in Delta Lake tables. Read them with Polars:

```python
import polars as pl

# Artifact index — every artifact in the pipeline
df_index = pl.read_delta(str(delta_root / "artifacts" / "index"))
print(df_index)

# Data — CSV dataset content
df_data = pl.read_delta(str(delta_root / "artifacts" / "data"))
print(df_data.select("artifact_id", "original_name", "origin_step_number"))

# Metrics — computed properties
df_metrics = pl.read_delta(str(delta_root / "artifacts" / "metrics"))
print(df_metrics)

# Execution records — what ran
df_execution = pl.read_delta(str(delta_root / "orchestration" / "executions"))
print(df_execution.select("operation_name", "success", "origin_step_number"))
```

### Visualize provenance

The macro graph shows the pipeline structure at the step level:

```python
from artisan.visualization import build_macro_graph

build_macro_graph(delta_root)
```

For artifact-level detail, use the provenance stepper:

```python
from artisan.visualization import display_provenance_stepper

stepper = display_provenance_stepper(delta_root)
stepper  # Interactive widget in Jupyter
```

---

## What you've seen

You built a pipeline that generates, transforms, scores, and filters data —
with automatic content addressing, provenance tracking, and deterministic
caching. To understand how these mechanisms work, see
[Architecture Overview](../concepts/architecture-overview.md) and
[Execution Flow](../concepts/execution-flow.md).

---

## Next steps

- [Orientation](orientation.md) — Understand the mental model behind
  artifacts, operations, provenance, and storage
- [Building a Pipeline](../how-to-guides/building-a-pipeline.md) — Detailed
  guide to `PipelineManager`, input patterns, and step configuration
- [Pipeline Patterns](../tutorials/pipeline-patterns/01-sources-and-chains.ipynb) — Reusable
  patterns with provenance graphs
