# Build a Pipeline

How to create a pipeline, wire steps together, and run it to completion.

**Prerequisites:** [Operations Model](../concepts/operations-model.md)
and at least one [operation type](writing-creator-operations.md).

**Key types:** `PipelineManager`, `StepResult`, `StepFuture`, `OutputReference`,
`CompositeDefinition`, `FailurePolicy`, `CachePolicy`

---

## Minimal working example

A complete pipeline that generates data, transforms it, and computes metrics:

```python
from artisan.orchestration import PipelineManager
from artisan.operations.examples import DataGenerator, DataTransformer, MetricCalculator

pipeline = PipelineManager.create(
    name="quickstart",
    delta_root="runs/delta",
    staging_root="runs/staging",
)
output = pipeline.output

pipeline.run(operation=DataGenerator, name="generate", params={"count": 5})
pipeline.run(
    operation=DataTransformer,
    name="transform",
    inputs={"dataset": output("generate", "datasets")},
)
pipeline.run(
    operation=MetricCalculator,
    name="metrics",
    inputs={"dataset": output("transform", "dataset")},
)
summary = pipeline.finalize()
```

The rest of this guide breaks down each piece.

---

## Create a pipeline

```python
pipeline = PipelineManager.create(
    name="my_pipeline",
    delta_root="runs/delta",
    staging_root="runs/staging",
)
```

`delta_root` contains the durable store; `staging_root` holds workers' pending
results. Use [Configure Execution](configuring-execution.md) for worker
resources, batching, cache policy, and diagnostic directories. Exact arguments
are available through the [Python API entry points](../reference/python-api.md).

Both `delta_root` and `staging_root` are created automatically if they do not
exist. Cluster runner providers can map the default `working_root` to
node-local scratch to avoid shared filesystem contention.

---

## Add steps

Every step calls `pipeline.run()` with an operation class. There are three
patterns depending on whether the step has inputs.

### Source (no inputs)

A generative step creates artifacts from nothing:

```python
pipeline.run(operation=DataGenerator, name="generate", params={"count": 10})
```

### Sequential (one input)

Wire the output of one step to the input of the next using `output()`:

```python
output = pipeline.output

pipeline.run(
    operation=DataTransformer,
    name="transform",
    inputs={"dataset": output("generate", "datasets")},
    params={"scale_factor": 2.0},
)
```

`output("generate", "datasets")` returns an `OutputReference` — a lazy pointer
resolved to concrete artifact IDs at dispatch time. The dict key (`"dataset"`)
must match the downstream operation's input role name.

### Ingest (external files)

Bring files from disk into the pipeline as artifacts:

```python
from artisan.operations.curator import IngestData

pipeline.run(operation=IngestData, name="ingest", inputs=["/data/a.csv", "/data/b.csv"])
```

Raw file paths are auto-promoted to `FileRefArtifact` and committed to Delta
Lake before the operation runs. The output role for `IngestData` is `"data"`.

---

## Name your steps

By default, steps are named after the operation. Pass `name=` to give a step a
custom name, then use `output()` to reference it later:

```python
output = pipeline.output

pipeline.run(operation=DataGenerator, name="gen", params={"count": 10})
pipeline.run(
    operation=DataTransformer,
    name="transform",
    inputs={"dataset": output("gen", "datasets")},
)
```

`output(name, role)` returns an `OutputReference` — a lazy pointer resolved
to concrete artifact IDs at dispatch time. Bind it once after creating the
pipeline with `output = pipeline.output` for concise wiring throughout.

When a pipeline contains multiple steps with the same name, `output()` returns
the most recent one by default. To reference a specific instance, pass
`step_number`:

```python
output("gen", "datasets", step_number=0)
```

:::{tip} Two ways to wire steps
**Name-based (preferred):**
```python
output = pipeline.output
pipeline.run(operation=DataGenerator, name="generate", params={"count": 5})
pipeline.run(
    operation=DataTransformer, inputs={"dataset": output("generate", "datasets")}
)
```

**Direct chaining (also works):**
```python
step0 = pipeline.run(operation=DataGenerator, params={"count": 5})
pipeline.run(operation=DataTransformer, inputs={"dataset": step0.output("datasets")})
```
:::

---

## Finalize

`finalize()` waits for all pending futures, shuts down the executor, and returns
a summary dict:

```python
summary = pipeline.finalize()
print(summary["pipeline_name"])  # "my_pipeline"
print(summary["total_steps"])  # 3
print(summary["overall_success"])  # True
```

`finalize()` is required when using `submit()` (see below). With `run()` only,
it is optional but still recommended — it produces the summary and cleans up
the executor.

---

## Common patterns

### `run()` vs `submit()`

`run()` waits and returns a `StepResult`. `submit()` returns a `StepFuture`
after preparing the step and waiting for any referenced predecessors. Both wire
downstream with `.output("role")`. Use `submit()` to keep the caller available
while execution runs, then call `finalize()` to wait and clean up. The current
manager executes steps serially; workers within a step can run concurrently.

For composites, the equivalent surface is `submit_composite()` /
`run_composite()`.

```python
output = pipeline.output
pipeline.submit(operation=DataGenerator, name="generate", params={"count": 100})
pipeline.submit(
    operation=DataTransformer,
    name="transform",
    inputs={"dataset": output("generate", "datasets")},
)
summary = pipeline.finalize()
```

### Context-manager form (scripts only)

`PipelineManager` supports `with`-block usage in scripts that exit when the
block ends:

```python
with PipelineManager.create(
    name="batch", delta_root="runs/delta", staging_root="runs/staging"
) as pipeline:
    pipeline.run(operation=DataGenerator, name="generate")
    pipeline.run(
        operation=DataTransformer,
        name="transform",
        inputs={"dataset": pipeline.output("generate", "datasets")},
    )
# __exit__ calls finalize() automatically.
```

This form is **for scripts only** — do not use it in notebooks. `__exit__`
calls `finalize()`, which shuts down the executor; subsequent cells that try
to submit more steps will crash. In notebooks, call `finalize()` explicitly
in the last cell.

### Step-level overrides

Both `run()` and `submit()` accept override parameters beyond `operation`,
`inputs`, `params`, and `name`:

| Parameter | Purpose |
|-----------|---------|
| `step_runner` | Override the pipeline's default step runner for this step |
| `runner_resources` | Override runner resource allocation (CPUs, memory, GPUs, time limit) |
| `batch_strategy` | Override batching settings (`artifacts_per_unit`, `max_workers`) |
| `compute_provider` | Override the compute provider — local, modal, etc. |
| `compute_resources` | Patch the operation’s compute configuration; deployed hardware still requires redeployment |
| `environment` | Override the operation's runtime environment |
| `tool` | Override the operation's external tool configuration |
| `failure_policy` | Override the pipeline's failure policy for this step |
| `cache_policy` | Decide whether a prior partial step may supply a whole-step cache hit |
| `skip_cache` | Bypass both step and execution caching for this step |
| `compact` | Control Delta Lake compaction after commit |

See [Configuring Execution](configuring-execution.md) for details on each.

### Branching

Feed the same output into multiple independent steps:

```python
output = pipeline.output
pipeline.run(operation=DataGenerator, name="generate", params={"count": 10})
pipeline.submit(
    operation=TransformA,
    name="branch_a",
    inputs={"data": output("generate", "datasets")},
)
pipeline.submit(
    operation=TransformB,
    name="branch_b",
    inputs={"data": output("generate", "datasets")},
)
```

### Merging branches

Combine multiple streams into one with `Merge`:

```python
from artisan.operations.curator import Merge

pipeline.run(
    operation=Merge,
    name="merge",
    inputs=[output("branch_a", "result"), output("branch_b", "result")],
)
# Downstream uses: output("merge", "merged")
```

Pass inputs as a list. The merged output role is always `"merged"`.

### Filtering by metrics

Use `Filter` to keep artifacts that meet criteria:

```python
from artisan.operations.curator import Filter

pipeline.run(
    operation=Filter,
    name="filter",
    inputs={"passthrough": output("transform", "dataset")},
    params={
        "criteria": [
            {"metric": "distribution.median", "operator": "gt", "value": 0.5},
        ],
    },
)
# Downstream uses: output("filter", "passthrough")
```

- `"passthrough"` is both the input and output role name. Filter auto-discovers
  associated metrics via forward provenance walk.
- Criteria use bare field names (e.g., `"distribution.median"`).
- When field names collide across metric sources, add `step` or `step_number`
  to the criterion to disambiguate.
- All criteria are AND'd.

### Composing operations with composites

A composite exposes a sequence of operations as one reusable definition. Use the
`TransformAndScore` class from [Write Composite Operations](writing-composite-operations.md),
which transforms a dataset and scores the result:

Run a composite with `pipeline.run_composite()`. Each internal operation
becomes its own pipeline step with independent caching and dispatch:

```python
output = pipeline.output
pipeline.run(operation=DataGenerator, name="gen", params={"count": 10})

# Each internal operation becomes its own step
pipeline.run_composite(
    composite=TransformAndScore,
    name="ts",
    inputs={"dataset": output("gen", "datasets")},
)
```

Composite-level execution overrides (`step_runner`, `runner_resources`,
`batch_strategy`, …) become the default for every child step. For the full
guide on writing composites, see
[Writing Composite Operations](writing-composite-operations.md).

### Optional SLURM execution

Install `artisan-submitit`, then pass its runner instance to dispatch a step to
SLURM:

```python
from artisan_submitit import SlurmRunner

pipeline.run(
    operation=DataTransformer,
    name="transform",
    inputs={"dataset": output("generate", "datasets")},
    step_runner=SlurmRunner(),
    runner_resources={"gpus": 1, "memory_gb": 16, "extra": {"slurm_partition": "gpu"}},
)
```

See [Configuring Execution](configuring-execution.md) for resource and batching recipes.

### Resume a previous run

Re-running a pipeline skips steps with matching inputs and parameters
(content-addressed caching). To continue a run that failed partway through:

```python
pipeline = PipelineManager.resume(
    delta_root="runs/delta",
    staging_root="runs/staging",
)
```

`resume()` reconstructs step results from Delta Lake and sets the step counter
so new steps continue the sequence. Pass `pipeline_run_id="..."` to resume a
specific run; omit it to resume the most recent. Pass `name="..."` to override
the pipeline name.

If the pipeline used an external default step runner, create that provider
again and pass the instance when resuming. Artisan persists the runner's stable
name for provenance, not the configured Python object:

```python
from artisan_submitit import SlurmRunner

pipeline = PipelineManager.resume(
    delta_root="runs/delta",
    staging_root="runs/staging",
    default_step_runner=SlurmRunner(slurm_partition="gpu"),
)
```

Core can reconstruct the built-in `"local"` runner by name; it does not
register or reconstruct external providers.

### List previous runs

Inspect all pipeline runs stored in a delta root:

```python
from artisan.orchestration import list_runs

runs = list_runs(delta_root="runs/delta")
print(runs)  # polars DataFrame with run IDs, step counts, and timestamps
```

---

## Common pitfalls

| Problem | Cause | Fix |
|---------|-------|-----|
| `Output role 'X' not available` | Mismatched role name in `.output()` | Check the operation's output role names |
| Downstream step receives 0 artifacts | Upstream step failed or filtered everything out | Check `step.status` and `step.succeeded_count` |
| `Raw file paths are not allowed for creator operations` | Passed a file path list to a creator operation | Use `IngestData` first, then wire its output |
| Pipeline hangs on exit | Forgot `finalize()` after using `submit()` | Call `pipeline.finalize()` |
| Re-run a step after changing code or external state | A prior cached result is still eligible | Pass `skip_cache=True`; see [Bypass caching](configuring-execution.md#bypass-caching-and-resume). Use a fresh store only when you want separate history |

---

## Verify

Run your pipeline with a small dataset to confirm wiring and output:

```python
from artisan.orchestration import StepStatus

pipeline = PipelineManager.create(
    name="test",
    delta_root="test/delta",
    staging_root="test/staging",
)
step = pipeline.run(operation=DataGenerator, params={"count": 3})
assert step.status is StepStatus.SUCCEEDED
assert step.succeeded_count == 1  # One source execution creates three artifacts.

from artisan.visualization import inspect_step

outputs = inspect_step(
    "test/delta", step.step_number, pipeline_run_id=pipeline.config.pipeline_run_id
)
assert outputs.filter(outputs["artifact_type"] == "data").height == 3
pipeline.finalize()
```

---

## Cross-references

- [Configuring Execution](configuring-execution.md) — resources, batching, step runners
- [CompositeDefinition Reference](../reference/composite-definition.md) — composite entry points and usage
- [First Pipeline Tutorial](../tutorials/01-getting-started/01-first-pipeline.ipynb) — interactive walkthrough
- [Execution Flow](../concepts/execution-flow.md) — what happens under the hood
- [Writing Creator Operations](writing-creator-operations.md) — building custom operations
