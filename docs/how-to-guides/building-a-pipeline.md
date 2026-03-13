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

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | — | Pipeline identifier (used in logging and run IDs) |
| `delta_root` | `Path \| str` | — | Where Delta Lake tables are written |
| `staging_root` | `Path \| str` | — | Where workers write intermediate files before commit |
| `working_root` | `Path \| str \| None` | `tempfile.gettempdir()` | Worker sandbox directory. Defaults to `$TMPDIR` |
| `failure_policy` | `FailurePolicy` | `CONTINUE` | How to handle step failures (`CONTINUE` or `FAIL_FAST`) |
| `cache_policy` | `CachePolicy` | `ALL_SUCCEEDED` | When completed steps qualify as cache hits (`ALL_SUCCEEDED` or `STEP_COMPLETED`) |
| `backend` | `str \| BackendBase` | `"local"` | Default execution backend. Accepts an instance or string name (`"local"`, `"slurm"`) |
| `preserve_staging` | `bool` | `False` | Keep staging files after commit (debugging) |
| `preserve_working` | `bool` | `False` | Keep worker sandboxes after execution (debugging) |
| `recover_staging` | `bool` | `True` | Commit leftover staging files from prior crashed runs at init |

Both `delta_root` and `staging_root` are created automatically if they do not
exist. For production SLURM runs, omit `working_root` — the default uses
node-local scratch, which avoids shared filesystem contention.

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
pipeline.run(operation=DataTransformer, inputs={"dataset": output("generate", "datasets")})
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
print(summary["pipeline_name"])     # "my_pipeline"
print(summary["total_steps"])       # 3
print(summary["overall_success"])   # True
```

`finalize()` is required when using `submit()` (see below). With `run()` only,
it is optional but still recommended — it produces the summary and cleans up
the executor.

---

## Common patterns

### `run()` vs `submit()`

Both accept the same parameters. The difference is blocking behavior:

| | `run()` | `submit()` |
|---|---------|------------|
| Returns | `StepResult` (blocks until done) | `StepFuture` (returns immediately) |
| Wiring downstream | `step.output("role")` | `future.output("role")` — works identically |
| Use when | Steps must complete before continuing | Steps can overlap |
| `finalize()` | Optional | Required — waits for all futures |

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

### Step-level overrides

Both `run()` and `submit()` accept override parameters beyond `operation`,
`inputs`, `params`, and `name`:

| Parameter | Purpose |
|-----------|---------|
| `backend` | Override the pipeline's default compute backend for this step |
| `resources` | Override resource allocation (CPUs, memory, GPUs, time limit) |
| `execution` | Override batching settings (`artifacts_per_unit`, `max_workers`) |
| `environment` | Override the operation's runtime environment |
| `tool` | Override the operation's external tool configuration |
| `failure_policy` | Override the pipeline's failure policy for this step |
| `compact` | Run Delta Lake compaction after commit (default `True`) |

See [Configuring Execution](configuring-execution.md) for details on each.

### Branching (parallel paths)

Feed the same output into multiple independent steps:

```python
output = pipeline.output
pipeline.run(operation=DataGenerator, name="generate", params={"count": 10})
pipeline.submit(operation=TransformA, name="branch_a", inputs={"data": output("generate", "datasets")})
pipeline.submit(operation=TransformB, name="branch_b", inputs={"data": output("generate", "datasets")})
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

A composite groups multiple operations into a reusable unit. Define one by
subclassing `CompositeDefinition` and implementing `compose()`:

```python
from enum import StrEnum
from typing import ClassVar

from artisan.composites import CompositeDefinition, CompositeContext
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

class TransformAndScore(CompositeDefinition):
    """Transform data then compute metrics."""

    name = "transform_and_score"

    class InputRole(StrEnum):
        DATASET = "dataset"

    class OutputRole(StrEnum):
        METRICS = "metrics"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATASET: InputSpec(artifact_type="data", required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.METRICS: OutputSpec(artifact_type="metric"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        transformed = ctx.run(
            DataTransformer,
            inputs={"dataset": ctx.input("dataset")},
            params={"scale_factor": 2.0},
        )
        scored = ctx.run(
            MetricCalculator,
            inputs={"dataset": transformed.output("dataset")},
        )
        ctx.output("metrics", scored.output("metrics"))
```

Use `pipeline.run()` for **collapsed** execution (single step, in-memory
artifact passing) or `pipeline.expand()` for **expanded** execution (each
internal operation becomes its own pipeline step):

```python
output = pipeline.output
pipeline.run(operation=DataGenerator, name="gen", params={"count": 10})

# Collapsed — single step
pipeline.run(operation=TransformAndScore, name="ts",
             inputs={"dataset": output("gen", "datasets")})

# Expanded — each internal operation is a separate step
pipeline.expand(composite=TransformAndScore, name="ts",
                inputs={"dataset": output("gen", "datasets")})
```

For the full guide on writing composites, see
[Writing Composite Operations](writing-composite-operations.md).

**Intermediates handling** controls what happens to artifacts produced by
operations before the final one:

| Mode | Behavior |
|------|----------|
| `"discard"` (default) | Intermediates discarded after the composite completes |
| `"persist"` | Intermediates committed to Delta Lake with internal provenance edges |
| `"expose"` | Like `"persist"`, but execution edges include intermediate outputs |

### SLURM execution

Dispatch a step to SLURM:

```python
from artisan.orchestration import Backend

pipeline.run(
    operation=DataTransformer,
    name="transform",
    inputs={"dataset": output("generate", "datasets")},
    backend=Backend.SLURM,
    resources={"gpus": 1, "memory_gb": 16, "extra": {"partition": "gpu"}},
)
```

See [Configuring Execution](configuring-execution.md) for the full list of
resource and batching options.

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

### List previous runs

Inspect all pipeline runs stored in a delta root:

```python
runs = PipelineManager.list_runs(delta_root="runs/delta")
print(runs)  # polars DataFrame with run IDs, step counts, and timestamps
```

---

## Common pitfalls

| Problem | Cause | Fix |
|---------|-------|-----|
| `Output role 'X' not available` | Mismatched role name in `.output()` | Check the operation's output role names |
| Downstream step receives 0 artifacts | Upstream step failed or filtered everything out | Check `step.success` and `step.succeeded_count` |
| `Raw file paths are not allowed for creator operations` | Passed a file path list to a creator operation | Use `IngestData` first, then wire its output |
| Pipeline hangs on exit | Forgot `finalize()` after using `submit()` | Call `pipeline.finalize()` |
| Stale results after code change | Content-addressed cache hit from a previous run | Use a fresh `delta_root` |

---

## Verify

Run your pipeline with a small dataset to confirm wiring and output:

```python
pipeline = PipelineManager.create(
    name="test", delta_root="test/delta", staging_root="test/staging",
)
step = pipeline.run(operation=DataGenerator, params={"count": 3})
assert step.success
assert step.succeeded_count == 3
```

---

## Cross-references

- [Configuring Execution](configuring-execution.md) — resources, batching, backends
- [First Pipeline Tutorial](../tutorials/getting-started/01-first-pipeline.ipynb) — interactive walkthrough
- [Execution Flow](../concepts/execution-flow.md) — what happens under the hood
- [Writing Creator Operations](writing-creator-operations.md) — building custom operations
