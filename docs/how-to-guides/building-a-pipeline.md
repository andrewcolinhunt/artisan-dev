# Build a Pipeline

How to create a pipeline, wire steps together, and run it to completion.

**Prerequisites:** Familiarity with [Operations Model](../concepts/operations-model.md)
and at least one [operation type](writing-creator-operations.md).

**Key types:** `PipelineManager`, `StepResult`, `StepFuture`, `OutputReference`,
`FailurePolicy`, `CachePolicy`

---

## Minimal working example

A complete pipeline in 10 lines — generate data, transform it, compute metrics:

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

### Chain (one input)

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
Lake before the operation runs.

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
    inputs={
        "passthrough": output("transform", "dataset"),
        "quality": output("metrics", "metrics"),
    },
    params={
        "criteria": [
            {"metric": "quality.distribution.median", "operator": "gt", "value": 0.5},
        ],
    },
)
```

- `"passthrough"` is the stream being filtered. `"quality"` is a named metric
  stream.
- Criteria reference metrics as `"role.field"` (explicit) or bare `"field"`
  (implicit, auto-resolved via provenance).
- All criteria are AND'd.

### SLURM execution

Dispatch a step to SLURM:

```python
from artisan.orchestration import Backend

pipeline.run(
    operation=DataTransformer,
    name="transform",
    inputs={"dataset": output("generate", "datasets")},
    backend=Backend.SLURM,
    resources={"partition": "gpu", "gres": "gpu:1", "mem_gb": 16},
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
specific run; omit it to resume the most recent.

---

## Common pitfalls

| Problem | Cause | Fix |
|---------|-------|-----|
| `Output role 'X' not available` | Mismatched role name in `.output()` | Check the operation's output role names |
| Downstream step receives 0 artifacts | Upstream step failed or filtered everything out | Check `step.success` and `step.succeeded_count` |
| `Raw file paths are not allowed for creator operations` | Passed a file path list to a non-curator operation | Use `IngestData` first, then wire its output |
| Pipeline hangs on exit | Forgot `finalize()` after using `submit()` | Call `pipeline.finalize()` |
| Stale results after code change | Content-addressed cache hit from a previous run | Use a fresh `delta_root` or change `name` |

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
