# Build a Pipeline

Create a pipeline, use operations or composites as steps, and connect their
outputs to the next step.

**Prerequisites:** [Operations Model](../concepts/operations-model.md)
and operations to compose. The example below uses Artisan's built-in examples.

Check your application's existing operations before writing a new one. Use the
[creator authoring guide](writing-creator-operations.md) if none fits your task.

---

## Minimal working example

A complete pipeline that generates data, transforms it, and computes metrics:

```python
from pathlib import Path

from artisan.orchestration import PipelineManager
from artisan.operations.examples import DataGenerator, DataTransformer, MetricCalculator

run_root = Path("runs").resolve()
pipeline = PipelineManager.create(
    name="quickstart",
    delta_root=str(run_root / "delta"),
    staging_root=str(run_root / "staging"),
)
generated = pipeline.run(
    DataGenerator, name="generate", params={"count": 5, "seed": 42}
)
transformed = pipeline.run(
    DataTransformer,
    name="transform",
    inputs={"dataset": generated.output("datasets")},
    params={"scale_factor": 2.0, "noise_amplitude": 0.0},
)
pipeline.run(
    MetricCalculator,
    name="metrics",
    inputs={"dataset": transformed.output("dataset")},
)
summary = pipeline.finalize()
assert summary["overall_success"]
```

The remaining examples assume the imports above and an open pipeline. Use
distinct names when trying several examples in the same run.

---

## Create a pipeline

```python
pipeline = PipelineManager.create(
    name="my_pipeline",
    delta_root=str(run_root / "delta"),
    staging_root=str(run_root / "staging"),
)
```

`delta_root` contains the durable store; `staging_root` holds workers' pending
results. Use absolute local paths so workers can find the same locations from
their execution directories. Use [Configure Execution](configuring-execution.md) for worker
resources, batching, cache policy, and diagnostic directories. Exact arguments
are available through the [Python API entry points](../reference/python-api.md).

Both `delta_root` and `staging_root` are created automatically if they do not
exist. Cluster runner providers can map the default `working_root` to
node-local scratch to avoid shared filesystem contention.

---

## Add steps

Pass an operation or composite class to `pipeline.run()`. Both return a
`StepResult` whose `.output(role)` connects to another step. Source operations
need no inputs; external files enter through ingestion.

### Source (no inputs)

A source step creates artifacts without upstream inputs:

```python
generated = pipeline.run(DataGenerator, name="generate", params={"count": 10})
```

### Sequential (one input)

Wire the output of one step to the input of the next using `output()`:

```python
transformed = pipeline.run(
    DataTransformer,
    name="transform",
    inputs={"dataset": generated.output("datasets")},
    params={"scale_factor": 2.0},
)
```

`generated.output("datasets")` returns an `OutputReference` that already
identifies the source store, exact step attempt, and output role. Artisan resolves
its accepted artifacts when the consumer runs. A later retry does not change the
reference. The dict key (`"dataset"`) must match the consumer's input role name.

### Ingest (external files)

Bring files from disk into the pipeline as artifacts:

```python
from artisan.operations.curator import IngestCSV

pipeline.run(operation=IngestCSV, name="ingest", inputs=["/data/a.csv", "/data/b.csv"])
```

Raw file paths are auto-promoted to `FileRefArtifact` and committed to Delta
Lake before the operation runs. The output role for `IngestCSV` is `"csv"`.

`IngestCSV` converts and stages each file before moving to the next. It keeps
one execution and one cache entry for the complete import. Each CSV must fit
in worker memory, but the converted contents of all files do not stay in memory
together. The commit also needs
[local scratch space](../concepts/storage-and-delta-lake.md#memory-and-local-scratch-space).

The converted outputs become accepted only after the complete execution seals
and its logical commit completes. If conversion fails partway through, none of
that execution's converted CSVs become accepted outputs. Registered raw file
references remain available, and a retry starts conversion from the beginning.

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

When a pipeline contains multiple steps with the same name, `output()` returns
the most recent one by default. To reference a specific instance, pass
`step_number`:

```python
output("gen", "datasets", step_number=0)
```

Prefer returned handles when wiring new steps:

```python
generated = pipeline.run(DataGenerator, params={"count": 5})
pipeline.run(DataTransformer, inputs={"dataset": generated.output("datasets")})
```

Name lookup is useful when retrieving a step you did not keep in a variable.
Both forms capture an exact source attempt when the reference is created.

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

Call `finalize()` when you have finished submitting steps, including when every
step used `run()`. It releases the pipeline's executor and runner resources.

---

## Common patterns

### `run()` vs `submit()`

`run()` waits and returns a `StepResult`. `submit()` freezes the call, records a
pending step, and returns a `StepFuture`. Both wire downstream with `.output("role")`.
Use `submit()` to keep the caller available, then call `.result()` for the result
or `finalize()` to wait for all work and clean up. Operations and composites use
these same methods. A curator with output roles discovered during execution
requires `future.result().output(role)` for those roles.

```python
generated = pipeline.submit(DataGenerator, name="generate", params={"count": 100})
pipeline.submit(
    DataTransformer,
    name="transform",
    inputs={"dataset": generated.output("datasets")},
)
summary = pipeline.finalize()
```

### Context-manager form

Use a `with` block when all steps belong to one block of code:

```python
with PipelineManager.create(
    name="batch",
    delta_root=str(run_root / "delta"),
    staging_root=str(run_root / "staging"),
) as pipeline:
    generated = pipeline.run(DataGenerator, name="generate")
    pipeline.run(
        DataTransformer,
        name="transform",
        inputs={"dataset": generated.output("datasets")},
    )
# __exit__ calls finalize() automatically.
```

Leaving the block finalizes the pipeline. For a notebook that adds steps across
cells, keep the manager open and call `finalize()` in the last cell.

### Step-level overrides

Both `run()` and `submit()` accept per-step runner, resource, batching,
cache, and failure-policy overrides. See
[Configuring Execution](configuring-execution.md) for examples and inheritance
rules, and [Python API lookup](../reference/python-api.md) for exact arguments.

### Branching

Feed the same output into separate steps with different parameters:

```python
generated = pipeline.run(DataGenerator, params={"count": 10, "seed": 42})
branch_a = pipeline.submit(
    DataTransformer,
    name="branch_a",
    inputs={"dataset": generated.output("datasets")},
    params={"scale_factor": 2.0, "noise_amplitude": 0.0},
)
branch_b = pipeline.submit(
    DataTransformer,
    name="branch_b",
    inputs={"dataset": generated.output("datasets")},
    params={"scale_factor": 3.0, "noise_amplitude": 0.0},
)
```

### Merging branches

Combine multiple streams into one with `Merge`:

```python
from artisan.operations.curator import Merge

merged = pipeline.run(
    Merge,
    name="merge",
    inputs=[branch_a.output("dataset"), branch_b.output("dataset")],
)
# Downstream uses: merged.output("merged")
```

Pass inputs as a list. The merged output role is always `"merged"`.

### Filtering by metrics

Use `Filter` to keep artifacts that meet criteria:

```python
from artisan.operations.curator import Filter

filtered = pipeline.run(
    Filter,
    name="filter",
    inputs={"passthrough": transformed.output("dataset")},
    params={
        "criteria": [
            {"metric": "distribution.median", "operator": "gt", "value": 0.5},
        ],
    },
)
# Downstream uses: filtered.output("passthrough")
```

`"passthrough"` names both the input and output role. `Filter` discovers descendant
metrics and requires every criterion to pass. See the
[Filter guide](writing-curator-operations.md#filter) for ambiguous metric names
and threshold previews.

### Composing operations with composites

Use the same call for a reusable composite. This example uses the installed
`TransformAndScore`; [Write Composite Operations](writing-composite-operations.md)
shows how it is defined. Its children run in one worker per unit, with one
persistence and cache boundary:

```python
from artisan.composites.examples import TransformAndScore

generated = pipeline.run(DataGenerator, params={"count": 10})
combined = pipeline.run(
    TransformAndScore,
    inputs={"dataset": generated.output("datasets")},
)
```

`combined.output("dataset")` and `combined.output("metrics")` connect to later
steps exactly like an operation result. Execution settings apply to the outer
submission. See
[composite submission](writing-composite-operations.md#forward-execution-overrides).

### Optional SLURM execution

Install matching Artisan and `artisan-submitit` packages. Prepare a locked worker
project containing both packages and your application definitions, then pass the
configured runner to the step. This continuation assumes the existing pipeline's
store, staging, files, and working roots are visible at the same absolute paths
on the compute nodes. Replace the shared paths and partition with your site's
settings:

```python
from pathlib import Path

from artisan.schemas import Environment, RunnerResources
from artisan_submitit import SlurmRunner

shared = Path("/shared/artisan/example")
runner = SlurmRunner(
    environment=Environment.from_pixi(
        Path("/shared/software/pixi.toml"), name="worker"
    ),
    cache_root=shared / "prepared",
    log_folder=shared / "scheduler",
    slurm_partition="cpu",
)
resources = RunnerResources(cpus=1, memory_gb=2, time_limit="00:05:00")
runner.prepare(DataTransformer, runner_resources=resources)

pipeline.run(
    operation=DataTransformer,
    name="transform",
    inputs={"dataset": generated.output("datasets")},
    step_runner=runner,
    runner_resources=resources,
)
```

The worker project and prepared cache must also be visible at the same absolute
paths on the nodes. Preparation may install software; submission only verifies
and uses the prepared target.

See [Configuring Execution](configuring-execution.md) for resource and batching recipes.

(reuse-another-run)=
### Use results from another run

A pipeline run groups submissions and manages their execution. The store holds
accepted results that other runs can reuse. Pass a result's `.output(role)` to
another pipeline using that same store; the reference keeps its original source.

If you no longer have the result handle, retrieve it with `get_step`. This
complete example creates two runs in one temporary store:

```python
from pathlib import Path
from tempfile import TemporaryDirectory

from artisan.operations.examples import DataGenerator, MetricCalculator
from artisan.orchestration import PipelineManager, StepStatus
from artisan.visualization import inspect_step

with TemporaryDirectory() as directory:
    root = Path(directory)
    roots = {"delta_root": str(root / "delta"), "staging_root": str(root / "staging")}
    with PipelineManager.create(name="generate", **roots) as first:
        generated = first.run(DataGenerator, params={"count": 2, "seed": 42})
        assert generated.status is StepStatus.SUCCEEDED, generated.error
        source_run_id = first.config.pipeline_run_id

    with PipelineManager.create(name="measure", **roots) as second:
        saved = second.get_step(generated.step_number, pipeline_run_id=source_run_id)
        assert saved.output("datasets") == generated.output("datasets")
        measured = second.run(
            MetricCalculator,
            inputs={"dataset": saved.output("datasets")},
        )
        assert measured.status is StepStatus.SUCCEEDED, measured.error
        outputs = inspect_step(
            roots["delta_root"],
            measured.step_number,
            pipeline_run_id=second.config.pipeline_run_id,
        )
        assert outputs.height == 2
```

The temporary store is deleted when this example ends. Use persistent roots to
keep your results. `get_step` selects the latest attempt at the requested logical
step position when called; the returned handle stays bound to that attempt.
Unlike `pipeline[index]`, its number is a logical step number rather than a list
index. It reads the source result without resuming or changing the earlier run.
A failed latest attempt is returned as failed, without substituting an older success.

Within one manager, a downstream submission waits for a pending source future.
A source owned by another manager must already be terminal with resolved worker
ownership. Wait on that source's `.result()` before submitting the consumer if
needed. A consumer never takes over or cancels the other run's workers.
Different stores require an explicit import rather than an output reference.

### Resume a previous run

Use `create()` with the same roots to rerun a pipeline script. Already accepted
work may be reused when its inputs and qualified software match. Creation does
not scan orphan staging and adopt unclaimed results.
First confirm that the previous orchestrator has stopped: only one driver or
repair process may write these roots at a time.

Use `resume()` to restore an existing run and append steps:

```python
pipeline = PipelineManager.resume(
    delta_root=str(run_root / "delta"),
    staging_root=str(run_root / "staging"),
)
```

`resume()` reconstructs step results from Delta Lake and sets the step counter
so new steps continue the sequence. Pass `pipeline_run_id="..."` to resume a
specific run; omit it to resume the most recent. Pass `name="..."` to override
the pipeline name. An unresolved `pending` or `running` attempt still prevents
resume; recovering a previously claimed unit does not prove the old worker stopped.
Rerun with `create()` to reuse those units in new attempts.

`resume(recover_staging=False)` disables selected-result recovery.
`preserve_staging=True` retains staging after commitment. These controls are
independent of cache policy. See
[Staging preservation and recovery](../concepts/storage-and-delta-lake.md#staging-preservation)
for the retention rules and interrupted-work example.

If the pipeline used an external default step runner, recreate its configured
instance before resuming. For a Slurm run, use the explicit environment and
shared-path setup above, with the settings used by that run. Artisan persists
the runner's stable name for provenance, not the configured Python object:

```python
# `runner` is the configured SlurmRunner from the setup above.
pipeline = PipelineManager.resume(
    delta_root=str(run_root / "delta"),
    staging_root=str(run_root / "staging"),
    default_step_runner=runner,
)
```

Core can reconstruct the built-in `"local"` runner by name; it does not
register or reconstruct external providers.

### List previous runs

Inspect all pipeline runs stored in a delta root:

```python
from artisan.orchestration import list_runs

runs = list_runs(delta_root=str(run_root / "delta"))
print(runs)  # polars DataFrame with run IDs, step counts, and timestamps
```

---

## Common pitfalls

| Problem | Cause | Fix |
|---------|-------|-----|
| `Output role 'X' not available` | Mismatched role name in `.output()` | Check the operation's output role names |
| Downstream step receives 0 artifacts | Upstream step failed or filtered everything out | Check `step.status` and `step.succeeded_count` |
| `Raw file paths are not allowed for creator operations` | Passed a file path list to a creator operation | Use `IngestCSV` first, then wire its output |
| Pipeline hangs on exit | Forgot `finalize()` after using `submit()` | Call `pipeline.finalize()` |
| Re-run a step after changing code or external state | A prior cached result is still eligible | Pass `skip_cache=True`; see [Bypass caching](configuring-execution.md#bypass-caching-and-resume). Use a fresh store only when you want separate history |

---

## Verify

Run your pipeline with a small dataset to confirm wiring and output:

```python
from artisan.orchestration import StepStatus

test_root = Path("test").resolve()
pipeline = PipelineManager.create(
    name="test",
    delta_root=str(test_root / "delta"),
    staging_root=str(test_root / "staging"),
)
step = pipeline.run(operation=DataGenerator, params={"count": 3, "seed": 42})
assert step.status is StepStatus.SUCCEEDED
assert step.succeeded_count == 1  # One source execution creates three artifacts.

from artisan.visualization import inspect_step

outputs = inspect_step(
    str(test_root / "delta"),
    step.step_number,
    pipeline_run_id=pipeline.config.pipeline_run_id,
)
assert outputs.filter(outputs["artifact_type"] == "csv").height == 3
assert pipeline.finalize()["overall_success"]
```

---

## Cross-references

- [Configuring Execution](configuring-execution.md) — resources, batching, step runners
- [CompositeDefinition Reference](../reference/composite-definition.md) — composite entry points and usage
- [First Pipeline Tutorial](../tutorials/01-getting-started/01-first-pipeline.ipynb) — interactive walkthrough
- [Execution Flow](../concepts/execution-flow.md) — what happens under the hood
- [Writing Creator Operations](writing-creator-operations.md) — building custom operations
