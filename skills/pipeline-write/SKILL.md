---
name: pipeline-write
description: Write or scaffold an Artisan pipeline script. Use this skill when the user asks to create a pipeline, write a pipeline script, scaffold a pipeline, or build a data processing workflow. Trigger on phrases like "write a pipeline", "create a pipeline", "scaffold a pipeline", "build a pipeline script", "pipeline that does X", or any request to compose operations into a runnable pipeline.
---

# Write an Artisan Pipeline

Write the pipeline script described in the user's request. If the invoking
client supplies explicit skill arguments, treat them as the request details.

Before writing, read the example operations in `src/artisan/operations/examples/`
and at least one integration test in `tests/integration/test_data_flow_patterns.py`
to match established patterns.

If the pipeline uses operations from a plugin package (any package that depends
on artisan), explore that package's `operations/` directory to discover available
operations, their input/output roles, and parameter classes. Plugin operations
are used identically to core operations — just different import paths. Core
curator operations (`Filter`, `Merge`) work with any artifact type including
types defined by plugins.

---

## Pipeline Script Template

Follow this structure. Adjust imports, steps, and wiring to match the request.

```python
"""One-line description of what this pipeline does."""

from __future__ import annotations

from artisan.operations.curator import Filter, Merge
from artisan.operations.examples import DataGenerator, DataTransformer, MetricCalculator
from artisan.orchestration import PipelineManager


def main() -> None:
    """Run the pipeline."""
    delta_root = "output/delta"
    staging_root = "output/staging"

    pipeline = PipelineManager.create(
        name="my_pipeline",
        delta_root=delta_root,
        staging_root=staging_root,
    )
    output = pipeline.output

    pipeline.run(
        DataGenerator,
        name="generate",
        params={"count": 4, "seed": 42},
    )

    pipeline.run(
        DataTransformer,
        name="transform",
        inputs={"dataset": output("generate", "datasets")},
        params={"scale_factor": 2.0},
    )

    pipeline.run(
        MetricCalculator,
        name="score",
        inputs={"dataset": output("transform", "dataset")},
    )

    summary = pipeline.finalize()
    print(summary)


if __name__ == "__main__":
    main()
```

---

## run() / submit() API

Both methods accept identical parameters. `run()` blocks and returns
`StepResult`. `submit()` prepares the step synchronously, including waiting for
its inputs, then returns a `StepFuture` for background execution.

```python
result = pipeline.run(
    operation,                # type[OperationDefinition]
    inputs=None,              # Input wiring (see below)
    params=None,              # dict — operation parameters
    step_runner=None,         # "local" | RunnerBase — None uses pipeline default
    runner_resources=None,    # dict | RunnerResources — CPU/memory/GPU
    batch_strategy=None,      # dict | BatchStrategy — batching and concurrency
    environment=None,         # str | dict | Environments — runtime environment
    tool=None,                # dict | ToolSpec — external tool config
    compute_provider=None,    # str | dict | ComputeProvider
    compute_resources=None,   # dict | ComputeResources
    failure_policy=None,      # FailurePolicy — CONTINUE or FAIL_FAST
    cache_policy=None,        # CachePolicy — None inherits pipeline default
    group_by=None,            # GroupByStrategy — per-step pairing override
    compact=True,             # bool — compact provenance
    name=None,                # str — step name for wiring and display
    skip_cache=False,         # bool — bypass cache lookup for this step
)
```

Core accepts `"local"` as its only string runner name. For any optional runner,
import the provider package and pass a configured instance:

```python
from artisan_submitit import SlurmRunner

slurm = SlurmRunner(slurm_partition="gpu")
pipeline.run(
    TrainModel,
    name="train",
    inputs={"dataset": output("prepare", "dataset")},
    step_runner=slurm,
    runner_resources={"gpus": 1, "memory_gb": 32},
)
```

External runners apply to creator operations. Curator operations always run in
an isolated local subprocess, so do not add a runner override to a curator
step.

## run_composite() / submit_composite() API

Runs a composite — each internal `ctx.run()` becomes its own pipeline step.
`run_composite()` waits for the composite's child steps; `submit_composite()`
expands and submits the graph, then returns a `CompositeResult`. Accepts
`CompositeDefinition` subclasses only (passing a composite to `run`/`submit`
raises `TypeError`).

```python
result = pipeline.run_composite(
    composite,                # type[CompositeDefinition] — the composite class
    inputs=None,              # Input wiring (same as run())
    params=None,              # dict — composite parameters
    name=None,                # str — prefix for child step names
    step_runner=None,         # default for every child step
    runner_resources=None,    # default for every child step
    batch_strategy=None,      # default for every child step
    environment=None,         # default for every child step
    tool=None,                # default for every child step
    compute_provider=None,    # default for every child step
    compute_resources=None,   # default for every child step
    failure_policy=None,      # default for every child step
    cache_policy=None,        # default whole-step cache policy for children
    compact=True,             # default for every child step
    skip_cache=False,         # default for every child step
)
```

Composite-level overrides are defaults for each child step; a value set on a
`ctx.run()` call wins for that step. `submit_composite()` returns a
`CompositeResult` with `.output(role)` for downstream wiring and `.wait()` to
block on the children; `run_composite()` returns the resolved `CompositeResult`.
External runner defaults apply to creator children; curator children remain
local.

Import `CachePolicy` from `artisan.schemas`. Use `CachePolicy.ALL_SUCCEEDED`
to accept only succeeded whole steps, or `CachePolicy.STEP_COMPLETED` to also
accept partial steps. `None` inherits the nearest explicit composite policy,
then the pipeline default. An explicit child enum replaces either default.
Successful execution units can still be reused when a partial whole-step hit
is rejected. `skip_cache=True` at the pipeline or step and an operation's
`cacheable=False` declaration bypass both layers regardless of policy.

---

## Input Wiring

| Format | Example | When to use |
|---|---|---|
| No inputs | `None` | Generative operations |
| Single upstream | `{"role": step.output("role")}` | Wire from a StepResult |
| Named lookup | `{"role": output("step_name", "role")}` | Wire by step name (preferred) |
| Multiple streams | `{"a": output("x", "r"), "b": output("y", "r")}` | Merge or multi-input |
| Raw file paths | `["/path/a.csv", "/path/b.csv"]` | Curator ingest only (IngestData) |
| List of OutputReferences | `[output("a", "r"), output("b", "r")]` | Merge with auto-flattened streams |

**Prefer `output("step_name", "role")` over `step.output("role")`** in complex
pipelines for readability. Bind `output = pipeline.output` at the top.

---

## Step Override Reference

| Parameter | Type | Purpose |
|---|---|---|
| `name` | `str` | Step name for wiring and display |
| `params` | `dict` | Operation parameters (keys match `Params` fields) |
| `tool` | `dict \| ToolSpec` | External tool overrides |
| `environment` | `str \| dict \| Environments` | Runtime environment override |
| `step_runner` | `str \| RunnerBase` | `"local"` or an initialized external provider instance |
| `runner_resources` | `dict \| RunnerResources` | `cpus`, `memory_gb`, `gpus`, `time_limit`, provider `extra` values |
| `batch_strategy` | `dict \| BatchStrategy` | `artifacts_per_unit`, `units_per_worker`, `max_workers` |
| `compute_provider` | `str \| dict \| ComputeProvider` | Execute-phase routing target |
| `compute_resources` | `dict \| ComputeResources` | Compute-provider CPU, memory, GPU, and timeout |
| `failure_policy` | `FailurePolicy` | `CONTINUE` (default) or `FAIL_FAST` |
| `cache_policy` | `CachePolicy \| None` | Whole-step acceptance: `ALL_SUCCEEDED` or `STEP_COMPLETED`; `None` inherits |
| `group_by` | `GroupByStrategy` | Per-step input-pairing override |
| `compact` | `bool` | Compact Delta tables after commit (default `True`) |
| `skip_cache` | `bool` | Bypass cache lookup for this step |

---

## Pattern Catalog

### Generative Source

```python
pipeline.run(DataGenerator, name="generate", params={"count": 4, "seed": 42})
```

### File Ingest

```python
pipeline.run(IngestData, name="ingest", inputs=["/data/a.csv", "/data/b.csv"])
```

### Linear Pipeline

```python
pipeline.run(DataGenerator, name="generate", params={"count": 2})
pipeline.run(DataTransformer, name="transform",
    inputs={"dataset": output("generate", "datasets")})
pipeline.run(MetricCalculator, name="score",
    inputs={"dataset": output("transform", "dataset")})
```

### Branching (Fan-out)

Same output reference wired to multiple downstream steps:

```python
pipeline.run(DataGenerator, name="generate", params={"count": 4})
pipeline.run(DataTransformer, name="branch_a",
    inputs={"dataset": output("generate", "datasets")},
    params={"scale_factor": 0.5})
pipeline.run(DataTransformer, name="branch_b",
    inputs={"dataset": output("generate", "datasets")},
    params={"scale_factor": 2.0})
```

### Merge (Fan-in)

Combine multiple streams. Input role names are arbitrary. Output role is always
`"merged"`:

```python
pipeline.run(Merge, name="merge", inputs={
    "small": output("branch_a", "dataset"),
    "large": output("branch_b", "dataset"),
})
# Downstream: output("merge", "merged")
```

### Diamond DAG

Branch, process independently, merge, continue:

```python
pipeline.run(DataGenerator, name="generate", params={"count": 4})
pipeline.run(DataTransformer, name="branch_a",
    inputs={"dataset": output("generate", "datasets")}, params={"scale_factor": 0.5})
pipeline.run(DataTransformer, name="branch_b",
    inputs={"dataset": output("generate", "datasets")}, params={"scale_factor": 2.0})
pipeline.run(Merge, name="merge", inputs={
    "a": output("branch_a", "dataset"),
    "b": output("branch_b", "dataset"),
})
pipeline.run(DataTransformer, name="final",
    inputs={"dataset": output("merge", "merged")})
```

### Metrics + Filter

Score artifacts then filter by thresholds:

```python
pipeline.run(MetricCalculator, name="score",
    inputs={"dataset": output("transform", "dataset")})
pipeline.run(Filter, name="filter",
    inputs={"passthrough": output("transform", "dataset")},
    params={"criteria": [
        {"metric": "distribution.median", "operator": "gt", "value": 0.5},
    ]})
# Downstream: output("filter", "passthrough")
```

Filter auto-discovers metrics via forward provenance. The `passthrough` input
points to the artifacts being filtered (not the metrics step).

### Multi-Metric Filter

Combine criteria from multiple metric sources (AND semantics). Use `step` to
disambiguate collisions:

```python
params={"criteria": [
    {"metric": "distribution.median", "operator": "gt", "value": 0.5},
    {"metric": "quality.score", "step": "quality_check", "operator": "ge", "value": 0.8},
]}
```

### Multi-Input with Pairing

Operations with multiple input roles use `group_by` to pair artifacts:

```python
# On the operation class:
# group_by: GroupByStrategy | None = GroupByStrategy.LINEAGE

pipeline.run(MyMultiInputOp, name="process", inputs={
    "dataset": output("generate", "datasets"),
    "config": output("configure", "configs"),
})
```

Strategies: `LINEAGE` (shared ancestry), `ZIP` (positional), `CROSS_PRODUCT`
(all combinations).

### Iterative Refinement

Use a Python loop — no special API:

```python
datasets = output("generate", "datasets")
for i in range(3):
    pipeline.run(MetricCalculator, name=f"score_r{i}",
        inputs={"dataset": datasets})
    pipeline.run(Filter, name=f"filter_r{i}",
        inputs={"passthrough": datasets},
        params={"criteria": [{"metric": "distribution.median", "operator": "gt", "value": 0.5}]})
    pipeline.run(DataTransformer, name=f"transform_r{i}",
        inputs={"dataset": output(f"filter_r{i}", "passthrough")})
    datasets = output(f"transform_r{i}", "dataset")
```

### Composite

Name a reusable multi-operation sequence, then run it with `run_composite`.
Each internal operation becomes its own pipeline step with full provenance:

```python
from enum import StrEnum
from typing import ClassVar

from artisan.composites import CompositeContext, CompositeDefinition
from artisan.schemas import InputSpec, OutputSpec


class TransformAndScore(CompositeDefinition):
    name = "transform_and_score"
    description = "Transform data then compute metrics"

    class InputRole(StrEnum):
        DATA = "data"

    inputs: ClassVar[dict[str, InputSpec]] = {
        "data": InputSpec(artifact_type="data", required=True),
    }

    class OutputRole(StrEnum):
        METRICS = "metrics"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "metrics": OutputSpec(artifact_type="metric"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        transformed = ctx.run(DataTransformer, inputs={"dataset": ctx.input("data")})
        metrics = ctx.run(MetricCalculator, inputs={"dataset": transformed.output("dataset")})
        ctx.output("metrics", metrics.output("metrics"))

# Blocking — expands into one step per internal ctx.run()
pipeline.run_composite(
    TransformAndScore, inputs={"data": output("generate", "datasets")}
)

# Non-blocking — wire downstream from the CompositeResult
result = pipeline.submit_composite(
    TransformAndScore, inputs={"data": output("generate", "datasets")}
)
pipeline.run(Filter, name="filter",
    inputs={"passthrough": result.output("metrics")})
```

`submit_composite` returns a `CompositeResult` with `.output(role)` for wiring.
Composite-level execution overrides are defaults for every child step.

### Async Steps (submit)

Use `submit()` for non-blocking execution. Always call `finalize()` after:

```python
future_a = pipeline.submit(DataTransformer, name="branch_a",
    inputs={"dataset": output("generate", "datasets")}, params={"scale_factor": 0.5})
future_b = pipeline.submit(DataTransformer, name="branch_b",
    inputs={"dataset": output("generate", "datasets")}, params={"scale_factor": 2.0})

# Can wire futures before they complete
pipeline.run(Merge, name="merge", inputs={
    "a": future_a.output("dataset"),
    "b": future_b.output("dataset"),
})
summary = pipeline.finalize()
```

---

## Pipeline-Level Config

| Parameter | Type | Default | Purpose |
|---|---|---|---|
| `name` | `str` | *(required)* | Pipeline name |
| `delta_root` | `str` | *(required)* | Delta Lake storage URI |
| `staging_root` | `str` | *(required)* | Worker output and recovery evidence URI |
| `working_root` | `str \| None` | `$TMPDIR` | Sandbox for execution |
| `files_root` | `str \| None` | Derived beside `delta_root` | Artisan-managed external files URI |
| `failure_policy` | `FailurePolicy` | `CONTINUE` | Default for all steps |
| `cache_policy` | `CachePolicy` | `ALL_SUCCEEDED` | Default whole-step cache policy |
| `default_step_runner` | `str \| RunnerBase` | `"local"` | Built-in local runner or external provider instance |
| `preserve_staging` | `bool` | `False` | Keep staging after verified commitment; uncommitted evidence is always retained |
| `preserve_working` | `bool` | `False` | Keep working dirs after execution |
| `recover_staging` | `bool` | `True` | Recover eligible completed executions before startup cache lookup |
| `skip_cache` | `bool` | `False` | Bypass cache lookups for every step |

---

## Filter Criteria Format

```python
{
    "metric": "field.name",       # Dot-path into metric content
    "operator": "gt",             # gt, ge, lt, le, eq, ne
    "value": 0.5,                 # Numeric, string, or Boolean comparison value
    "step": "step_name",          # Optional: disambiguate metric source
    "step_number": 3,             # Optional: alternative to step name
}
```

---

## Resume

Only one orchestrator or repair process may write a store at a time. Confirm
that the old driver has stopped before reopening its roots. Both `create()` and
`resume()` recover eligible finished staging before any cache lookup by default.
`recover_staging=False` leaves earlier staging untouched; `preserve_staging=True`
retains files after commitment. These flags do not change computation hashes or
cache policy, and cancellation never authorizes deletion of uncommitted staging.

After interruption, rerun the script with `create()` to reuse recovered execution
units in fresh attempts. The old failed/cancelled status is preserved. `resume()`
restores accepted results and appends steps, but still refuses unresolved pending
or running attempts. It does not reconnect to live provider jobs.

Reconstruct pipeline state from Delta Lake and continue:

`resume(cache_policy=...)` sets the default for new steps. Restored steps keep
their outcomes and recorded policies. Omitting it uses `ALL_SUCCEEDED` for
new steps, even if completed steps used another policy.

```python
pipeline = PipelineManager.resume(
    delta_root=delta_root,
    staging_root=staging_root,
    # pipeline_run_id="...",  # Omit to resume most recent
)
output = pipeline.output

# Completed steps are restored — wire new steps from their outputs
pipeline.run(DataTransformer, name="new_step",
    inputs={"dataset": output("previous_step", "dataset")})
```

Artisan persists an external runner's stable name, not its configured object.
Recreate the provider and pass it as `default_step_runner` when resuming a
pipeline that used it as the default:

```python
from artisan_submitit import SlurmRunner

provider = SlurmRunner(slurm_partition="gpu")
pipeline = PipelineManager.resume(
    delta_root=delta_root,
    staging_root=staging_root,
    default_step_runner=provider,
)
```

---

## Common Gotchas

- **Always call `finalize()`** after using `submit()` — otherwise the pipeline
  hangs on exit
- **Use `IngestData` for file paths** — raw paths are only accepted by curator
  operations, not creators
- **Role names must match** the operation's `InputRole`/`OutputRole` enum values
  exactly
- **Force a re-run** — pass `skip_cache=True`. Step names do not change cache
  identity. Increment the operation's `version` when its computational
  behavior changes so future runs use the new identity.
- **Skipped steps are not cached** — steps that receive empty inputs skip
  gracefully and re-evaluate on next run
- **Filter points to data, not metrics** — the `passthrough` input wires to the
  artifacts being filtered; Filter auto-discovers metrics via provenance
- **Merge output role is always `"merged"`** — regardless of input role names

---

## Style Rules

- Wrap pipeline logic in a `main()` function with `if __name__ == "__main__":`
- Bind `output = pipeline.output` at the top for readability
- Always set `name=` on every step
- Group related steps with `# -- Section --` comments
- Use `Path` objects for roots, not raw strings
- One-line module docstring describing what the pipeline does
- Import operations explicitly — no star imports
