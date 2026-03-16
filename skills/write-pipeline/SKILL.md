---
name: write-pipeline
description: Write or scaffold an Artisan pipeline script. Use this skill when the user asks to create a pipeline, write a pipeline script, scaffold a pipeline, or build a data processing workflow. Trigger on phrases like "write a pipeline", "create a pipeline", "scaffold a pipeline", "build a pipeline script", "pipeline that does X", or any request to compose operations into a runnable pipeline.
argument-hint: "[description of what the pipeline should do, or list of operations/steps]"
---

# Write an Artisan Pipeline

Write a pipeline script for `$ARGUMENTS`.

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

from pathlib import Path

from artisan.orchestration import PipelineManager

# Import operations used in this pipeline
from artisan.operations.examples.data_generator import DataGenerator
from artisan.operations.examples.data_transformer import DataTransformer
from artisan.operations.examples.metric_calculator import MetricCalculator
from artisan.operations.curator.filter import Filter
from artisan.operations.curator.merge import Merge


def main() -> None:
    """Run the pipeline."""
    delta_root = Path("output/delta")
    staging_root = Path("output/staging")

    pipeline = PipelineManager.create(
        name="my_pipeline",
        delta_root=delta_root,
        staging_root=staging_root,
    )
    output = pipeline.output

    # -- Generate --
    pipeline.run(
        DataGenerator,
        name="generate",
        params={"count": 4, "seed": 42},
    )

    # -- Transform --
    pipeline.run(
        DataTransformer,
        name="transform",
        inputs={"dataset": output("generate", "datasets")},
        params={"scale_factor": 2.0},
    )

    # -- Score --
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
`StepResult`. `submit()` returns `StepFuture` immediately.

```python
pipeline.run(
    operation,                # type[OperationDefinition | CompositeDefinition]
    inputs=None,              # Input wiring (see below)
    name=None,                # str — step name for wiring and display
    params=None,              # dict — operation parameters
    tool=None,                # dict — external tool config
    environment=None,         # str | dict — Apptainer/env config
    execution=None,           # dict — batching config
    backend=None,             # str — "local" or "slurm"
    resources=None,           # dict — CPU/memory/GPU
    failure_policy=None,      # FailurePolicy — CONTINUE or FAIL_FAST
    compact=True,             # bool — compact provenance
    intermediates="discard",  # str — "discard", "persist", or "expose" (composites only)
) -> StepResult
```

## expand() API

Runs a composite in expanded mode — each internal `ctx.run()` becomes its own
pipeline step. Accepts `CompositeDefinition` subclasses only.

```python
pipeline.expand(
    composite,                # type[CompositeDefinition] — the composite class
    inputs=None,              # Input wiring (same as run())
    params=None,              # dict — composite parameters
    resources=None,           # dict — per-operation override
    execution=None,           # dict — per-operation override
    backend=None,             # str — per-operation override
    environment=None,         # str | dict — per-operation override
    tool=None,                # dict — per-operation override
    name=None,                # str — prefix for expanded step names
) -> ExpandedCompositeResult
```

Returns `ExpandedCompositeResult` with `.output(role)` for downstream wiring.
No `intermediates` parameter — all internal steps are naturally visible.

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
| `tool` | `dict` | External tool overrides |
| `environment` | `str \| dict` | Apptainer/environment config |
| `execution` | `dict` | Batching: `artifacts_per_unit`, `units_per_worker`, `max_workers` |
| `backend` | `str` | `"local"` (default) or `"slurm"` |
| `resources` | `dict` | `cpus`, `memory_gb`, `gpus`, `time_limit` |
| `failure_policy` | `FailurePolicy` | `CONTINUE` (default) or `FAIL_FAST` |
| `compact` | `bool` | Compact provenance graph (default `True`) |
| `intermediates` | `str` | `"discard"` (default), `"persist"`, or `"expose"` (composites only) |

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
    {"metric": "quality.score", "step": "quality_check", "operator": "gte", "value": 0.8},
]}
```

### Multi-Input with Pairing

Operations with multiple input roles use `group_by` to pair artifacts:

```python
# On the operation class:
# group_by: ClassVar[GroupByStrategy | None] = GroupByStrategy.LINEAGE

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

### Composite (Collapsed)

Combine tightly coupled operations into a single step with in-memory passing:

```python
class TransformAndScore(CompositeDefinition):
    name = "transform_and_score"
    description = "Transform data then compute metrics"

    class OutputRole(StrEnum):
        METRICS = "metrics"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "metrics": OutputSpec(artifact_type="metric"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        transformed = ctx.run(DataTransformer, inputs={"dataset": ctx.input("data")})
        metrics = ctx.run(MetricCalculator, inputs={"dataset": transformed.output("dataset")})
        ctx.output("metrics", metrics.output("metrics"))

pipeline.run(TransformAndScore, inputs={"data": output("generate", "datasets")})
```

Options: `intermediates="discard"` (default), `"persist"`, or `"expose"`.

### Composite (Expanded)

Each internal operation becomes its own pipeline step with full provenance:

```python
expanded = pipeline.expand(
    TransformAndScore,
    name="expand_ts",
    inputs={"data": output("generate", "datasets")},
)
# Wire downstream from expanded outputs
pipeline.run(Filter, name="filter",
    inputs={"passthrough": expanded.output("metrics")})
```

`expand()` returns `ExpandedCompositeResult` with `.output(role)` for wiring.
No `intermediates` parameter — all internal steps are visible by design.

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
| `delta_root` | `Path \| str` | *(required)* | Delta Lake storage path |
| `staging_root` | `Path \| str` | *(required)* | Temporary worker output path |
| `working_root` | `Path \| str \| None` | `$TMPDIR` | Sandbox for execution |
| `failure_policy` | `FailurePolicy` | `CONTINUE` | Default for all steps |
| `cache_policy` | `CachePolicy` | `ALL_SUCCEEDED` | When to cache step results |
| `backend` | `str` | `"local"` | Default backend for all steps |
| `preserve_staging` | `bool` | `False` | Keep staging dirs after commit |
| `preserve_working` | `bool` | `False` | Keep working dirs after execution |
| `recover_staging` | `bool` | `True` | Recover incomplete staging on resume |

---

## Filter Criteria Format

```python
{
    "metric": "field.name",       # Dot-path into metric content
    "operator": "gt",             # gt, lt, gte, lte, eq, ne, between
    "value": 0.5,                 # Threshold (scalar or [low, high] for between)
    "step": "step_name",          # Optional: disambiguate metric source
    "step_number": 3,             # Optional: alternative to step name
}
```

---

## Resume

Reconstruct pipeline state from Delta Lake and continue:

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

---

## Common Gotchas

- **Always call `finalize()`** after using `submit()` — otherwise the pipeline
  hangs on exit
- **Use `IngestData` for file paths** — raw paths are only accepted by curator
  operations, not creators
- **Role names must match** the operation's `InputRole`/`OutputRole` enum values
  exactly
- **Content-addressed cache** — change `name` or `delta_root` to force re-run
  after code changes
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
