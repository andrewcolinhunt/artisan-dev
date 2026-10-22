# Composites and Composition

A composite reuses operation wiring. A transform-and-score composite, for
example, connects a transformer to a metric calculator and exposes their
outputs through one declared interface. Each child still runs as an ordinary
pipeline step.

Use a composite when several pipelines need the same sequence or when a named
composition makes the pipeline easier to read. Keep a single operation as an
operation; a composite adds no computation lifecycle of its own.

## Anatomy of a CompositeDefinition

A composite subclasses `CompositeDefinition` and declares `name`, `InputRole`,
`OutputRole`, `inputs`, and `outputs`. Its `compose()` method connects child
operations.

The transform-and-score example connects its dataset input to a transformer,
passes the transformed data to a metric calculator, and exposes the metrics.
The [writing guide](../how-to-guides/writing-composite-operations.md) provides the
complete implementation.

### compose() vs the operation lifecycle

| | `OperationDefinition` | `CompositeDefinition` |
|---|---|---|
| Method to implement | Creator lifecycle with `execute_function` or `execute_command`, or curator `execute_curator` | `compose` |
| Receives | Phase input models or curator DataFrames | `CompositeContext` |
| Produces | Artifacts directly | Nothing — delegates to operations |
| Registered in | Operation registry | Composite registry |

---

## How compose() wires operations

`compose()` receives a `CompositeContext` with methods to connect operations:

- `ctx.input(role)` references a declared input.
- `ctx.run(operation, ...)` submits an operation or nested composite and returns
  a handle. Its `.output(role)` references an output for the next child.
- `ctx.output(role, ref)` exposes an internal result as a composite output.

The composite's `.output(role)` exposes refs mapped through `ctx.output()`.
Child steps remain visible in pipeline inspection and provenance. See the [composite API entry points](../reference/composite-definition.md)
for the public classes and current definitions.

---

## How a composite executes

Running a composite calls `compose()`, which submits its children as ordinary
pipeline steps. This is **macro-expansion**: there is no separate worker runtime
or whole-composite cache entry.

```
    generate ──▶ transform_and_score.data_transformer ──▶ transform_and_score.metric_calculator
   (3 datasets)              (3 datasets)                            (3 metrics)
```

Every child has its own cache entry, persisted results, lifecycle and failure
records, provenance edges, and dispatch. It participates in cancellation in the
same way as other steps. Step names are prefixed with the composite name
(dot-separated: `outer.inner.operation` for nested composites) so the
grouping stays legible in the graph.

### Composite-level overrides are step defaults

Execution overrides on `submit_composite`/`run_composite` act as **defaults for
every child step**. These include placement, resources, batching, and policies
such as `failure_policy` and `cache_policy`. A value set explicitly on a
`ctx.run()` call wins for that setting; anything the child leaves unset falls
back to the composite-level default.

Each override replaces a setting as a whole; nested dictionaries are not merged.
For ordinary steps, the default depends on the setting:
`step_runner` falls back to the pipeline default, while `compute_provider`
falls back to the operation declaration. `cache_policy` inherits from the
nearest composite default, then the pipeline default. Algorithm `params` are
forwarded explicitly by `compose()` rather than inherited as child defaults.

### Submission and waiting

`submit_composite()` calls `compose()` and submits its children synchronously.
Preparation and predecessor waits can therefore block the call. Its returned
`CompositeResult` exposes output references and can wait for remaining children.
`run_composite()` also waits for every child before returning.

The current `PipelineManager` executes steps serially. Parallelism is within a
creator step's worker batches; wrapping steps in a composite does not add
parallel step execution. See
[Synchronous and asynchronous execution](architecture-overview.md#synchronous-and-asynchronous-execution).

---

## What about running everything in one allocation?

Use a runner when creator steps need to share one compute allocation. The
choice applies to ordinary steps and to composite children alike.

The optional `artisan-submitit` package's `SlurmIntraRunner` dispatches creator steps via
`srun` inside an existing allocation, whether or not they are wrapped in a
composite. Because composite-level overrides are child-step defaults, passing
`step_runner=SlurmIntraRunner()` to the composite forwards it to creator
children. Curator children remain in isolated local subprocesses. They query the store
and return results for the orchestrator to commit.

---

## Nesting composites

A composite can contain other composites. `ctx.run()` accepts both operation
and composite definitions. A generate-and-score composite could run a generator
and then pass its outputs to a nested transform-and-score composite.

The nested child's operations become ordinary pipeline steps with dot-separated
names (`outer.inner.operation`). Nesting adds grouping without changing the
execution or persistence model. See the
[nesting example](../how-to-guides/writing-composite-operations.md#author-composite-nesting).

---

## Relationship to operations

A composite and an operation both declare input and output roles, but have
separate base classes and registries. Submit operations with `pipeline.run()`
or `submit()`; submit composites with `run_composite()` or `submit_composite()`.
Either can be a child of another composite.

---

## Cross-references

- [CompositeDefinition Reference](../reference/composite-definition.md) — Public
  entry points and links to current definitions
- [Writing Composite Operations](../how-to-guides/writing-composite-operations.md) —
  step-by-step guide
- [Composable Operations Tutorial](../tutorials/02-pipeline-design/07-composites.ipynb) —
  interactive examples
- [Operations Model](operations-model.md) — the operation abstractions that
  composites compose
- [Execution Flow](execution-flow.md) — how a composite's steps fit into the
  dispatch-execute-commit lifecycle
- [Architecture Overview](architecture-overview.md) — where composites sit in
  the five-layer architecture
