# Composites and Composition

Operations are the unit of computation. Composites are the unit of
*reuse*. When operations are tightly coupled — when you always run
transform-then-score, or preprocess-then-analyze — copying that wiring
into every pipeline duplicates it, and the copies drift over time. A
composite solves this by naming the grouping once: a reusable unit with
declared inputs, outputs, and internal wiring.

This page explains what composites are, why they exist, how they
execute, and how they relate to operations.

---

## The problem composites solve

Consider three approaches to running two tightly coupled operations:

| Approach | Reusable? | Trade-off |
|----------|-----------|-----------|
| Separate `pipeline.run()` calls | No | Flexible but verbose; the wiring is re-typed everywhere it is used |
| Copy-paste the wiring into every pipeline | No | Duplication; the copies diverge over time |
| **Composite** | **Yes** | Define the wiring once, use it anywhere with the same contract |

A composite encapsulates the wiring once. Every pipeline that uses it
gets the same internal structure, the same parameter forwarding, and the
same output contract — without duplicating code.

**Why this matters:** the value of a composite is *meaning* — "these
steps form one named, reusable thing." That is a grouping concern, and it
is the only concern a composite owns. How and where those steps run is a
separate question, answered by the pipeline and the runner layer.

---

## Anatomy of a CompositeDefinition

A composite is a subclass of `CompositeDefinition`. It looks similar to
an `OperationDefinition` in structure — `name`, `InputRole`, `OutputRole`,
`inputs`, `outputs` — but instead of implementing a computation lifecycle,
it implements `compose()`.

For example, a transform-and-score composite connects its dataset input to a
transformer, connects the transformer's output to a metric calculator, and
exposes the calculator's metrics as its own output. `compose()` declares that
wiring; the child operations perform the computation. The
[writing guide](../how-to-guides/writing-composite-operations.md) provides the
complete implementation.

### compose() vs the operation lifecycle

| | `OperationDefinition` | `CompositeDefinition` |
|---|---|---|
| Method to implement | `preprocess`, `execute_function`, `postprocess` (or `execute_curator`) | `compose` |
| Receives | Raw inputs (files, DataFrames) | `CompositeContext` |
| Produces | Artifacts directly | Nothing — delegates to operations |
| Registered in | Operation registry | Composite registry |

---

## How compose() wires operations

`compose()` receives a `CompositeContext`, the wiring surface it uses to
connect operations through three actions:

- **Referencing inputs.** `ctx.input(role)` hands back a reference to one
  of the composite's declared inputs, ready to feed into a step.
- **Running steps.** `ctx.run(operation, ...)` runs an operation (or a
  nested composite) as a real pipeline step and returns a handle. Asking
  that handle for one of its outputs yields a reference you wire into the
  next step.
- **Mapping outputs.** `ctx.output(role, ref)` maps an internal result
  onto one of the composite's declared outputs.

Only refs mapped through `ctx.output()` are visible outside the composite.
Everything else is an internal wiring reference between steps, so the
composite's external contract is exactly its declared inputs and mapped
outputs. See the [composite API entry points](../reference/composite-definition.md)
for the public classes and current definitions.

---

## How a composite executes

A composite has one execution model: **macro-expansion**. When the
pipeline runs a composite, `compose()` runs, and each `ctx.run()` call
submits a *real pipeline step*. There is no separate composite runtime,
no in-worker mini-engine, and no whole-composite cache entry.

```
    generate ──▶ transform_and_score.data_transformer ──▶ transform_and_score.metric_calculator
   (3 datasets)              (3 datasets)                            (3 metrics)
```

Every child has its own cache entry, persisted results, lifecycle and failure
records, provenance edges, and dispatch. It participates in cancellation in the
same way as other steps. Step names are prefixed with the composite name
(dot-separated: `outer.inner.operation` for nested composites) so the
grouping stays legible in the graph.

**Why there is only one model:** "step" is the unit of identity for
caching, tracking, provenance, cancellation, and override validation. A
construct that is one step to some subsystems and many steps to others
would need a hand-maintained parallel implementation in each — and those
implementations drift. A composite is a *naming* over real steps, not a
second kind of step.

### Composite-level overrides are step defaults

Execution overrides on `submit_composite`/`run_composite` act as **defaults for
every child step**. These include placement, resources, batching, and policies
such as `failure_policy` and `cache_policy`. A value set explicitly on a
`ctx.run()` call wins for that setting; anything the child leaves unset falls
back to the composite-level default.

| | Composite-level override | Per-op `ctx.run()` override |
|---|---|---|
| Scope | Default for every child step | One specific child step |
| Precedence | Loses to a per-op value for the same knob | Wins for the knob it sets |
| Granularity | Whole knob (no deep merge) | Whole knob |

This mirrors the default-then-explicit-override shape used by ordinary steps.
The source of an ordinary step's default depends on the knob: for example,
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

A common reason to reach for "run the whole composite as one unit" is
placement: you want the composite's steps to share a single compute
allocation with a warm environment, rather than each step acquiring its
own. That is a real need — but it is a **placement** concern, not a
**grouping** concern, and it belongs to the runner layer, not the
composite.

**Why the separation:** grouping (this composite *means* transform-then-
score) is about identity and reuse. Placement (these steps should run in
one allocation) is about where work lands. Tying the two together forces
every step-level subsystem — caching, provenance, cancellation — to grow
a parallel path for the grouped case. Keeping them separate lets each
concern be solved once, in the layer that owns it.

The runner layer answers placement directly. The optional
`artisan-submitit` package's `SlurmIntraRunner` dispatches creator steps via
`srun` inside an existing allocation, whether or not they are wrapped in a
composite. Because composite-level overrides are child-step defaults, passing
`step_runner=SlurmIntraRunner()` to the composite forwards it to creator
children. Curator children remain in isolated local subprocesses because they
read and write pipeline metadata directly. This gives creator work
one-allocation execution without a second execution model.

---

## Nesting composites

A composite can contain other composites. `ctx.run()` accepts both operation
and composite definitions. A generate-and-score composite could run a generator
and then pass its outputs to a nested transform-and-score composite.

The nested child's operations become ordinary pipeline steps with dot-separated
names (`outer.inner.operation`). Nesting adds grouping without changing the
execution or persistence model. See the
[nesting example](../how-to-guides/writing-composite-operations.md#nesting-composites).

---

## Relationship to operations

A composite is **not** a subclass of `OperationDefinition`. It does not
compute — it composes. The two share structural similarities
(`InputRole`, `OutputRole`, `inputs`, `outputs`) because both need to
declare their data contract, but they are distinct abstractions:

| | Operation | Composite |
|---|---|---|
| Base class | `OperationDefinition` | `CompositeDefinition` |
| Registry | Operation registry | Composite registry |
| Implements | Computation (lifecycle phases or `execute_curator`) | Wiring (`compose`) |
| Can be nested in composites | Yes | Yes |
| Run as pipeline step via | `pipeline.run` / `submit` | `pipeline.run_composite` / `submit_composite` |

Operations are leaves. Composites are branches. Both are nodes in the
pipeline DAG.

---

## Key design decisions

| Decision | Rationale |
|----------|-----------|
| Separate class hierarchy (`CompositeDefinition` not `OperationDefinition`) | Composites wire; operations compute. Mixing them would blur the lifecycle contract |
| One execution model (macro-expansion into real steps) | "Step" is the unit of identity for caching, provenance, cancellation, and validation. A composite names real steps rather than being a second kind of step, so those subsystems need no parallel path |
| Composite-level overrides are step defaults | Every override demonstrably takes effect on the child steps; per-op values win per knob, matching the ordinary default-then-explicit-override shape |
| Placement lives in the runner layer | Co-locating steps in one allocation is a placement concern that benefits all steps, not just composite-wrapped ones; keeping it out of the composite avoids a second execution model |
| `CompositeContext` as the API surface | Provides a uniform wiring interface for `compose()` |
| Frozen `CompositeRef` | Prevents accidental mutation of wiring state between `ctx.run()` calls |
| Subclass validation at definition time | Mismatched roles, missing `compose()`, or missing enums fail at import, not at runtime |

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
