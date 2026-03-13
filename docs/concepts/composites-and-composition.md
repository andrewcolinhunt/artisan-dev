# Composites and Composition

Operations are the unit of computation. Composites are the unit of
*reuse*. When operations are tightly coupled — when you always run
transform-then-score, or preprocess-then-analyze — extracting each pair
as a separate pipeline step wastes I/O on intermediate artifacts that are
immediately consumed. A composite solves this by grouping operations into
a reusable unit with declared inputs, outputs, and internal wiring.

This page explains what composites are, why they exist, how they execute,
and when to use them.

---

## The problem composites solve

Consider three approaches to running two tightly coupled operations:

| Approach | Reusable? | Intermediate I/O | Trade-off |
|----------|-----------|-------------------|-----------|
| Separate `pipeline.run()` calls | No | Full Delta Lake round-trip | Flexible but verbose; intermediates written and re-read |
| Copy-paste the wiring into every pipeline | No | Full round-trip | Duplication; wiring diverges over time |
| **Composite** | **Yes** | **Configurable** | Define once, use anywhere; caller chooses execution mode |

A composite encapsulates the wiring once. Every pipeline that uses it
gets the same internal structure, the same parameter forwarding, and the
same output contract — without duplicating code.

---

## Anatomy of a CompositeDefinition

A composite is a subclass of `CompositeDefinition`. It looks similar to
an `OperationDefinition` in structure — `name`, `InputRole`, `OutputRole`,
`inputs`, `outputs` — but instead of implementing a computation lifecycle,
it implements `compose()`.

```python
class TransformAndScore(CompositeDefinition):
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
        )
        scored = ctx.run(
            MetricCalculator,
            inputs={"dataset": transformed.output("dataset")},
        )
        ctx.output("metrics", scored.output("metrics"))
```

The key difference from an operation: **compose does not compute**.
It wires. Each `ctx.run()` delegates to a real operation. The composite
itself produces no artifacts — it orchestrates the operations that do.

### compose() vs the operation lifecycle

| | `OperationDefinition` | `CompositeDefinition` |
|---|---|---|
| Method to implement | `preprocess`, `execute`, `postprocess` (or `execute_curator`) | `compose` |
| Receives | Raw inputs (files, DataFrames) | `CompositeContext` |
| Produces | Artifacts directly | Nothing — delegates to operations |
| Registered in | Operation registry | Composite registry |

---

## How compose() wires operations

`compose()` receives a `CompositeContext` with three methods:

### `ctx.input(role)` → `CompositeRef`

Reference a declared input of the composite. The returned `CompositeRef`
is passed as an input to `ctx.run()`:

```python
dataset_ref = ctx.input("dataset")
```

### `ctx.run(operation, ...)` → `CompositeStepHandle`

Execute an operation (or nested composite). Returns a handle whose
`.output(role)` method produces a `CompositeRef` for wiring to the next
operation:

```python
handle = ctx.run(DataTransformer, inputs={"dataset": dataset_ref})
transformed_ref = handle.output("dataset")
```

### `ctx.output(role, ref)`

Map an internal result to a declared output of the composite:

```python
ctx.output("metrics", scored.output("metrics"))
```

Only refs that are mapped via `ctx.output()` are visible outside the
composite. Everything else is an intermediate.

---

## Collapsed vs expanded execution

The same composite definition supports two execution modes. The pipeline
caller chooses.

| | Collapsed | Expanded |
|---|---|---|
| Pipeline method | `pipeline.run(MyComposite, ...)` | `pipeline.expand(MyComposite, ...)` |
| Pipeline steps | 1 | N (one per internal operation) |
| Internal I/O | In-memory | Delta Lake round-trips |
| Caching | Composite-level | Per-operation |
| Batching | Composite-level | Per-operation |
| Worker dispatch | All internal ops on one worker | Each op dispatched independently |
| Intermediates | Configurable (discard/persist/expose) | Always persisted |

### When to use each

**Collapsed** when:
- Internal operations are always run together
- Intermediate artifacts are not needed after the composite completes
- You want to minimize Delta Lake I/O
- The combined computation fits on a single worker

**Expanded** when:
- Internal operations have different resource requirements (e.g., GPU vs CPU)
- You want independent caching per operation
- You need to inspect intermediate results as first-class pipeline steps
- Operations can benefit from independent parallelism

### How it works

```
Collapsed:
  Worker
  ┌────────────────────────────────────────┐
  │  ctx.run(OpA) ──in-memory──▶ ctx.run(OpB)  │
  │         │                         │         │
  │         ▼                         ▼         │
  │    intermediates            declared outputs │
  │    (configurable)           (committed)      │
  └────────────────────────────────────────┘

Expanded:
  Step N                    Step N+1
  ┌──────────────┐         ┌──────────────┐
  │  OpA         │──Delta──│  OpB         │
  │  (dispatch,  │  Lake   │  (dispatch,  │
  │   execute,   │         │   execute,   │
  │   commit)    │         │   commit)    │
  └──────────────┘         └──────────────┘
```

---

## Intermediate artifact handling

In collapsed mode, the `intermediates` parameter controls what happens
to artifacts produced by non-final internal operations:

| Mode | Intermediates in Delta Lake | Provenance edges | Use when |
|------|---------------------------|-----------------|----------|
| `DISCARD` (default) | No | Shortcut edges (input → final output) | Production: minimize storage |
| `PERSIST` | Yes | Internal edges (`step_boundary=False`) | Debugging: inspect intermediate results |
| `EXPOSE` | Yes | Full edges (`step_boundary=True`) | Downstream steps need to reference intermediates |

```python
pipeline.run(
    operation=TransformAndScore,
    inputs={"dataset": output("gen", "datasets")},
    intermediates="persist",  # keep intermediates for debugging
)
```

In expanded mode, intermediates are always full pipeline steps — each
gets its own Delta Lake commit, provenance edges, and cache entry.

---

## Nesting composites

A composite can contain other composites. `ctx.run()` accepts both
`OperationDefinition` and `CompositeDefinition` subclasses:

```python
class GenerateAndScore(CompositeDefinition):
    name = "generate_and_score"
    # ...

    def compose(self, ctx: CompositeContext) -> None:
        generated = ctx.run(DataGenerator, params={"count": 3})
        scored = ctx.run(
            TransformAndScore,  # nested composite
            inputs={"dataset": generated.output("datasets")},
        )
        ctx.output("metrics", scored.output("metrics"))
```

In collapsed mode, nested composites execute recursively on the same
worker. In expanded mode, each nested composite's internal operations
become their own pipeline steps (step names are dot-separated:
`outer.inner.operation`).

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
| Can be run as pipeline step | Yes (`pipeline.run`) | Yes (`pipeline.run` or `pipeline.expand`) |

Operations are leaves. Composites are branches. Both are nodes in the
pipeline DAG.

---

## Key design decisions

| Decision | Rationale |
|----------|-----------|
| Separate class hierarchy (`CompositeDefinition` not `OperationDefinition`) | Composites wire; operations compute. Mixing them would blur the lifecycle contract |
| Caller chooses collapsed vs expanded | The composite author defines *what* happens; the pipeline author decides *how* |
| `CompositeContext` as the API surface | Provides a uniform interface regardless of execution mode |
| `DISCARD` as default intermediates | Most composites exist to avoid intermediate I/O; persisting by default would defeat the purpose |
| Frozen `CompositeRef` | Prevents accidental mutation of wiring state between `ctx.run()` calls |
| Subclass validation at definition time | Mismatched roles, missing `compose()`, or missing enums fail at import, not at runtime |

---

## Cross-references

- [CompositeDefinition Reference](../reference/composite-definition.md) — API
  signatures and field tables
- [Writing Composite Operations](../how-to-guides/writing-composite-operations.md) —
  step-by-step guide
- [Composable Operations Tutorial](../tutorials/pipeline-design/06-composable-operations.ipynb) —
  interactive examples
- [Operations Model](operations-model.md) — the operation abstractions that
  composites compose
- [Execution Flow](execution-flow.md) — how collapsed and expanded composites
  fit into the dispatch-execute-commit lifecycle
- [Architecture Overview](architecture-overview.md) — where composites sit in
  the five-layer architecture
