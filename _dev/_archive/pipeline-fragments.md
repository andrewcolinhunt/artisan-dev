# Analysis: Composites — Composable Pipeline Chunks with Configurable Execution Boundaries

**Date:** 2026-03-11  **Status:** Active  **Author:** Claude + ach94

---

## Scope

This document analyzes how to evolve artisan's chain system into a more
general composition primitive. Chains currently allow multiple creator
operations to execute within a single worker, but are constrained to linear
topology and creator-only operations. The analysis explores what a more
powerful composition primitive would look like — one that supports arbitrary
pipeline logic (branching, filtering, merging, conditionals) and can execute
either within a single worker or expanded across multiple workers.

The key insight: we are not defining "subpipelines." We are defining
**composites** — reusable compositions of operations with declared I/O and a
configurable execution boundary.

---

## Current State: Chains

Chains execute multiple creator operations sequentially within a single
worker, passing artifacts in-memory via `ArtifactSource.from_artifacts()`.

**API surface:**

```python
chain = pipeline.chain(inputs={"data": gen.output("data")})
chain.add(DataTransformer, params={"mode": "normalize"})
chain.add(MetricCalculator)
result = chain.run()
```

**Hard constraints baked into chaining:**

- **Linear only** — A → B → C, no branching or merging
- **Creators only** — curators (Filter, Merge, IngestFiles) are banned via
  `is_curator_operation` check in `ChainBuilder.add()`
- **No conditional logic** — every operation runs unconditionally
- **Single data flow** — each op gets the previous op's output (plus optional
  extra inputs via `role_mapping`), no complex routing

The curator ban exists because curators normally run locally (not dispatched
to workers). Within a composite context where everything runs on one worker,
there is no fundamental reason to exclude them.

**Key implementation files:**

- `src/artisan/orchestration/chain_builder.py` — fluent builder API
- `src/artisan/execution/executors/chain.py` — in-memory execution loop
- `src/artisan/execution/models/execution_chain.py` — transport model
- `src/artisan/orchestration/engine/step_executor.py` —
  `execute_chain_step()` dispatch
- `src/artisan/orchestration/engine/dispatch.py` — routes `ExecutionChain` to
  `run_creator_chain()`

---

## The Three Levels of Composition

Artisan has three levels at which users compose behavior, each with distinct
characteristics:

| Feature | Operation | Composite (proposed) | Pipeline |
|---|---|---|---|
| **What it is** | Atomic transform | Composed transform | Top-level orchestration |
| **Form** | Declarative class | Declarative class | Imperative script |
| **Declares I/O** | Yes (InputSpec/OutputSpec) | Yes (InputSpec/OutputSpec) | No (implicit from steps) |
| **Declares params** | Yes (Params model) | Yes (Params model) | No (script variables) |
| **Logic method** | `execute()` — per artifact | `compose()` — wires operations | Top-level script |
| **Contains operations** | No (is atomic) | Yes | Yes |
| **Execution scope** | Always one worker | Caller decides | Always multi-worker |
| **Artifact passing** | In-memory (within execute) | Depends on mode | Delta Lake (between steps) |
| **Reusable** | Yes (class) | Yes (class) | No (script) |
| **Composable** | No (atomic) | Yes (nest composites) | No (top-level) |
| **Aware of other ops** | No | Yes (wires them) | Yes (wires them) |

The composite sits between operation and pipeline because it *is* between
them — it's pipeline logic packaged as a reusable unit. It shares the
operation's declarative class form and I/O specs, but its `compose()` method
contains pipeline-like orchestration logic.

---

## Naming: Why "Composite"

The naming challenge: the new concept sits between operations and pipelines.
It must be clearly differentiated from both — especially from operations,
since both are declarative classes passed to `pipeline.run()`.

**Evaluation axes:**

- **Differentiates atomic from molecular.** The name must inherently convey
  "composed of multiple operations," not describe something that an
  operation also is. This ruled out "component" (an operation is also a
  pipeline component) and "module" (too generic, plus Python namespace
  conflict).
- **Literal, not metaphorical.** The codebase uses literal, descriptive
  terms throughout (artifact, step, pipeline, role, worker). No recipes,
  blueprints, or workflows. This ruled out metaphorical options.
- **Conveys declared I/O boundary.** The name should suggest that the thing
  has a defined interface — inputs and outputs — not just that it groups
  things together. This ruled out "group," "block," and "bundle."
- **Conveys reusability.** The name should suggest the thing is defined once
  and used in multiple contexts. This ruled out "fragment" and "chunk"
  (which imply a broken-off piece, not a designed unit).
- **No conflicts with existing vocabulary.** The codebase already uses
  "step," "unit" (`ExecutionUnit`), "stage" (staging area), "phase"
  (lifecycle phases), and "chain." The name must not collide.
- **Works as a standalone noun.** Users should be able to say "I wrote a
  composite" naturally, and the class name (`CompositeDefinition`) should
  parallel `OperationDefinition`.

**The atom/molecule distinction is the core differentiator.** Operations are
atomic — they transform artifacts directly via `execute()`. Composites are
molecular — they wire multiple operations together via `compose()`. The name
must make this clear.

The strongest candidates were:

- **Composite** — "made up of various parts." Established CS term (the GoF
  Composite pattern treats atomic and composed objects uniformly through the
  same interface, which is exactly what `pipeline.run()` does). Works well
  as a standalone noun. 3 syllables.
- **Compound** — "multiple elements bonded together." Clean chemistry
  parallel (element/compound maps to operation/compound). Slightly unusual
  as a standalone software noun. 2 syllables.
- **Composition** — the noun form of "compose." Perfect verb/noun pairing
  (`compose()` method). But also means "the act of composing," adding
  ambiguity. 4 syllables.
- **Assembly** — "parts fitted together." Strong engineering connotation but
  less common in software contexts. Assembly language association.

**Decision: Composite.** It's the most natural standalone noun in software,
specifically conveys multiplicity, and the GoF parallel (treating atoms and
molecules uniformly) is an asset. The pairing with Operation is clean:
operations are atomic, composites are molecular.

Class name: `CompositeDefinition` (parallels `OperationDefinition`).

### All naming decisions

| Decision | Choice | Rationale |
|---|---|---|
| **Base class** | `CompositeDefinition` | Parallels `OperationDefinition`. "Composite" conveys multiplicity. |
| **User method** | `compose(self, ctx)` | "Compose" = wire operations together. Distinct from `execute()` on operations. |
| **Context object** | `CompositeContext` | Consistent with `ExecutionContext` naming pattern. Active object providing `input()`, `run()`, `output()`. |
| **Context: reference input** | `ctx.input(role)` | Matches the `inputs` class attribute. |
| **Context: declare output** | `ctx.output(role, ref)` | Matches the `outputs` class attribute. |
| **Context: run operation** | `ctx.run(Op, inputs=..., params=...)` | Same verb as `pipeline.run()`. Familiar. |
| **Pipeline: collapsed mode** | `pipeline.run(Composite, ...)` | Default. Same entry point as operations. |
| **Pipeline: expanded mode** | `pipeline.expand(Composite, ...)` | Distinct method — makes the execution mode choice explicit. |
| **Execution mode names** | `collapsed` / `expanded` | Clean antonyms. Collapsed = multiple operations in one step. Expanded = one step per operation. |
| **Transport model** | `ExecutionComposite` | Parallels `ExecutionChain`, `ExecutionUnit`. |
| **Intermediates enum** | `CompositeIntermediates` | Same values as `ChainIntermediates`: `discard`, `persist`, `expose`. |
| **Worker executor** | `run_composite()` | In `executors/composite.py`. Parallels `run_creator_chain()`. |
| **Step executor** | `execute_composite_step()` | In `engine/step_executor.py`. Parallels `execute_chain_step()`. |
| **Pipeline manager internal** | `_submit_composite()` | Collapsed mode. Parallels `_submit_chain()`. |
| **Pipeline manager expand** | `_expand_composite()` | Expanded mode. New method. |
| **Spec ID function** | `compute_composite_spec_id()` | In `utils/hashing.py`. Parallels `compute_chain_spec_id()`. |
| **Internal operations** | "internal operations" | In docs, logs, errors. |
| **The I/O boundary** | "composite boundary" | In docs. The declared inputs/outputs. |

---

## Proposed Design: CompositeDefinition

### Class structure

```python
class NormalizeFilterScore(artisan.CompositeDefinition):
    name = "normalize_filter_score"
    inputs = {"raw": InputSpec(artifact_type=ArtifactTypes.DATA)}
    outputs = {"metrics": OutputSpec(artifact_type=ArtifactTypes.METRIC)}

    class Params(BaseModel):
        quality_threshold: float = 0.8

    def compose(self, ctx: CompositeContext):
        norm = ctx.run(DataTransformer,
            inputs={"data": ctx.input("raw")},
            params={"mode": "normalize"})

        filtered = ctx.run(Filter,
            inputs={"data": norm.output("data")},
            params={"predicate": f"quality > {self.params.quality_threshold}"})

        metrics = ctx.run(MetricCalculator,
            inputs={"data": filtered.output("data")})

        ctx.output("metrics", metrics.output("metrics"))
```

### The `CompositeContext`

The `ctx` argument to `compose()` exposes a pipeline-like API:

- `ctx.input(role)` — reference a declared input (the composite boundary)
- `ctx.run(op, inputs=..., params=...)` — execute an operation
- `ctx.output(role, ref)` — declare an output mapping (the composite boundary)
- Wiring via `.output(role)` on results of `ctx.run()`

This is the same API shape as `PipelineManager` (`pipeline.run()`, wiring
via `result.output()`), with `ctx.input()` and `ctx.output()` added to
define the composite boundary. The additions are minimal.

`CompositeContext` has two implementations, selected by the execution mode:

- **Collapsed:** an in-memory executor — `ctx.run()` executes eagerly,
  artifacts pass in-memory via `ArtifactSource.from_artifacts()`.
- **Expanded:** a pipeline-delegating executor — `ctx.run()` creates real
  pipeline steps on the parent `PipelineManager`.

### Execution modes

The same composite can execute in two modes. The execution strategy is a
deployment decision, not a logic decision:

**Collapsed** (single worker): All operations run in one process, artifacts
pass in-memory. This is what chains do today. Good for tightly-coupled
operations where Delta Lake round-trips are waste.

```python
scored = pipeline.run(NormalizeFilterScore,
    inputs={"raw": gen.output("data")},
    params={"quality_threshold": 0.9})
```

**Expanded** (multi-worker): Each operation becomes a real pipeline step
with its own worker dispatch. Artifacts go through Delta Lake. Good for
expensive operations that benefit from independent batching, caching, and
parallelism.

```python
scored = pipeline.expand(NormalizeFilterScore,
    inputs={"raw": gen.output("data")},
    params={"quality_threshold": 0.9})
```

In collapsed mode, the whole composite is one step from the outer
pipeline's perspective.

In expanded mode, the composite "dissolves" into the parent pipeline — each
internal operation becomes its own step. It is essentially macro expansion.

### Nesting

Composites can contain composites. In collapsed mode, this is nested
in-memory execution. In expanded mode, each composite expands recursively.

```python
class FullScoring(artisan.CompositeDefinition):
    inputs = {"raw": InputSpec(...)}
    outputs = {"report": OutputSpec(...)}

    def compose(self, ctx: CompositeContext):
        scored = ctx.run(NormalizeFilterScore,
            inputs={"raw": ctx.input("raw")},
            params={"quality_threshold": 0.9})

        report = ctx.run(ReportBuilder,
            inputs={"metrics": scored.output("metrics")})

        ctx.output("report", report.output("report"))
```

---

## Validation Story

Composites declare `inputs` and `outputs` as class-level dicts of
`InputSpec`/`OutputSpec`, identical to `OperationDefinition`. This means the
entire existing validation pipeline works unchanged:

| Validation | Mechanism | Works for composites? |
|---|---|---|
| Input roles exist | `_validate_input_roles()` reads `operation.inputs` | Yes — composite has `.inputs` |
| Required inputs present | `_validate_required_inputs()` reads `operation.inputs` | Yes |
| Artifact types match | `_validate_input_types()` reads `InputSpec.accepts_type()` | Yes |
| Output roles for wiring | `StepFuture` reads `operation.outputs` | Yes — composite has `.outputs` |
| Param validation | Pydantic `Params` model | Yes |

No special-casing in `pipeline.run()`, `StepFuture`, or any validation
code. The composite is a first-class citizen for caching, provenance, and
wiring.

At class definition time, `__init_subclass__` (or `__pydantic_init_subclass__`
if we make composites Pydantic models like operations) can enforce:

- `compose()` is implemented
- `inputs` and `outputs` are valid spec dicts
- `Params` model fields are consistent

---

## Design Decisions

### Eager execution within `compose()`

`ctx.run()` inside `compose()` executes immediately (eager), not lazily as
a DAG. This means:

- Arbitrary Python control flow works — `if/else`, loops, early returns
- Simple to implement and debug
- No intra-worker parallelism for independent branches (not typically the
  bottleneck — the outer pipeline handles parallelism across workers)

### Curators allowed

Filter, Merge, and IngestData are essential for interesting composite
topologies (branching and merging require them). Within a collapsed
composite, curators run in-process on the same worker. The curator execution
path needs to work with in-memory `ArtifactSource` instead of requiring
Delta-backed IDs.

### Batching is external

Composites do not batch internally. Each operation within a composite
processes whatever input set it receives. Batching is an outer-pipeline
concern — split work across workers before feeding into the composite. This
matches how chains work today.

### Intermediates handling

The existing chain intermediates modes carry over:

- `discard` (default): Only the composite's declared outputs are committed.
  Shortcut provenance edges connect composite inputs to composite outputs.
- `persist`: All internal operations' artifacts are committed. Internal
  provenance edges are stored.
- `expose`: Like persist, with internal edges marked `step_boundary=False`
  and shortcut edges marked `step_boundary=True`.

---

## What Exists vs What's New

### Reused (no changes needed)

- `ArtifactSource` — already handles in-memory artifacts
- `run_creator_lifecycle()` — already accepts custom sources
- `remap_output_roles()` / `validate_required_roles()` — role mapping
- `update_ancestor_map()` — works with DAGs (edges are edges)
- `_collect_chain_edges()` / `_collect_chain_artifacts()` — intermediates
- `record_execution_success()` — staging and commit
- All validation functions in `pipeline_manager.py`
- Step caching, provenance, Delta commit

### New implementation needed

- `CompositeDefinition` base class with `compose()`, `inputs`, `outputs`,
  `Params`
- `CompositeContext` with two implementations: in-memory executor
  (collapsed) and pipeline-delegating executor (expanded)
- In-process curator execution (curators currently use subprocess dispatch)
- `execute_composite_step()` in step_executor (collapsed mode)
- `_submit_composite()` / `_expand_composite()` in pipeline_manager
- `ExecutionComposite` transport model (like `ExecutionChain`)
- Dispatch routing in `execute_unit_task()` for composite units
- `compute_composite_spec_id()` in utils/hashing

---

## Relationship to Chains

Chains become a strict subset of collapsed composites: a linear composite
with no curators. The recommendation is to deprecate chains in favor of
composites, since composites strictly generalize chain behavior.

Migration path: the `pipeline.chain()` API could be kept as sugar that
internally constructs and runs a linear composite, or deprecated with a
clear migration guide.

---

## Open Questions

**Expanded mode step naming:** When a composite expands into multiple
pipeline steps, how are the internal steps named? Prefixed with the
composite name? (e.g., `normalize_filter_score.DataTransformer`,
`normalize_filter_score.Filter`)

**Composite-level caching vs step-level caching:** In collapsed mode, the
whole composite is one cached step. In expanded mode, each internal
operation is independently cached. Should collapsed mode also support
internal operation caching, or is the composite the caching unit?

**Shared base class:** Should `OperationDefinition` and
`CompositeDefinition` share a common `StepDefinition` base? Both declare
`inputs`, `outputs`, `Params`, and are passed to `pipeline.run()`. A shared
base would formalize this, but adds a layer of abstraction.

**`CompositeContext` return type from `ctx.run()`:** What type does
`ctx.run()` return for wiring within `compose()`? It needs `.output(role)`.
Could reuse `StepResult`, or define a lighter `CompositeStepHandle`.
