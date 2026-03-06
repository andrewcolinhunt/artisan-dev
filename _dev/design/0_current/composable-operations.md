# Design: Composable Operations

**Status:** Draft
**Date:** 2026-03-06

---

## Context

Each pipeline step executes a single operation. Between steps, artifacts are
committed to Delta Lake, then re-read for the next step. This works well for
durability and observability, but introduces overhead when two operations are
tightly coupled and intermediate artifacts have no independent value:

```
ToolRunner (produces raw output files)
  → Parser (converts raw files to structured data)
    → Scorer (computes metrics from structured data)
```

Today this requires three pipeline steps, three Delta Lake round-trips, and
three sets of staged Parquet files. The intermediate artifacts (raw files,
parsed data) are never queried independently — the I/O is pure overhead.

**Goal:** Allow multiple operations to execute within a single worker,
streaming artifacts in-memory between them, with user control over when
intermediate artifacts are persisted.

---

## Terminology

| Term | Scope | Example |
|------|-------|---------|
| **Step** | Pipeline-level unit of work | `execute_step()`, `step_number`, `StepResult` |
| **Chain** | Multiple operations composed within a single step | `ExecutionChain`, `run_creator_chain()` |
| **Operation** | One `OperationDefinition` instance within a chain (or standalone) | `ToolRunner`, `Parser`, `Scorer` |
| **Phase** | One stage of an operation's lifecycle | preprocess, execute, postprocess |

The hierarchy is **step > chain > operation > phase**. No additional
vocabulary levels are introduced.

---

## Codebase Analysis

### Execution flow (single operation today)

```
PipelineManager.submit()
  execute_step()
    resolve_inputs()                     # OutputReference → artifact IDs via Delta
    generate_execution_unit_batches()    # split into ExecutionUnits
    backend.create_flow()               # dispatch to workers via Prefect
      execute_unit_task()               # on worker (local process or SLURM job)
        run_creator_flow()
          setup        → sandbox dirs, build ExecutionContext
          instantiate  → artifact IDs → hydrated Artifact objects (from Delta)
          materialize  → write artifacts to sandbox disk (if needed)
          preprocess   → Artifact objects → domain format (paths, content)
          execute      → user's core logic
          postprocess  → domain output → draft Artifacts
          lineage      → finalize artifacts, build provenance edges
          record       → stage artifacts + metadata to Parquet
    DeltaCommitter.commit_all_tables()   # merge staged Parquet → Delta
```

### Key types

| Type | Role | File |
|------|------|------|
| `ExecutionUnit` | Transport: operation + input artifact IDs | `execution/models/execution_unit.py` |
| `RuntimeEnvironment` | Paths and backend config for the worker | `schemas/execution/runtime_environment.py` |
| `ExecutionContext` | Immutable context built inside the worker | `schemas/execution/execution_context.py` |
| `PreprocessInput` | Artifacts keyed by role → passed to `preprocess()` | `schemas/specs/input_models.py` |
| `ExecuteInput` | Prepared inputs + `execute_dir` → passed to `execute()` | `schemas/specs/input_models.py` |
| `PostprocessInput` | File/memory outputs → passed to `postprocess()` | `schemas/specs/input_models.py` |
| `ArtifactResult` | Draft artifacts keyed by output role | `schemas/execution/curator_result.py` |
| `StagingResult` | Outcome of staging (paths, artifact IDs, success/error) | `execution/staging/parquet_writer.py` |

### Coupling points

These are the places where the current design assumes "one operation per
worker execution, with Delta Lake between steps."

**Inputs are artifact IDs resolved from Delta.** `ExecutionUnit.inputs` is
`dict[str, list[str]]` — 32-char hex IDs. These are hydrated into `Artifact`
objects via `instantiate_inputs()`, which reads from Delta. Operation B cannot
receive Operation A's outputs without a Delta round-trip.
(`execution/inputs/instantiation.py`, `execution/inputs/materialization.py`)

**Outputs go through staging → commit.** `postprocess()` returns draft
`Artifact` objects. These are finalized (content-hashed), serialized to
Parquet, and staged to disk. There is no path to hand off artifacts in-memory.
(`execution/staging/recorder.py`, `execution/staging/parquet_writer.py`,
`storage/io/commit.py`)

**ExecutionUnit is a single-operation transport.** One `ExecutionUnit` = one
`OperationDefinition` instance + its inputs. No concept of a chain.
(`execution/models/execution_unit.py`)

**Lineage is per-operation.** Provenance edges link input → output artifacts
for one operation. `OutputSpec.infer_lineage_from` references input roles of
the *same* operation. No transitive lineage across a chain.
(`execution/lineage/capture.py`, `execution/lineage/builder.py`)

**Caching uses a per-operation spec hash.** `compute_execution_spec_id()`
hashes one operation's identity. Cache granularity is one operation.
(`utils/hashing.py`)

**The lifecycle is coupled to filesystem sandboxing.** Each operation gets
its own isolated sandbox with `preprocess_dir`, `execute_dir`,
`postprocess_dir`. (`execution/context/sandbox.py`)

### What does NOT change

- **`OperationDefinition` interface** — operations stay completely agnostic
  to whether they run standalone or in a chain
- **`preprocess/execute/postprocess` lifecycle** — each operation still goes
  through all three phases
- **`Artifact` model** — already supports draft → finalize pattern
- **Delta Lake storage model** — still the source of truth for persisted
  artifacts

---

## Design Goals

- Operations remain unchanged (no new methods, no contract changes)
- Each operation goes through full preprocess/execute/postprocess lifecycle
- Lineage is configurable: collapsed (chain inputs → chain outputs) or full
  (all intermediate edges)
- Works with local and SLURM backends
- Per-operation error attribution and timing
- User control over intermediate persistence

---

## Design Decisions

### ArtifactSource

**Decision:** Introduce `ArtifactSource` as a thin wrapper that unifies
"artifacts loaded from Delta" and "artifacts passed in-memory."

**Value:** The chain executor needs to feed artifacts into
`run_creator_lifecycle()` from two different sources: Delta (first operation)
and in-memory (subsequent operations). Without `ArtifactSource`, the lifecycle
function needs two optional parameters and conditional logic per invocation.
`ArtifactSource` gives us:

- **Single input signature** for `run_creator_lifecycle()` — takes
  `dict[str, ArtifactSource]` instead of two optional parameters. One code
  path, not two.
- **Mixed sources per role** — an operation could receive role "data" from
  the previous operation (in-memory) and role "config" from Delta (by ID).
  Each role is independently sourced; `hydrate()` does the right thing.
- **Future backing stores** — designed with awareness that a future
  `from_query(InputRef)` will handle lazy query descriptors from the Level 3
  scalability roadmap (see "Relationship to Scalability" below). Consumers
  won't change when that arrives.

Operations never see this type. It's consumed by the executor when building
`PreprocessInput`.

```python
@dataclass
class ArtifactSource:
    _ids: list[str] | None = None
    _artifacts: list[Artifact] | None = None

    @staticmethod
    def from_ids(ids: list[str]) -> ArtifactSource: ...

    @staticmethod
    def from_artifacts(artifacts: list[Artifact]) -> ArtifactSource: ...

    # Future: from_query(ref: InputRef) -> ArtifactSource
    # for Level 3 lazy execution units

    def hydrate(self, store: ArtifactStore, spec: InputSpec) -> list[Artifact]:
        if self._artifacts is not None:
            return self._artifacts
        return instantiate_from_ids(self._ids, store, spec)

    @property
    def is_materialized(self) -> bool:
        return self._artifacts is not None
```

---

### Chain Executor

**Decision:** Add a chain executor at the executor layer that loops over
multiple operations, passing output artifacts in-memory between them.

This is the core of the design. Each operation goes through the full
preprocess/execute/postprocess lifecycle via an extracted
`run_creator_lifecycle()`. Each operation gets its own sandbox
(preprocess_dir, execute_dir, postprocess_dir) just as standalone operations
do today. All operations run in the same worker process — no subprocess per
operation. In-memory artifact passing is the whole point; subprocess
boundaries would reintroduce serialization overhead.

**`run_creator_lifecycle()` return type:**

```python
@dataclass
class LifecycleResult:
    input_artifacts: dict[str, list[Artifact]]   # hydrated inputs (for lineage)
    artifacts: dict[str, list[Artifact]]          # output artifacts keyed by role
    edges: list[ArtifactProvenanceEdge]           # provenance edges
    timings: dict[str, float]                     # phase → elapsed seconds
```

**Chain executor pseudocode:**

```python
def run_creator_chain(
    chain: ExecutionChain,
    runtime_env: RuntimeEnvironment,
    worker_id: int = 0,
) -> StagingResult:
    current_sources: dict[str, ArtifactSource] | None = None
    initial_input_artifacts: dict[str, list[Artifact]] | None = None
    ancestor_map: dict[str, list[str]] = {}  # artifact_id → initial_input_ids
    all_artifacts: list[dict[str, list[Artifact]]] = []
    all_edges: list[list[ArtifactProvenanceEdge]] = []
    timings: list[dict[str, float]] = []

    for i, unit in enumerate(chain.operations):
        is_first = (i == 0)

        if is_first:
            sources = {
                role: ArtifactSource.from_ids(ids)
                for role, ids in unit.inputs.items()
            }
        else:
            mapping = chain.role_mappings[i - 1]
            sources = remap_output_roles(
                current_sources, unit.input_spec, mapping
            )

        result = run_creator_lifecycle(unit, sources, runtime_env, worker_id)

        if is_first:
            initial_input_artifacts = result.input_artifacts

        # Update ancestor map from this operation's stem-matched edges
        ancestor_map = update_ancestor_map(
            ancestor_map, result.edges, is_first
        )

        all_artifacts.append(result.artifacts)
        all_edges.append(result.edges)
        timings.append(result.timings)

        current_sources = {
            role: ArtifactSource.from_artifacts(arts)
            for role, arts in result.artifacts.items()
        }

    # Stage everything atomically at the end
    return record_chain_success(
        all_artifacts=all_artifacts,
        all_edges=all_edges,
        ancestor_map=ancestor_map,
        initial_input_artifacts=initial_input_artifacts,
        persist_intermediates=chain.persist_intermediates,
    )
```

**Transport model:**

```python
@dataclass
class ExecutionChain:
    operations: list[ExecutionUnit]
    role_mappings: list[dict[str, str] | None]  # len = len(operations) - 1
    persist_intermediates: bool = False
```

The first `ExecutionUnit` carries real Delta-backed input IDs. Subsequent
units carry empty inputs (filled at execution time from the previous
operation's output).

**Extracting `run_creator_lifecycle()`:** The inner body of
`run_creator_flow()` in `execution/executors/creator.py` becomes
`run_creator_lifecycle()`, which accepts pre-resolved artifacts via
`ArtifactSource` instead of always loading from Delta.
`run_creator_flow()` becomes a thin wrapper that runs a single-operation
lifecycle plus recording. This is a pure refactor with no behavior change.

```
run_creator_flow()  = run_creator_lifecycle() + record_execution_success()
run_creator_chain() = N × run_creator_lifecycle() + record_chain_success()
```

**Dispatch routing:** `execute_unit_task()` in `orchestration/engine/dispatch.py`
gains a branch: if the unit is an `ExecutionChain`, call `run_creator_chain()`
instead of `run_creator_flow()`.

---

### Role Mapping

**Decision:** When role names match between operations, mapping is inferred
automatically. Explicit mapping only needed when names differ.

```python
# Inferred: operation N output "data" → operation N+1 input "data"
role_mappings = [None, None]

# Explicit: operation N output "processed_data" → operation N+1 input "data"
role_mappings = [{"processed_data": "data"}, None]
```

**`remap_output_roles()` semantics:**

```python
def remap_output_roles(
    prev_outputs: dict[str, ArtifactSource],
    next_input_spec: InputSpec,
    mapping: dict[str, str] | None,
) -> dict[str, ArtifactSource]:
```

When `mapping` is `None`, role names are matched by identity — each output
role name that matches an input role name is forwarded directly. When
`mapping` is provided, its entries take precedence: keys are previous
operation's output role names, values are next operation's input role names.
Unmapped output roles whose names match an input role are still forwarded
(the mapping is additive, not exclusive).

After remapping, the function validates that all *required* input roles
(per `next_input_spec`) are satisfied. Missing required roles raise
`ValueError` with a message naming the missing roles and available output
roles. Extra output roles that don't map to any input role are silently
dropped — they're simply not needed by the next operation.

---

### Lineage

A chain needs a boundary that distinguishes "what crossed the boundary"
(inputs/outputs visible to the pipeline) from "what happened inside"
(intermediate transformations). This boundary concept drives both lineage
and execution record design.

**A chain is one step.** From the pipeline's perspective, a chain is a
single step with one step number, one `execution_run_id`, and one execution
record. Downstream steps reference the chain's step number via
`OutputReference` and get the chain's final outputs. `resolve_inputs()`
works without special filtering — the step boundary *is* the chain boundary.

**`step_boundary` field on `ArtifactProvenanceEdge`.** New boolean field,
defaults to `True`. Distinguishes edges that cross a step boundary from
edges internal to a chain.

- All existing edges today: `step_boundary=True` (backward compatible —
  every edge crosses a step boundary since one step = one operation)
- Internal chain edges (`persist_intermediates=True`):
  `step_boundary=False` — these are within the step
- Shortcut chain edges (initial inputs → final outputs):
  `step_boundary=True` — these cross the step boundary

Pipeline-level lineage queries filter on `WHERE step_boundary = True` to
skip chain internals. Detailed debugging queries omit the filter.

**Ancestor propagation via transitive closure.** Each operation runs its
normal per-op stem matching (unchanged). The chain executor additionally
tracks an ancestor map in-memory (`dict[str, list[str]]` — artifact_id →
initial_input_ids) that flows forward through the chain:

- Operation 1: ancestors are the stem-matched initial inputs
- Operation N: output O stem-matched to intermediate I, so O's ancestors
  = `ancestor_map[I]`

At commit time, shortcut edges (`step_boundary=True`) are emitted from each
final output to its ancestors. Per-artifact granularity is preserved — this
is transitive closure, not all-to-all.

**One execution record per chain.** The chain gets one execution record with
one `execution_run_id`. Execution edges show only the chain's initial inputs
and final outputs. This is why `resolve_inputs()` works without special
filtering: `OutputReference` resolves via execution edges, so downstream
steps only see final outputs — even when `persist_intermediates=True`
commits intermediate artifacts to the artifacts table. Per-operation detail
lives in the provenance edges (when `persist_intermediates=True`), not the
execution record.

**How `persist_intermediates` interacts with lineage:**

| | `False` (default) | `True` |
|---|---|---|
| Intermediate artifacts | Discarded | Committed |
| Per-operation edges (`step_boundary=False`) | Discarded | Committed |
| Shortcut edges (`step_boundary=True`) | Committed | Committed |
| Execution record | One for chain | One for chain |
| Execution edges | Initial inputs + final outputs | Initial inputs + final outputs |

Both modes share the same ancestor propagation mechanism. Full mode
additionally commits intermediate artifacts and their internal edges.

**`record_chain_success()` edge handling.** `run_creator_lifecycle()`
produces edges with the default `step_boundary=True`.
`record_chain_success()` is responsible for adjusting them:

- Sets `step_boundary=False` on all per-operation edges (they are internal
  to the chain, not visible at the step boundary)
- Creates new shortcut edges with `step_boundary=True` from the ancestor
  map (these are the step-boundary-crossing edges)
- When `persist_intermediates=False`, the internal edges are simply
  discarded rather than adjusted

---

### Caching

**Decision:** Cache at the chain level only. No per-operation caching within
a chain.

```python
chain_spec_id = compute_chain_spec_id(
    [(unit.operation.name, unit.params) for unit in chain.operations],
    initial_inputs,
)
```

If any operation's params change, the entire chain re-executes. This is the
correct granularity — a chain is a single logical unit of work.

---

### Persistence

**Decision:** `persist_intermediates` controls whether intermediate
operations' artifacts are committed — but all writes happen atomically at the
end of the chain, not as-you-go.

- `persist_intermediates=False` (default): Only the final operation's
  artifacts are staged and committed. Intermediate artifacts are discarded
  after the chain completes.
- `persist_intermediates=True`: All operations' artifacts are collected
  during execution and staged together at the end. One atomic commit covers
  everything.

This keeps the chain atomic in both modes — either the full chain succeeds
and everything is committed, or it fails and nothing is.

---

### Error Handling

**Decision:** Chains are atomic. If any operation fails, the chain fails.

- On failure: record the failure against the chain (using the chain spec ID),
  referencing the initial inputs
- In-memory intermediate artifacts are discarded
- Error message attributes the failure to the specific operation
  (e.g. `"Chain operation 2/3 (Parser) failed: ..."`)
- Per-operation timing is still captured for diagnostics

**Logging:** Each operation logs with a `[chain 2/3: Parser]` prefix so
that chain execution is distinguishable from standalone execution in log
output. Phase-level logs (setup, preprocess, execute, postprocess, lineage)
nest under the operation prefix.

---

### Execution Model

**Decision:** All operations run in the same worker process. No subprocess
per operation.

Subprocesses would reintroduce serialization between operations (pickle),
which defeats the purpose of in-memory artifact passing. Memory bounding via
subprocess exit is the right tool for a different problem (massive data
through a single operation, e.g. 22M artifacts through a filter). Chains
are for tightly coupled operations with moderate data volume.

This generalizes cleanly across CPU and GPU workers — both local and SLURM
backends run the chain in a single worker process.

---

### Pipeline API

**Decision:** New builder-style method on `PipelineManager` for declaring
chains. Does not overload the existing `run()` method. The builder object
is a `ChainBuilder`.

```python
chain = pipeline.chain(inputs={"raw": some_ref})
chain.add(ToolRunner, params={"tool": "blast"})
chain.add(Parser, params={"format": "xml"})
chain.add(Scorer)
result = chain.run()
```

The `ChainBuilder` validates wiring at `add()` time: checks that the
previous operation's output roles are compatible with the next operation's
input roles. `chain.run()` constructs the `ExecutionChain` and dispatches it
through the normal `execute_step()` path.

Configuration that applies to the chain as a whole (backend, resources) is
set on the `chain()` call. Per-operation params are set on each `add()` call.

```python
chain = pipeline.chain(
    inputs={"raw": some_ref},
    backend=Backend.SLURM,
    resources={"gpus": 1},
    persist_intermediates=False,
)
chain.add(ToolRunner, params={"tool": "blast"}, command={"image": "blast:latest"})
chain.add(Parser, params={"format": "xml"})
chain.add(Scorer)
result = chain.run()
```

---

## New Abstractions Summary

| Name | Type | Purpose |
|------|------|---------|
| `ArtifactSource` | Dataclass | Wraps artifact sources (IDs, in-memory, future InputRef). Provides `hydrate()`. |
| `ExecutionChain` | Dataclass | Transport model: list of `ExecutionUnit` operations + role mappings + persist flag. |
| `LifecycleResult` | Dataclass | Return type of `run_creator_lifecycle()`: artifacts, edges, timings, input artifacts. |
| `ChainBuilder` | Class | Builder returned by `pipeline.chain()`. Methods: `add()`, `run()`. |
| `step_boundary` | Field | Boolean on `ArtifactProvenanceEdge`. `True` = crosses step boundary. `False` = internal to chain. |
| `run_creator_lifecycle()` | Function | One operation through setup → preprocess → execute → postprocess → lineage. |
| `run_creator_chain()` | Function | Chain executor loop: N × `run_creator_lifecycle()` + `record_chain_success()`. |
| `record_chain_success()` | Function | Stages artifacts atomically at chain completion. Emits shortcut edges from ancestor map. |
| `update_ancestor_map()` | Function | Composes per-operation stem-matched edges into transitive ancestor tracking. |
| `compute_chain_spec_id()` | Function | Hashes all operations + params + initial inputs for caching. |
| `remap_output_roles()` | Function | Applies role mapping between operations in a chain. |

---

## Relationship to Scalability

See `_dev/analysis/scalability-common-threads.md` for the full scalability
roadmap.

### Complementary, not conflicting

Composable operations and the scalability work attack unnecessary
materialization at **different boundaries**:

- **Lazy execution units (scalability)** eliminate materialization between
  orchestrator → worker. The dispatch payload becomes a ~200 byte query
  descriptor instead of materialized artifact IDs.
- **Composable operations** eliminate materialization between operation →
  operation. Artifacts stay in-memory instead of round-tripping through
  Delta Lake.

A chain with lazy entry combines both: the `InputRef` gets the first
operation's inputs to the worker cheaply, then in-memory passing keeps
subsequent operations cheap.

### ArtifactSource as convergence point

`ArtifactSource` is designed with awareness of the scalability roadmap. Today
it has `from_ids()` and `from_artifacts()`. When Level 3 lazy execution units
arrive, `from_query(InputRef)` will handle lazy query descriptors — and no
consumer code changes. This is the single abstraction that unifies all three
artifact sources.

### Chain-level caching with query-derived spec IDs

The scalability roadmap proposes hashing query descriptors instead of
materialized artifact IDs for spec-ID computation. Chain-level caching is
compatible with this — hashing the chain's query descriptor is semantically
equivalent and avoids the materialization-for-hashing bottleneck.

### Where chains are NOT the right tool

Memory bounding via short-lived subprocesses (scalability thread 4) is for
massive data through a single operation. Chains keep all operations'
artifacts live in a single process. For millions of artifacts, separate steps
with subprocess exit are the right approach. Chains are for tightly coupled
operations with moderate data volume — different problems, different tools.

---

## Implementation Order

- **Extract `run_creator_lifecycle()`** from `run_creator_flow()`. Pure
  refactor, no behavior change. `run_creator_flow()` becomes a wrapper.

- **Introduce `ArtifactSource`** in `execution/models/`. Wire it into
  `run_creator_lifecycle()` as the input type.

- **Implement `run_creator_chain()`** — the chain executor loop.

- **Add `ExecutionChain`** transport model. Wire into `execute_unit_task()`
  dispatch routing.

- **Lineage for chains** — add `step_boundary` field to
  `ArtifactProvenanceEdge`. Implement `update_ancestor_map()` and shortcut
  edge emission in `record_chain_success()`. Internal edges
  (`step_boundary=False`) behind `persist_intermediates` flag.

- **Chain-level caching** — `compute_chain_spec_id()` and cache lookup.

- **Pipeline API** — `ChainBuilder` and `pipeline.chain()` on
  `PipelineManager`.

The first three steps can be developed and tested without changing any
public API.

---

## Changes Per Component

| Component | Current | Change |
|-----------|---------|--------|
| `run_creator_flow()` | Monolithic 6-phase function | Thin wrapper around `run_creator_lifecycle()` |
| `instantiate_inputs()` | Always reads from Delta | Accepts `ArtifactSource` (pass through in-memory artifacts) |
| `materialize_inputs()` | Writes to sandbox disk | Reuse already-materialized paths from previous operation |
| `record_execution_success()` | Always stages to Parquet | Called by `record_chain_success()`, which controls which operations are staged |
| `compute_execution_spec_id()` | Per single operation | New `compute_chain_spec_id()` hashing the full chain |
| `execute_unit_task()` | Routes `ExecutionUnit` | Also routes `ExecutionChain` |
| `ArtifactProvenanceEdge` | No `step_boundary` field | New `step_boundary: bool = True` field |
| `PipelineManager` | `run()` / `submit()` for single ops | New `chain()` method returning `ChainBuilder` |

## What Does NOT Change

| Component | Why |
|-----------|-----|
| `OperationDefinition` interface | Operations are agnostic to chains |
| `preprocess/execute/postprocess` lifecycle | Each operation still goes through all three phases |
| `Artifact` model | Already supports draft → finalize |
| Delta Lake storage model | Still the source of truth |
| Existing `run()` / `submit()` API | Chains use a new method |
| SLURM/local backend dispatch | `ExecutionChain` serializes via pickle like `ExecutionUnit` |

---

## Open Questions

**Batching within chains.** If operation A produces 100 artifacts per input
and operation B expects 10 at a time, how does batching work within the
chain? For v1, the chain operates on the full set — no intra-chain batching.
The outer batching (at the `ExecutionUnit` level) still applies to the
chain's initial inputs.

**Per-operation command/resource overrides.** The builder API shows
per-operation `command` overrides. Do we also need per-operation `resources`?
Likely not for v1 since a chain runs on a single worker — resources are
shared.
