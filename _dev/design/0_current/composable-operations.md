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
    backend.create_flow(operation)       # configure workers from operation.resources/execution
      execute_unit_task()               # on worker (local process or SLURM job)
        run_creator_flow()
          setup        → sandbox dirs, build ExecutionContext,
                         instantiate (artifact IDs → hydrated Artifacts from Delta),
                         materialize (write artifacts to sandbox disk if needed)
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
(`execution/lineage/capture.py`, `execution/lineage/builder.py`,
`execution/lineage/enrich.py`)

**Caching uses a per-operation spec hash.** `compute_execution_spec_id()`
hashes one operation's identity. Cache granularity is one operation.
(`utils/hashing.py`)

**The lifecycle is coupled to filesystem sandboxing.** Each operation gets
its own isolated sandbox with `preprocess_dir`, `execute_dir`,
`postprocess_dir`. (`execution/context/sandbox.py`)

**`create_flow()` is coupled to the operation model.**
`backend.create_flow(operation, step_number)` reads `operation.resources`
and `operation.execution` to configure the worker pool. This assumes one
operation per step — for a chain, there is no single representative
operation. (`orchestration/backends/local.py`, `orchestration/backends/slurm.py`)

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

`edges` contains fully enriched `ArtifactProvenanceEdge` objects. Today,
`run_creator_flow()` produces these via `build_edges()` in
`execution/lineage/builder.py` (returns `SourceTargetPair`) →
`build_artifact_edges_from_dict()` in `execution/lineage/enrich.py`
(enriches into `ArtifactProvenanceEdge`). Both steps move into
`run_creator_lifecycle()` so the chain executor receives edges it can
directly inspect for ancestor map tracking and `step_boundary` adjustment.

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
                current_sources, unit.operation.input_spec, mapping
            )
            # Merge any Delta-backed inputs declared on the unit itself
            # (for roles not satisfied by the previous operation)
            for role, ids in unit.inputs.items():
                if role not in sources:
                    sources[role] = ArtifactSource.from_ids(ids)
            # Validate after merge so Delta-sourced roles count
            validate_required_roles(sources, unit.operation.input_spec)

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
        intermediates=chain.intermediates,
    )
```

**Transport model:**

```python
class ChainIntermediates(str, Enum):
    DISCARD = "discard"    # Default. Intermediates discarded after chain completes.
    PERSIST = "persist"    # Intermediates committed to Delta + internal edges stored.
                           # Execution edges reference only final outputs.
    EXPOSE = "expose"      # Like PERSIST, but execution edges include intermediate
                           # outputs too. Downstream steps can OutputReference them.

@dataclass
class ExecutionChain:
    operations: list[ExecutionUnit]
    role_mappings: list[dict[str, str] | None]  # len = len(operations) - 1
    resources: ResourceConfig
    execution: ExecutionConfig
    intermediates: ChainIntermediates = ChainIntermediates.DISCARD
```

`resources` and `execution` are chain-level — they configure the worker,
not individual operations. Each `ExecutionUnit.operation` within the chain
carries its own `params` and `command`. See "Configuration Scoping" below
for rationale.

The first `ExecutionUnit` carries real Delta-backed input IDs. Subsequent
units carry empty inputs by default (filled at execution time from the
previous operation's output), but may carry Delta-backed IDs for roles
that come from a prior pipeline step rather than the previous chain
operation (see "Mixed-source inputs" under Role Mapping).

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
gains a branch: its signature changes to accept
`Union[ExecutionUnit, ExecutionChain]`. If the payload is an
`ExecutionChain`, call `run_creator_chain()` instead of
`run_creator_flow()`. Both types are pickle-serializable for SLURM
dispatch. `ExecutionChain` is not a subclass of `ExecutionUnit` — they are
sibling types discriminated by `isinstance` at dispatch time.

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

Extra output roles that don't map to any input role are silently dropped —
they're simply not needed by the next operation. `remap_output_roles()`
does **not** validate required roles itself — validation happens after
Delta-backed sources are merged (see below).

**Mixed-source inputs.** An intermediate operation may need inputs from
both the previous operation (in-memory) and from Delta (a prior step).
The `ExecutionUnit` for that operation carries Delta-backed IDs in its
`inputs` dict for the Delta-sourced roles. The chain executor merges
both sources after role remapping, then validates that all required input
roles are satisfied (see the pseudocode above). This lets an operation
receive role "data" in-memory from the previous operation and role
"config" from Delta simultaneously.

---

### Lineage

A chain needs a boundary that distinguishes "what crossed the boundary"
(inputs/outputs visible to the pipeline) from "what happened inside"
(intermediate transformations). This boundary concept drives both lineage
and execution record design.

**A chain is one step.** From the pipeline's perspective, a chain is a
single step with one step number, one `execution_run_id`, and one execution
record. Downstream steps reference the chain's step number via
`OutputReference` and by default get the chain's final outputs (or all
outputs when `intermediates="expose"`). `resolve_inputs()` works without
special filtering — the step boundary *is* the chain boundary.

**`step_boundary` field on `ArtifactProvenanceEdge`.** New boolean field,
defaults to `True`. Distinguishes edges that cross a step boundary from
edges internal to a chain.

- All existing edges today: `step_boundary=True` (backward compatible —
  every edge crosses a step boundary since one step = one operation)
- Internal chain edges (`intermediates="persist"` or `"expose"`):
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

**`update_ancestor_map()` algorithm.** Each operation's edges contain
source → target pairs from `build_edges()` stem matching. The ancestor map
tracks which initial inputs each intermediate artifact descends from:

```python
def update_ancestor_map(
    ancestor_map: dict[str, list[str]],
    edges: list[ArtifactProvenanceEdge],
    is_first: bool,
) -> dict[str, list[str]]:
    updated = dict(ancestor_map)
    for edge in edges:
        src, tgt = edge.source_artifact_id, edge.target_artifact_id
        if is_first:
            # First operation: source IS an initial input
            updated.setdefault(tgt, []).append(src)
        elif src in ancestor_map:
            # Subsequent: target inherits source's ancestors
            updated.setdefault(tgt, []).extend(ancestor_map[src])
    return updated
```

"Stem matching" refers to the existing `build_edges()` logic in
`execution/lineage/builder.py`, which pairs input and output artifacts
by matching `OutputSpec.infer_lineage_from` role references. Each edge
represents one such pairing. The ancestor map simply composes these
pairings transitively across the chain.

**One execution record per chain.** The chain gets one execution record with
one `execution_run_id`. By default, execution edges show only the chain's
initial inputs and final outputs. This is why `resolve_inputs()` works
without special filtering: `OutputReference` resolves via execution edges,
so downstream steps only see final outputs. When `intermediates="expose"`,
execution edges additionally include intermediate outputs, making them
referenceable by downstream steps via `OutputReference`. Per-operation
detail lives in the provenance edges (when intermediates are persisted),
not the execution record.

**How `intermediates` interacts with lineage and storage:**

| | `"discard"` (default) | `"persist"` | `"expose"` |
|---|---|---|---|
| Intermediate artifacts | Discarded | Committed | Committed |
| Per-operation edges (`step_boundary=False`) | Discarded | Committed | Committed |
| Shortcut edges (`step_boundary=True`) | Committed | Committed | Committed |
| Execution record | One for chain | One for chain | One for chain |
| Execution edges | Initial inputs + final outputs | Initial inputs + final outputs | Initial inputs + **all** outputs |

The only difference between `"persist"` and `"expose"` is the last row —
whether execution edges include intermediate operation outputs, making them
resolvable by downstream `OutputReference`. `"persist"` is useful for
debugging and provenance inspection without affecting the pipeline DAG.
`"expose"` makes intermediates first-class step outputs.

All three modes share the same ancestor propagation mechanism.

**`record_chain_success()` edge handling.** `run_creator_lifecycle()`
produces edges with the default `step_boundary=True`.
`record_chain_success()` is responsible for adjusting them:

- Sets `step_boundary=False` on all per-operation edges (they are internal
  to the chain, not visible at the step boundary)
- Creates new shortcut edges with `step_boundary=True` from the ancestor
  map (these are the step-boundary-crossing edges)
- When `intermediates="discard"`, the internal edges are simply discarded
  rather than adjusted
- When `intermediates="expose"`, execution edges are emitted for all
  operations' outputs (not just the final operation's)

---

### Caching

**Decision:** Cache at the chain level only. No per-operation caching within
a chain.

```python
chain_spec_id = compute_chain_spec_id(
    [(unit.operation.name, unit.operation.params) for unit in chain.operations],
    initial_inputs,
)
```

If any operation's params change, the entire chain re-executes. This is the
correct granularity — a chain is a single logical unit of work.

---

### Persistence

**Decision:** `intermediates` controls whether and how intermediate
operations' artifacts are committed — but all writes happen atomically at
the end of the chain, not as-you-go.

- `"discard"` (default): Only the final operation's artifacts are staged
  and committed. Intermediate artifacts are discarded after the chain
  completes.
- `"persist"`: All operations' artifacts are collected during execution and
  staged together at the end. One atomic commit covers everything.
  Execution edges reference only final outputs — intermediates are
  queryable via provenance edges but not via `OutputReference`.
- `"expose"`: Like `"persist"`, but execution edges additionally include
  intermediate outputs. Downstream steps can reference any operation's
  outputs via `OutputReference`.

This keeps the chain atomic in all modes — either the full chain succeeds
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

**Trade-off: no partial persistence on failure.** For long chains with
expensive operations (e.g., GPU inference → parsing → scoring), all
intermediate results are lost on failure. This is intentional — partial
persistence would require mid-chain commits, breaking atomicity and
complicating resume logic. If partial failure recovery is needed, use
separate pipeline steps instead of a chain.

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

### Configuration Scoping

**Decision:** Chain-level config for the worker (`resources`, `execution`),
per-operation config for the operation itself (`params`, `command`).

A chain runs on a single worker with fixed resources. The worker's hardware
allocation and scheduling are decided up front for the whole chain.
Per-operation configuration — parameters and command spec — remains on each
operation since these configure the operation's logic, not the worker.

| Config | Scope | Set via | Rationale |
|--------|-------|---------|-----------|
| `resources` | Chain | `pipeline.chain(resources=...)` | One worker = one resource allocation (partition, GPUs, memory, time) |
| `execution` | Chain | `pipeline.chain(execution=...)` | `max_workers`, `units_per_worker`, `artifacts_per_unit` schedule the chain's initial inputs |
| `params` | Per-operation | `chain.add(..., params=...)` | Each operation has its own parameters |
| `command` | Per-operation | `chain.add(..., command=...)` | Each operation may use a different script, container, or interpreter |

**`create_flow()` refactor.** Today, `backend.create_flow(operation,
step_number)` reads `operation.resources` and `operation.execution` from a
single operation, coupling the backend to the operation model. For chains
there is no single representative operation. We refactor `create_flow()`
to accept config directly:

```python
# Before
def create_flow(
    self,
    operation: OperationDefinition,
    step_number: int,
) -> Callable[...]:
    r = operation.resources
    e = operation.execution
    ...

# After
def create_flow(
    self,
    resources: ResourceConfig,
    execution: ExecutionConfig,
    step_number: int,
    job_name: str,
) -> Callable[...]:
    ...
```

Both `LocalBackend` and `SlurmBackend` read only `resources` and
`execution` from the operation today — no other fields — so this is a
clean extraction. `job_name` is pulled out explicitly since the backend
currently reads it from `execution.job_name` or falls back to
`operation.name`.

For standalone steps, `execute_step()` receives an operation class,
instantiates it via `instantiate_operation()`, then extracts
`operation.resources` and `operation.execution` before calling
`create_flow()`. For chains, `execute_step()` passes the chain-level
config directly. Same code path into `create_flow()`, same interface.

---

### Scope: Creator Operations Only

**Decision:** Only creator operations can participate in chains. Curator
operations (Filter, Merge, Ingest variants) cannot.

Curator operations run via `run_curator_flow()`, which has a fundamentally
different execution model: they operate on the full artifact set at the
orchestrator level, not per-unit on workers. Their return types
(`ArtifactResult`, `PassthroughResult`) and dispatch path are incompatible
with the chain executor's per-unit lifecycle loop. If a curator operation
is needed between two creators, use separate pipeline steps.

`ChainBuilder.add()` raises `TypeError` if passed a curator operation
class (detected via `is_curator_operation()`).

---

### Batching

**Decision:** No intra-chain batching. This is a permanent design choice,
not a limitation.

The chain operates on the full artifact set produced by each operation.
Outer batching (at the `ExecutionUnit` level) still applies to the chain's
initial inputs via `execution.artifacts_per_unit`.

Intra-chain batching would mean the chain executor reimplements the step
executor's batching logic internally — partitioning outputs, running the
next operation multiple times, and merging results. If operations have
different batching needs, they belong in separate steps. Chains are for
tightly coupled operations that share the same data granularity.

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
input roles (both role name matching and artifact type compatibility via
`InputSpec.artifact_type` and `InputSpec.accepts_type()`).
`chain.run()` and `chain.submit()` construct the
`ExecutionChain` and dispatch it through the normal `execute_step()` path —
mirroring `pipeline.run()` (blocking, returns `StepResult`) and
`pipeline.submit()` (non-blocking, returns `StepFuture`).

Chain-level config (`backend`, `resources`, `execution`,
`intermediates`) is set on the `chain()` call. Per-operation config
(`params`, `command`) is set on each `add()` call. This mirrors the
separation described in "Configuration Scoping" above.

```python
chain = pipeline.chain(
    inputs={"raw": some_ref},
    backend=Backend.SLURM,
    resources={"partition": "gpu", "gres": "gpu:1", "mem_gb": 16},
    execution={"artifacts_per_unit": 5, "max_workers": 4},
    intermediates="discard",  # or "persist" or "expose"
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
| `ChainIntermediates` | Enum | Controls intermediate artifact handling: `"discard"`, `"persist"`, `"expose"`. |
| `ArtifactSource` | Dataclass | Wraps artifact sources (IDs, in-memory, future InputRef). Provides `hydrate()`. |
| `ExecutionChain` | Dataclass | Transport model: operations + role mappings + chain-level resources/execution + intermediates mode. |
| `LifecycleResult` | Dataclass | Return type of `run_creator_lifecycle()`: artifacts, edges, timings, input artifacts. |
| `ChainBuilder` | Class | Builder returned by `pipeline.chain()`. Methods: `add()`, `run()`, `submit()`. |
| `step_boundary` | Field | Boolean on `ArtifactProvenanceEdge`. `True` = crosses step boundary. `False` = internal to chain. |
| `run_creator_lifecycle()` | Function | One operation through setup → preprocess → execute → postprocess → lineage. |
| `run_creator_chain()` | Function | Chain executor loop: N × `run_creator_lifecycle()` + `record_chain_success()`. |
| `record_chain_success()` | Function | Stages artifacts atomically at chain completion. Emits shortcut edges from ancestor map. |
| `update_ancestor_map()` | Function | Composes per-operation stem-matched edges into transitive ancestor tracking. |
| `compute_chain_spec_id()` | Function | Hashes all operations + params + initial inputs for caching. |
| `remap_output_roles()` | Function | Applies role mapping between operations in a chain. |
| `validate_required_roles()` | Function | Checks all required input roles are satisfied after source merging. |

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

- **Refactor `backend.create_flow()`** to accept `ResourceConfig`,
  `ExecutionConfig`, `step_number`, and `job_name` directly instead of
  reading from the operation model. Update `LocalBackend`, `SlurmBackend`,
  and `execute_step()`. Pure refactor, no behavior change — prerequisite
  for chain support.

- **Extract `run_creator_lifecycle()`** from `run_creator_flow()`. Pure
  refactor, no behavior change. `run_creator_flow()` becomes a wrapper.

- **Introduce `ArtifactSource`** in `execution/models/`. Wire it into
  `run_creator_lifecycle()` as the input type.

- **Implement `run_creator_chain()`** — the chain executor loop.

- **Add `ExecutionChain`** transport model (with chain-level `resources`
  and `execution`). Wire into `execute_unit_task()` dispatch routing.

- **Lineage for chains** — add `step_boundary` field to
  `ArtifactProvenanceEdge` and `ARTIFACT_EDGES_SCHEMA`. Existing Delta Lake
  tables lack this column; reads must default missing values to `True`
  (backward compatible — all pre-chain edges cross a step boundary).
  Implement `update_ancestor_map()` and shortcut edge emission in
  `record_chain_success()`. Internal edges (`step_boundary=False`) behind
  `intermediates` enum.

- **Chain-level caching** — `compute_chain_spec_id()` and cache lookup.

- **Pipeline API** — `ChainBuilder` and `pipeline.chain()` on
  `PipelineManager`.

The first four steps can be developed and tested without changing any
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
| `backend.create_flow()` | Reads `operation.resources` and `operation.execution` | Accepts `ResourceConfig`, `ExecutionConfig`, `step_number`, `job_name` directly |
| `execute_unit_task()` | Routes `ExecutionUnit` | Accepts `Union[ExecutionUnit, ExecutionChain]`, routes by `isinstance` |
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
| SLURM/local backend dispatch model | `ExecutionChain` serializes via pickle like `ExecutionUnit` |

---

## Open Questions

None at this time.
