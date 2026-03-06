# Analysis: Composable Operations Within a Worker

**Status:** Draft / Design exploration
**Date:** 2026-03-06

## Motivation

Currently, each pipeline step executes a single operation. If you want to run
operation A followed by operation B, you define two pipeline steps. Between
steps, artifacts are committed to Delta Lake, then re-read for the next step.

This works well for durability and observability, but introduces overhead when
two operations are tightly coupled and the intermediate artifacts have no
independent value. Consider a chain like:

```
ToolRunner (produces raw output files)
  → Parser (converts raw files to structured data)
    → Scorer (computes metrics from structured data)
```

Today this requires three pipeline steps, three Delta Lake round-trips, and
three sets of staged Parquet files. If the intermediate artifacts (raw files,
parsed data) are never queried independently, the I/O is pure overhead.

**Goal:** Allow multiple operations to execute within a single worker,
streaming artifacts in-memory between them, writing only the final outputs to
Delta Lake.

---

## Current Architecture

### End-to-end data flow

```
PipelineManager.run(step)
  execute_step()
    resolve_inputs()                    # read Delta Lake → artifact IDs
    generate_execution_unit_batches()   # split into ExecutionUnits
    backend.create_flow()              # dispatch to workers via Prefect
      execute_unit_task()              # on worker
        run_creator_flow()
          1. setup       sandbox dirs, hydrate inputs from Delta
          2. preprocess  Artifact → domain format (paths, content)
          3. execute     user's core logic
          4. postprocess domain format → draft Artifacts
          5. lineage     build provenance edges
          6. record      stage artifacts + metadata to Parquet
    DeltaCommitter.commit_all_tables()  # merge staged Parquet → Delta
```

### Key types in the flow

| Type | Role |
|------|------|
| `ExecutionUnit` | Transport: operation instance + input artifact IDs |
| `RuntimeEnvironment` | Paths and backend config for the worker |
| `ExecutionContext` | Immutable context built inside the worker |
| `PreprocessInput` | Artifacts keyed by role → passed to `preprocess()` |
| `ExecuteInput` | Prepared inputs + `execute_dir` → passed to `execute()` |
| `PostprocessInput` | File/memory outputs → passed to `postprocess()` |
| `ArtifactResult` | Draft artifacts keyed by output role |
| `StagingResult` | Outcome of staging (paths, artifact IDs, success/error) |

### The six coupling points

These are the places where the current design assumes "one operation per
worker execution, with Delta Lake between steps."

#### 1. Inputs are artifact IDs resolved from Delta Lake

`ExecutionUnit.inputs` is `dict[str, list[str]]` — raw 32-char hex IDs.
These are hydrated into `Artifact` objects via `instantiate_inputs()`, which
reads from Delta Lake. Operation B cannot receive Operation A's outputs
without a Delta round-trip.

**Key files:**
- `execution/inputs/instantiation.py` — bulk type lookup + hydration
- `execution/inputs/materialization.py` — write content to sandbox disk

#### 2. Outputs go through staging → commit

`postprocess()` returns `ArtifactResult` with draft `Artifact` objects.
These are finalized (content-hashed), serialized to Parquet, and staged to
disk. The orchestrator then commits staged files into Delta Lake. There is no
path to hand off artifacts in-memory to the next operation.

**Key files:**
- `execution/staging/recorder.py` — `record_execution_success()`
- `execution/staging/parquet_writer.py` — Parquet serialization
- `storage/io/commit.py` — `DeltaCommitter.commit_all_tables()`

#### 3. The three-phase lifecycle is coupled to filesystem sandboxing

`create_sandbox()` creates isolated `preprocess_dir`, `execute_dir`,
`postprocess_dir` directories. `execute()` receives a physical `execute_dir`
path. `output_snapshot()` reads files from the sandbox after execute. Each
operation expects its own isolated sandbox.

**Key files:**
- `execution/context/sandbox.py` — `create_sandbox()`, `output_snapshot()`

#### 4. ExecutionUnit is a single-operation transport

One `ExecutionUnit` = one `OperationDefinition` instance + its inputs. There
is no concept of a chain or sequence.

**Key file:**
- `execution/models/execution_unit.py`

#### 5. Lineage is per-operation

Provenance edges link input artifacts → output artifacts for one operation.
`OutputSpec.infer_lineage_from` references input roles of the *same*
operation. There is no concept of transitive lineage across a chain.

**Key files:**
- `execution/lineage/capture.py` — stem-matching inference
- `execution/lineage/builder.py` — edge construction

#### 6. Caching uses a per-operation spec hash

`compute_execution_spec_id(operation_name, inputs, params)` hashes one
operation's identity. Cache granularity is one operation.

**Key file:**
- `utils/hashing.py`

---

## Design Options

### Option A: CompoundOperation (composition at the operation level)

Create an `OperationDefinition` subclass that wraps a sequence of operations:

```python
class CompoundOperation(OperationDefinition):
    """Chains operations, passing artifacts in-memory."""
    name = "compound"
    stages: list[OperationDefinition]
    wiring: dict[str, str]  # "stage_0.output_role" → "stage_1.input_role"

    def execute(self, inputs: ExecuteInput) -> Any:
        current_artifacts = ...  # from preprocess
        for stage in self.stages:
            current_artifacts = self._run_stage(stage, current_artifacts)
        return current_artifacts
```

**Pros:**
- Operations remain unchanged — composition is external
- Fits within the existing executor; looks like any other operation
- Natural caching: the compound as a unit

**Cons:**
- Hides internal structure from the framework (lineage, timing, error
  attribution are all rolled into one blob)
- The `_run_stage()` method must replicate much of `run_creator_flow()`
  (sandbox, preprocess, execute, postprocess) — significant internal
  duplication
- Difficult to mix creator and curator operations in a chain

### Option B: Chain-aware executor (composition at the executor level)

Modify the executor to accept a sequence of operations:

```python
def run_creator_chain(
    chain: list[ExecutionUnit],
    role_mapping: list[dict[str, str]],
    runtime_env: RuntimeEnvironment,
    worker_id: int = 0,
) -> StagingResult:
    """Execute a chain, passing artifacts in-memory between stages."""
    artifacts = None

    for i, unit in enumerate(chain):
        is_last = (i == len(chain) - 1)

        if artifacts is None:
            input_artifacts = instantiate_inputs(unit.inputs, ...)
        else:
            input_artifacts = remap_outputs_to_inputs(
                artifacts, role_mapping[i - 1]
            )

        result = run_single_stage(unit, input_artifacts, sandbox)
        artifacts = result.artifacts

        if is_last:
            # Only the final stage writes to staging
            record_execution_success(...)
```

**Pros:**
- Each stage still goes through preprocess/execute/postprocess — no
  lifecycle duplication
- Framework retains visibility into each stage (can log timing per stage,
  attribute errors to specific operations)
- Lineage can be captured per-stage and composed

**Cons:**
- Requires a new transport model (`ChainUnit` or similar)
- The dispatch layer must know about chains
- `role_mapping` adds configuration surface area

### Option C: ArtifactStream abstraction (composition at the data level)

Introduce a unified abstraction for "artifacts that may or may not be in
Delta yet":

```python
class ArtifactStream:
    """Lazy artifact source — Delta-backed or in-memory."""

    @staticmethod
    def from_delta(ids: list[str], store: ArtifactStore) -> ArtifactStream: ...

    @staticmethod
    def from_memory(artifacts: list[Artifact]) -> ArtifactStream: ...

    def hydrate(self) -> list[Artifact]: ...
    def as_ids(self) -> list[str]: ...
```

`ExecutionUnit.inputs` becomes `dict[str, ArtifactStream]` instead of
`dict[str, list[str]]`. The executor chains operations by wiring output
streams to input streams. Delta Lake writes become optional for intermediate
steps.

**Pros:**
- Operations do not change at all — they still receive `PreprocessInput`
  with `dict[str, list[Artifact]]`
- Clean separation: the stream abstraction decides where data comes from,
  the executor decides when to persist
- Naturally supports both "skip intermediate writes" and "write everything"
  modes

**Cons:**
- `ExecutionUnit` validation currently requires 32-char hex artifact IDs;
  in-memory artifacts may not be finalized yet (no ID)
- Serialization for Prefect dispatch (pickle) must handle in-memory content
- Most complexity shifts to the orchestrator (deciding what to chain vs.
  what to persist)

---

## Recommended Approach

**Hybrid of B and C: chain-aware executor with ArtifactStream plumbing.**

### Layer 1: ArtifactStream (data abstraction)

A thin wrapper that unifies "artifacts from Delta" and "artifacts from the
previous stage's output." This is the smallest possible change:

```python
@dataclass
class ArtifactStream:
    _ids: list[str] | None = None
    _artifacts: list[Artifact] | None = None

    @staticmethod
    def from_ids(ids: list[str]) -> ArtifactStream:
        return ArtifactStream(_ids=ids)

    @staticmethod
    def from_artifacts(artifacts: list[Artifact]) -> ArtifactStream:
        return ArtifactStream(_artifacts=artifacts)

    def hydrate(self, store: ArtifactStore, spec: InputSpec) -> list[Artifact]:
        if self._artifacts is not None:
            return self._artifacts
        return instantiate_from_ids(self._ids, store, spec)

    @property
    def is_materialized(self) -> bool:
        return self._artifacts is not None
```

Operations never see this type — it's consumed by the executor when building
`PreprocessInput`.

### Layer 2: Role mapping (wiring declaration)

A simple dict declaring how one stage's outputs feed the next stage's inputs:

```python
# Convention: if omitted, match by role name
# Explicit: {"processed_data": "data"} means
#   stage N output role "processed_data" → stage N+1 input role "data"

RoleMapping = dict[str, str]  # output_role → input_role
```

When all output roles match input roles by name, the mapping is inferred
automatically (single-output → single-input is the common case).

### Layer 3: ChainUnit (transport model)

```python
@dataclass
class ChainUnit:
    stages: list[ExecutionUnit]
    role_mappings: list[RoleMapping | None]  # len = len(stages) - 1
    persist_intermediates: bool = False
```

The first `ExecutionUnit` carries the real Delta-backed input IDs. Subsequent
units carry empty inputs (filled at execution time from the previous stage's
outputs).

### Layer 4: Chain executor

```python
def run_creator_chain(
    chain: ChainUnit,
    runtime_env: RuntimeEnvironment,
    worker_id: int = 0,
) -> StagingResult:
    current_streams: dict[str, ArtifactStream] | None = None

    for i, unit in enumerate(chain.stages):
        is_first = (i == 0)
        is_last = (i == len(chain.stages) - 1)

        # --- resolve inputs ---
        if is_first:
            input_artifacts = instantiate_inputs(unit.inputs, store, ...)
        else:
            mapping = chain.role_mappings[i - 1] or {}
            input_artifacts = remap_and_hydrate(current_streams, mapping)

        # --- run preprocess/execute/postprocess ---
        result = run_stage(unit, input_artifacts, sandbox)

        # --- capture outputs as streams ---
        current_streams = {
            role: ArtifactStream.from_artifacts(arts)
            for role, arts in result.artifacts.items()
        }

        # --- optionally persist intermediates ---
        if not is_last and chain.persist_intermediates:
            record_execution_success(...)  # write to staging

    # --- final stage: always persist ---
    return record_execution_success(...)
```

### Layer 5: Lineage strategy

Two modes, selectable per chain:

1. **Collapsed lineage** (default): The chain's initial inputs link directly
   to its final outputs. Intermediate artifacts are ephemeral and leave no
   provenance trace. Simple, matches the mental model of "this is one
   logical operation."

2. **Full lineage** (`persist_intermediates=True`): Every stage writes
   artifacts and edges. The provenance graph shows the full chain. Useful
   for debugging or when intermediate artifacts have independent value.

For collapsed lineage, the final stage's `infer_lineage_from` must reference
the *chain's* initial inputs, not the previous stage's outputs. This requires
threading the original input artifacts through the chain — straightforward
since they're already available in the first `PreprocessInput`.

### Layer 6: Caching

Cache at the chain level: hash all operations, all params, and the initial
inputs together. This is the natural granularity — if any stage's params
change, the entire chain must re-execute.

```python
chain_spec_id = compute_chain_spec_id(
    [(unit.operation.name, unit.params) for unit in chain.stages],
    initial_inputs,
)
```

Per-stage caching within a chain is a future optimization (requires
persisting intermediate artifacts to check against).

---

## What changes per component

| Component | Current | Change |
|-----------|---------|--------|
| `ExecutionUnit.inputs` | `dict[str, list[str]]` | No change (first stage); empty for subsequent stages |
| `instantiate_inputs()` | Always reads from Delta | Accept `ArtifactStream` (pass through in-memory artifacts) |
| `materialize_inputs()` | Writes to sandbox disk | Reuse already-materialized paths from previous stage |
| `run_creator_flow()` | Single operation | Extract inner `run_stage()` that skips staging; new `run_creator_chain()` wraps the loop |
| `record_execution_success()` | Always stages to Parquet | Called only for final stage (or all stages if `persist_intermediates=True`) |
| `OutputSpec.infer_lineage_from` | References own input roles | For collapsed lineage: thread chain-initial inputs as lineage source |
| `compute_execution_spec_id` | Per single operation | New `compute_chain_spec_id()` hashing the full chain |
| `PipelineManager.run()` | One step = one operation | New API for declaring chains (details TBD) |
| `dispatch.py` | Dispatches `ExecutionUnit` | Also dispatches `ChainUnit` |

## What does NOT change

- **`OperationDefinition` interface** — operations stay completely agnostic
  to whether they run standalone or in a chain
- **`preprocess/execute/postprocess` lifecycle** — each stage still goes
  through all three phases
- **`Artifact` model** — already supports draft → finalize pattern
- **Delta Lake storage model** — still the source of truth for persisted
  artifacts
- **Curator operations** — remain single-step (they operate on metadata,
  not content; chaining them is a different problem)

---

## Open Questions

### 1. Should intermediate artifacts ever be visible?

If we skip Delta writes for intermediates, they're ephemeral. This is fine
when they have no independent value, but some users may want to inspect them
for debugging. The `persist_intermediates` flag addresses this, but the
default matters for UX.

**Recommendation:** Default to collapsed (no intermediates). Offer a debug
mode that persists everything.

### 2. Error handling in chains

If stage B fails in a chain A → B → C:
- Should A's outputs be preserved? They're in-memory drafts, not committed.
- Should the failure record reference A's inputs or B's?
- Does `failure_policy` (continue vs. fail_fast) apply within a chain?

**Recommendation:** On any stage failure, record the failure against the
*chain* (using the chain spec ID), referencing the initial inputs. A's
in-memory outputs are discarded. The chain is atomic: all-or-nothing.

### 3. Batching within chains

If A produces 100 artifacts per input and B expects 10 at a time, how does
batching work within the chain?

**Recommendation:** For v1, the chain operates on the full set — no
intra-chain batching. The outer batching (at the `ExecutionUnit` level) still
applies to the chain's initial inputs. Intra-chain batching is a future
optimization.

### 4. Pipeline API for declaring chains

How does a user express "run A then B in the same worker"?

Options:
```python
# Option 1: explicit chain step
pipeline.run_chain([
    (ToolRunner, {"tool": "blast"}),
    (Parser, {"format": "xml"}),
    (Scorer, {}),
])

# Option 2: context manager
with pipeline.chain() as chain:
    chain.run(ToolRunner, params={"tool": "blast"})
    chain.run(Parser, params={"format": "xml"})
    chain.run(Scorer, params={})

# Option 3: declarative wiring
pipeline.run(
    ToolRunner,
    params={"tool": "blast"},
    then=[
        (Parser, {"format": "xml"}, {"raw_output": "data"}),
        (Scorer, {}, None),  # auto-wire by role name
    ],
)
```

**Recommendation:** Defer API design until the executor layer is proven.
Start by testing `run_creator_chain()` directly in integration tests.

### 5. Interaction with caching

If a chain A → B → C is cached, but the user changes B's params, the entire
chain re-executes (including A). This is correct but potentially wasteful.
Per-stage caching would allow skipping A, but requires persisting A's outputs
to verify the cache hit.

**Recommendation:** Accept chain-level caching for v1. Per-stage caching is
an optimization for later, gated on `persist_intermediates=True`.

---

## Implementation Order

1. **Extract `run_stage()`** from `run_creator_flow()` — the inner loop body
   that runs preprocess/execute/postprocess without staging. This is a pure
   refactor with no behavior change.

2. **Introduce `ArtifactStream`** — a thin wrapper in
   `execution/models/`. Wire it into `instantiate_inputs()` as an
   alternative to raw IDs.

3. **Implement `run_creator_chain()`** — the chain executor that loops over
   stages, passing `ArtifactStream.from_artifacts()` between them.

4. **Add `ChainUnit`** — the transport model. Wire it into
   `execute_unit_task()` dispatch routing.

5. **Lineage for chains** — implement collapsed lineage (initial inputs →
   final outputs). Add `persist_intermediates` flag for full lineage.

6. **Chain-level caching** — `compute_chain_spec_id()` and cache lookup.

7. **Pipeline API** — expose chain declaration in `PipelineManager`.

Steps 1-3 can be developed and tested without changing any public API.
