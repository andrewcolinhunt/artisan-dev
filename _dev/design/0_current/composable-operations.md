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

**The three-phase lifecycle is coupled to filesystem sandboxing.** Each
operation gets its own isolated sandbox with `preprocess_dir`, `execute_dir`,
`postprocess_dir`. (`execution/context/sandbox.py`)

### What does NOT change

- **`OperationDefinition` interface** — operations stay completely agnostic
  to whether they run standalone or in a chain
- **`preprocess/execute/postprocess` lifecycle** — each stage still goes
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
- Per-stage error attribution and timing
- User control over intermediate persistence

---

## Design Decisions

### ArtifactStream

**Decision:** Introduce `ArtifactStream` as a thin wrapper that unifies
"artifacts loaded from Delta" and "artifacts passed in-memory."

**Value:** The chain executor needs to feed artifacts into `run_single_stage()`
from two different sources: Delta (first stage) and in-memory (subsequent
stages). Without ArtifactStream, the stage function needs two optional
parameters and conditional logic per invocation. ArtifactStream gives us:

- **Single input signature** for `run_single_stage()` — takes
  `dict[str, ArtifactStream]` instead of two optional parameters. One code
  path in the stage, not two.
- **Mixed sources per role** — a stage could receive role "data" from the
  previous stage (in-memory) and role "config" from Delta (by ID). Each role
  is independently sourced; `hydrate()` does the right thing.
- **Future backing stores** — if we later add `from_parquet()`,
  `from_cache()`, or `from_shared_memory()`, consumers don't change.

Operations never see this type. It's consumed by the executor when building
`PreprocessInput`.

```python
@dataclass
class ArtifactStream:
    _ids: list[str] | None = None
    _artifacts: list[Artifact] | None = None

    @staticmethod
    def from_ids(ids: list[str]) -> ArtifactStream: ...

    @staticmethod
    def from_artifacts(artifacts: list[Artifact]) -> ArtifactStream: ...

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
multiple operations, passing output artifacts in-memory between stages.

This is the core of the design. Each stage goes through the full
preprocess/execute/postprocess lifecycle via an extracted `run_single_stage()`.

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

        if is_first:
            streams = {
                role: ArtifactStream.from_ids(ids)
                for role, ids in unit.inputs.items()
            }
        else:
            mapping = chain.role_mappings[i - 1] or {}
            streams = remap_streams(current_streams, mapping)

        result = run_single_stage(unit, streams, runtime_env, worker_id)
        current_streams = {
            role: ArtifactStream.from_artifacts(arts)
            for role, arts in result.artifacts.items()
        }

        if not is_last and chain.persist_intermediates:
            record_execution_success(...)

    return record_execution_success(...)  # final stage always persists
```

**Transport model:**

```python
@dataclass
class ChainUnit:
    stages: list[ExecutionUnit]
    role_mappings: list[dict[str, str] | None]  # len = len(stages) - 1
    persist_intermediates: bool = False
```

The first `ExecutionUnit` carries real Delta-backed input IDs. Subsequent
units carry empty inputs (filled at execution time from the previous stage's
output).

**Extracting `run_single_stage()`:** The inner body of `run_creator_flow()`
(lines 82-260 of `execution/executors/creator.py`) becomes
`run_single_stage()`, which accepts pre-resolved artifacts via
`ArtifactStream` instead of always loading from Delta.
`run_creator_flow()` becomes a thin wrapper that creates a single-stage
chain. This is a pure refactor with no behavior change.

**Dispatch routing:** `execute_unit_task()` in `orchestration/engine/dispatch.py`
gains a branch: if the unit is a `ChainUnit`, call `run_creator_chain()`
instead of `run_creator_flow()`.

---

### Role Mapping

**Decision:** When role names match between stages, mapping is inferred
automatically. Explicit mapping only needed when names differ.

```python
# Inferred: stage N output "data" → stage N+1 input "data"
role_mappings = [None, None]

# Explicit: stage N output "processed_data" → stage N+1 input "data"
role_mappings = [{"processed_data": "data"}, None]
```

---

### Lineage

**Decision:** Two modes, configurable per chain.

**Collapsed lineage** (default): The chain's initial inputs link directly to
its final outputs. Intermediate artifacts are ephemeral and leave no
provenance trace. Simple, matches the mental model of "this is one logical
operation."

**Full lineage** (`persist_intermediates=True`): Every stage writes artifacts
and edges. The provenance graph shows the full chain. Useful for debugging or
when intermediate artifacts have independent value.

For collapsed lineage, the final stage's lineage must reference the *chain's*
initial inputs, not the previous stage's outputs. The chain executor threads
the original input artifacts through for this purpose.

---

### Caching

**Decision:** Cache at the chain level for v1.

```python
chain_spec_id = compute_chain_spec_id(
    [(unit.operation.name, unit.params) for unit in chain.stages],
    initial_inputs,
)
```

If any stage's params change, the entire chain re-executes. Per-stage caching
within a chain is a future optimization (requires `persist_intermediates=True`
to verify cache hits against intermediate artifacts).

---

### Error Handling

**Decision:** Chains are atomic. If any stage fails, the chain fails.

- On failure: record the failure against the chain (using the chain spec ID),
  referencing the initial inputs
- In-memory intermediate artifacts are discarded
- Error message attributes the failure to the specific stage
- Per-stage timing is still captured for diagnostics

---

### Pipeline API

**Decision:** New builder-style method on `PipelineManager` for declaring
chains. Does not overload the existing `run()` method.

```python
chain = pipeline.chain(inputs={"raw": some_ref})
chain.add(ToolRunner, params={"tool": "blast"})
chain.add(Parser, params={"format": "xml"})
chain.add(Scorer)
result = chain.run()
```

The builder validates wiring at `add()` time: checks that the previous
stage's output roles are compatible with the next stage's input roles.
`chain.run()` constructs the `ChainUnit` and dispatches it through the
normal `execute_step()` path.

Configuration that applies to the chain as a whole (backend, resources) is
set on the `chain()` call. Per-stage params are set on each `add()` call.

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

## Implementation Order

- **Extract `run_single_stage()`** from `run_creator_flow()`. Pure refactor,
  no behavior change. `run_creator_flow()` becomes a wrapper.

- **Introduce `ArtifactStream`** in `execution/models/`. Wire it into
  `run_single_stage()` as the input type.

- **Implement `run_creator_chain()`** — the chain executor loop.

- **Add `ChainUnit`** transport model. Wire into `execute_unit_task()`
  dispatch routing.

- **Lineage for chains** — collapsed lineage (initial inputs → final
  outputs). Full lineage behind `persist_intermediates` flag.

- **Chain-level caching** — `compute_chain_spec_id()` and cache lookup.

- **Pipeline API** — `pipeline.chain()` builder on `PipelineManager`.

Steps 1-3 can be developed and tested without changing any public API.

---

## Changes Per Component

| Component | Current | Change |
|-----------|---------|--------|
| `run_creator_flow()` | Monolithic 6-phase function | Thin wrapper around `run_single_stage()` |
| `instantiate_inputs()` | Always reads from Delta | Accepts `ArtifactStream` (pass through in-memory artifacts) |
| `materialize_inputs()` | Writes to sandbox disk | Reuse already-materialized paths from previous stage |
| `record_execution_success()` | Always stages to Parquet | Called only for final stage (or all stages if `persist_intermediates`) |
| `compute_execution_spec_id()` | Per single operation | New `compute_chain_spec_id()` hashing the full chain |
| `execute_unit_task()` | Routes `ExecutionUnit` | Also routes `ChainUnit` |
| `PipelineManager` | `run()` / `submit()` for single ops | New `chain()` builder method |

## What Does NOT Change

| Component | Why |
|-----------|-----|
| `OperationDefinition` interface | Operations are agnostic to chains |
| `preprocess/execute/postprocess` lifecycle | Each stage still goes through all three phases |
| `Artifact` model | Already supports draft → finalize |
| Delta Lake storage model | Still the source of truth |
| Existing `run()` / `submit()` API | Chains use a new method |
| SLURM/local backend dispatch | `ChainUnit` serializes via pickle like `ExecutionUnit` |

---

## Open Questions

**Batching within chains.** If operation A produces 100 artifacts per input
and operation B expects 10 at a time, how does batching work within the chain?
For v1, the chain operates on the full set — no intra-chain batching. The
outer batching (at the `ExecutionUnit` level) still applies to the chain's
initial inputs.

**Per-stage command/resource overrides.** The builder API shows per-stage
`command` overrides. Do we also need per-stage `resources`? Likely not for v1
since a chain runs on a single worker — resources are shared.

**Chain-level vs per-stage caching.** Chain-level caching is simpler but
wasteful when only one stage's params change. Per-stage caching requires
`persist_intermediates=True`. Defer per-stage caching to a future iteration.
