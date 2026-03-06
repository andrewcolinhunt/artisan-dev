# Composable Operations: Solution Enumeration

**Status:** Draft
**Date:** 2026-03-06
**Parent:** `composable-operations.md`

---

## Problem Recap

Operations A → B → C require three pipeline steps and three Delta Lake
round-trips. When intermediate artifacts have no independent value, the I/O is
pure overhead. We want in-memory artifact streaming between operations within a
single worker, with control over when persistence happens.

---

## Solution Space

Organized by the architectural layer where composition happens.

---

### Layer 1: Operation (composition inside an OperationDefinition)

#### Solution 1: CompoundOperation (framework-provided wrapper)

A framework-provided `OperationDefinition` subclass that wraps a sequence of
operations and runs them internally.

```python
class CompoundOperation(OperationDefinition):
    stages: list[OperationDefinition]
    wiring: dict[str, str]  # "stage_0.output_role" → "stage_1.input_role"

    def execute(self, inputs: ExecuteInput) -> Any:
        current = inputs
        for stage in self.stages:
            current = self._run_stage(stage, current)
        return current
```

**What changes:** New OperationDefinition subclass. Nothing else in the
framework changes.

**Pros:**
- Zero changes to executor, orchestrator, dispatch, or storage
- Fits naturally into existing pipeline API (`pipeline.run(CompoundOperation)`)
- Caching works automatically (the compound is one operation)

**Cons:**
- `_run_stage()` must replicate `run_creator_flow()` internals (sandbox,
  preprocess, execute, postprocess) — significant duplication
- Framework loses visibility: timing, errors, and lineage are opaque blobs
- Lineage only shows compound inputs → compound outputs (no intermediate trace)
- Mixing creator and curator operations in a chain is awkward
- Testing requires testing the wrapper + the replication of executor logic

**Verdict:** Expedient but architecturally wrong. Duplicates executor logic
inside an operation, violating the separation between "what to compute" and
"how to execute."

---

#### Solution 2: User-authored composite operation

The user writes a single operation whose `execute()` method calls other
operations' logic directly (not through the framework).

```python
class ParseAndScore(OperationDefinition):
    def execute(self, inputs: ExecuteInput) -> Any:
        parsed = my_parser(inputs.inputs["raw"])
        scored = my_scorer(parsed)
        return scored
```

**What changes:** Nothing. This is just how users write code today.

**Pros:**
- Zero framework changes
- Maximum user control
- No overhead at all

**Cons:**
- Loses all framework benefits: no lineage, no caching, no error attribution
  per stage, no reuse of existing operations
- Not composable — the user must manually wire logic
- Doesn't solve the problem (we want to compose *existing* operations)

**Verdict:** Not a solution — it's the workaround users do today. Listed for
completeness.

---

### Layer 2: Executor (composition inside the worker)

#### Solution 3: Chain executor (loop over operations)

A new `run_creator_chain()` function that loops over multiple operations in
sequence, passing output artifacts from one stage as input artifacts to the
next. Each stage still goes through the full preprocess/execute/postprocess
lifecycle.

```python
def run_creator_chain(
    units: list[ExecutionUnit],
    role_mappings: list[dict[str, str]],
    runtime_env: RuntimeEnvironment,
    worker_id: int = 0,
    persist_intermediates: bool = False,
) -> StagingResult:
    current_artifacts = None
    for i, unit in enumerate(units):
        if current_artifacts is None:
            input_artifacts = instantiate_inputs(unit.inputs, ...)
        else:
            input_artifacts = remap(current_artifacts, role_mappings[i-1])
        result = run_single_stage(unit, input_artifacts, sandbox)
        current_artifacts = result.artifacts
        if not is_last and persist_intermediates:
            record_execution_success(...)
    return record_execution_success(...)  # final stage
```

**What changes:**
- Extract inner lifecycle from `run_creator_flow()` into `run_single_stage()`
- New `run_creator_chain()` wrapping the loop
- New transport model (`ChainUnit`) for dispatch
- Dispatch layer routes `ChainUnit` to chain executor

**Pros:**
- Each stage goes through full lifecycle — no duplication
- Framework retains per-stage visibility (timing, errors)
- Clean separation: operations unchanged, executor manages the chain
- `persist_intermediates` flag gives user control

**Cons:**
- New transport model adds complexity to dispatch
- Role mapping between stages adds configuration surface
- Lineage across stages needs explicit threading
- Caching granularity question (chain-level vs per-stage)

**Verdict:** Strong option. Clean separation of concerns, no lifecycle
duplication, retains framework visibility.

---

#### Solution 4: Postprocess-to-preprocess bridge (minimal adapter)

Instead of a full chain executor, add a thin adapter that takes postprocess
output from operation A and converts it to preprocess input for operation B.
The existing `run_creator_flow()` calls this bridge between operations.

```python
def bridge_artifacts(
    source_result: ArtifactResult,
    target_input_specs: dict[str, InputSpec],
    role_mapping: dict[str, str],
    materialized_dir: Path,
) -> tuple[dict[str, list[Artifact]], dict]:
    """Convert one operation's output artifacts into another's input artifacts."""
    remapped = {}
    for out_role, in_role in role_mapping.items():
        artifacts = source_result.artifacts[out_role]
        for a in artifacts:
            a.finalize()
        remapped[in_role] = artifacts
    return remapped, {}  # (artifacts, associated)
```

**What changes:**
- New `bridge_artifacts()` function (~30 lines)
- `run_creator_flow()` gains optional `pre_resolved_inputs` parameter
- Or: new `run_creator_flow_chained()` that accepts artifacts directly

**Pros:**
- Minimal code change
- No new transport model needed
- Each operation still uses `run_creator_flow()` — zero duplication

**Cons:**
- Caller (dispatch or orchestrator) must manage the loop
- Pushes chain logic up to whoever calls the executor
- Each stage creates its own sandbox, execution context, etc. — some overhead
- Doesn't address who controls the chain (just provides the plumbing)

**Verdict:** Good building block, but incomplete. This is the "how do you pass
artifacts" piece without the "who manages the chain" piece.

---

#### Solution 5: Coroutine/generator pipeline

Operations become generators that yield intermediate artifacts. The executor
pipes yields from one operation's output to the next operation's input.

```python
class StreamingOperation(OperationDefinition):
    def execute_stream(self, inputs: ExecuteInput) -> Iterator[Artifact]:
        for item in inputs.inputs["data"]:
            yield process(item)

# Executor pipes:
for artifact in op_a.execute_stream(inputs):
    op_b_input.append(artifact)
    if len(op_b_input) >= batch_size:
        yield from op_b.execute_stream(make_input(op_b_input))
```

**What changes:**
- New `execute_stream()` method on OperationDefinition
- New streaming executor
- Significant rework of the preprocess/execute/postprocess lifecycle

**Pros:**
- True streaming — memory-efficient for large artifact counts
- Back-pressure naturally handled by generator protocol
- Could enable pipelining (A processes item 2 while B processes item 1)

**Cons:**
- Fundamentally changes the operation contract (batch → stream)
- Existing operations all return batch results; none would work without
  rewriting
- Postprocess phase doesn't fit (it expects all outputs at once for
  finalization)
- Lineage becomes per-artifact instead of per-batch — major complexity
- Sandbox model (snapshot all files after execute) breaks with streaming
- Over-engineered for the actual use case

**Verdict:** Interesting for a different problem (true streaming pipelines),
but wrong fit here. The existing batch-oriented lifecycle is well-suited to
the workloads. The overhead we're eliminating is Delta Lake I/O between steps,
not per-artifact memory pressure.

---

### Layer 3: Orchestrator (composition above the worker)

#### Solution 6: Multi-op step (single step, multiple operations)

Extend `execute_step()` to accept a list of operations. The step executor
creates execution units for the chain and dispatches them as a unit.

```python
pipeline.run(
    [ToolRunner, Parser, Scorer],
    inputs={"raw": some_ref},
    params=[{"tool": "blast"}, {"format": "xml"}, {}],
)
```

**What changes:**
- `PipelineManager.run/submit()` accepts `list[type[OperationDefinition]]`
- `execute_step()` creates chain units instead of single units
- Batching must account for the chain
- Step result aggregates results across stages

**Pros:**
- User-facing API makes chains explicit at the pipeline level
- Orchestrator has full knowledge of the chain (can optimize dispatch)
- Step-level caching covers the entire chain naturally

**Cons:**
- Significant changes to the pipeline API surface
- Params, resources, execution config become lists (parallel arrays = fragile)
- Input validation becomes multi-stage
- Step results must attribute errors to specific stages
- Batching across a chain is complex (what if stage 1 produces N:1?)

**Verdict:** Reasonable, but the pipeline API changes are heavy. Better to
keep the pipeline API simple and push chain management to the executor layer.

---

#### Solution 7: Step fusion (automatic optimization)

The orchestrator detects adjacent pipeline steps that can be fused (same
backend, compatible input/output types, no other consumers of intermediate
artifacts) and automatically creates chain units.

```python
# User writes normal pipeline:
pipeline.run(ToolRunner, inputs=..., params={"tool": "blast"})
pipeline.run(Parser, inputs=step_0_ref, params={"format": "xml"})
pipeline.run(Scorer, inputs=step_1_ref)

# Orchestrator detects: step 0 → 1 → 2 form a chain
# Fuses into single dispatch with in-memory passing
```

**What changes:**
- New fusion analysis pass in `PipelineManager` (dependency graph analysis)
- Must detect: linear chains, no external consumers, compatible backends
- Fused steps dispatched as chain units
- Step results must be split back for user-facing reporting

**Pros:**
- Zero API change — existing pipelines get faster automatically
- Optimal: framework makes the best decision with full graph knowledge

**Cons:**
- Significant complexity in the orchestrator (dependency analysis, fusion
  eligibility, breaking fusions on constraints)
- Surprising behavior — user writes 3 steps, framework runs 1
- Debugging is harder (which step failed inside the fused chain?)
- Must handle: step-level caching, per-step overrides, mixed backends,
  branching/merging pipelines
- The "no other consumers" check requires full graph analysis
- Premature optimization — we don't know the fusion rules yet

**Verdict:** Attractive end-state but premature. Requires a working chain
executor first (this is an optimization *on top of* explicit chains). Revisit
after explicit chains are proven.

---

#### Solution 8: Lazy/deferred commit

Don't change how operations execute. Instead, defer Delta Lake commits. Hold
staged artifacts in memory (or local Parquet) and only commit when a
downstream step actually needs to read from Delta (or at pipeline end).

```python
class DeferredCommitter:
    """Holds staged results, commits lazily."""
    _pending: dict[int, StagingResult]  # step_number → result

    def get_artifacts(self, step_number: int, role: str) -> list[Artifact]:
        """Return artifacts from pending (uncommitted) results."""
        return self._pending[step_number].artifacts[role]

    def flush(self):
        """Commit all pending to Delta."""
        for result in self._pending.values():
            DeltaCommitter.commit(result)
```

**What changes:**
- New `DeferredCommitter` wrapping `DeltaCommitter`
- `resolve_inputs()` checks deferred results before querying Delta
- Pipeline manager decides when to flush (step boundary, pipeline end, etc.)
- Staging still writes Parquet (just doesn't commit to Delta)

**Pros:**
- Operations and executors don't change at all
- Staging files still written (crash recovery possible)
- Reduces Delta Lake commits (batched instead of per-step)
- Simple mental model: "same pipeline, fewer commits"

**Cons:**
- Still writes staging Parquet files to disk — doesn't eliminate all I/O
- `resolve_inputs()` must have two code paths (deferred vs Delta)
- Operations still run in separate worker processes — artifacts must be
  serialized between workers (pickle or disk)
- Doesn't enable in-memory artifact passing between operations (different
  worker processes)
- Only helps when the bottleneck is Delta Lake commit overhead, not the
  staging I/O itself

**Verdict:** Solves a different (narrower) problem — reducing Delta commit
overhead. Doesn't enable in-memory streaming between operations. Could be
a complementary optimization, but not the primary solution.

---

### Layer 4: Data (composition at the artifact/storage level)

#### Solution 9: ArtifactStream abstraction

Introduce a type that unifies "artifacts loaded from Delta" and "artifacts
passed in-memory from a previous operation."

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

**What changes:**
- New `ArtifactStream` type in `execution/models/`
- `instantiate_inputs()` accepts `dict[str, ArtifactStream]` (or stays the
  same and `ArtifactStream.hydrate()` is called before it)
- Whoever manages the chain creates `ArtifactStream.from_artifacts()` for
  in-memory passing

**Pros:**
- Clean abstraction — operations never see it
- Decouples "where artifacts come from" from "how they're used"
- Naturally supports both modes (Delta-backed and in-memory)
- Small, testable, independent of chain management

**Cons:**
- By itself, doesn't solve the problem — needs a chain manager (Solution 3
  or 6) to actually create in-memory streams
- `ExecutionUnit.inputs` validation requires 32-char hex IDs; in-memory
  artifacts may not be finalized yet
- Adds a layer of indirection

**Verdict:** Excellent building block, but not a complete solution. Combines
naturally with Solution 3 (chain executor) or Solution 6 (multi-op step).

---

#### Solution 10: Worker-local artifact cache

Add an in-memory cache to `ArtifactStore` that holds recently-produced
artifacts. When the next operation requests artifacts by ID, they're served
from cache instead of Delta.

```python
class CachingArtifactStore(ArtifactStore):
    _cache: dict[str, Artifact] = {}

    def put(self, artifact: Artifact):
        self._cache[artifact.artifact_id] = artifact

    def get_artifact(self, artifact_id, ...):
        if artifact_id in self._cache:
            return self._cache[artifact_id]
        return super().get_artifact(artifact_id, ...)
```

**What changes:**
- `ArtifactStore` gains an in-memory cache layer
- After `record_execution_success()`, produced artifacts are cached
- `instantiate_inputs()` hits cache before Delta
- No changes to operations, executor lifecycle, or dispatch

**Pros:**
- Minimal code change (decorator pattern on ArtifactStore)
- Transparent to everything above — operations, executor, orchestrator
  unchanged
- Works with existing `ExecutionUnit` (still uses artifact IDs)
- Artifacts are finalized (have IDs) before caching — no draft issues

**Cons:**
- Only works when operations run in the same process (local backend)
- SLURM workers are separate processes — cache isn't shared
- Still writes staging Parquet to disk (just avoids the Delta *read*)
- Still goes through full staging pipeline (write + commit)
- Doesn't skip staging writes — only optimizes the input resolution path
- The Delta read is rarely the bottleneck; staging writes + commits are

**Verdict:** Marginal optimization for the wrong bottleneck. The expensive
part is writing staging files and committing to Delta, not reading from it.
This caches reads but doesn't eliminate writes.

---

### Layer 5: Do nothing (accept the cost)

#### Solution 11: Accept Delta round-trips, optimize Delta performance

Instead of changing the architecture, make the Delta round-trips cheaper:
faster Parquet writes (Arrow IPC instead of Parquet), in-memory Delta tables
(`deltalake` supports in-memory storage), or local SSD staging.

**What changes:**
- Possibly switch staging format or add in-memory Delta mode
- No architectural changes

**Pros:**
- Zero risk — no new abstractions, no new code paths
- May be "good enough" if the overhead is < 1 second per step

**Cons:**
- Doesn't solve the fundamental problem (N operations = N round-trips)
- Performance ceiling is still O(N) in number of operations
- Doesn't address the conceptual issue (tightly coupled operations should
  feel like one thing)

**Verdict:** Worth benchmarking first. If Delta round-trips are < 100ms per
step, maybe this problem isn't urgent. But for tool-calling chains with many
small artifacts, the overhead compounds.

---

## Comparison Matrix

| # | Solution | Layer | Code Change | In-Memory Pass | Preserves Lifecycle | Framework Visibility | Complexity |
|---|----------|-------|-------------|----------------|---------------------|---------------------|------------|
| 1 | CompoundOperation | Operation | ~200 lines | Yes | Duplicated | None (opaque) | Medium |
| 2 | User composite | Operation | 0 | Yes | N/A | None | Low |
| 3 | Chain executor | Executor | ~200 lines | Yes | Yes (per stage) | Per-stage | Medium |
| 4 | Bridge adapter | Executor | ~50 lines | Yes | Yes | Per-stage | Low |
| 5 | Coroutine pipeline | Executor | ~500+ lines | Yes (streaming) | Fundamentally changed | Per-artifact | Very High |
| 6 | Multi-op step | Orchestrator | ~300 lines | Yes | Yes | Per-stage | High |
| 7 | Step fusion | Orchestrator | ~500+ lines | Yes | Yes | Per-stage | Very High |
| 8 | Lazy commit | Orchestrator | ~150 lines | No (still serialized) | Yes | Per-step | Medium |
| 9 | ArtifactStream | Data | ~50 lines | Enables it | N/A (building block) | N/A | Low |
| 10 | Artifact cache | Data | ~80 lines | Partial (reads only) | Yes | Unchanged | Low |
| 11 | Do nothing | N/A | 0 | No | Yes | Unchanged | None |

---

## Evaluation Criteria

**Must have:**
- Operations remain unchanged (no new methods, no contract changes)
- Each operation goes through full preprocess/execute/postprocess lifecycle
- Lineage is traceable (at minimum: chain inputs → chain outputs)
- Works with local backend; SLURM is a bonus

**Should have:**
- Per-stage error attribution
- Per-stage timing
- User control over intermediate persistence
- Minimal changes to existing code paths

**Nice to have:**
- Automatic fusion of adjacent steps
- Per-stage caching within a chain
- Works across worker processes (SLURM)

---

## Solutions That Meet All Must-Haves

Only **3, 4, 6, and 9** (combined with 3 or 6) meet all must-have criteria.

- **Solution 5** (coroutines) changes the operation contract
- **Solution 1** (CompoundOperation) duplicates the lifecycle
- **Solution 7** (fusion) meets criteria but is premature
- **Solutions 8, 10, 11** don't achieve in-memory passing

---

## Recommended Combinations

### Option A: Minimal — Bridge adapter (4) + ArtifactStream (9)

Build the two smallest pieces and let the caller manage the chain loop.

- `ArtifactStream`: ~50 lines, unifies artifact sources
- `bridge_artifacts()`: ~30 lines, converts outputs to inputs
- Caller (could be a test, a script, or later a chain executor) manages
  the loop

**When to choose:** You want to validate the concept with minimal investment
before building the full chain executor.

### Option B: Standard — Chain executor (3) + ArtifactStream (9)

The chain executor manages the loop, using ArtifactStream for artifact
plumbing. This is the recommended approach from the original analysis.

- `ArtifactStream`: ~50 lines
- `run_single_stage()` extraction: ~100 lines refactor
- `run_creator_chain()`: ~100 lines new
- `ChainUnit` transport: ~50 lines
- Dispatch routing: ~20 lines

**When to choose:** You're confident this is the right direction and want
the complete solution.

### Option C: Full — Chain executor (3) + ArtifactStream (9) + Multi-op step (6)

Everything in Option B, plus pipeline API support for declaring chains.

**When to choose:** After Option B is proven in integration tests and you
want to expose it to pipeline users.

---

## What the Original Analysis Got Right

The original analysis identified the correct coupling points (all 6 verified
against the codebase), recommended the right architectural layer (executor),
and proposed a sound hybrid (B+C → our Solutions 3+9). The implementation
order is also correct: extract `run_single_stage()` first as a pure refactor.

## What the Original Analysis Missed

- **Solution 4 (bridge adapter)** as a simpler starting point
- **Solution 8 (lazy commit)** as a complementary optimization
- **Solution 11 (do nothing)** — we should benchmark the actual overhead
  before committing to a solution
- The distinction between "skipping Delta reads" (Solution 10) and "skipping
  Delta writes" (Solutions 3/4/9) — the write side is the real bottleneck
