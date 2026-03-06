# Composable Operations: Viable Solutions

**Status:** Draft
**Date:** 2026-03-06
**Parent:** `composable-operations-v2.md` (full enumeration)

---

## Problem

Operations A → B → C require three pipeline steps and three Delta Lake
round-trips. When intermediate artifacts have no independent value, the I/O is
pure overhead. We want in-memory artifact streaming between operations within a
single worker, with control over when persistence happens.

## Requirements

- Operations remain unchanged (no new methods, no contract changes)
- Each operation goes through full preprocess/execute/postprocess lifecycle
- Lineage is traceable (configurable: all stages or just input > output)
- Works with local and SLURM backend
- Per-stage error attribution and timing
- User control over intermediate persistence

NOTE: SLURM is required.
QUESTION: what if we relax the contract or method changes. does anything useful shake out?

---

## Viable Solutions

Four solutions survive the must-have filter. One is a building block (pairs
with the others); three are complete approaches at different layers.

---

### ArtifactStream (building block)

A type that unifies "artifacts loaded from Delta" and "artifacts passed
in-memory from a previous operation." Operations never see it — it's consumed
by the executor when building `PreprocessInput`.

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
```

**~50 lines. Pairs with any of the three solutions below.**

QUESTION: do we need a new abstraction for this? Could this be added to Artifact? or is this a collection of artifacts?

---

### Solution A: Bridge adapter (executor layer, minimal)

A thin adapter that converts one operation's output artifacts into the next
operation's input artifacts. The caller manages the loop.

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
    return remapped, {}
```

**Changes:**
- New `bridge_artifacts()` function (~30 lines)
- `run_creator_flow()` gains optional `pre_resolved_inputs` parameter to
  accept artifacts directly instead of loading from Delta

**Pros:**
- Minimal code change (~50 lines total with ArtifactStream)
- Each operation still uses `run_creator_flow()` — zero duplication
- No new transport model needed

**Cons:**
- Incomplete — doesn't address who manages the chain loop
- Each stage creates its own sandbox, execution context, etc.
- Pushes chain orchestration to the caller

**Best for:** Validating the concept before committing to a full chain
executor. Build this first, test it, then wrap it in Solution B.

NOTE: we want a robust longterm solution. this doesn't seem like it.

---

### Solution B: Chain executor (executor layer, standard)

A new `run_creator_chain()` that loops over multiple operations in sequence,
passing output artifacts in-memory between stages. Each stage goes through the
full preprocess/execute/postprocess lifecycle via an extracted `run_single_stage()`.

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

**Changes:**
- Extract inner lifecycle from `run_creator_flow()` into `run_single_stage()`
  (pure refactor, no behavior change)
- New `run_creator_chain()` wrapping the loop (~100 lines)
- New `ChainUnit` transport model (~50 lines)
- Dispatch routing for `ChainUnit` (~20 lines)

**Pros:**
- Each stage goes through full lifecycle — no duplication
- Framework retains per-stage visibility (timing, errors)
- Clean separation: operations unchanged, executor manages the chain
- `persist_intermediates` flag gives user control over intermediate writes

**Cons:**
- New transport model adds complexity to dispatch
- Role mapping between stages adds configuration surface
- Lineage across stages needs explicit threading of initial inputs
- Caching granularity question (chain-level vs per-stage)

**Best for:** The production solution. Builds naturally on Solution A.

---

### Solution C: Multi-op step (orchestrator layer)

Extend `execute_step()` and `PipelineManager` to accept a list of operations
as a single step. The orchestrator creates chain units and dispatches them.

```python
pipeline.run(
    [ToolRunner, Parser, Scorer],
    inputs={"raw": some_ref},
    params=[{"tool": "blast"}, {"format": "xml"}, {}],
)
```

**Changes:**
- `PipelineManager.run/submit()` accepts `list[type[OperationDefinition]]`
- `execute_step()` creates chain units instead of single units
- Batching logic accounts for multi-op chains
- Step result aggregates results across stages

**Pros:**
- Chains are explicit at the pipeline level
- Orchestrator has full knowledge of the chain
- Step-level caching covers the entire chain naturally

**Cons:**
- Significant changes to the pipeline API surface
- Params, resources, execution config become parallel lists (fragile)
- Input validation becomes multi-stage
- Batching across a chain is complex (what if stage 1 produces N:1?)

**Best for:** User-facing API after Solution B is proven. This is the
pipeline-level wrapper around the chain executor.

NOTE: this interfact feels hacky. lets imagine what the most robust long term solution would look like. a new method (or methods) on pipeline manager?

---

## Comparison

| | A: Bridge | B: Chain Executor | C: Multi-op Step |
|---|-----------|-------------------|------------------|
| **Layer** | Executor | Executor | Orchestrator |
| **Code** | ~80 lines | ~300 lines | ~300+ lines |
| **In-memory pass** | Yes | Yes | Yes |
| **Full lifecycle** | Yes | Yes | Yes |
| **Per-stage visibility** | Yes | Yes | Yes |
| **Chain management** | Caller's job | Built-in | Built-in |
| **Pipeline API change** | None | None | Yes |
| **Transport model** | None | ChainUnit | ChainUnit |
| **Prerequisite** | ArtifactStream | A + run_single_stage() | B |

---

## Recommended Path

**A → B → C**, incrementally:

- **A (bridge + ArtifactStream):** Validate in-memory passing works.
  Testable in isolation without any dispatch or pipeline API changes.

- **B (chain executor):** Wrap the bridge in a chain loop. Extract
  `run_single_stage()` from `run_creator_flow()`. Add `ChainUnit` and
  dispatch routing. This is the core deliverable.

- **C (multi-op step):** Expose chains in the pipeline API. Only after B
  is proven in integration tests.

Each step is independently shippable and testable.
