# Implementation Plan: Composable Operations

**Status:** Ready
**Date:** 2026-03-06
**Design doc:** `composable-operations.md`

---

## PR Structure

| PR | Scope | Steps | Risk |
|----|-------|-------|------|
| PR 1 | Internal refactors | 1–2 | Low — pure refactors, no behavior change |
| PR 2 | Chain core | 3–5 | Medium — new executor, dispatch routing |
| PR 3 | Lineage + caching | 6–7 | Medium — schema change, backward compat |
| PR 4 | Pipeline API | 8 | Low — new public surface, no existing changes |

---

## PR 1: Internal Refactors

### Step 1: Refactor `backend.create_flow()`

Decouple `create_flow()` from `OperationDefinition` so it accepts config directly.

**Files to modify:**

- `src/artisan/orchestration/backends/base.py` — change abstract signature:
  ```python
  # Before
  def create_flow(self, operation: OperationDefinition, step_number: int)
  # After
  def create_flow(self, resources: ResourceConfig, execution: ExecutionConfig, step_number: int, job_name: str)
  ```
- `src/artisan/orchestration/backends/local.py` — update `LocalBackend.create_flow()`, reads `execution.max_workers`
- `src/artisan/orchestration/backends/slurm.py` — update `SlurmBackend.create_flow()`, reads `resources.*` and `execution.*`, uses `job_name` param instead of `e.job_name or operation.name`
- `src/artisan/orchestration/engine/step_executor.py:737` — update call site:
  ```python
  step_flow = backend.create_flow(
      operation.resources, operation.execution, step_number,
      job_name=operation.execution.job_name or operation.name,
  )
  ```

**Tests:** Existing backend tests update to pass config objects instead of operation. All existing tests must pass.

### Step 2: Extract `run_creator_lifecycle()`

Split `run_creator_flow()` into lifecycle (setup→lineage) + recording wrapper.

**File: `src/artisan/execution/executors/creator.py`**

Add `LifecycleResult` dataclass:
```python
@dataclass
class LifecycleResult:
    input_artifacts: dict[str, list[Artifact]]   # hydrated inputs (for lineage)
    artifacts: dict[str, list[Artifact]]          # finalized output artifacts by role
    edges: list[ArtifactProvenanceEdge]           # provenance edges
    timings: dict[str, float]                     # phase → elapsed seconds
```

Extract `run_creator_lifecycle()`:
```python
def run_creator_lifecycle(
    unit: ExecutionUnit,
    runtime_env: RuntimeEnvironment,
    worker_id: int = 0,
    execution_run_id: str | None = None,
) -> LifecycleResult:
```

- Includes: setup, preprocess, execute, postprocess, lineage (lines 82–246)
- Excludes: record phase (lines 248–260), outer error handler recording
- Raises on failure (all phases). Caller handles error recording.
- Manages its own sandbox creation and cleanup

`run_creator_flow()` becomes:
```
run_creator_flow() = generate_run_id + try/except(run_creator_lifecycle() + record_execution_success())
```

**Reuse:** `build_creator_execution_context()` from `execution/context/builder.py`, `instantiate_inputs()` from `execution/inputs/instantiation.py`, `materialize_inputs()` from `execution/inputs/materialization.py`, all lineage functions unchanged.

**Tests:** Existing `test_executor_creator.py` must pass unchanged. Add unit test verifying `run_creator_lifecycle()` returns correct `LifecycleResult` structure.

---

## PR 2: Chain Core

### Step 3: Introduce `ArtifactSource`

**New file: `src/artisan/execution/models/artifact_source.py`**

```python
@dataclass
class ArtifactSource:
    _ids: list[str] | None = None
    _artifacts: list[Artifact] | None = None

    @staticmethod
    def from_ids(ids: list[str]) -> ArtifactSource: ...
    @staticmethod
    def from_artifacts(artifacts: list[Artifact]) -> ArtifactSource: ...
    def hydrate(self, store: ArtifactStore, spec: InputSpec) -> list[Artifact]: ...
    @property
    def is_materialized(self) -> bool: ...
```

`hydrate()` with ID backing delegates to `instantiate_inputs()` logic from `execution/inputs/instantiation.py`. Extract a per-role helper from `instantiate_inputs()` for reuse.

**File: `src/artisan/execution/executors/creator.py`**

Update `run_creator_lifecycle()` to accept optional `sources: dict[str, ArtifactSource] | None`. When provided, hydrate from sources instead of calling `instantiate_inputs()` with `unit.inputs`.

**Tests:** `tests/artisan/execution/test_artifact_source.py` — test `from_ids` hydration, `from_artifacts` passthrough, `is_materialized` property.

### Step 4: Implement `run_creator_chain()`

**New file: `src/artisan/execution/executors/chain.py`**

Contains:
- `run_creator_chain(chain, runtime_env, worker_id)` → `StagingResult` — the chain executor loop per design doc pseudocode
- `remap_output_roles(prev_outputs, next_input_spec, mapping)` → `dict[str, ArtifactSource]`
- `validate_required_roles(sources, input_spec)` — raises if required role missing

Chain loop: iterate operations, first op gets `ArtifactSource.from_ids()`, subsequent ops get `ArtifactSource.from_artifacts()` from previous output. After all ops complete, call `record_chain_success()` (initially a stub that delegates to `record_execution_success()` for final op only — full lineage in PR 3).

**Tests:** `tests/artisan/execution/test_chain_executor.py` — test role remapping (identity, explicit, additive), required role validation, two-op chain with mock operations, mixed-source inputs.

### Step 5: `ExecutionChain` + dispatch routing

**New file: `src/artisan/execution/models/execution_chain.py`**

```python
class ChainIntermediates(str, Enum):
    DISCARD = "discard"
    PERSIST = "persist"
    EXPOSE = "expose"

@dataclass
class ExecutionChain:
    operations: list[ExecutionUnit]
    role_mappings: list[dict[str, str] | None]  # len = len(operations) - 1
    resources: ResourceConfig
    execution: ExecutionConfig
    intermediates: ChainIntermediates = ChainIntermediates.DISCARD
```

Uses `@dataclass` (not Pydantic) for pickle serialization compatibility with Prefect/SLURM.

**File: `src/artisan/orchestration/engine/dispatch.py`**

Update `execute_unit_task()` to accept `ExecutionUnit | ExecutionChain`:
```python
if isinstance(unit, ExecutionChain):
    from artisan.execution.executors.chain import run_creator_chain
    result = run_creator_chain(unit, runtime_env, worker_id=worker_id)
    ...
```

Update `_save_units`/`_load_units` type hints accordingly.

**Tests:** Test dispatch routing with `ExecutionChain`, pickle serialization round-trip.

---

## PR 3: Lineage + Caching

### Step 6: Lineage for chains

**File: `src/artisan/schemas/artifact/provenance.py`**

Add field to `ArtifactProvenanceEdge`:
```python
step_boundary: bool = Field(default=True, description="...")
```
Update `__hash__()` to include it.

**File: `src/artisan/storage/core/table_schemas.py`**

Add `"step_boundary": pl.Boolean` to `ARTIFACT_EDGES_SCHEMA`.

**Backward compat:** Where artifact edges are read from Delta, add `with_columns(pl.lit(True).alias("step_boundary"))` when the column is missing. Check `storage/core/artifact_store.py` and `visualization/graph/` for read sites.

**File: `src/artisan/execution/executors/chain.py`**

Implement full `record_chain_success()`:
- Set `step_boundary=False` on all per-operation edges
- Create shortcut edges (`step_boundary=True`) from ancestor map
- Intermediates mode controls what gets staged (see design doc table)

Implement `update_ancestor_map()` per design doc algorithm.

**Tests:** `tests/artisan/execution/test_chain_lineage.py` — test ancestor map propagation, shortcut edge creation, all three intermediates modes, per-artifact granularity (transitive closure, not all-to-all).

### Step 7: Chain-level caching

**File: `src/artisan/utils/hashing.py`**

Add `compute_chain_spec_id()`:
```python
def compute_chain_spec_id(
    operations: list[tuple[str, dict[str, Any] | None]],
    initial_inputs: dict[str, list[str]],
) -> str:
```
Hashes all operation names+params in order + sorted initial input IDs.

**File: `src/artisan/execution/executors/chain.py`**

Wire cache lookup into `run_creator_chain()` before executing.

**Tests:** Test determinism, param sensitivity, order sensitivity, input sensitivity.

---

## PR 4: Pipeline API

### Step 8: `ChainBuilder` and `pipeline.chain()`

**New file: `src/artisan/orchestration/chain_builder.py`**

```python
class ChainBuilder:
    def __init__(self, pipeline, inputs, backend, resources, execution, intermediates): ...
    def add(self, operation, params, command, role_mapping) -> ChainBuilder: ...
    def run(self) -> StepResult: ...
    def submit(self) -> StepFuture: ...
```

`add()` validates at call time:
- Raises `TypeError` if operation is a curator (via `is_curator_operation()`)
- Checks role compatibility: previous op's `OutputSpec.artifact_type` matches next op's `InputSpec.accepts_type()`

`run()`/`submit()` construct `ExecutionChain`, route through step execution.

**File: `src/artisan/orchestration/pipeline_manager.py`**

Add `chain()` method returning `ChainBuilder`.

**File: `src/artisan/orchestration/engine/step_executor.py`**

Add `_execute_chain_step()` parallel to `_execute_creator_step()`. Uses `chain.resources` and `chain.execution` for `backend.create_flow()`.

**Tests:**
- `tests/artisan/orchestration/test_chain_builder.py` — add creator, reject curator, role compatibility, empty chain raises, builder chaining
- `tests/integration/test_chain_pipeline.py` (`@pytest.mark.slow`) — full end-to-end: `pipeline.chain().add().add().run()` with real Delta Lake

---

## Key Reuse Points

| Existing function | File | Reused by |
|---|---|---|
| `instantiate_inputs()` | `execution/inputs/instantiation.py` | `ArtifactSource.hydrate()` (extract per-role helper) |
| `materialize_inputs()` | `execution/inputs/materialization.py` | `run_creator_lifecycle()` (unchanged) |
| `build_edges()` | `execution/lineage/builder.py` | Each op in chain (unchanged) |
| `build_artifact_edges_from_dict()` | `execution/lineage/enrich.py` | Each op in chain (unchanged) |
| `record_execution_success()` | `execution/staging/recorder.py` | `record_chain_success()` delegates for staging |
| `build_execution_edges()` | `execution/staging/recorder.py` | Chain execution edge building |
| `is_curator_operation()` | `execution/executors/curator.py` | `ChainBuilder.add()` validation |
| `compute_execution_spec_id()` | `utils/hashing.py` | Pattern for `compute_chain_spec_id()` |

---

## Verification

After each PR:

```bash
~/.pixi/bin/pixi run -e dev fmt
~/.pixi/bin/pixi run -e dev test-unit
~/.pixi/bin/pixi run -e dev test-integration
~/.pixi/bin/pixi run -e docs docs-build
```

End-to-end verification (after PR 4):
```python
pipeline = PipelineManager.create("test")
pipeline.run(DataGenerator, params={"n": 10})

chain = pipeline.chain(inputs={"data": pipeline.output("data", step_number=1)})
chain.add(DataTransformer, params={"mode": "normalize"})
chain.add(MetricCalculator)
result = chain.run()

assert result.success
assert result.step_number == 2  # chain is one step
```
