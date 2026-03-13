# Design: Composites

**Date:** 2026-03-11  **Status:** Draft  **Author:** Claude + ach94

**Preceding analysis:** `_dev/analysis/pipeline-fragments.md`

---

## Summary

Composites are reusable compositions of operations with declared I/O and
configurable execution boundaries. A `CompositeDefinition` is a declarative
class — like `OperationDefinition` — whose `compose()` method wires
operations together using a `CompositeContext`. The same composite can run
**collapsed** (single worker, in-memory artifact passing) or **expanded**
(each internal operation becomes its own pipeline step).

Composites replace chains completely (no deprecation period, no backwards
compatibility — pre-1.0 project).

---

## User-Facing API

### Defining a composite

The class structure closely mirrors `OperationDefinition`. The only
structural difference is `compose()` instead of
`execute()`/`preprocess()`/`postprocess()`.

```python
from enum import StrEnum
from typing import ClassVar

from pydantic import BaseModel, Field

from artisan.composites import CompositeContext, CompositeDefinition
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class NormalizeFilterScore(CompositeDefinition):
    """Normalize data, filter by quality, compute metrics."""

    # ---------- Metadata ----------
    name = "normalize_filter_score"
    description = "Normalize data, filter by quality, compute metrics."

    # ---------- Inputs ----------
    class InputRole(StrEnum):
        RAW = "raw"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.RAW: InputSpec(
            artifact_type="data",
            required=True,
            description="Raw data to normalize, filter, and score",
        ),
    }

    # ---------- Outputs ----------
    class OutputRole(StrEnum):
        METRICS = "metrics"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.METRICS: OutputSpec(
            artifact_type="metric",
            description="Computed quality metrics",
        ),
    }

    # ---------- Parameters ----------
    class Params(BaseModel):
        quality_threshold: float = Field(
            default=0.8,
            description="Minimum quality score for filtering",
        )

    params: Params = Params()

    # ---------- Compose ----------
    def compose(self, ctx: CompositeContext) -> None:
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

### Running a composite (collapsed — default)

```python
pipeline = artisan.pipeline("scoring", delta_root="./data")
gen = pipeline.run(DataGenerator, params={"n": 1000})

# Same call pattern as operations
scored = pipeline.run(NormalizeFilterScore,
    inputs={"raw": gen.output("data")},
    params={"quality_threshold": 0.9})

pipeline.run(ReportBuilder,
    inputs={"metrics": scored.output("metrics")})
```

`pipeline.run()` accepts both `OperationDefinition` and
`CompositeDefinition` subclasses. From the outer pipeline's perspective,
the composite is one step.

### Expanding a composite

```python
scored = pipeline.expand(NormalizeFilterScore,
    inputs={"raw": gen.output("data")},
    params={"quality_threshold": 0.9})
```

Each internal operation becomes its own pipeline step with independent
worker dispatch, batching, and caching. The composite dissolves into the
parent pipeline.

### Per-operation execution configuration within `compose()`

`ctx.run()` accepts the same overrides as `pipeline.run()`: `resources`,
`execution`, `backend`, `environment`, `tool`. These are forwarded to the
internal operation, not applied to the composite itself.

```python
def compose(self, ctx: CompositeContext):
    # Heavy operation — request GPU and SLURM
    heavy = ctx.run(HeavyOp,
        inputs={"data": ctx.input("raw")},
        resources={"gpus": 1, "memory_gb": 64},
        backend=Backend.SLURM)

    # Light operation — default resources
    light = ctx.run(LightOp,
        inputs={"data": heavy.output("data")})

    ctx.output("results", light.output("results"))
```

**In collapsed mode:** Per-operation `resources` and `backend` overrides
are **ignored** — everything runs on one worker. The composite-level
`resources` (set at the class level or overridden at the `pipeline.run()`
call site) determines the worker allocation. The worker must be sized for
the most demanding internal operation.

**In expanded mode:** Per-operation overrides are passed through to
`pipeline.submit()` on the parent pipeline. Each internal step gets its
own resource allocation and backend.

### Nesting

```python
class FullScoring(CompositeDefinition):
    """Run full scoring pipeline: normalize, filter, score, and report."""

    # ---------- Metadata ----------
    name = "full_scoring"
    description = "Run full scoring pipeline: normalize, filter, score, and report."

    # ---------- Inputs ----------
    class InputRole(StrEnum):
        RAW = "raw"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.RAW: InputSpec(
            artifact_type="data",
            required=True,
            description="Raw data to process",
        ),
    }

    # ---------- Outputs ----------
    class OutputRole(StrEnum):
        REPORT = "report"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.REPORT: OutputSpec(
            artifact_type="file_ref",
            description="Final scoring report",
        ),
    }

    # ---------- Compose ----------
    def compose(self, ctx: CompositeContext) -> None:
        scored = ctx.run(NormalizeFilterScore,
            inputs={"raw": ctx.input("raw")},
            params={"quality_threshold": 0.9})

        report = ctx.run(ReportBuilder,
            inputs={"metrics": scored.output("metrics")})

        ctx.output("report", report.output("report"))
```

In collapsed mode, nested composites execute recursively in-memory. In
expanded mode, they expand recursively into the parent pipeline.

---

## Design Decisions

### Execution model: eager

`ctx.run()` executes immediately. This enables arbitrary Python control
flow — `if/else`, loops, early returns — inside `compose()`. No DAG
builder, no lazy evaluation. The trade-off is no intra-worker parallelism
for independent branches, which is rarely the bottleneck (the outer pipeline
parallelizes across workers via batching).

### Curators are allowed

Chains ban curators because curators currently run in a subprocess spawned
from the orchestrator (for memory isolation from the PipelineManager
process). In a collapsed composite, the entire composite already runs on a
worker (a separate process from the orchestrator), so the memory isolation
concern is already addressed. Curators inside collapsed composites run
in-process on the worker — no subprocess needed.

For failure isolation: all internal operations (creators and curators alike)
run in-process on the worker. If any operation throws an exception, the
composite's error handling catches it and records the failure. If any
operation crashes the process (OOM, segfault), the worker dies and Prefect
reports the task failure. This is the same failure model as creators in the
composite — consistent treatment for all internal operations.

In expanded mode, curators become regular pipeline steps and execute through
the normal curator path (subprocess from orchestrator).

### Batching is external

Composites do not batch internally. Each internal operation processes
whatever input set it receives. Batching happens at the outer pipeline level
before feeding into the composite. This matches chain behavior.

### Intermediates handling

Same three modes as chains (`CompositeIntermediates` replaces
`ChainIntermediates`):

- **`discard`** (default): Only declared outputs are committed. Only
  shortcut edges (`step_boundary=True`) connecting composite inputs to
  composite outputs. Internal artifacts and internal edges are discarded.
- **`persist`**: All internal artifacts are committed (intermediate roles
  prefixed to avoid collisions). Internal edges stored with
  `step_boundary=False`, shortcut edges stored with `step_boundary=True`.
  Macro provenance queries filtering on `step_boundary=True` see only the
  composite-level shortcuts; micro queries see the full internal graph.
- **`expose`**: All internal artifacts are committed (same as persist).
  Internal edges stored with `step_boundary=True`, shortcut edges stored
  with `step_boundary=True`. The internal structure is fully visible at
  the macro provenance level — the composite is transparent.

Note: The current chain implementation treats PERSIST and EXPOSE
identically (both set `step_boundary=False` on internal edges). Composites
implement the intended distinction: EXPOSE sets `step_boundary=True` on
internal edges, making them visible to macro provenance queries.

### `pipeline.run()` for collapsed, `pipeline.expand()` for expanded

`pipeline.run()` accepts both operations and composites (collapsed). This
keeps one entry point for "execute this as a single step" regardless of
whether it's atomic or molecular.

`pipeline.expand()` is a separate method because it fundamentally changes
the pipeline topology (one step vs many). Using a parameter on `run()` would
hide a major semantic difference behind a kwarg.

### No shared base class

`OperationDefinition` and `CompositeDefinition` are independent classes.
They share the same interface pattern (inputs, outputs, Params, resources,
execution) but their internals are different enough that a shared base adds
abstraction without meaningful code reuse.

Rationale:
- Operations have substantial additional complexity: `tool`, `environments`,
  `runtime_defined_inputs`, `independent_input_streams`, `group_by`,
  `hydrate_inputs`, `preprocess()`/`execute()`/`postprocess()`,
  `execute_curator()`. These would need to be excluded from any shared base.
- The shared part is ~10 lines of ClassVar declarations and ~30 lines of
  `__init_subclass__` validation. Not enough to justify a base class.
- `pipeline.run()` routes differently for each type anyway.
- If duplication becomes a maintenance burden after implementation, a shared
  base can be extracted then. YAGNI.

### Chains are removed entirely

No deprecation period, no backwards compatibility shims. Chains are
replaced by composites in the same release. The chain implementation
(`ChainBuilder`, `ExecutionChain`, `run_creator_chain()`,
`execute_chain_step()`, `_submit_chain()`) is deleted.

---

## `CompositeDefinition` Base Class

### Structure

Mirrors `OperationDefinition` as closely as possible. Same Pydantic model
pattern, same ClassVar declarations, same Params convention, same
InputRole/OutputRole enums.

```python
class CompositeDefinition(BaseModel):
    """Base class for composite operations.

    Subclasses declare input/output specs, implement compose(), and are
    automatically validated and registered on definition.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    _registry: ClassVar[dict[str, type[CompositeDefinition]]] = {}

    # ---------- Metadata ----------
    name: ClassVar[str] = ""
    description: ClassVar[str] = ""

    # ---------- Inputs ----------
    inputs: ClassVar[dict[str, InputSpec]] = {}

    # ---------- Outputs ----------
    outputs: ClassVar[dict[str, OutputSpec]] = {}

    # ---------- Resources ----------
    resources: ResourceConfig = ResourceConfig()

    # ---------- Execution ----------
    execution: ExecutionConfig = ExecutionConfig()
    # Subclasses declare: params: Params = Params()

    # ---------- Compose ----------
    def compose(self, ctx: CompositeContext) -> None:
        raise NotImplementedError

    # ---------- Registry ----------
    @classmethod
    def get(cls, name: str) -> type[CompositeDefinition]:
        """Look up a composite class by name."""
        if name not in cls._registry:
            raise KeyError(
                f"Unknown composite: {name!r}. "
                f"Registered: {list(cls._registry.keys())}"
            )
        return cls._registry[name]

    @classmethod
    def get_all(cls) -> dict[str, type[CompositeDefinition]]:
        """Return a copy of the composite registry."""
        return dict(cls._registry)
```

### Differences from OperationDefinition

| Attribute | OperationDefinition | CompositeDefinition |
|---|---|---|
| `name`, `description` | Yes | Yes |
| `inputs`, `outputs` | Yes (InputSpec/OutputSpec) | Yes (same types) |
| `InputRole`, `OutputRole` enums | Required | Required |
| `Params` + `params` field | Yes | Yes (same pattern) |
| `resources`, `execution` | Yes | Yes |
| `_registry`, `get()`, `get_all()` | Yes | Yes (separate registry) |
| `tool`, `environments` | Yes | No — composites don't invoke external tools |
| `execute()` / `preprocess()` / `postprocess()` | Yes | No |
| `execute_curator()` | Yes | No |
| `compose()` | No | Yes |
| `runtime_defined_inputs` | Yes | No — inputs are always declared |
| `independent_input_streams` | Yes | No — irrelevant |
| `group_by` | Yes | No — irrelevant |
| `infer_lineage_from` on OutputSpec | Required | Not required — see below |

### `__pydantic_init_subclass__` validation

Runs at class definition time:

- Skip if `name` is falsy (abstract base).
- `compose()` must be overridden. Raise `TypeError` if not.
- `inputs` and `outputs` must be valid spec dicts.
- `InputRole` enum must match `inputs` keys (if inputs non-empty).
- `OutputRole` enum must match `outputs` keys (if outputs non-empty).
- Register in `CompositeDefinition._registry[cls.name]`.

### No `infer_lineage_from` on outputs

Operations require explicit lineage declarations on every `OutputSpec`.
Composites do not — their provenance is assembled from the internal
operations' edges, composed transitively via `update_ancestor_map()`. The
`OutputSpec` on a composite declares the type and role but not lineage.

---

## `CompositeContext`

### Interface

```python
class CompositeContext:
    def input(self, role: str) -> CompositeRef:
        """Reference a declared input of this composite."""

    def run(
        self,
        operation: type[OperationDefinition] | type[CompositeDefinition],
        inputs: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        resources: dict[str, Any] | None = None,
        execution: dict[str, Any] | None = None,
        backend: str | BackendBase | None = None,
        environment: str | dict[str, Any] | None = None,
        tool: dict[str, Any] | None = None,
    ) -> CompositeStepHandle:
        """Execute an operation or nested composite."""

    def output(self, role: str, ref: CompositeRef) -> None:
        """Map an internal result to a declared output of this composite."""
```

### `CompositeRef`

A lightweight reference for wiring within `compose()`. Represents either a
composite boundary input or an internal operation's output. Used as a value
in the `inputs={}` dict passed to `ctx.run()`.

```python
@dataclass(frozen=True)
class CompositeRef:
    source: ArtifactSource | None      # collapsed mode: in-memory artifacts
    output_reference: OutputReference | None  # expanded mode: lazy reference
    role: str
```

### `CompositeStepHandle`

Returned by `ctx.run()`. Provides `.output(role)` for wiring to downstream
internal operations. A single class with mode-dependent internals — the
external API is identical (just `.output(role)`), so separate classes behind
an interface would add indirection for no user-facing benefit.

```python
class CompositeStepHandle:
    def __init__(
        self,
        *,
        artifacts: dict[str, list[Artifact]] | None = None,
        step_future: StepFuture | None = None,
        operation_outputs: dict[str, OutputSpec] | None = None,
    ):
        self._artifacts = artifacts          # collapsed mode
        self._step_future = step_future      # expanded mode
        self._operation_outputs = operation_outputs  # for validation

    def output(self, role: str) -> CompositeRef:
        """Reference an output role of this internal operation."""
        if self._operation_outputs and role not in self._operation_outputs:
            available = sorted(self._operation_outputs.keys())
            raise ValueError(
                f"Unknown output role '{role}'. Available: {available}"
            )

        if self._artifacts is not None:
            # Collapsed: ref backed by in-memory artifacts
            return CompositeRef(
                source=ArtifactSource.from_artifacts(self._artifacts[role]),
                output_reference=None,
                role=role,
            )
        else:
            # Expanded: ref wrapping parent pipeline's OutputReference
            return CompositeRef(
                source=None,
                output_reference=self._step_future.output(role),
                role=role,
            )
```

### Two implementations of CompositeContext

**`CollapsedCompositeContext`** — for `pipeline.run()`:

- `ctx.input(role)` returns a `CompositeRef` backed by the resolved
  `ArtifactSource` for that input role.
- `ctx.run()` executes immediately:
  - For creators: calls `run_creator_lifecycle()` with in-memory sources.
  - For curators: calls `run_curator_flow()` in-process on the worker.
  - For nested composites: recursively creates a new
    `CollapsedCompositeContext` and calls `compose()`.
- `ctx.output()` records which internal artifacts map to each declared
  output role.
- Tracks all artifacts, edges, and timings for provenance assembly.
- Per-operation `resources`/`backend`/`execution` overrides are ignored
  (logged as debug warning).

**`ExpandedCompositeContext`** — for `pipeline.expand()`:

- `ctx.input(role)` returns a `CompositeRef` wrapping the `OutputReference`
  from the parent pipeline's input wiring.
- `ctx.run()` delegates to the parent `PipelineManager`:
  - Translates `CompositeRef` → `OutputReference` using parent step numbers.
  - Calls `pipeline.submit(operation, inputs=..., params=..., resources=...,
    backend=..., ...)` with per-operation overrides passed through.
  - Returns a `CompositeStepHandle` backed by the `StepFuture`.
- `ctx.output()` records the mapping from internal `OutputReference` to
  composite output roles.
- After `compose()` returns, `_expand_composite()` builds an
  `ExpandedCompositeResult` from the recorded output mappings (see below).

---

## Collapsed Mode: Execution Flow

```
pipeline.run(MyComposite, inputs={...}, params={...})
  → PipelineManager.submit() detects CompositeDefinition
  → _submit_composite():
      → wait for predecessors
      → compute composite_spec_id
      → check step cache → return cached result on hit
      → on miss: submit _run() closure to ThreadPoolExecutor
  → on worker (via Prefect):
      → execute_composite_step():
          → resolve inputs (OutputReference → artifact IDs)
          → build ExecutionComposite transport model
          → dispatch to backend
      → run_composite():
          → instantiate the CompositeDefinition (with params)
          → create CollapsedCompositeContext with resolved inputs
          → call composite.compose(ctx)
              → each ctx.run() executes eagerly:
                  → creator: run_creator_lifecycle() with in-memory sources
                  → curator: run_curator_flow() in-process
                  → nested composite: recursive compose()
              → ctx.output() records output mappings
          → collect artifacts + edges based on intermediates mode
          → record_execution_success()
  → verify staging → commit to Delta → compact
  → return StepResult
```

### Curator execution in collapsed mode

The whole composite runs on a worker (a separate process from the
orchestrator). Curators inside the composite run in-process on that worker
— no subprocess spawn needed. The memory isolation that curators normally
get (via `ProcessPoolExecutor(mp_context="spawn")`) is already provided by
the worker process itself.

The curator receives `dict[str, pl.DataFrame]` with artifact ID columns,
same as normal. The adaptation:

- Call `run_curator_flow()` directly (or a thin wrapper) instead of going
  through `_run_curator_in_subprocess()`.
- Curators that hydrate artifacts (e.g., Filter checking metrics) need the
  prior internal artifacts to be queryable in the `ArtifactStore`.

### Commit behavior in collapsed mode

There are two kinds of Delta commits in a collapsed composite:

**Pre-curator commits** (incremental, for hydration): Creator operations
produce in-memory artifacts that are not yet in Delta. When a curator
needs to run, the `CollapsedCompositeContext` commits all pending internal
artifacts to Delta first (via `DeltaCommitter`). The curator can then
query the `ArtifactStore` normally. This is necessary because curators
like Filter load artifact metrics from the store.

**Final commit** (at composite completion): After all operations complete,
the composite's output artifacts and provenance edges are committed per
the intermediates mode. Pre-curator commits are additive — artifacts
committed early for curator hydration are already in Delta and do not
need to be committed again.

The cost is one extra Delta write before each curator step that hydrates.
For typical composites (0-2 curators), this is negligible. An in-memory
`ArtifactStore` overlay could optimize this away later if needed.

Note: not all curators need hydration. Merge just concatenates artifact
IDs and does not query the store. Filter loads metrics from the store,
so it does.

### Partial failure in collapsed mode

If an internal `ctx.run()` call fails mid-composite, the composite step
fails as a whole. Any artifacts committed early (pre-curator commits)
remain in Delta as orphans — they are not rolled back. This is acceptable:
orphaned artifacts are inert (no provenance edges reference them from
the pipeline level), and a future compaction or garbage collection pass
can clean them up. The alternative (transactional rollback of Delta
commits) adds significant complexity for minimal benefit.

### Resume semantics

In collapsed mode, the composite is a single pipeline step. On resume,
it is either skipped (if cached via `step_spec_id`) or re-executed from
scratch. There is no partial resume of internal operations within a
collapsed composite.

In expanded mode, each internal operation is an independent pipeline step.
Resume works normally — completed steps are skipped via step caching,
pending steps re-execute.

---

## Expanded Mode: Execution Flow

```
pipeline.expand(MyComposite, inputs={...}, params={...})
  → PipelineManager._expand_composite():
      → instantiate CompositeDefinition (with params)
      → create ExpandedCompositeContext with parent pipeline reference
      → call composite.compose(ctx)
          → each ctx.run() delegates to parent pipeline:
              → translates CompositeRef → OutputReference
              → calls pipeline.submit(operation, inputs=..., params=...,
                  resources=..., backend=..., ...)
              → wraps StepFuture in CompositeStepHandle
          → ctx.output() records which StepFuture outputs map to
            composite output roles
      → build ExpandedCompositeResult from output mappings
      → return ExpandedCompositeResult
```

### Step numbering in expanded mode

Each `ctx.run()` inside `compose()` claims a step number from the parent
pipeline's `_current_step` counter. If a composite has 3 internal
operations, it consumes 3 step numbers. Subsequent steps in the parent
pipeline shift accordingly.

This is straightforward because `compose()` executes eagerly — step numbers
are claimed sequentially as `ctx.run()` calls happen.

### Step naming in expanded mode

Internal steps are prefixed with the composite name:

```
Step 0: DataGenerator
Step 1: normalize_filter_score.DataTransformer
Step 2: normalize_filter_score.Filter
Step 3: normalize_filter_score.MetricCalculator
Step 4: ReportBuilder
```

For nested composites, prefixes chain: if `full_scoring` expands and
contains an expanded `normalize_filter_score`, the steps are named
`full_scoring.normalize_filter_score.DataTransformer`, etc.

### Caching in expanded mode

Each internal operation is independently cached at the step level (normal
step caching via `step_spec_id`). There is no composite-level cache in
expanded mode — the composite itself is not a step.

### Output wiring in expanded mode

`pipeline.expand()` returns an `ExpandedCompositeResult` — a lightweight
object that maps composite output roles to the appropriate internal step's
`OutputReference`.

```python
class ExpandedCompositeResult:
    """Returned by pipeline.expand(). Maps composite outputs to internal steps."""

    def __init__(
        self,
        output_map: dict[str, OutputReference],
        output_types: dict[str, str | None],
    ):
        self._output_map = output_map
        self._output_types = output_types

    def output(self, role: str) -> OutputReference:
        if role not in self._output_map:
            available = sorted(self._output_map.keys())
            raise ValueError(
                f"Unknown output role '{role}'. Available: {available}"
            )
        return self._output_map[role]
```

The `output_map` is built from `ctx.output()` calls during `compose()`.
Each entry maps a composite output role to an `OutputReference` pointing
at the real parent pipeline step that produces it. Downstream steps wire
to these references normally — no special handling needed.

`ExpandedCompositeResult` duck-types with `StepResult` and `StepFuture`
(all three have `.output(role) -> OutputReference`), so downstream wiring
code is identical regardless of whether the upstream was an operation, a
collapsed composite, or an expanded composite.

---

## Transport Model: `ExecutionComposite`

For collapsed mode dispatch (pickle-serializable, sent to workers via
Prefect):

```python
@dataclass
class ExecutionComposite:
    composite: CompositeDefinition       # instantiated, with params
    inputs: dict[str, list[str]]         # resolved artifact IDs per role
    step_number: int
    execution_spec_id: str
    resources: ResourceConfig
    execution: ExecutionConfig
    intermediates: CompositeIntermediates
```

The `CompositeDefinition` instance carries the `compose()` method and
params. The worker calls `composite.compose(ctx)` with a
`CollapsedCompositeContext` initialized from the resolved inputs.

---

## Provenance

### Collapsed mode

Same approach as chains:

- Each internal `ctx.run()` produces `ArtifactProvenanceEdge` records.
- `update_ancestor_map()` composes edges transitively (works for DAGs).
- `_build_shortcut_edges()` creates edges from composite inputs to composite
  outputs.
- Edge collection follows the intermediates mode (discard/persist/expose).

### Expanded mode

Each internal operation is a real pipeline step, so provenance is handled
normally by the step executor. No composite-level provenance assembly
needed.

---

## Validation

Composites declare `inputs` and `outputs` as `ClassVar[dict[str, InputSpec]]`
and `ClassVar[dict[str, OutputSpec]]`, identical to `OperationDefinition`.

All existing validation in `PipelineManager.submit()` works unchanged:

- `_validate_input_roles()` — checks provided roles against `composite.inputs`
- `_validate_required_inputs()` — checks required roles are present
- `_validate_input_types()` — checks `OutputReference` artifact types
  against `InputSpec.accepts_type()`
- `StepFuture` — populated from `composite.outputs` (roles and types)
- `Params` — validated by Pydantic at instantiation

The `pipeline.run()` dispatch path detects composites via
`isinstance(operation, type) and issubclass(operation, CompositeDefinition)`
and routes to `_submit_composite()` instead of the normal step path.

The type signatures of `pipeline.run()` and `pipeline.submit()` change from
`operation: type[OperationDefinition]` to
`operation: type[OperationDefinition] | type[CompositeDefinition]`.
`pipeline.expand()` accepts only `type[CompositeDefinition]`.

---

## New Files

```
src/artisan/composites/
├── __init__.py                          # Public re-exports
└── base/
    ├── __init__.py
    ├── composite_definition.py          # CompositeDefinition base class
    ├── composite_context.py             # CompositeContext + Collapsed/Expanded impls
    └── provenance.py                    # Provenance helpers

src/artisan/schemas/composites/
├── __init__.py
└── composite_ref.py                     # CompositeRef, CompositeStepHandle,
                                         # ExpandedCompositeResult

src/artisan/execution/models/
└── execution_composite.py               # ExecutionComposite, CompositeIntermediates

src/artisan/execution/executors/
└── composite.py                         # run_composite() executor
```

## Modified Files

```
src/artisan/orchestration/
├── pipeline_manager.py         # Add _submit_composite(), _expand_composite(),
│                               # detect composites in run()/submit()/expand()
└── engine/
    ├── step_executor.py        # Add execute_composite_step()
    └── dispatch.py             # Route ExecutionComposite to run_composite()

src/artisan/utils/
└── hashing.py                  # Add compute_composite_spec_id()
```

## Deleted Files

```
src/artisan/orchestration/chain_builder.py
src/artisan/execution/executors/chain.py
src/artisan/execution/models/execution_chain.py
```

All chain references removed from `step_executor.py`, `dispatch.py`,
`pipeline_manager.py` (including the public `chain()` method and
`_submit_chain()`), and tests.

Reusable functions from `chain.py` (`update_ancestor_map()`,
`_build_shortcut_edges()`, `_collect_chain_edges()`,
`_collect_chain_artifacts()`, `remap_output_roles()`,
`validate_required_roles()`) move to the composite module (likely
`composite_context.py` or a shared `provenance.py` within the composites
package).

---

## Testing

Tests live at `tests/artisan/composites/`, mirroring source structure.
`test_composite_ref.py` moved to `tests/artisan/schemas/composites/` and
`test_execution_composite.py` moved to `tests/artisan/execution/models/`
(following the source reorganization into `schemas/composites/` and
`execution/models/`).

### Unit tests

- `test_composite_definition.py` — subclass validation
  (`__pydantic_init_subclass__`), registry, Params, missing `compose()`
  raises `TypeError`, InputRole/OutputRole mismatch detection
- `test_composite_context.py` — collapsed context: `ctx.input()`,
  `ctx.run()` with creators, curators, nested composites, `ctx.output()`;
  expanded context: delegation to parent pipeline, output mapping
- `test_composite_ref.py` — `CompositeRef` construction,
  `CompositeStepHandle.output()` in collapsed and expanded modes,
  `ExpandedCompositeResult.output()` validation
- `test_composite_provenance.py` — `update_ancestor_map()`,
  `_build_shortcut_edges()`, edge collection under each intermediates
  mode (discard/persist/expose), `step_boundary` values
- `test_execution_composite.py` — `ExecutionComposite` pickle
  serialization, `CompositeIntermediates` enum values

### Integration tests

- `tests/integration/test_composite_collapsed.py` — end-to-end collapsed
  composite: creators only, creators + curators, nesting, each
  intermediates mode, pre-curator commit behavior
- `tests/integration/test_composite_expanded.py` — end-to-end expanded
  composite: step numbering, step naming, per-operation resource
  overrides, caching of individual internal steps, nesting
- `tests/integration/test_composite_pipeline.py` — composites wired into
  full pipelines with upstream/downstream operations, `pipeline.run()`
  and `pipeline.expand()` in the same pipeline, resume after failure
