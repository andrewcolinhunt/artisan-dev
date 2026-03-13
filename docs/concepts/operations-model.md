# Operations Model

An operation is a self-contained computation that the framework runs, tracks,
and caches on your behalf. You write the logic; the framework handles
everything around it — sandboxing, input delivery, provenance capture, worker
dispatch, and result staging.

This page explains the two operation types, the three-phase creator lifecycle,
the spec system that connects operations to the rest of the framework, and the
configuration patterns that control how operations behave.

---

## Two kinds of work

Computational pipelines contain two fundamentally different kinds of work, and
the framework treats them differently.

**Creators** wrap heavy computation — running external tools, performing ML
inference, transforming files. They need isolated working directories, input
files written to disk, and the ability to run on remote workers (SLURM nodes,
process pools). The framework provides a three-phase lifecycle that separates
input preparation from computation from output construction.

**Curators** perform lightweight coordination — filtering artifacts, merging
streams, ingesting external files. They receive DataFrames of artifact metadata,
return immediately, and never leave the orchestrator process. A single method
replaces the three-phase lifecycle because the overhead would add complexity
with no benefit.

| Aspect | Creator | Curator |
|--------|---------|---------|
| Purpose | Heavy computation, file I/O | Metadata coordination |
| Lifecycle | `preprocess` → `execute` → `postprocess` | `execute_curator` |
| Sandboxing | Isolated directories per phase | None (in-memory) |
| Input delivery | Files written to disk | DataFrames of artifact metadata |
| Worker dispatch | SLURM, ProcessPool | Local only |
| Return type | `ArtifactResult` (from postprocess) | `ArtifactResult` or `PassthroughResult` |

**The framework detects the type automatically.** If your class overrides
`execute_curator()`, it is a curator. Otherwise, it is a creator. No type flag,
no registration step.

---

## The creator lifecycle

Creator operations follow three phases, each with a single responsibility:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   PREPROCESS    │ ──▶ │     EXECUTE     │ ──▶ │   POSTPROCESS   │
│  Adapt inputs   │     │  Run the work   │     │  Build outputs  │
└─────────────────┘     └─────────────────┘     └─────────────────┘
      ▲                                                │
      │                                                ▼
  Artifacts in                                 Files + return value
  → plain dict out                             → ArtifactResult
```

### Preprocess: adapt

Preprocess translates framework-managed artifacts into whatever format the
computation expects. Extract file paths from materialized artifacts, parse JSON
content, generate configuration files — then return a plain `dict[str, Any]`.
No framework types, no artifact objects. The return value becomes the input to
`execute`.

**Why a separate phase?** Because the framework delivers artifacts in its own
format (materialized paths, content bytes, metadata). The computation has its
own expectations (a list of file paths, a JSON config, a batch file). Preprocess
bridges the gap, and you can test it independently of the actual computation.

### Execute: compute

Execute runs the core work. It receives a frozen `ExecuteInput` containing the
prepared dict from preprocess and a working directory. Write output files to
that directory, call external tools, run inference — the framework treats this
method as a black box. It does not inspect the return value. Any exception is
caught and recorded as a failure.

**Why a black box?** Because external tools know nothing about artifacts,
lineage, or pipelines. They take files in and produce files out. By isolating
the computation behind a clean boundary, you can test it by constructing an
`ExecuteInput` manually — no framework, no pipeline, no storage.

### Postprocess: construct

Postprocess builds draft artifacts from whatever `execute` produced. It
receives the files written to the execute directory and whatever value `execute`
returned. This is where you create typed artifact drafts and assign
`original_name` — the filename stem that drives the lineage matching algorithm.

**Why not return artifacts from execute?** Because artifact construction
requires framework knowledge (draft types, role names, step numbers) that does
not belong inside a black-box computation. Separating construction from
computation keeps `execute` testable without framework dependencies.

### Generative creators

Creators with no inputs (empty `inputs` dict) skip preprocess entirely. They
only implement `execute` and `postprocess`. The framework does not require a
`preprocess` override when there are no input artifacts to adapt. Their outputs
declare `infer_lineage_from={"inputs": []}` to signal that the produced
artifacts have no parents.

### Why this design matters

Three properties fall directly out of the phase separation:

- **Testability.** Each phase can be tested independently. Execute can be tested
  without the framework. Preprocess and postprocess can be tested without
  running the actual computation.
- **Debuggability.** Each phase runs in its own sandbox subdirectory
  (`preprocess/`, `execute/`, `postprocess/`). When something fails, the
  relevant directory contains exactly the inputs and outputs for that phase.
- **Portability.** External tools run inside `execute` without knowing about
  artifacts or lineage. Preprocess adapts inputs; postprocess interprets
  outputs. The tool itself is unchanged.

---

## The curator lifecycle

Curators skip the three-phase lifecycle entirely. A single `execute_curator`
method receives DataFrames of artifact metadata (each with at least an
`artifact_id` column, keyed by role name) and returns either new artifacts
(`ArtifactResult`) or routed artifact IDs (`PassthroughResult`).

Curators have no sandbox directories, no input materialization to disk, and no
worker dispatch. They run locally and immediately. This makes them fast and
simple, but limits them to work that does not require heavy computation or
file I/O.

### Two result shapes

**`ArtifactResult`** creates new draft artifacts. The curator hydrates input
data from storage, constructs new artifacts, and returns them keyed by output
role. Ingestion curators use this pattern — they read file references from
storage, convert them to domain artifacts, and return the drafts for the
framework to finalize.

**`PassthroughResult`** forwards existing artifact IDs through the pipeline
without creating new artifacts — the curator is routing, not transforming.
Filter and Merge use this pattern: they decide which artifacts continue, not
what new artifacts to create.

### Explicit lineage in ArtifactResult

When a curator (or a creator's postprocess) returns an `ArtifactResult`, it can
optionally include a `lineage` dict mapping output roles to lists of
`LineageMapping` objects. Each mapping declares an explicit parent-child
relationship between a draft artifact's `original_name` and a source artifact
ID. When `lineage` is provided, the framework uses it directly instead of
inferring lineage from filename matching. This is useful when the default
filename-matching algorithm cannot determine the correct parent — for example,
when input and output filenames share no common stem.

### Abstract curator bases

Curators can define abstract base classes by leaving `name` empty. The abstract
base implements `execute_curator` with shared logic, and concrete subclasses
set `name`, `outputs`, `OutputRole`, and a conversion method. The IngestFiles
base class uses this pattern: it handles hydration and iteration over file
references, while subclasses like IngestData implement only the
`convert_file()` method that produces the target artifact type.

---

## Declaring inputs and outputs

Operations declare their data contract through `inputs` and `outputs`
dictionaries. These declarations serve three purposes: validation at pipeline
construction time, control over how artifacts are delivered, and configuration
of lineage tracking.

### Input specs

Each entry in `inputs` maps a role name to an `InputSpec` that controls what
type of artifact the role accepts, whether the artifact is materialized to disk
or delivered in memory, and how much data is loaded from storage.

Two choices stand out. **Materialization** determines whether the framework
writes artifact content to a file in the sandbox (for external tools that read
from disk) or delivers content bytes directly (faster for in-memory Python
processing). **Hydration** controls whether the full artifact is loaded or only
the artifact ID — passthrough operations like Filter that route artifacts
without reading content benefit from ID-only hydration.

For the full field reference, see [Writing Creator Operations — Input specs](../how-to-guides/writing-creator-operations.md).

### Output specs

Each entry in `outputs` maps a role name to an `OutputSpec` that declares the
artifact type produced, whether the output is required, and — for creator
operations — which inputs the output derives from for provenance tracking.

The `infer_lineage_from` field is the core of per-output lineage control.
It tells the framework which artifacts to consider when matching output
filenames to establish parent-child provenance edges. Three patterns:

- `{"inputs": ["data"]}` — output derives from the named input role
- `{"outputs": ["processed"]}` — output derives from another output role
  (output-to-output lineage, e.g., a metric derived from a data artifact that the
  same operation produced)
- `{"inputs": []}` — generative output with no parents

Creator operations must set `infer_lineage_from` on every output. Curator
operations leave it as `None` (lineage for passthrough results is handled
differently). An empty dict `{}` is always invalid — it is ambiguous whether
you meant "no lineage" or "default matching." Combining both `"inputs"` and
`"outputs"` keys in the same dict is also invalid; use separate output roles
instead.

### Role enums

Operations with inputs define an `InputRole(StrEnum)` inner class whose values
match the `inputs` dict keys. Operations with outputs define an
`OutputRole(StrEnum)` inner class whose values match the `outputs` dict keys.

The framework validates this match at class definition time. If the enum values
diverge from the dict keys, a `TypeError` is raised immediately. This
constraint ensures role names are type-safe and discoverable via IDE
autocomplete — you reference `MyOp.InputRole.DATASET` rather than a raw
string.

---

## Validation at class definition time

When you define an `OperationDefinition` subclass, the framework validates
several rules before any instance is created. Classes with an empty `name` are
treated as abstract and skip validation entirely — this is how you create
intermediate base classes.

For concrete classes (non-empty `name`):

- Either `execute()` or `execute_curator()` must be overridden (not neither)
- Creator outputs must have explicit `infer_lineage_from` (not `None`)
- Creator operations with inputs must implement `preprocess()`
- `OutputRole` enum values must match `outputs` keys
- `InputRole` enum values must match `inputs` keys (when inputs exist)

Violations raise `TypeError` at import time. A misconfigured operation cannot
be instantiated, cannot be added to a pipeline, and cannot fail silently at
runtime hours into a cluster job.

### The operation registry

Every concrete operation (non-empty `name`) is automatically registered in a
global registry at class definition time. The registry maps operation names to
their classes, enabling lookup by name via `OperationDefinition.get("name")`.
This is an implementation detail used by the orchestration layer — you rarely
interact with it directly, but it means operation names must be unique across
the entire process.

---

## Configuration

Operations use two distinct configuration patterns: infrastructure
configuration through built-in fields, and algorithm configuration through a
nested `Params` class.

### Infrastructure fields

Built-in fields control how the framework runs the operation:

- **`resources`** — portable hardware requirements: CPU count, memory, GPUs,
  time limit, plus an `extra` dict for backend-specific settings like SLURM
  partition
- **`execution`** — batching and scheduling: artifacts per unit, units per
  worker, max workers, estimated seconds per unit
- **`tool`** — external executable specification: path, interpreter, subcommand
- **`environments`** — execution environment selection: local, Docker,
  Apptainer, or Pixi

Both `resources` and `execution` can be overridden at the pipeline step level.
The operation provides sensible defaults; the pipeline adapts them to specific
cluster configurations.

### Algorithm parameters

Operations that need algorithm-specific configuration define a nested
`Params(BaseModel)` class as a Pydantic model, then declare a `params` instance
field with a default. This pattern separates domain parameters (scale factor,
noise amplitude, random seed) from infrastructure concerns (CPUs, time limit,
batch size), and gives each parameter its own type, default, validation, and
documentation.

The `Params` class is a convention, not a framework requirement — the framework
does not inspect it. But the pattern is consistent across all built-in
operations and provides a clean namespace for algorithm tuning.

---

## Multi-input operations

Most operations consume a single input role — one stream of artifacts in, one
stream out. Operations that consume multiple input roles need the framework to
**pair** artifacts across roles before delivery.

### Pairing strategies

The `group_by` ClassVar controls how inputs from different roles are matched:

| Strategy | Behavior | When to use |
|----------|----------|-------------|
| `LINEAGE` | Pairs inputs that share provenance ancestry | Inputs from different steps that process the same original artifact |
| `ZIP` | Pairs inputs by position (index-aligned) | Inputs in a known, consistent order |
| `CROSS_PRODUCT` | Every combination of inputs across roles | When every input should be combined with every other |
| `None` | No pairing (single-role or independent) | Operations with one input role |

Pairing happens between the resolve and batch phases in the orchestrator. The
operation iterates paired inputs via the `grouped()` method on
`PreprocessInput`.

### Behavioral flags

Three ClassVar flags handle edge cases in input delivery:

**`runtime_defined_inputs`** — when `True`, input roles are provided by the
user at pipeline construction time instead of being declared in `inputs`. This
enables operations like Merge that accept a variable number of streams. Inputs
can be provided as a list (all artifacts flattened into a single
`_merged_streams` role) or as a dict with explicit role names.

**`hydrate_inputs`** — the default hydration mode for runtime-defined inputs
when no `InputSpec` exists for the role. Set to `False` for passthrough
operations that route artifact IDs without reading content.

**`independent_input_streams`** — when `True`, input roles can have different
numbers of artifacts. Most operations require equal-length roles for 1:1
pairing. Set to `True` for operations that concatenate streams rather than
pair them.

---

## Operations in the bigger picture

Operations sit at the center of the framework's layer stack, but they depend
only downward — on schemas. They know nothing about orchestration, scheduling,
storage, caching, or infrastructure.

This is by design. An operation receives data in, produces data out, and
declares its contract through specs. Everything else — input resolution, cache
lookup, worker dispatch, sandbox creation, input materialization, lineage
capture, result staging, atomic commit — is handled by the execution and
orchestration layers above.

The consequence: you can unit test an operation by constructing its inputs
directly. You can run the same operation unchanged on a laptop or a thousand
SLURM nodes. You can compose operations freely because they have no hidden
dependencies on each other or on global state.

---

## Cross-references

- [Writing Creator Operations](../how-to-guides/writing-creator-operations.md)
  — Step-by-step guide to implementing a creator operation
- [Writing Curator Operations](../how-to-guides/writing-curator-operations.md)
  — Step-by-step guide to implementing a curator operation
- [Writing an Operation Tutorial](../tutorials/writing-operations/01-writing-an-operation.ipynb)
  — Hands-on walkthrough of building an operation from scratch
- [First Pipeline Tutorial](../tutorials/getting-started/01-first-pipeline.ipynb)
  — See operations in action in a complete pipeline
- [Execution Flow](execution-flow.md) — How operations execute within the
  dispatch-execute-commit pipeline
- [Provenance System](provenance-system.md) — How `infer_lineage_from` drives
  lineage tracking
- [Design Principles](design-principles.md) — Rationale for pure operations
  and the layered architecture
