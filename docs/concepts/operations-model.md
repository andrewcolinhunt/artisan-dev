# Operations Model

An operation is a self-contained computation that the framework runs, tracks,
and caches on your behalf. You write the logic; the framework handles
everything around it — sandboxing, input delivery, provenance capture, worker
dispatch, and result staging.

This page explains the two operation types, the three-phase creator lifecycle,
and the spec system that connects operations to the rest of the framework.

---

## Two kinds of work

Computational pipelines contain two fundamentally different kinds of work, and
the framework treats them differently.

**Creators** wrap heavy computation — running external tools, performing ML
inference, transforming files. They need isolated working directories, input
files written to disk, and the ability to run on remote workers (SLURM nodes,
thread pools). The framework provides a three-phase lifecycle that separates
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
| Worker dispatch | SLURM, ThreadPool | Local only |
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
own expectations (a list of PDB paths, a JSON config, a batch file). Preprocess
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

**PassthroughResult** is unique to curators. It forwards existing artifact IDs
through the pipeline without creating new artifacts — the curator is routing,
not transforming. Filter and Merge use this pattern: they decide which artifacts
continue, not what new artifacts to create.

---

## Declaring inputs and outputs

Operations declare their data contract through `inputs` and `outputs`
dictionaries. These declarations serve three purposes: validation at pipeline
construction time, control over how artifacts are delivered, and configuration
of lineage tracking.

### Input specs

Each entry in `inputs` maps a role name to an `InputSpec`:

| Field | What it controls |
|-------|-----------------|
| `artifact_type` | What type of artifact the role accepts (`ArtifactTypes.ANY` for generic operations) |
| `required` | Whether the pipeline fails if this input is missing |
| `materialize` | Whether to write the artifact to disk (`True`, default) or deliver it in memory (`False`) |
| `hydrate` | Whether to load full content (`True`) or just the artifact ID (`False`, for passthrough) |
| `with_associated` | Artifact types to auto-resolve via provenance (e.g., annotations for a structure) |

**Materialization** is the most consequential choice. When `materialize=True`
(the default), the framework writes the artifact's content to a file in the
sandbox, and the operation receives a file path. When `materialize=False`, the
operation receives the content bytes directly. Materialization is necessary for
external tools that read files from disk; in-memory delivery is faster for
operations that parse data in Python.

**Hydration** controls how much data is loaded from storage. Full hydration
loads the complete artifact including content. ID-only mode (`hydrate=False`)
loads just the artifact ID and type, skipping the Delta Lake scan entirely.
This matters for passthrough operations like Filter that route artifacts without
reading their content — processing 10,000 artifacts without loading 10,000
files is orders of magnitude faster.

### Output specs

Each entry in `outputs` maps a role name to an `OutputSpec`:

| Field | What it controls |
|-------|-----------------|
| `artifact_type` | What type of artifact the role produces |
| `infer_lineage_from` | Which artifacts this output derives from (for provenance) |

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
you meant "no lineage" or "default matching."

### Role enums

Operations with inputs define an `InputRole(StrEnum)` inner class whose values
match the `inputs` dict keys. Operations with outputs define an
`OutputRole(StrEnum)` inner class whose values match the `outputs` dict keys.

The framework validates this match at class definition time. If the enum values
diverge from the dict keys, a `TypeError` is raised immediately. This
constraint ensures role names are type-safe and discoverable via IDE
autocomplete — you reference `MyOp.InputRole.STRUCTURE` rather than a raw
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

---

## Configuration

Operations declare configuration as Pydantic fields on the class. These fields
are validated at instantiation and accessible via `self` in all lifecycle
methods.

Built-in fields control infrastructure behavior:

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
enables operations like Merge that accept a variable number of streams.

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

- [Execution Flow](execution-flow.md) — How operations execute within the
  dispatch-execute-commit pipeline
- [Provenance System](provenance-system.md) — How `infer_lineage_from` drives
  lineage tracking
- [Design Principles](design-principles.md) — Rationale for pure operations
  and the layered architecture
- [First Pipeline Tutorial](../tutorials/getting-started/01-first-pipeline.ipynb)
  — See operations in action in a complete pipeline
- [Pipeline Patterns Tutorials](../tutorials/pipeline-patterns/01-sources-and-chains.ipynb)
  — Reusable patterns for composing operations
