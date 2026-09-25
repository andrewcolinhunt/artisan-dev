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
files written to disk, and the ability to run in process pools or through a
remote-runner provider. The framework provides a three-phase lifecycle that separates
input preparation from computation from output construction.

**Curators** perform lightweight coordination — filtering artifacts, merging
streams, ingesting external files. They receive DataFrames of artifact metadata,
and run in an isolated local subprocess owned by the orchestrator. They never
use an external step runner. A single method replaces the three-phase lifecycle
because the sandbox and remote-dispatch overhead would add complexity with no
benefit.

| Aspect | Creator | Curator |
|--------|---------|---------|
| Purpose | Heavy computation, file I/O | Metadata coordination |
| Lifecycle | `preprocess` → `execute_function` / `execute_command` → `postprocess` | `execute_curator` |
| Sandboxing | Isolated directories per phase | None (in-memory) |
| Input delivery | Files written to disk | DataFrames of artifact metadata |
| Worker dispatch | Built-in local runner or external provider instance | Local spawned subprocess only |
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
`execute_function`.

**Why a separate phase?** Because the framework delivers artifacts in its own
format (materialized paths, content bytes, metadata). The computation has its
own expectations (a list of file paths, a JSON config, a batch file). Preprocess
bridges the gap, and you can test it independently of the actual computation.

### Execute: compute

Execute runs the core work through `execute_function` — a Python body that
receives a frozen `ExecuteInput` containing the prepared dict from preprocess
and a working directory. Write output files to that directory, call external
tools, run inference — the framework treats this method as a black box. It does
not inspect the return value. Any exception is caught and recorded as a failure.

Operations whose work should ship to a remote backend implement `execute_command`
instead, returning an argv the framework runs as a subprocess or on a deployed
tool endpoint. See [Command operations and tool
endpoints](#command-operations-and-tool-endpoints).

**Why a black box?** Because external tools know nothing about artifacts,
lineage, or pipelines. They take files in and produce files out. By isolating
the computation behind a clean boundary, you can test it by constructing an
`ExecuteInput` manually — no framework, no pipeline, no storage.

### Postprocess: construct

Postprocess builds draft artifacts from whatever `execute_function` produced. It
receives the files written to the execute directory and whatever value
`execute_function` returned. This is where you create typed artifact drafts and
assign `original_name` for the artifact's human-readable name. Declare each
output's exact parents in `ArtifactResult.lineage`; the framework never uses
that name to select parents.

**Why not return artifacts from `execute_function`?** Because artifact
construction requires framework knowledge (draft types, role names, step numbers)
that does not belong inside a black-box computation. Separating construction from
computation keeps `execute_function` testable without framework dependencies.

### Generative creators

Creators with no inputs (empty `inputs` dict) skip preprocess entirely. They
only implement `execute_function` and `postprocess`. The framework does not require a
`preprocess` override when there are no input artifacts to adapt. Their outputs
declare `derives_from={"inputs": []}` to signal that the produced
artifacts have no parents.

### Why this design matters

Three properties fall directly out of the phase separation:

- **Testability.** Each phase can be tested independently. Execute can be tested
  without the framework. Preprocess and postprocess can be tested without
  running the actual computation.
- **Debuggability.** Each phase runs in its own sandbox subdirectory
  (`preprocess/`, `execute/`, `postprocess/`). When something fails, the
  relevant directory contains exactly the inputs and outputs for that phase.
- **Portability.** External tools run inside the execute phase without knowing
  about artifacts or lineage. Preprocess adapts inputs; postprocess interprets
  outputs. The tool itself is unchanged.

(command-operations-and-tool-endpoints)=
### Command operations and tool endpoints

A creator ships its execute phase in one of two forms. A **function op**
implements `execute_function` — a Python body that runs where the lifecycle
worker is. A **command op** declares a `tool` and implements `execute_command`,
which returns an argv the framework runs as a local subprocess or on a deployed
tool endpoint. Command ops are how work reaches a remote backend like Modal,
where the execute phase becomes an HTTP client of the operation's endpoint.

A Python-body op opts into the same path by setting `execute_as_tool=True`: the
framework supplies the argv (`artisan op run <module:Qualname>`), runs it as a
subprocess locally, and deploys it as a tool endpoint remotely. This is why the
single execute slot split into `execute_function` and `execute_command` — the
same operation can run inline or as a deployable, backend-portable tool without
the author rewriting its logic.

See [Writing Creator Operations — Python body as a
command](../how-to-guides/writing-creator-operations.md#execute-as-tool) and
[Op Container Images](../how-to-guides/op-container-images.md) for the
deployment mechanics.

---

## The curator lifecycle

Curators skip the three-phase lifecycle entirely. A single `execute_curator`
method receives DataFrames of artifact metadata (each with at least an
`artifact_id` column, keyed by role name) and returns either new artifacts
(`ArtifactResult`) or routed artifact IDs (`PassthroughResult`).

Curators run in an isolated local subprocess without creator sandbox phases,
input materialization, or configurable creator-runner dispatch. This keeps
coordination separate from the orchestrator while avoiding the creator lifecycle
for metadata work.

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

When a curator or creator returns an `ArtifactResult`, it must include a
`lineage` dictionary with exactly the emitted artifact role keys. Each mapping
addresses a target by its role-local index and names an exact input ID or
sibling-output index. Root roles explicitly contain an empty list. The optional
`add_artifact` method constructs the same records while appending a draft;
manual construction remains supported.

Names, config content, and dispatch positions never supply missing parents.
The [provenance system](provenance-system.md#explicit-lineage) explains the
contract and joint parent groups.

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
without reading content receive ID-only inputs. This controls delivery to the
operation; orchestration still verifies stored input content before cache lookup
or dispatch. See [Hydration](artifacts-and-content-addressing.md#hydration-controlling-what-gets-loaded).

See [Writing Creator Operations](../how-to-guides/writing-creator-operations.md)
for usage and [Python API](../reference/python-api.md) for the spec definitions.

### Output specs

Each entry in `outputs` maps a role name to an `OutputSpec` that declares the
artifact type produced, whether the output is required, and which roles must
and may supply parents when the operation emits new drafts.

The `derives_from` field constrains each output occurrence’s explicit parent
declarations. It selects no artifacts. Three patterns:

- `{"inputs": ["data"]}` — output derives from the named input role
- `{"outputs": ["processed"]}` — output derives from another output role
  (output-to-output lineage, e.g., a metric derived from a data artifact that the
  same operation produced)
- `{"inputs": []}` — generative output with no parents

Creator operations must set `derives_from` on every output. Curator outputs
may leave it as `None` only when returning passthrough artifacts. Curators
returning new drafts receive the same runtime validation as creators.
An empty dict `{}` and mixed `"inputs"`/`"outputs"` parent kinds are invalid.
Output-role dependency cycles are rejected. Each listed role is both required
and allowed for every derived occurrence; reference-only inputs stay outside
ancestry unless explicitly included.

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

- Exactly one of `execute_function()`, `execute_command()`, or
  `execute_curator()` must be overridden (command ops also need a `tool`
  ToolSpec); overriding more than one raises `TypeError`
- Creator outputs must have explicit `derives_from` (not `None`)
- Creator operations with inputs must implement `preprocess()`
- `OutputRole` enum values must match `outputs` keys
- `InputRole` enum values must match `inputs` keys (when inputs exist)
- Algorithm configuration must use a matched nested `Params` model and `params`
  field; operations without algorithm parameters omit both

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

### Cacheability

An operation's `cacheable` class declaration says whether its declared inputs
and configuration are sufficient to reuse an earlier execution. It defaults to
`True`. Operations such as `IngestPipelineStep` read mutable external state and
set it to `False`, so each invocation observes that state again.

A false declaration bypasses both whole-step and execution cache lookups,
regardless of the step's cache policy or an explicit `skip_cache=False`. It
is a class contract, absent from instance configuration, parameter schemas,
and computational identity. Repeated executions can still produce identical
artifact IDs and use the store's ordinary artifact deduplication.

### Infrastructure fields

Built-in fields control how the framework runs the operation:

- **`runner_resources`** — portable hardware requirements for the step runner:
  CPU count, memory, GPUs, time limit, plus an `extra` dict for provider-specific
  settings such as a scheduler partition
- **`batch_strategy`** — batching and scheduling: artifacts per unit, units per
  worker, max workers, estimated seconds per unit
- **`compute_provider`** — where the execute phase runs: local, or a Modal tool
  endpoint
- **`compute_resources`** — hardware requested from the compute provider (Modal)
- **`tool`** — external executable specification: path, interpreter, subcommand
- **`environments`** — execution environment selection: local, Docker,
  Apptainer, or Pixi

Both `runner_resources` and `batch_strategy` can be overridden at the pipeline
step level. The operation provides sensible defaults; the pipeline adapts them to
specific cluster configurations.

### Algorithm parameters

Operations that need algorithm-specific configuration define a nested
`Params(BaseModel)` class as a Pydantic model, then declare a `params` instance
field annotated with that exact model. This contract separates domain
parameters (scale factor, noise amplitude, random seed) from infrastructure
concerns (CPUs, time limit, batch size), and gives each parameter its own type,
validation, documentation, and optional default.

The framework validates this pair at class definition time and inspects it for
parameter schemas, validation, serialization, and hashing. `params` may be
required or have a default instance of the exact nested model. Parameterless
operations omit both members. Subclasses may inherit the pair unchanged or
redefine both members together; redefining only one is invalid. Flat per-run
configuration fields are rejected.

See [Writing Creator Operations](../how-to-guides/writing-creator-operations.md)
for authoring examples.

---

## Multi-input operations

Most operations consume a single input role — one stream of artifacts in, one
stream out. Operations that consume multiple input roles need the framework to
**pair** artifacts across roles before delivery.

### Pairing strategies

The `group_by` field controls how inputs from different roles are matched
(overridable per step via `pipeline.run(..., group_by=...)`):

| Strategy | Behavior | When to use |
|----------|----------|-------------|
| `LINEAGE` | Pairs a candidate with its nearest target ancestor along directed provenance edges | An artifact paired with a result derived from it |
| `ZIP` | Pairs inputs by position (index-aligned) | Inputs in a known, consistent order |
| `CROSS_PRODUCT` | Every combination of inputs across roles | When every input should be combined with every other |
| `NAME` | Pairs inputs whose `original_name` stems match exactly | Independently-ingested streams that share a filename convention but no ancestry |
| `None` | No pairing (single-role or independent) | Operations with one input role |

Pairing happens between the resolve and batch phases in the orchestrator. The
operation iterates paired inputs via the `grouped()` method on
`PreprocessInput`.

`LINEAGE` normally pairs two roles, using the role from the earlier producing step as targets and
walking backward from each candidate. For `root → transformed → metric`,
`root` can pair with `metric`; if both `root` and `transformed` are targets,
`transformed` is the nearer match. For sibling branches `root → left` and
`root → right`, `left` and `right` do not pair merely because they share
`root`. An unmatched candidate is dropped with a warning. Equally near target
ancestors are ambiguous and raise an error. Operations with a primary role use
that role as the anchor when matching the other roles.

`NAME` strips all extensions before matching (`sample_001.csv` and
`sample_001.json` both reduce to the stem `sample_001`), so different
formats of the same logical entity pair naturally. Each role must have
unique stems among artifacts that carry an `original_name`; duplicates
raise `ValueError`. Stems present in some but not all roles are skipped
and logged at WARNING.

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
capture, result staging, logical commit — is handled by the execution and
orchestration layers above.

The consequence: you can unit test an operation by constructing its inputs
directly. You can run the same operation unchanged on a laptop or through a
cluster provider. You can compose operations freely because they have no
hidden dependencies on each other or on global state.

---

## Cross-references

- [Writing Creator Operations](../how-to-guides/writing-creator-operations.md)
  — Step-by-step guide to implementing a creator operation
- [Writing Curator Operations](../how-to-guides/writing-curator-operations.md)
  — Step-by-step guide to implementing a curator operation
- [Writing an Operation Tutorial](../tutorials/09-writing-operations/01-writing-an-operation.ipynb)
  — Hands-on walkthrough of building an operation from scratch
- [First Pipeline Tutorial](../tutorials/01-getting-started/01-first-pipeline.ipynb)
  — See operations in action in a complete pipeline
- [Execution Flow](execution-flow.md) — How operations execute within the
  dispatch-execute-commit pipeline
- [Provenance System](provenance-system.md) — How declarations constrain
  lineage tracking
- [Design Principles](design-principles.md) — Rationale for pure operations
  and the layered architecture
