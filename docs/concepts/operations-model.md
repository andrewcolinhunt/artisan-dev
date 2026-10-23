# Operations Model

An operation declares its inputs, outputs, and configuration, then implements
the work. Artisan delivers inputs, dispatches workers, validates output lineage,
and records results.

An operation keeps a tool's input preparation, computation, and output handling
together. Test that integration independently, then reuse it in pipelines or
[composites](composites-and-composition.md).

---

## Two kinds of work

**Creators** wrap computation such as external tools, ML inference, and file
transforms. **Curators** filter, merge, or ingest artifacts using metadata
queries. They use different lifecycles and input delivery:

| Aspect | Creator | Curator |
|--------|---------|---------|
| Purpose | Heavy computation, file I/O | Metadata coordination |
| Lifecycle | `preprocess` → `execute_function` / `execute_command` → `postprocess` | `execute_curator` |
| Sandboxing | Isolated directories per phase | Isolated process without creator phase directories |
| Input delivery | Artifact models, optionally hydrated and materialized | DataFrames of artifact metadata |
| Worker dispatch | Built-in local runner or external provider instance | Local spawned subprocess only |
| Return type | `ArtifactResult` (from postprocess) | `ArtifactResult` or `PassthroughResult` |

The framework detects the type from its methods. If your class overrides
`execute_curator()`, it is a curator. A concrete class must override exactly one
execute method. Its module must be imported before the operation can be used.

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
content, or generate configuration files, then return a `dict[str, Any]`.
Wrap per-artifact values in `PerArtifact` so execution receives the appropriate
slice. Ordinary lists are shared unchanged across execute calls. This separation
lets you test input adaptation without running the computation.

### Execute: compute

`execute_function` receives a frozen `ExecuteInput` containing the prepared
dict and a working directory. It can write output files, call external tools,
or run inference. Artisan passes its return value to postprocess without
inspecting it and records exceptions as failures.

Operations whose work should ship to a remote backend implement `execute_command`
instead, returning an argv the framework runs as a subprocess or on a deployed
tool endpoint. See [Command operations and tool
endpoints](#command-operations-and-tool-endpoints).

You can test the method directly with an `ExecuteInput`. Such a test exercises
the computation; a pipeline test also checks dispatch and artifact handling.

### Postprocess: construct

Postprocess builds typed draft artifacts from the execute directory's files
and the value returned by `execute_function`. Assign `original_name` for each
artifact's human-readable name and declare each
output's exact parents in `ArtifactResult.lineage`; the framework never uses
that name to select parents.

### Generative creators

Creators with no inputs (empty `inputs` dict) need no `preprocess` override.
The framework still calls the default method, which returns an empty dict.
Such creators can implement `execute_function` and `postprocess`. Their outputs
declare `derives_from={"inputs": []}` to signal that the produced
artifacts have no parents.

### Why this design matters

You can test each phase independently and inspect its files in the matching
`preprocess/`, `execute/`, or `postprocess/` directory. External tools work with
ordinary files; preprocess and postprocess handle their integration with
artifacts and lineage.

(command-operations-and-tool-endpoints)=
### Command operations and tool endpoints

A **function op** runs `execute_function` in the lifecycle worker. A **command
op** declares a `tool` and returns an argv from `execute_command`. Artisan runs
that command as a local subprocess or through a deployed tool endpoint, such
as a Modal endpoint.

A Python-body op opts into the same path by setting `execute_as_tool=True`: the
framework supplies the argv (`artisan op run <module:Qualname>`), runs it as a
subprocess locally, and deploys it as a tool endpoint remotely. The operation
must satisfy the file-based command contract: prepared file inputs, file
outputs, and no Python return-value transport.

See [Writing Creator Operations — Python body as a
command](../how-to-guides/writing-creator-operations.md#execute-as-tool) and
[Op Container Images](../how-to-guides/op-container-images.md) for the
deployment mechanics.

---

## The curator lifecycle

Curators implement a single `execute_curator` method that receives DataFrames
of artifact metadata (each with at least an
`artifact_id` column, keyed by role name) and returns either new artifacts
(`ArtifactResult`) or routed artifact IDs (`PassthroughResult`).

They run in an isolated local subprocess without creator sandbox phases,
input materialization, or configurable creator-runner dispatch.

### Two result shapes

**`ArtifactResult`** creates new draft artifacts. The curator hydrates input
data from storage, constructs new artifacts, and returns them keyed by output
role. Ingestion curators use this pattern — they read file references from
storage, convert them to domain artifacts, and return the drafts for the
framework to finalize.

**`PassthroughResult`** forwards existing artifact IDs without creating artifacts.
Filter and Merge use this pattern to select which artifacts continue.

### Explicit lineage in ArtifactResult

Every `ArtifactResult` needs a `lineage` entry for each emitted artifact role.
Derived outputs name exact input IDs or sibling-output indices; roots use an
empty list. Build declarations manually or with `add_artifact`. The
[provenance system](provenance-system.md#explicit-lineage) defines the contract
and joint parent groups.

### Abstract curator bases

Curators can define abstract base classes by leaving `name` empty. The abstract
base implements `execute_curator` with shared logic, and concrete subclasses
set `name`, `outputs`, `OutputRole`, and a conversion method. The IngestFiles
base class uses this pattern: it handles hydration and iteration over file
references, while subclasses like IngestData implement only the
`convert_file()` method that produces the target artifact type.

---

## Declaring inputs and outputs

The `inputs` and `outputs` dictionaries declare types, delivery settings, and
lineage requirements. Artisan validates calls and results against them.

### Input specs

Each entry in `inputs` maps a role name to an `InputSpec` that controls what
type of artifact the role accepts, whether the artifact is materialized to disk
or delivered in memory, and how much data is loaded from storage.

**Materialization** writes content to a file for tools that read from disk.
In-memory operations can receive content bytes instead. **Hydration** selects
full content or ID-only delivery. Orchestration still verifies stored input
content before cache lookup or dispatch; see
[Hydration](artifacts-and-content-addressing.md#hydration-controlling-what-gets-loaded).

See [Writing Creator Operations](../how-to-guides/writing-creator-operations.md)
for usage and [Python API](../reference/python-api.md) for the spec definitions.

Config inputs can opt into `resolve_references=True` with `materialize=False`
and hydration enabled. Preprocess and postprocess then access direct
dependencies through `inputs.referenced_artifacts(config)`. The operation
prepares any files needed for portable execution and still declares lineage
explicitly. See [Read typed config dependencies](../how-to-guides/writing-creator-operations.md#read-typed-config-dependencies).

### Output specs

Each entry in `outputs` maps a role name to an `OutputSpec` that declares the
artifact type produced, whether the output is required, and which roles must
and may supply parents when the operation emits new drafts.

The `derives_from` field constrains the roles that may supply parents. The
operation selects the exact artifacts:

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

Mismatched enum values and dict keys raise `TypeError` at class definition time.
Use `MyOp.InputRole.DATASET` to reference a role with IDE autocomplete.

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

Violations raise `TypeError` when the module is imported, before work can run.

### The operation registry

Every concrete operation (non-empty `name`) is automatically registered in a
global registry at class definition time. The registry maps operation names to
their classes, enabling lookup by name via `OperationDefinition.get("name")`.
Operation names must be unique across the applications loaded into a process.
Trusted application loading rejects registration conflicts or failed imports
before execution. Use the same application modules for workers, inspection,
repair, and replay; see [application loading](../how-to-guides/configuring-execution.md#load-application-definitions-in-every-process).

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
artifact IDs and use the store's ordinary artifact deduplication. Increment the
operation's `version` when changed code or dependencies can change its results
for the same inputs and configuration.

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

Override `runner_resources` and `batch_strategy` per step to adapt the
operation's defaults to the available infrastructure.

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

Operations with multiple input roles can **pair** artifacts across roles before
delivery.

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

ClassVar flags control runtime input roles and independent streams:

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

Creators adapt inputs, compute, and construct outputs. Curators also use the
store for collection queries. The framework owns dispatch, input preparation,
cache selection, result validation, and commits. Pipeline code chooses runner
instances and per-step execution overrides.

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
- [Design Principles](design-principles.md) — Rationale for separating computation and coordination
  and the layered architecture
