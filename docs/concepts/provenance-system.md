# Provenance System

When a pipeline produces thousands of results, you need to answer questions
like "where did this output come from?", "what parameters produced this
result?", and "which inputs failed?". Provenance is how the framework records
the full computational history so that every one of these questions is
answerable -- without detective work, log parsing, or guesswork.

This page explains the two provenance systems, why both exist, how lineage
edges are captured and validated, and how the provenance graph is stored and
visualized.

---

## Two complementary systems

The framework maintains two provenance systems because they answer
fundamentally different questions.

**Execution provenance** records what ran: which operation, with what
parameters, consuming which artifacts and producing which artifacts. It is
an activity log -- ground truth directly observable from actual events.

**Artifact provenance** records where things came from: which specific input
produced which specific output. It captures the individual derivation chains
(A->D, B->E, C->F) that execution provenance cannot express.

| System | Question it answers | Perspective |
|--------|---------------------|-------------|
| Execution provenance | "What computation happened?" | Activity-centric |
| Artifact provenance | "Where did this artifact come from?" | Entity-centric |

Both align with the [W3C PROV](https://www.w3.org/TR/prov-overview/) standard:

| Framework concept | W3C PROV equivalent |
|-------------------|---------------------|
| `ExecutionRecord` | Activity |
| `Artifact` | Entity |
| `ArtifactProvenanceEdge` | wasDerivedFrom |
| `ExecutionEdge` (input) | used |
| `ExecutionEdge` (output) | wasGeneratedBy |

---

## Why both systems are necessary

This is the core design constraint. Execution provenance cannot replace
artifact provenance, and vice versa.

An operation processes a batch of artifacts. A single execution record shows
"consumed {A, B, C}, produced {D, E, F}" -- but not which input produced which
output. The correspondence A->D, B->E, C->F is invisible from the execution
record alone.

```
Execution provenance              Artifact provenance

ExecutionRecord                   A ──→ D
  consumed: [A, B, C]            B ──→ E
  produced: [D, E, F]            C ──→ F

      ↑                               ↑
"What went in and out"          "Which produced which"
```

**Why you cannot derive artifact provenance later.** The information needed to
match outputs to inputs -- filename stems and grouping indices -- is available
only during the operation's lineage phase. Once execution finishes, this context
is gone. Attempting to reconstruct lineage after the fact by scanning filenames
or guessing relationships is brittle and unreliable.

**The framework's solution:** Lineage inference runs during a dedicated lineage
phase (after postprocess), while context is still available. The resulting edges
are staged alongside artifacts and committed atomically. There is no separate
"lineage reconstruction" step.

---

## Dual identity for executions

Each execution carries two identities because caching and provenance have
conflicting requirements.

| Identity | Purpose | Computed from |
|----------|---------|---------------|
| `execution_spec_id` | Cache key (deterministic) | operation name + sorted input IDs + merged params + config overrides |
| `execution_run_id` | Provenance tracking (unique per attempt) | spec_id + timestamp + worker_id |

Same `spec_id` means "same request" -- a cache hit. Different `run_id` means
"different attempt" -- distinct provenance even when the same computation runs
twice. This separation lets the framework cache aggressively without losing the
ability to distinguish separate executions in the provenance graph.

---

## How lineage is captured

Lineage capture happens automatically for most operations. The framework uses
filename stem matching -- the observation that operations naturally preserve
filename stems through transformations. A file named `sample_001.csv` that
gets transformed produces `sample_001_transformed.csv`. The shared stem
connects them.

### The algorithm

The stem matching algorithm strips extensions and probes prefixes:

- Strip all extensions from output and input filenames
  (`sample_001_transformed.csv` -> `sample_001_transformed`). Multi-part
  extensions like `.tar.gz` are fully stripped.
- Try exact match first -- if the output stem equals an input stem and there is
  exactly one candidate, that is the match
- If no exact match, try prefix matches longest-first: progressively shorten
  the output stem, checking whether the prefix matches any input stem
- Apply digit boundary protection at each prefix cut: the character being
  dropped must not be a digit (`design_1` does **not** match `design_10`)
- At every level, require exactly one candidate -- if multiple candidates share
  the same stem or prefix, skip that level and keep shortening
- If no level yields a unique match, the output has no lineage mapping. The
  subsequent completeness validation then raises a `LineageCompletenessError`

**Why the uniqueness requirement?** Better to fail loudly than record incorrect
lineage. If two candidates match a single output, the framework cannot determine
which is the true parent, so it rejects the result rather than guessing. This
ensures that every edge in the provenance graph is trustworthy.

### Digit boundary protection

This rule prevents a common class of false matches when filenames contain
numeric suffixes. When the algorithm shortens the output stem to test a prefix,
it only considers a cut point if the character being dropped is not a digit:

| Output stem | Candidate stem | Character at cut | Result |
|-------------|----------------|------------------|--------|
| `design_001_relaxed` | `design_001` | `_` (not a digit) | Match |
| `design_10_relaxed` | `design_1` | `0` (a digit) | Skipped |
| `report_cleaned` | `report` | `_` (not a digit) | Match |

Without this protection, `design_1` would falsely match `design_10`,
`design_100`, and `design_1000`. The digit boundary ensures that numeric
identifiers are treated as indivisible tokens.

---

## Explicit lineage

While stem matching handles most cases automatically, operations can also
declare explicit parent-child relationships. A `LineageMapping` specifies the
exact source artifact ID for each output draft, bypassing stem inference
entirely.

This is useful when:

- Output filenames do not share stems with their inputs (the operation renames
  files entirely)
- The operation has custom grouping logic that stem matching cannot express
- Output-to-output edges need precise control

Explicit mappings use the same `group_id` mechanism as inferred edges, so
co-input semantics (joint derivation) work identically in both paths.

---

## Lineage declaration

Operations control how lineage edges are created through the
[`infer_lineage_from`](operations-model.md#output-specs) field on `OutputSpec`.
This field tells the framework which artifacts to consider when matching output
filenames -- input roles, other output roles, or no parents (generative).

Three declaration patterns are supported:

| Pattern | Meaning | Example use |
|---------|---------|-------------|
| `{"inputs": ["data"]}` | Output derives from the named input role(s) | Transformer that processes input data |
| `{"outputs": ["data"]}` | Output derives from another output of the same operation | Metric summarizing a generated structure |
| `{"inputs": []}` | Generative -- no parents | Random data generator |

`None` (no declaration) is valid for curator operations, which handle
provenance through passthrough semantics.

The framework validates these declarations at class definition time. Creator
operations must declare lineage explicitly on every output. Combined
`{"inputs": [...], "outputs": [...]}` is not supported -- use separate output
roles instead.

### Output-to-output edges

When one output derives from another output of the same operation (for example,
a metric artifact that summarizes a generated structure), the
`infer_lineage_from` field references the output role. The framework matches
the metric's filename stem against the structure output's filenames rather than
the input filenames.

---

## The lineage pipeline

Lineage capture is not a single step -- it flows through a multi-stage pipeline
that progressively refines raw metadata into fully typed provenance edges.

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│     Capture      │     │      Build      │     │     Enrich      │
│                  │     │                 │     │                 │
│  Stem-match or   │──→  │  Resolve draft  │──→  │  Look up types  │
│  explicit mapping│     │  IDs to final   │     │  and add exec   │
│                  │     │  artifact IDs   │     │  context        │
│  → LineageMapping│     │  → SourceTarget │     │  → ArtifactProv │
│    (per role)    │     │    Pair         │     │    enanceEdge   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

**Capture** matches each output artifact to its source using stem matching or
explicit declarations, producing `LineageMapping` entries keyed by output role.
For multi-input operations with `group_by` pairing, co-input edges are created
for all non-primary input roles at the matched index, all sharing the same
`group_id`.

**Build** resolves draft references. During capture, output artifacts do not yet
have their final content-addressed IDs. The build stage maps draft
`original_name` values to finalized `artifact_id` values, producing lightweight
`SourceTargetPair` records.

**Enrich** adds execution context and artifact types. Source and target artifact
types are looked up (either from an in-memory dict or a bulk Delta Lake scan)
and combined with the execution run ID to produce final `ArtifactProvenanceEdge`
records ready for staging.

---

## Lineage validation

The framework validates lineage at three levels, failing fast when something is
wrong rather than staging incorrect provenance.

**Artifact validation** checks that output artifacts satisfy their declared
specs: required roles are present, artifact types match, and no undeclared
output roles exist.

**Completeness validation** verifies that every non-orphan output artifact has
a lineage mapping. An orphan is an output whose `infer_lineage_from` is
`{"inputs": []}` (generative). All other outputs must have at least one source
edge, or the framework raises a `LineageCompletenessError`.

**Integrity validation** checks that all lineage references point to real
artifacts: source IDs must exist in the input or output artifact sets, draft
names must correspond to actual outputs, and no duplicate mappings for the same
draft are allowed. Violations raise a `LineageIntegrityError`.

These validations run before any data is staged, so invalid lineage never
reaches Delta Lake.

---

## Edge types

All artifact provenance relationships are stored as directed
`ArtifactProvenanceEdge` records. The edge direction follows W3C PROV
`wasDerivedFrom` semantics: source is the parent, target is the derived
artifact.

Four edge patterns appear in practice:

| Pattern | Description | Example |
|---------|-------------|---------|
| Input -> Output | Standard derivation | `sample_001.csv` -> `sample_001_transformed.csv` |
| Output -> Output | Same-execution derivation | `data_001.dat` -> `data_001_metrics.json` |
| Co-input -> Output | Joint derivation (shared `group_id`) | `{dataset_a, dataset_b}` -> `comparison_report` |
| Config reference | Configuration referencing an artifact | `referenced_artifact` -> `execution_config` |

The first two are independent edges -- each parent artifact is sufficient on its
own to explain the derivation. Co-input edges are different (see below). Config
reference edges connect artifacts referenced in execution configurations to the
config artifact that contains the reference, enabling "what configs used this
artifact?" queries.

---

## Co-input edges and joint derivation

Some outputs cannot be produced from any single input alone. When you compare
two datasets, both are jointly necessary -- neither by itself could produce the
comparison result. Co-input edges represent this joint derivation.

**The test:** can the output be produced from any proper subset of the inputs?
If yes, use independent edges. If no -- if all inputs were jointly necessary --
use co-input edges.

| Scenario | Subset test | Edge pattern |
|----------|-------------|--------------|
| Filter pass-through | Single input suffices | Independent |
| Batch processing 1:1 | Each input independently | Independent |
| Compare(dataset_a, dataset_b) | Requires both datasets | Co-input |
| Aggregate({d1, d2, d3}) | Requires all inputs | Co-input |
| Join(left_table, right_table) | Requires both tables | Co-input |

Co-input edges share a `group_id` -- a deterministic hash computed from the set
of source artifact IDs. Multiple `ArtifactProvenanceEdge` records with the same
`group_id` and `target_artifact_id` represent a single joint derivation. This
allows queries like "what were ALL the inputs to this derivation?" without
requiring intermediate aggregate artifacts.

### How multi-input pairing works

Operations declare how inputs across roles should be
[paired](operations-model.md#pairing-strategies) via a `group_by` class
variable. The orchestrator pairs inputs before dispatch, and each pairing gets
a deterministic `group_id` that flows through to the
`ArtifactProvenanceEdge.group_id` field.

---

## Composite provenance and step boundaries

When operations are composed into composites (multiple creators wired together
via a `CompositeDefinition`), the provenance graph contains both internal edges
between intermediate artifacts and shortcut edges from composite inputs to final
outputs.

Each `ArtifactProvenanceEdge` carries a `step_boundary` flag that distinguishes
these two kinds:

| `step_boundary` | Meaning | When created |
|-----------------|---------|-------------|
| `True` (default) | Visible at the pipeline level | All normal edges, plus shortcut edges from composite inputs to final outputs |
| `False` | Internal to a composite | Edges between intermediate composite artifacts (when intermediates are persisted) |

Whether internal edges are persisted depends on the composite's `intermediates`
configuration. When intermediates are discarded, only shortcut edges appear in
the provenance graph. When intermediates are persisted or exposed, both internal
and shortcut edges are stored, with `step_boundary` distinguishing them. This
lets pipeline-level queries filter to step-boundary edges for a clean
high-level view while preserving full detail for debugging.

---

## Storage layout

Provenance data lives in three Delta Lake tables:

| Table | Path | Contents |
|-------|------|----------|
| Artifact edges | `provenance/artifact_edges` | Artifact-to-artifact derivation edges (`ArtifactProvenanceEdge` records) |
| Execution edges | `provenance/execution_edges` | Artifact-to-execution consumption/production edges (`ExecutionEdge` records) |
| Executions | `orchestration/executions` | Execution records with dual identity, timing, parameters, and status |

Artifact types are denormalized onto edge records (both `source_artifact_type`
and `target_artifact_type` appear on each `ArtifactProvenanceEdge`). This
avoids joins when filtering provenance queries by type -- a common pattern when
you want "all metric descendants of artifact X" without scanning the full
artifact index.

All provenance data is written through the
[staging-commit pattern](storage-and-delta-lake.md#the-staging-commit-pattern):
workers stage Parquet files, and the orchestrator commits them atomically.
Provenance tables are committed after content and index tables, so partial
failures leave artifacts reachable even if their provenance edges are
incomplete.

---

## Visualization

The framework provides two graph views built from provenance data, each
answering a different question about pipeline structure.

**Macro graphs** show the pipeline at the step level. Each step appears as an
execution node, each output role as a data node, and edges trace the data flow
between steps. This view answers "what is the pipeline shape?" and comes from
the steps table alone -- no artifact-level provenance needed.

**Micro graphs** show individual artifacts and executions. Every artifact and
execution record appears as its own node, with both execution edges
(artifact-to-execution links) and lineage edges (artifact-to-artifact
derivations) overlaid. This view answers "what happened to this specific
artifact?" and uses all three provenance tables.

Both graphs use left-to-right layout with strict column ordering
(execution column -> data column -> next execution column) to maintain
readability. Backward edges (such as passthrough artifacts consumed by a later
step) are rendered as dashed lines to avoid breaking the layout.

For interactive exploration in Jupyter, a stepper widget lets you navigate
the micro graph one step at a time, so you can watch the provenance graph
build up as the pipeline progresses.

---

## Querying provenance

The provenance graph supports several query patterns, from simple one-hop
lookups to full transitive walks.

**Backward queries** ("where did this come from?") start from a target artifact
and follow edges to its sources. A single hop returns direct parents; a
transitive walk returns all ancestors.

**Forward queries** ("what was derived from this?") start from a source artifact
and follow edges to its targets. These can be filtered by artifact type --
for example, finding all metrics derived from a specific data artifact.

**Type-filtered queries** combine forward or backward traversal with artifact
type filtering, taking advantage of the denormalized type fields on edge records
to avoid extra index scans.

**Full graph maps** load the entire backward or forward provenance map in a
single Delta scan, enabling efficient batch analysis when you need to explore
the full graph rather than starting from a single artifact.

For the practical API, see the
[inspecting provenance](../how-to-guides/inspecting-provenance.md) how-to
guide.

---

## Terminology

The provenance system uses two distinct vocabularies to avoid confusion between
execution-level and artifact-level perspectives:

| Context | Terms | Example |
|---------|-------|---------|
| Execution provenance | **inputs** and **outputs** | "The operation consumed inputs A, B and produced outputs D, E" |
| Artifact provenance | **source** and **target** | "Artifact D has source artifact A" (A is the parent, D is derived) |

This distinction matters when reading code and querying provenance tables. An
"input" is always relative to an execution. A "source" is always relative to a
derivation edge.

---

## Key design decisions

| Decision | Rationale |
|----------|-----------|
| Dual provenance (execution + artifact) | Different questions require different data structures |
| Lineage captured at execution time | Context needed for inference is lost after execution finishes |
| Multi-stage lineage pipeline (capture -> build -> enrich) | Separates matching logic from ID resolution from type enrichment, keeping each stage testable |
| Conservative stem matching (unique match or error) | Incorrect lineage is worse than missing lineage |
| Digit boundary protection | Prevents false matches across numeric suffixes (`design_1` vs `design_10`) |
| Distinct terminology (inputs/outputs vs source/target) | Avoids confusion between execution and artifact provenance contexts |
| Denormalized artifact types on edges | Query performance on large provenance tables without joins |
| Deterministic `group_id` for co-inputs | Enables "all parents of this derivation" queries without intermediate artifacts |
| Dual execution identity (spec_id + run_id) | Deterministic caching without losing per-attempt provenance |
| `step_boundary` flag on edges | Lets composite-internal edges coexist with pipeline-visible edges without separate tables |
| Three-level validation (artifacts, completeness, integrity) | Catches errors before staging rather than storing invalid provenance |

---

## Cross-references

- [Composites and Composition](composites-and-composition.md) -- How composites
  handle provenance for internal operations
- [Operations Model](operations-model.md) -- How operations declare
  inputs/outputs and lineage configuration
- [Execution Flow](execution-flow.md) -- When and how provenance is captured
  during the three execution phases
- [Storage and Delta Lake](storage-and-delta-lake.md) -- Table layout and the
  staging-commit pattern
- [Design Principles](design-principles.md) -- The "provenance is always
  captured, never reconstructed" principle
- [Inspect Pipeline Results and Provenance](../how-to-guides/inspecting-provenance.md) -- Practical guide to querying and visualizing provenance
- [Provenance Graphs Tutorial](../tutorials/analysis/01-provenance-graphs.ipynb) -- Interactive macro and micro provenance visualization
