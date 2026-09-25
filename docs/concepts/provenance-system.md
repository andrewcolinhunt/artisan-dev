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
| Executions row | Activity |
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

Executions row                    A ──→ D
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
are staged alongside artifacts and exposed through the same completed logical
commit. There is no separate "lineage reconstruction" step.

---

## Dual identity for executions

Each execution carries two identities because caching and provenance have
conflicting requirements.

| Identity | Purpose | Computed from |
|----------|---------|---------------|
| `execution_spec_id` | Cache key (deterministic) | operation config + ordered role/group/position/type/ID input occurrences |
| `execution_run_id` | Provenance tracking (unique per attempt) | spec_id + timestamp + worker_id |

Same `spec_id` means "same request" -- a cache hit. Different `run_id` means
"different attempt" -- distinct provenance even when the same computation runs
twice. This separation lets the framework cache aggressively without losing the
ability to distinguish separate executions in the provenance graph.

---

## How lineage is captured

Operations declare which artifacts produced each output. Artisan checks these
declarations and records them while the execution context is available.
It does not choose parents from filenames, input order, dispatch groups, or
config contents. Execution participation is still recorded automatically.

For example, an operation transforms dataset X and computes a metric from the
transformed output. It declares `X -> transformed -> metric`. An additional
reference dataset used during execution appears in execution provenance and
cache identity, but becomes an artifact parent only if the operation declares it.

## Explicit lineage

Every successful `ArtifactResult` contains matching role keys in `artifacts` and
`lineage`. Each derived output occurrence has mappings to all its required
parent roles. Root roles explicitly contain an empty mapping list. The rule is
the same for creators and artifact-producing curators.

The result addresses outputs by their exact index within a role's list.
A `LineageMapping` supplies a target `draft_index`, a `source_role`, and either
an input `source_artifact_id` or a sibling `source_output_index`. Duplicate
human names are legal; names never resolve a declaration. List positions must
remain stable until finalization.

Authors can build these records manually or use the optional
`ArtifactResult.add_artifact` method. The method appends a draft and its supplied
parents together and returns its index. It chooses no parents. Both forms receive
the same validation. See the [creator authoring guide](../how-to-guides/writing-creator-operations.md#explicit-lineage)
for examples.

## Lineage declaration

The [`derives_from`](operations-model.md#output-specs) field on `OutputSpec`
constrains the mappings supplied for each output occurrence:

| Pattern | Required declaration |
| --- | --- |
| `{"inputs": ["data"]}` | At least one parent from input role `data`, and no other roles |
| `{"inputs": ["left", "right"]}` | At least one input parent from each role; multiple parents within a role are allowed |
| `{"outputs": ["processed"]}` | At least one co-produced output addressed by its index in `processed` |
| `{"inputs": []}` | A root role, with an explicit empty lineage list |

Every creator output needs a contract. `None` is permitted only for passthrough
outputs; curators that emit drafts need contracts too. A curator with no static
outputs may emit runtime-named root roles with explicit empty lineage lists.
Mixed input/output parent kinds, empty dictionaries, and output-role cycles are
rejected. A present optional output role with no drafts has an empty lineage
list; an omitted role appears in neither result dictionary.

### Output-to-output edges

A metric derived from a co-produced dataset declares that dataset's output
index. Finalization resolves the index to an artifact ID without matching names.
The operation can use the index returned by `add_artifact` or construct a
`LineageMapping` itself.

### Config ancestry

A config containing `{"$artifact": X}` still resolves X to a tool-local path
during materialization. Its producing operation separately declares `X -> config`
using the ordinary input-role contract. Several referenced parents can share a
role; an operation may call `get_artifact_references()` and deduplicate the IDs
when constructing its declarations. The executor never scans config contents
for missing edges. Config-producing operation tests should assert the intended
parent set as well as the resolved file paths.

`IngestPipelineStep` deliberately imports artifacts as new roots, including
configs. That import boundary does not preserve foreign ancestry.

### Optional filename matching

An operation whose tool encodes parent identity in output filenames can call
`match_outputs_to_inputs_by_stem` from `artisan.operations.lineage`. The operation
supplies candidate names and IDs, chooses their role, and uses the returned IDs
to declare parents. Executors never call this helper.

(the-algorithm)=
#### The algorithm

The helper normalizes basenames and compound extensions, tries exact stems,
then the longest eligible prefix. A prefix cannot split a numeric suffix:
`design_1` does not match `design_10`. At the first matching level, several
distinct IDs raise an ambiguity error; the helper never falls back to a shorter
prefix. Missing matches also raise. Repeated identical candidates are harmless.

This is useful only when the operation knows its naming convention is sufficient.
Other tools can return an operation-specific manifest linking relative output
paths to exact input IDs. The operation writes and reads that manifest; Artisan
has no generic manifest or filename-to-parent discovery step.

## The lineage pipeline

```text
Operation declarations -> validate roles and exact references
                       -> finalize drafts in their original order
                       -> resolve indices to artifact IDs
                       -> label declared parent sets and attach types
                       -> stage provenance records
```

Resolution uses input IDs and exact output indices. Type enrichment uses the
already-loaded creator inputs or a bulk curator type lookup; unknown types fail.
Neither phase reads names or source content to discover parents. The existing
artifact edge table stores resolved IDs, types, roles, and execution context.

## Lineage validation

Output validation checks roles, artifact types, and required outputs. Integrity
validation checks role-key coverage, target and sibling indices, source-kind
agreement with the static contract, input membership in the named role, and
duplicate exact declarations. Two different parents in one role are valid.

Completeness validation requires every derived output occurrence to have a
parent from every listed role. Roots must have no mappings. These checks run
before recording success or staging artifact edges. A perfectly matching filename
cannot rescue an omitted declaration; an unrelated filename does not invalidate
a correct one.

Structural validation cannot decide whether the author selected the scientifically
correct parents. That responsibility belongs to the operation and its tests.

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

All parents declared for one output occurrence form one joint derivation.
This applies equally to input parents, sibling outputs, and explicitly declared
config parents. Config edges support “what configs used this artifact?” queries.

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

Co-input edges share a `group_id` -- a deterministic hash computed from the
role, concrete type, and artifact ID of each unique declared parent.
One unique parent has no group ID. Several parents share the hash of their
sorted tuples, so declaration order does not change the label. Equivalent
finalized sibling parents collapse before this choice. Multiple
`ArtifactProvenanceEdge` records with the same
`group_id` and `target_artifact_id` represent a single joint derivation. This
allows queries like "what were ALL the inputs to this derivation?" without
requiring intermediate aggregate artifacts.

### How multi-input pairing works

Operations declare how inputs across roles should be
[paired](operations-model.md#pairing-strategies) via a `group_by` class
variable. The orchestrator pairs inputs before dispatch. Its group identifiers
serve batching and cache identity; artifact edge groups are calculated separately
from the operation’s declared parent sets. For example, `S+A -> P` and `S+B -> P`
retain distinct derivations even when P has the same semantic artifact ID.

---

## Composite provenance

A composite expands into real pipeline steps — each internal `ctx.run()`
runs as an ordinary step. Its provenance is therefore ordinary
step-to-step provenance: the edges linking a composite's internal
operations are the same `ArtifactProvenanceEdge` records that any sequence
of steps produces, and pipeline-level queries see the composite's data
flow exactly as they see any other steps.

Because every internal operation is a real step, there is no separate
composite-internal edge kind. The grouping is expressed through the
composite's step-name prefix (`composite_name.operation`), not through a
special provenance edge.

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
workers stage Parquet files, and the orchestrator includes them in a logical
commit with the artifacts and terminal step snapshot. Supported readers expose
these rows only after logical completion and validate the recorded effects.
Raw Delta reads bypass that boundary and can show incomplete physical writes.

---

## Visualization

The framework provides two graph views built from provenance data, each
answering a different question about pipeline structure.

**Macro graphs** show the pipeline at the step level. Each step appears as an
execution node, each output role as a data node, and edges trace the data flow
between steps. This view answers "what is the pipeline shape?" and comes from
the steps table alone -- no artifact-level provenance needed.

**Micro graphs** show individual artifacts and executions. Every artifact and
every row in the executions table appears as its own node, with both execution edges
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

Artifact ancestry belongs to the shared store. To inspect what a particular
run executed or reused, select that run explicitly; artifact origin step
numbers alone do not identify its outputs. See
[Select the run you want to inspect](../how-to-guides/inspecting-provenance.md#select-the-run-you-want-to-inspect)
for run selection and practical queries.

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
| Operation-owned declarations | The operation knows the exact parents; the framework validates and records them |
| Validate -> resolve -> enrich | Separate contract checks, exact reference resolution, and type lookup |
| Optional operation-called matching | Reuse naming helpers without implicit executor behavior |
| Digit boundary protection | Prevents false matches across numeric suffixes (`design_1` vs `design_10`) |
| Distinct terminology (inputs/outputs vs source/target) | Avoids confusion between execution and artifact provenance contexts |
| Denormalized artifact types on edges | Query performance on large provenance tables without joins |
| Deterministic `group_id` for co-inputs | Enables "all parents of this derivation" queries without intermediate artifacts |
| Dual execution identity (spec_id + run_id) | Deterministic caching without losing per-attempt provenance |
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
- [Provenance Graphs Tutorial](../tutorials/08-analysis/01-provenance-graphs.ipynb) -- Interactive macro and micro provenance visualization
