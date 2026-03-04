# Provenance System

When a pipeline produces thousands of results, you need to answer questions
like "where did this output come from?", "what parameters produced this
result?", and "which inputs failed?". Provenance is how the framework records
the full computational history so that every one of these questions is
answerable — without detective work, log parsing, or guesswork.

This page explains the two provenance systems, why both exist, and how lineage
edges are captured.

---

## Two complementary systems

The framework maintains two provenance systems because they answer
fundamentally different questions.

**Execution provenance** records what ran: which operation, with what
parameters, consuming which artifacts and producing which artifacts. It is
an activity log — ground truth directly observable from actual events.

**Artifact provenance** records where things came from: which specific input
produced which specific output. It captures the individual derivation chains
(A→D, B→E, C→F) that execution provenance cannot express.

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
"consumed {A, B, C}, produced {D, E, F}" — but not which input produced which
output. The correspondence A→D, B→E, C→F is invisible from the execution
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
match outputs to inputs — filename stems, pairing order, position within a
batch — is available only during the operation's postprocess phase. Once
execution finishes, this context is gone. Attempting to reconstruct lineage
after the fact by scanning filenames or guessing relationships is brittle and
unreliable.

**The framework's solution:** Lineage inference runs during the postprocess
phase, while context is still available. The resulting edges are staged
alongside artifacts and committed atomically. There is no separate "lineage
reconstruction" step.

---

## Dual identity for executions

Each execution carries two identities because caching and provenance have
conflicting requirements.

| Identity | Purpose | Computed from |
|----------|---------|---------------|
| `execution_spec_id` | Cache key (deterministic) | operation name + sorted input IDs + merged params + command overrides |
| `execution_run_id` | Provenance tracking (unique per attempt) | spec_id + timestamp + worker_id |

Same `spec_id` means "same request" — a cache hit. Different `run_id` means
"different attempt" — distinct provenance even when the same computation runs
twice. This separation lets the framework cache aggressively without losing the
ability to distinguish separate executions in the provenance graph.

---

## How lineage is captured

Lineage capture happens automatically for most operations. The framework uses
filename stem matching — the observation that operations naturally preserve
filename stems through transformations. A file named `sample_001.csv` that
gets transformed produces `sample_001_transformed.csv`. The shared stem
connects them.

### The algorithm

1. Strip all extensions from both output and input filenames
   (`sample_001_transformed.csv` → `sample_001_transformed`)
2. Check if the output stem starts with a candidate input stem
3. Apply digit boundary protection: `design_1` does **not** match `design_10`
   (the character after the match must not be a digit)
4. Require exactly one match — zero or multiple matches produce an orphan
   artifact (no parent edges)

**Why the uniqueness requirement?** Better to have no lineage than incorrect
lineage. If two candidates match a single output, the framework cannot determine
which is the true parent, so it records neither. This conservative behavior
ensures that every edge in the provenance graph is trustworthy.

### Digit boundary protection

This rule prevents a common class of false matches when filenames contain
numeric suffixes:

| Output stem | Candidate stem | Next character | Result |
|-------------|----------------|----------------|--------|
| `design_001_relaxed` | `design_001` | `_` (not a digit) | Match |
| `design_10_relaxed` | `design_1` | `0` (a digit) | No match |
| `report_cleaned` | `report` | `_` (not a digit) | Match |

Without this protection, `design_1` would falsely match `design_10`,
`design_100`, and `design_1000`. The digit boundary ensures that numeric
identifiers are treated as indivisible tokens.

---

## Lineage declaration

Operations control how lineage edges are created through the
[`infer_lineage_from`](operations-model.md#output-specs) field on `OutputSpec`.
This field tells the framework which artifacts to consider when matching output
filenames — input roles, other output roles, or no parents (generative).

The framework validates these declarations at class definition time. Creator
operations must declare lineage explicitly on every output. For the declaration
patterns and constraints, see the [output specs](operations-model.md#output-specs)
section of the Operations Model.

### Output-to-output edges

When one output derives from another output of the same operation (for example,
a metric artifact that summarizes a generated structure), the
`infer_lineage_from` field references the output role. The framework matches
the metric's filename stem against the structure output's filenames rather than
the input filenames.

For the complete field tables and validation rules, see the source at
`src/artisan/schemas/provenance/` and `src/artisan/execution/lineage/`.

---

## Edge types

All artifact provenance relationships are stored as directed
`ArtifactProvenanceEdge` records. The edge direction follows W3C PROV
`wasDerivedFrom` semantics: source is the parent, target is the derived
artifact.

Three edge patterns appear in practice:

| Pattern | Description | Example |
|---------|-------------|---------|
| Input → Output | Standard derivation | `sample_001.csv` → `sample_001_transformed.csv` |
| Output → Output | Same-execution derivation | `data_001.dat` → `data_001_metrics.json` |
| Co-input → Output | Joint derivation (shared `group_id`) | `{dataset_a, dataset_b}` → `comparison_report` |

The first two are independent edges — each parent artifact is sufficient on its
own to explain the derivation. Co-input edges are different.

---

## Co-input edges and joint derivation

Some outputs cannot be produced from any single input alone. When you compare
two datasets, both are jointly necessary — neither by itself could produce the
comparison result. Co-input edges represent this joint derivation.

**The test:** can the output be produced from any proper subset of the inputs?
If yes, use independent edges. If no — if all inputs were jointly necessary —
use co-input edges.

| Scenario | Subset test | Edge pattern |
|----------|-------------|--------------|
| Filter pass-through | Single input suffices | Independent |
| Batch processing 1:1 | Each input independently | Independent |
| Compare(dataset_a, dataset_b) | Requires both datasets | Co-input |
| Aggregate({d1, d2, d3}) | Requires all inputs | Co-input |
| Join(left_table, right_table) | Requires both tables | Co-input |

Co-input edges share a `group_id` — a deterministic hash computed from the set
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
| Conservative stem matching (unique match or nothing) | Incorrect lineage is worse than missing lineage |
| Digit boundary protection | Prevents false matches across numeric suffixes (`design_1` vs `design_10`) |
| Distinct terminology (inputs/outputs vs source/target) | Avoids confusion between execution and artifact provenance contexts |
| Denormalized artifact types on edges | Query performance on large provenance tables without joins |
| Deterministic `group_id` for co-inputs | Enables "all parents of this derivation" queries without intermediate artifacts |
| Dual execution identity (spec_id + run_id) | Deterministic caching without losing per-attempt provenance |

---

## Cross-references

- [Operations Model](operations-model.md) — How operations declare
  inputs/outputs and lineage configuration
- [Execution Flow](execution-flow.md) — When and how provenance is captured
  during the three execution phases
- [Design Principles](design-principles.md) — The "provenance is always
  captured, never reconstructed" principle
- [Provenance Graphs Tutorial](../tutorials/working-with-results/01-provenance-graphs.ipynb) — Interactive macro and micro provenance visualization
