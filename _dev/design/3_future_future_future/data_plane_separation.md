# Design: Data Plane Separation — DataFrames Inside, Artifacts Outside

**Status:** Draft
**Date:** 2026-02-27
**Context:** Scaling curator operations to millions of artifacts revealed a
fundamental architectural tension between the user-facing API (Artifact objects)
and the internal data plane (ID routing, edge building, staging). This document
captures the design philosophy that resolves that tension.

---

## The Insight

The system serves two distinct audiences with different needs:

1. **Users** write operations. They want nice objects with property access,
   `.draft()`, `.finalize()`, `.values`, `.content`. Pydantic models are perfect
   here — ergonomic, validated, self-documenting.

2. **The framework** routes IDs, builds provenance edges, stages Parquet, and
   commits to Delta Lake. None of this needs Pydantic objects. It needs columnar
   data (DataFrames) or flat lists of strings.

The original design conflated these two layers. Every internal path went through
the object layer, even when no user code would ever touch the result. This worked
fine at small scale but became the dominant cost at millions of artifacts.

---

## The Principle

**Stay columnar inside the framework. Only materialize to objects at the user
boundary.**

The "user boundary" is the moment when user-written code receives data: when
`execute_curator()`, `preprocess()`, or `execute()` is called on an operation
that the user authored. Everything before that point (input resolution, type
mapping, routing) and everything after (edge building, staging, committing)
should work with IDs, DataFrames, and flat lists.

```
Delta Lake  ──DataFrames──>  Framework  ──Artifacts──>  User Code
User Code   ──Artifacts──>   Framework  ──DataFrames──>  Delta Lake
```

The Artifact layer is a membrane at the boundary, not a universal internal
currency.

---

## What We Learned

### 1. Not all operations are the same

The original curator executor treated all curator operations uniformly: resolve
IDs, inflate to Artifacts, call `execute_curator`, handle the result. This
uniformity was appealing but misleading. Curator operations fall into three
distinct computational shapes:

- **ID routing** (Merge): Pure set operations on strings. Needs zero data from
  storage. The operation concatenates ID lists — that's it.

- **Columnar evaluation** (Filter): Joins passthrough IDs to metric values,
  applies predicates, returns a subset of IDs. Needs metric data but never reads
  passthrough artifact content.

- **Object transformation** (Ingest): Reads external data or artifact content,
  produces new Artifact objects. Actually needs the full object layer.

Forcing all three through the same materialization funnel meant that Merge paid
5 GB to construct Pydantic shells it immediately discarded, and Filter paid the
same to construct passthrough shells it only read `.artifact_id` from.

### 2. Boolean flags are a smell for missing dispatch

The operation class had `hydrate_inputs`, `runtime_defined_inputs`, and
`independent_input_streams` — three booleans to opt out of default behavior.
When an operation needs multiple flags to say "don't do the normal thing," the
normal thing isn't actually normal. The flags were compensating for a missing
distinction between operation types that have fundamentally different data needs.

The right solution is dispatch: give each computational shape its own execution
path that does exactly the work needed. The flags can remain as documentation,
but the executor should branch on operation type, not on flag combinations.

### 3. The round-trip is the antipattern

The most expensive antipattern was the inflate-deflate round-trip:

```
list[str]  →  list[Artifact]  →  extract .artifact_id  →  list[str]
```

This appeared in Merge (all IDs), Filter (passthrough IDs), and any passthrough
operation. The data started as strings, became objects, and immediately became
strings again. The object layer added no value — no validation, no content
access, no user interaction.

The general rule: if data enters as type X and leaves as type X without any
user code touching it in between, the intermediate conversion to type Y is waste.

### 4. `list[dict]` is the other antipattern

Before the DataFrame-native edge builder, execution edges were constructed as
`list[dict[str, str]]` — one Python dict per edge — then batch-converted to a
DataFrame for Parquet writing. At 22.2M edges, this meant 8.3 GB of dicts plus
a second copy during DataFrame conversion.

Building column-lists directly (`list[str]` per column) and constructing the
DataFrame from those is ~20x more memory-efficient. The `list[dict]` pattern
appears natural in Python but is the worst possible approach for columnar data.

The general rule: when the destination is a DataFrame or Parquet file, build
column-lists from the start. Never accumulate rows as dicts.

### 5. The type map scan is often unnecessary

`instantiate_inputs` always loads the full artifact type map (one Delta scan
over the artifact index) to resolve artifact IDs to their types. This is needed
for hydration (to know which Pydantic model class to construct) and for
non-hydrated shell construction (same reason).

But for pure ID routing (Merge), the type is irrelevant — the operation never
looks at it. And for Filter's passthrough role, the type is equally irrelevant.
Skipping the type map scan for operations that don't need it eliminates an 800 MB
allocation and a Delta scan entirely.

The general rule: don't resolve metadata about data that no code path will read.

### 6. Associated artifact resolution belongs at the operation level

The framework resolves `with_associated` artifacts during `instantiate_inputs`,
then stuffs them into `CuratorExecuteInput._associated`. This couples the
framework's input resolution to a specific operation's needs (Filter's implicit
criteria). If Filter needs associated metrics, it should ask the store directly —
it already has `artifact_store` as a parameter.

The general rule: framework-level input resolution should handle the common case
(IDs, hydration). Operation-specific data fetching (associated artifacts, lineage
matching) should be the operation's responsibility, using the store directly.

---

## Where the Boundaries Are

### User boundary (Artifacts appropriate)

- `execute_curator()` on Ingest ops — user code creates new Artifacts
- `preprocess()` / `execute()` / `postprocess()` on creator ops — user code
  reads and produces Artifacts
- Any public API where users query results (export, visualization)

### Internal plumbing (DataFrames / IDs appropriate)

- Input resolution (`resolve_output_reference`) — already efficient, returns
  `list[str]`
- Edge building (`build_execution_edges`) — already DataFrame-native
- Staging and committing — already DataFrame/Parquet
- Passthrough routing (Merge, Filter passthrough role) — should be `list[str]`
- Cache lookup and hit detection — should stay columnar
- Index and provenance queries — already efficient dict/list returns

### The gray zone

- Filter's metric hydration: needs actual metric values, but only for the metric
  artifacts (not passthrough). Could stay as Pydantic objects (small count) or
  move to a DataFrame join (future optimization). Either way, the passthrough
  role should not be inflated.

---

## Design Goals Going Forward

1. **New internal code defaults to DataFrames and ID lists.** Artifact objects
   are constructed only when user code will receive them.

2. **Operations declare what they need, the framework provides exactly that.**
   Different operation types get different execution paths. No lowest-common-
   denominator materialization.

3. **No `list[dict]` intermediaries on write paths.** Build column-lists and
   construct DataFrames directly.

4. **Passthrough operations never touch the Artifact layer.** If an operation
   routes IDs without reading content, it should receive and return `list[str]`.

5. **The executor is the dispatch point.** It inspects the operation type and
   chooses the minimal execution path. Operations don't need to know about other
   operations' paths.
