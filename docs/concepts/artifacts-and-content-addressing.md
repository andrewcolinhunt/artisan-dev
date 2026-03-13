# Artifacts and Content Addressing

Every piece of data flowing through a pipeline -- a metric value, a
configuration file, a dataset, a file reference -- is an **artifact**.
Artifacts are the fundamental unit of data in Artisan, and every other
subsystem (provenance, caching, storage, execution) is built on top of them.

Understanding how artifacts work and why they are content-addressed explains
three properties you get for free: redundant computation is skipped
automatically, provenance edges never go stale, and the same result is never
stored twice.

---

## Content addressing: one idea, three consequences

An artifact's identity is the hash of its content. Specifically,
`artifact_id = xxh3_128(content)` -- a 128-bit hash represented as a 32-character
hex string. There is no separate "name" or "version" field. The content *is* the
identity.

This single decision produces three consequences that underpin the rest of the
framework:

**Deduplication.** If two operations produce identical output, only one copy is
stored. The commit logic performs an anti-join on incoming artifact IDs against
the existing Delta Lake table and silently drops duplicates. No configuration
needed -- it is a structural guarantee.

**Deterministic caching.** Cache keys are computed from the operation name,
input artifact IDs, parameters, and any config overrides. Since artifact IDs are
content hashes, identical computation always produces the same cache key. No
manual cache invalidation, no cache drift, no stale entries.

**Immutable provenance.** The artifact ID *is* the content. Modifying the content
would change the hash, producing a different artifact. This means provenance
edges are permanent: "A produced B" means "this exact content A was used to
produce this exact content B." Edges cannot go stale because the things they
point to cannot change.

---

## Why xxh3_128?

The framework uses xxHash (xxh3_128) rather than a cryptographic hash like
SHA-256. The choice is deliberate:

- **Speed.** xxh3 processes data at memory bandwidth. On a typical pipeline
  producing thousands of artifacts, hashing adds negligible overhead. A
  cryptographic hash would be 10-50x slower with no benefit -- the threat model
  does not include adversarial content manipulation.
- **128-bit collision resistance.** 128 bits provides sufficient collision
  resistance for artifact identity in scientific workflows. The birthday bound
  is roughly 2^64 (~10^19) artifacts before a collision becomes probable.
- **Determinism.** xxh3_128 is deterministic across platforms, architectures, and
  Python versions. Same bytes in, same hash out, always.

---

## The draft/finalize lifecycle

Artifacts follow a two-phase lifecycle that separates construction from
identity assignment.

```
Draft phase                          Finalize phase
┌──────────────────────┐             ┌──────────────────────┐
│ content is set       │   ──────>   │ hash is computed     │
│ artifact_id is None  │  finalize() │ artifact_id is set   │
│ mutable              │             │ semantically frozen   │
└──────────────────────┘             └──────────────────────┘
```

**Why two phases?** Because you cannot compute the hash until the content is
complete, and content construction can involve multiple steps (reading files,
encoding JSON, computing derived fields). The draft phase gives operations
freedom to build artifacts incrementally. Once `finalize()` is called, the
content hash is computed and the artifact has its permanent identity.

Finalization is idempotent -- calling it again on a finalized artifact is a
no-op. The framework finalizes all draft artifacts in bulk after an operation's
postprocess phase completes.

**Why not freeze the model?** Artifact objects are Pydantic models, but they are
intentionally *not* frozen (the model uses `extra="forbid"` without `frozen=True`).
Drafts need mutation (setting content, metadata, original name). After
finalization, immutability is semantic rather than enforced -- the content hash
guarantees that any mutation would produce a different identity, making
accidental mutation detectable.

---

## Artifact types

The framework defines four built-in artifact types. Each represents a
different category of pipeline data, with storage characteristics matched to
its purpose.

| Type | What it stores | Content format | Why a separate type |
|------|---------------|----------------|---------------------|
| **Metric** | Computed properties (scores, statistics, counts) | JSON-encoded bytes | Queryable structured data, often aggregated across runs |
| **Config** | Execution parameters and tool configurations | JSON-encoded bytes | Can reference other artifacts via `$artifact` patterns |
| **Data** | Generic tabular data (CSV) | Raw CSV bytes | Captures schema metadata (columns, row count) at creation |
| **File ref** | Pointers to external files | Metadata only (no embedded content) | Lightweight reference without copying large files into storage |

Each type maps to its own Delta Lake table (e.g., `artifacts/metrics`,
`artifacts/configs`, `artifacts/data`, `artifacts/file_refs`). This means you
can query all metrics across a pipeline with a single table scan, without
filtering through unrelated artifact types.

In addition to the per-type content tables, the framework maintains an
**artifact index** -- a global table mapping every `artifact_id` to its type
and origin step number. This index enables fast type lookups without scanning
each content table, which matters when resolving provenance graphs or loading
artifacts by ID without knowing their type in advance.

### Shared fields

All artifact types inherit a common set of fields from the base artifact model:

- **artifact_id** -- the content hash (None for drafts, 32-char hex when finalized)
- **artifact_type** -- a string discriminator identifying the type
- **origin_step_number** -- the pipeline step that produced this artifact
- **metadata** -- a JSON-serializable dict for extensibility
- **external_path** -- an optional path to external content on disk

These shared fields appear in every content table. The artifact index stores
a subset (`artifact_id`, `artifact_type`, `origin_step_number`, `metadata`).
Type-specific fields (like `content`, `columns`, `row_count`, `content_hash`)
are added by each concrete type.

### Config artifacts and cross-references

Config artifacts have a unique capability: they can contain `$artifact`
references -- JSON values like `{"$artifact": "60d12409ab3e78e8..."}` that point
to other artifacts by ID. During materialization, the framework resolves these
references to file paths, so the consuming operation receives a config file with
real paths where it expects them.

This matters for operations that use configuration files referencing data files
that need to exist on disk. The framework handles the resolution transparently:
non-config artifacts are materialized first (so their paths are known), then
config artifacts resolve their `$artifact` references against those paths.

### File refs and the two-step hash

File ref artifacts hash differently from other types. Instead of hashing the
file content directly, they hash a JSON record containing the content hash,
the original path, and the file size. This means two identical files at
different paths produce different artifact IDs.

The rationale: file refs are *pointers*, and their identity should include
where they point. If you ingest the same file from two different locations, those
are two distinct provenance events, even though the underlying bytes are
identical.

---

## Extensibility: adding new artifact types

The artifact type system is open. Domain layers register new types without
modifying the framework.

Registration requires two things: an artifact model class (the data shape) and
a type definition (the registry entry binding a key, table path, and model
class together). Registration is automatic -- defining a type definition
subclass triggers `__init_subclass__`, which validates that the model has the
required serialization interface (`POLARS_SCHEMA`, `to_row`, `from_row`) and
registers the type in both the type namespace and the type definition registry.

**Why this matters:** the framework has zero knowledge of domain-specific data.
A domain layer can add a custom artifact type, and the framework automatically
provides Delta Lake persistence, content-addressed caching, deduplication, and
provenance tracking. No framework modifications, no explicit wiring, no plugin
loading. Define two classes, and the entire storage and execution infrastructure
extends to cover the new type.

---

## Hydration: controlling what gets loaded

When the framework loads artifacts from storage, it can operate in two modes:

**Full hydration** loads the complete artifact including content bytes. This is
what operations need when they read or transform data.

**ID-only mode** loads only the artifact ID and type, skipping the Delta Lake
content table scan entirely. This is what passthrough operations need --
operations like Filter and Merge that route artifacts without reading their
content.

Hydration is controlled at the input spec level, so different input roles on the
same operation can use different modes. The framework also skips materialization
(writing content to disk) for non-hydrated artifacts, since there is no content
to write.

**Why this matters for performance:** a Filter operation processing 10,000
artifacts does not need to load 10,000 data files from Delta Lake. It loads
only the metric artifacts it needs to evaluate filter conditions, and passes
artifact IDs through unchanged. The difference can be orders of magnitude in
I/O.

---

## How content addressing enables caching

Content addressing makes caching deterministic. The framework computes cache
keys at two levels:

**Execution-level cache keys** are computed from the operation name, input
artifact IDs (sorted and deduplicated across all roles), merged parameters,
and any config overrides. Same data + same computation = same cache key, so
identical work is never repeated regardless of when or where it runs.

**Step-level cache keys** operate on step references rather than resolved
artifact IDs. They incorporate the operation name, step number (position in
the pipeline), parameters, upstream step spec IDs, and config overrides. This
enables the framework to determine that a step can be skipped before resolving
its inputs, which avoids unnecessary upstream computation.

Both levels share the same guarantee: no false hits (any input change
invalidates the key), no false misses (identical computation always matches),
and no manual invalidation.

For the full two-level caching mechanism, see
[Execution Flow](execution-flow.md#two-level-caching).

---

## Artifacts in the bigger picture

Artifacts sit at the intersection of every other subsystem:

- **Provenance** records edges between artifact IDs. Because IDs are immutable,
  edges are permanent. Each edge captures source and target artifact IDs, their
  types, their roles, and the execution that established the relationship.
- **Storage** persists artifacts in Delta Lake tables, one table per type, plus
  a global artifact index for type resolution. Deduplication is an anti-join on
  artifact IDs during commit.
- **Execution** materializes input artifacts to disk, runs operations, and
  finalizes output drafts. The draft/finalize lifecycle ensures content is
  complete before IDs are assigned.
- **Caching** derives cache keys from artifact IDs. Same inputs = same key =
  skip execution.

Each of these relationships is a direct consequence of content addressing.
Change the identity model, and every subsystem would need to change with it.

---

## Cross-references

- [Design Principles](design-principles.md) -- Rationale for content addressing
  and other foundational decisions
- [Storage and Delta Lake](storage-and-delta-lake.md) -- How artifacts are
  persisted, table layout, and the staging-commit pattern
- [Execution Flow](execution-flow.md) -- How artifacts are materialized,
  produced, and finalized during execution
- [Provenance System](provenance-system.md) -- How provenance edges reference
  artifact IDs
- [Creating Artifact Types](../how-to-guides/creating-artifact-types.md) --
  Step-by-step guide to adding new artifact types
- [Exploring Results Tutorial](../tutorials/getting-started/02-exploring-results.ipynb) -- Query artifacts interactively in Delta Lake tables
