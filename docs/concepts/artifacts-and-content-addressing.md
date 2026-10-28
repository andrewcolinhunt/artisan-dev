# Artifacts and Content Addressing

An **artifact** is typed pipeline data, such as a dataset, metric, configuration,
or file reference. Its ID depends on its type, content, and semantic metadata.
Changing those values creates a different artifact.

---

## Content addressing

An artifact's identity is a versioned hash of three things: its registered type,
its type-owned canonical content, and common semantic metadata such as its name,
extension, and `metadata` mapping. The result is a 32-character hexadecimal
string. Equal bytes in different types or with different semantic metadata
identify distinct artifacts.

Runtime state does not enter identity. The producing step number, source URI,
managed storage URI, and materialized path may change without changing what the
artifact means.

**Deduplication.** If two operations produce the same typed semantic artifact,
only one content row is stored. Commit verification checks that content and
semantic metadata agree before reusing an existing ID; a conflict is an
integrity error.

**Deterministic caching.** Cache keys include artifact identities and the declared
invocation. Input roles, order, grouping, and duplicates affect the key.

**Stable provenance references.** Finalization prevents assignment to durable
fields. Serialization, verification, and materialization also check for nested
mutation. A lineage edge refers to validated artifact identities; changing the
content requires a new draft and identity.

---

## Why xxh3_128?

The framework uses the non-cryptographic xxHash algorithm `xxh3_128` for
deterministic content hashing with low overhead. It does not authenticate
untrusted content or provide protection against deliberate hash collisions.

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

Build content, names, metadata, and derived fields during the draft phase.
`finalize()` validates that state and assigns its identity.

Finalization is idempotent when the artifact is unchanged. Calling it again also
checks the saved identity snapshot, so nested mutation cannot pass silently. The
framework finalizes creator drafts after postprocess and curator drafts after
`execute_curator`.

Drafts remain mutable while an operation builds them. After finalization,
`materialized_path`
and locators explicitly declared by external artifact types remain mutable;
embedded source-path fields and all other durable state do not.

---

## Artifact types

The framework defines five built-in artifact types:

| Type | What it stores | Content format | Why a separate type |
|------|---------------|----------------|---------------------|
| **Metric** | Computed properties (scores, statistics, counts) | JSON-encoded bytes | Queryable structured data, often aggregated across runs |
| **Config** | Execution parameters and tool configurations | JSON-encoded bytes | Can reference other artifacts via `$artifact` patterns |
| **CSV** | CSV tabular data | Raw CSV bytes | Captures schema metadata (columns, row count) at creation |
| **File ref** | Complete external files, including existing inputs and generated outputs | Digest and size descriptor (bytes stay at a verified location) | Keeps files of any size outside content tables |
| **Appendable** | Individual records in a shared JSONL file | Metadata only (each record lives in the JSONL file) | Many records share one file; each is addressed by `record_id` |

Each type has its own Delta Lake table, so a metrics query can scan
`artifacts/metrics` without reading unrelated artifact types.

The global **artifact index** maps each `artifact_id` to its type and origin
step number. Readers use it to locate content by ID without scanning every
type's table.

### Shared fields

Content tables store `artifact_id`, `origin_step_number`, `metadata`, and the
type-specific fields added by each concrete type. `artifact_type` is implied by
the table and recorded explicitly in the artifact index. External locators are
stored separately in `artifacts/locations`, where one artifact may have more
than one verified URI. See the `Artifact` model through
[Python API lookup](../reference/python-api.md) for field definitions.

`origin_step_number` is global production metadata, not current-run ownership.
When an artifact is served from cache, run-scoped queries derive its current
logical step from that run's step-to-execution membership. Query results can
therefore expose both an original step and a current step without rewriting or
duplicating the content-addressed artifact.

### Config artifacts and cross-references

Config content can contain `$artifact` references: JSON objects such as
`{"$artifact": "<32-character artifact ID>"}`. The consuming input spec chooses
how to use them:

- With normal config materialization, Artisan writes referenced artifacts first
  and substitutes their local paths into the config file. This path substitution
  is local; endpoint execution rejects materialized configs with references.
- With `materialize=False` and `resolve_references=True`, preprocessing receives
  the unchanged config plus a read-only mapping of hydrated, materialized direct
  dependencies. The mapping remains available in postprocess. References to
  other configs are rejected in this mode.

For remote execution, the operation prepares portable files from those
dependencies. Artisan does not rewrite paths embedded inside arbitrary file
contents. Neither delivery mode creates lineage edges; the producing operation
declares the config's parents explicitly. See
[Read typed config dependencies](../how-to-guides/writing-creator-operations.md#read-typed-config-dependencies).

### External artifacts and verified locations

`FileRefArtifact` (`file_ref`) represents a complete external file of any size.
It identifies the bytes by their content digest and size, not by a path.
Existing input files and generated output files use this same type; there is
no size cutoff. `AppendableArtifact` identifies a record within a shared JSONL
file and adds its record ID to the digest and size. Moving verified bytes to
another URI preserves identity.

File references expose their current local path or object-store URI as `path`;
appendable records use `external_path`. The location table may hold several URIs
for one artifact, allowing a relocated or replicated copy to remain usable
without rewriting the artifact row. A missing, malformed, or mismatched location
fails instead of returning unverified bytes.

Where an operation writes a file determines how Artisan manages its location.
A creator writes generated files under its supplied `files_dir`; after
finalization, Artisan moves or uploads them to `files_root` and updates their
locators before sandbox cleanup. References to existing files outside
`files_dir` retain their locations. This lifecycle does not depend on file size.

Verification and materialization stream bytes in bounded chunks and compare
both digest and size. A finalized FileRef materializes to
`directory/artifact_id/basename(path)`: distinct artifacts with the same source
filename cannot overwrite each other, and filename-sensitive tools still see
the source basename. Always use the path returned by `materialize_to()` rather
than assuming the file sits directly in the requested directory. Drafts must
be finalized before materialization. For small files, `read_content()` is a
convenience that reads and caches the entire verified file in memory; for large
files, read the materialized path incrementally.

---

## Extensibility: adding new artifact types

Applications add types by defining an artifact model and a type definition
that binds its key, table path, and model. Registration happens when the type
definition class is imported. It requires a matching concrete artifact type,
a unique non-reserved table path, and, for external types, the common locator,
verification, and materialization contract.

Keep these classes in the application package. Every worker, reader, repair
process, and replay process that needs the type must load its definitions.
Declare trusted application modules before opening the store; importing a type
in the driver alone is not enough for fresh processes. See
[Create a New Artifact Type](../how-to-guides/creating-artifact-types.md#load-the-type-in-every-process).

---

## Hydration: controlling what gets loaded

Hydration controls what the operation receives. Full hydration supplies embedded
content or external content descriptors. For FileRef, the store loads metadata,
selects a location, and streams the file to verify its digest and size; it does
not cache the complete file bytes. ID-only delivery supplies references for
passthrough operations such as Filter and Merge. An input spec can choose a
different mode for each role. Non-hydrated artifacts are not materialized to disk.

Before cache lookup or dispatch, the orchestrator still loads input content
to validate identities and verifies external bytes. Filter can therefore route
dataset IDs while pipeline preparation still reads their content.
For supported result queries, see
[Reading pipeline results](storage-and-delta-lake.md#reading-pipeline-results).

---

## How content addressing enables caching

Cache keys combine artifact identities with the operation, parameters, and
effective configuration. Execution keys describe one batch; step keys describe
the full prepared input snapshot and include the step number. Both use the same
validated inputs, preserving roles, grouping, order, types, and duplicates.

Both levels identify the declared invocation. Reuse also depends on the
operation's cacheability and the applicable cache policy. Operations that read
mutable external state must declare that they are not cacheable.

A hit reuses an existing execution and its output edges. The current step keeps
its own attempt ID and records which executions it reused.

For the full two-level caching mechanism, see
[Execution Flow](execution-flow.md#two-level-caching).

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
- [Exploring Results Tutorial](../tutorials/01-getting-started/02-exploring-results.ipynb) -- Query artifacts interactively in Delta Lake tables
