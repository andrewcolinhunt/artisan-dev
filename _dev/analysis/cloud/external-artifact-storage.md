# Analysis: External Artifact Storage via Custom Artifact Types

**Date:** 2026-03-23
**Status:** Analysis
**Repo scope:** `artisan`

---

## Context

Artisan's current artifact types (DataArtifact, MetricArtifact,
ExecutionConfigArtifact) embed content directly in Delta Lake as binary columns.
This works well for small artifacts but breaks down for large files — a 1 GB CSV
or a collection of protein structures stored in a Rosetta silent file would bloat
the Delta Lake store and slow down queries against metadata that doesn't require
the content.

FileRefArtifact already demonstrates the pointer pattern: it stores metadata in
Delta Lake (path, content_hash, size_bytes) while actual file bytes stay on disk.
The question is how to generalize this for domain-specific artifact types that
need queryable metadata in Delta without embedded content.

---

## Current Architecture

### Artifact Type Registry

All artifact types plug into a generic registry (`ArtifactTypeDef` in
`schemas/artifact/registry.py`). Each type provides:

- `key` — string discriminator (e.g. `"data"`, `"file_ref"`)
- `table_path` — Delta Lake subdirectory (e.g. `"artifacts/data"`)
- `model` — Pydantic model with `POLARS_SCHEMA`, `to_row()`, `from_row()`

Registration is automatic via `__init_subclass__`. The storage layer
(staging, commit, retrieval) is completely type-agnostic — it dispatches
through the registry. This means **a new artifact type gets its own Delta
table, works with staging/commit, and is retrievable by type with zero
changes to the storage layer.**

### The `external_path` Field

The base `Artifact` class already has:

```python
external_path: str | None = Field(
    default=None,
    description="Path to external content on disk.",
)
```

This field is:

- Present on every artifact type
- Persisted in every Delta table (`POLARS_SCHEMA` includes it)
- Round-trips through `to_row()` / `from_row()`
- Does NOT affect `artifact_id` (content-addressing ignores it)
- Currently informational only — no code reads from it as a fallback

### FileRefArtifact as Prior Art

FileRefArtifact stores only pointer metadata in Delta:

| Column | Type | Purpose |
|---|---|---|
| `artifact_id` | String | Content-addressed ID |
| `origin_step_number` | Int32 | Pipeline step (partition key) |
| `content_hash` | String | xxh3_128 hash of file bytes |
| `path` | String | Absolute filesystem path |
| `size_bytes` | Int64 | File size at submission time |
| `original_name` | String | Filename stem |
| `extension` | String | File extension |
| `metadata` | String | JSON-encoded dict |
| `external_path` | String | Inherited, typically None |

Content is read on demand via `read_content()` → `Path(self.path).read_bytes()`.
The original file must remain at the recorded path.

### What DataArtifact Cannot Do Today

DataArtifact always embeds content:

- `draft()` requires `content: bytes` — not optional
- `_materialize_content()` raises if `content is None`
- `_finalize_content()` hashes `self.content` directly
- No fallback to `external_path` when `content` is None

The schema has the pieces but the behavioral logic for external-only mode was
never completed.

---

## Proposed Pattern: External-Content Artifact Types

Rather than modifying DataArtifact (which would add conditionals and complexity
to a working type), the clean approach is **new artifact types** that store
metadata in Delta and reference content on disk.

### Single-Structure-Per-File Example

For artifacts where one file = one artifact:

```python
class ExternalDataArtifact(Artifact):
    POLARS_SCHEMA = {
        "artifact_id": pl.String,
        "origin_step_number": pl.Int32,
        "external_path": pl.String,     # path to file on disk
        "content_hash": pl.String,      # integrity check
        "size_bytes": pl.Int64,
        "original_name": pl.String,
        "extension": pl.String,
        "columns": pl.String,           # CSV column headers (JSON)
        "row_count": pl.Int32,
        "metadata": pl.String,
    }
```

No `content: pl.Binary` column. `_materialize_content()` reads from
`external_path`. `_finalize_content()` hashes external_path + content_hash.

### Multiple-Artifacts-Per-File Pattern

For formats where a single file contains many structures (silent files,
HDF5, tar archives, concatenated CSVs), add addressing fields:

```python
class SilentStructureArtifact(Artifact):
    POLARS_SCHEMA = {
        "artifact_id": pl.String,
        "origin_step_number": pl.Int32,
        "external_path": pl.String,     # path to .silent file
        "decoy_tag": pl.String,         # unique structure ID within file
        "byte_offset": pl.Int64,        # optional, for fast seek
        "byte_length": pl.Int64,        # optional, for fast seek
        "content_hash": pl.String,      # hash of this structure's data
        "sequence": pl.String,          # amino acid sequence
        "total_score": pl.Float64,      # Rosetta energy
        "num_residues": pl.Int32,
        "metadata": pl.String,
    }
```

Multiple artifact rows point to the same `external_path` with different
`decoy_tag` / offset values. Each is independently addressable and queryable.

---

## Rosetta Silent Files: Format Properties

Silent files are the primary candidate for the multi-artifact-per-file pattern
in protein structure workflows.

### Format Summary

- **Text-based** (even the "binary" variant is ASCII-encoded IEEE 754 doubles)
- **Appendable** — `cat a.silent b.silent > combined.silent` or `>>` append
- **Tag-indexed** — every line of a structure ends with its unique decoy tag
- **Two variants:** protein (torsion angles, compact, lossy) and binary
  (full xyz coords); community consensus is always use binary

### Structure of a Silent File

Each structure ("decoy") consists of:

- `SEQUENCE` line — amino acid sequence
- `SCORE` header — column names for energy terms (once at file top)
- `SCORE` data — numerical scores, decoy tag as last column
- `REMARK` lines — key-value metadata
- `ANNOTATED_SEQUENCE` — full residue annotation
- Coordinate lines — one per residue, each ending with decoy tag

### Extraction Methods

- **grep:** `grep 'tag_name' file.silent` gets all lines for a structure
- **silent_tools** (Brian Coventry, `bcov77`): standalone Python toolkit that
  builds an in-memory index of byte offsets per tag for random access; no
  Rosetta license required
- **PyRosetta:** `poses_from_silent()` / `poses_to_silent()` for full Pose
  objects; requires license
- **rstoolbox:** pandas-based parsing of scores and sequences

### Appendability and Artisan

Silent files being appendable is a key property:

- A pipeline step can append new structures to an existing silent file
- Each new structure becomes a new artifact row in Delta Lake
- The file grows incrementally; Delta tracks what's in it
- Constraint: **decoy tags must be unique within a file** — the operation
  that appends is responsible for tag uniqueness

---

## How It Maps to Artisan

### Storage Split

| Layer | Stores | Queryable? |
|---|---|---|
| **Delta Lake** | artifact_id, decoy_tag, scores, sequence, metadata, external_path, offsets | Yes — filter by score, sequence, step, etc. |
| **Filesystem** | Full coordinate data in .silent file(s) | No — accessed on demand via tag/offset |

### Lifecycle

- **Creation:** Operation produces structures, appends to silent file, creates
  draft artifacts with tag + metadata, commits metadata rows to Delta
- **Query:** Scan Delta table — filter by score threshold, sequence length,
  step number, etc. — without touching the silent file
- **Retrieval:** `_materialize_content()` extracts the specific structure by
  tag (grep) or byte offset (seek + read) from the silent file
- **Append:** New pipeline step appends to the same silent file, commits new
  artifact rows; existing rows/artifacts are unchanged

### Integrity

- `content_hash` per structure enables verification that the silent file
  hasn't been corrupted or modified
- `byte_offset` / `byte_length` are optional performance hints — if the file
  is rewritten (compacted, sorted), these can be recomputed; `decoy_tag` is
  the stable identifier

### Python Tooling

`silent_tools` (MIT licensed, no Rosetta dependency) provides the core
read/write/index operations needed for `_materialize_content()` and
extraction. Key functions:

- `get_silent_index(filename)` — returns dict of tag → line numbers
- Tag-based extraction and concatenation
- Corruption detection

---

## Design Questions

- **File ownership:** Who creates/manages the silent file? Should Artisan
  manage it (create on first write, append on subsequent steps) or should the
  operation manage it and just tell Artisan where it is?

- **File immutability vs. appendability:** Artisan artifacts are immutable
  once finalized. An appendable file that grows across steps means the file
  itself is mutable even though individual artifacts within it are not. Is
  this a problem for caching/reproducibility? (Likely not — each artifact's
  content_hash provides integrity independent of file state.)

- **Path stability:** `external_path` stores an absolute path. If files are
  on shared NFS/Lustre, this works across nodes. If files move, artifacts
  break. Should there be a path resolution strategy (e.g., relative to a
  configurable root)?

- **Generality:** The same pattern works for HDF5 (dataset paths instead of
  tags), Parquet (row group indices), tar archives (member names), or any
  format with addressable sub-units. Should the base class be generic with
  the addressing scheme as a parameter?
