# Design: External Content Artifacts

**Date:** 2026-04-04
**Status:** Draft

---

## Problem

Artisan has two content storage strategies:

- **Embedded:** DataArtifact, MetricArtifact, ExecutionConfigArtifact store
  bytes directly in Delta Lake as `pl.Binary` columns
- **User-managed pointer:** FileRefArtifact stores a path to a user-owned
  file that Artisan doesn't manage

Neither supports the case where **Artisan owns and manages external files**
-- files that are too large or domain-specific for Delta Lake, but whose
lifecycle should be tied to the pipeline. The pattern applies to any format
where:

- Content is too large or binary for Delta Lake embedding
- A single file contains many independently addressable units (structures,
  datasets, records)
- Per-unit metadata should be queryable in Delta without touching the file
- Artisan should manage the file lifecycle (creation, consolidation, cleanup)

Additionally, when a pipeline step runs N parallel workers that each produce
an external file, those files need consolidation into a single file per step.
Artisan has no mechanism for this -- the commit phase only merges Parquet
metadata rows, never external content.

---

## Prior Art Survey

### Artifact Type Registry (`schemas/artifact/registry.py`)

`ArtifactTypeDef` auto-registers via `__init_subclass__`. Each type provides
`key`, `table_path`, and `model`. The storage layer (staging, commit,
retrieval) is completely type-agnostic -- it dispatches through the registry.
**A new artifact type gets its own Delta table with zero storage layer
changes.** Types can be defined in external packages -- they register at
import time, not at framework build time.

**Reuse:** Domain-specific external-content types register through this
mechanism. No changes needed.

### FileRefArtifact (`schemas/artifact/file_ref.py`)

Stores `path`, `content_hash`, `size_bytes` in Delta -- no `content:
pl.Binary` column. `_finalize_content()` hashes a JSON blob of reference
metadata (not file bytes). `_materialize_content()` copies the file into
the execution sandbox.

**Key limitation:** FileRefArtifact assumes the file is **user-managed** --
Artisan never creates, moves, or deletes the referenced file. The `path`
field stores an absolute path to wherever the user's file lives. If the user
deletes or moves the file, the artifact breaks.

**Reuse:** The external-content pattern (`_finalize_content` hashing
metadata, no Binary column) is the template for domain-specific
external-content types. The ownership model is different.

### `external_path` on `Artifact` base (`schemas/artifact/base.py:54`)

Present on every artifact, persisted in every Delta table, round-trips
through `to_row()`/`from_row()`. Currently **informational only** -- no
production code reads from it. Does NOT affect `artifact_id` (verified by
`tests/artisan/schemas/test_external_path.py`).

The only production write: `IngestData` sets
`external_path=file_ref.path` on DataArtifacts it creates -- purely for
traceability back to the original file.

**Gap:** No artifact type uses `external_path` as a functional content
pointer. No code falls back to it for content retrieval.

### IngestFiles / IngestData (`operations/curator/`)

`IngestFiles` is an abstract curator base. Hydrates FileRefArtifacts, calls
`convert_file()` on each, returns `ArtifactResult`. `IngestData` is the
concrete subclass that reads file bytes and creates DataArtifacts.

**Observation:** This pattern reads the file twice -- once in
`_promote_file_paths_to_store()` to hash, once in `convert_file()` to get
content. Works fine for small files but not ideal for large ones. The
external-content pattern avoids this by never embedding content.

### PipelineConfig (`schemas/orchestration/pipeline_config.py`)

Three configurable path roots: `delta_root`, `staging_root`, `working_root`.
Frozen Pydantic model. Sibling directories (`logs/`, `images/`) are
derived from `delta_root.parent` in consuming code. Adding `files_root`
follows the same pattern.

### ArtifactStore (`storage/core/artifact_store.py`)

Initialized with `base_path` (the Delta root). Has no concept of external
file storage. Curators that need to write consolidated files would need
access to `files_root`.

### Summary

| Existing code | Disposition |
|---|---|
| `ArtifactTypeDef` registry | Reuse as-is -- domain types auto-register from external packages |
| `FileRefArtifact` | Template for external-content pattern; stays as user-file provenance |
| `external_path` base field | Promote from informational to functional content pointer |
| `IngestFiles` curator pattern | Template for domain-specific consolidation curators |
| `PipelineConfig` | Extend with `files_root` |
| `ArtifactStore` | Extend with `files_root` access |
| Storage layer (staging, commit) | No changes needed |

---

## Design

### Two Categories of External Files

This design introduces a clear distinction between two categories of files
referenced by artifacts:

| | User-managed | Artisan-managed |
|---|---|---|
| **Who creates the file** | User, before pipeline runs | Operations/curators during pipeline |
| **Where it lives** | Arbitrary user path | `files_root/{step_number}/` |
| **Who manages lifecycle** | User (Artisan never touches) | Artisan (created, consolidated, potentially cleaned up) |
| **Artifact type** | FileRefArtifact | Domain-specific (defined in external packages) |
| **Content pointer field** | `path` (FileRefArtifact-specific) | `external_path` (base class) |
| **Role** | Provenance: "pipeline started from these files" | Storage: "content too large/complex for Delta" |

### `external_path` as a Functional Content Pointer

For artifact types that opt in, `external_path` becomes the primary content
location. The opt-in is implicit: if an artifact type has no `content` field
and uses `external_path` to reference its data, it is an external-content
type.

No changes to the base `Artifact` class are needed. The field already
exists, persists, and round-trips.

This means:

- **DataArtifact** (embedded): has `content` field, `external_path` remains
  informational. No behavioral change.
- **FileRefArtifact** (user pointer): has its own `path` field. No change.
- **Domain external types** (e.g. SilentStructureArtifact): no `content`
  field, `external_path` is functional. New pattern, defined in domain
  repos, auto-registered at import time.

### Materialization Model for External-Content Types

External-content types have a fundamentally different relationship to
materialization than embedded types. In the embedded model, one artifact =
one file. In the external-content model, **many artifacts point to the same
file** with different addressing within it (e.g., 1000 structures in one
silent file, each with a unique `decoy_tag`).

Per-artifact materialization is wrong for this pattern:
- Redundant I/O: N artifacts → N reads of the same file
- Wrong output: N tiny extracted files instead of one file
- Wrong interface: downstream operations want the file + tag list, not
  individual extractions

**External-content types skip materialization by default.** They inherit
the base class default `_default_hydrate = True` (metadata loaded from
Delta) and are consumed with `InputSpec(materialize=False)`. Operations
read from `external_path` directly and use addressing fields
(`decoy_tag`, byte offset, etc.) to access individual units within the
file.

```python
# Operation preprocess for external-content artifacts
def preprocess(self, inputs: PreprocessInput) -> dict:
    structures = inputs.input_artifacts["structures"]
    silent_file = structures[0].external_path  # all share one file
    tags = [s.decoy_tag for s in structures]
    return {"silent_file": silent_file, "tags": tags}
```

**This works on cloud.** `external_path` is a URI — a local path on NFS
or `s3://bucket/files/combined.silent` on cloud. The framework stores the
URI; the operation decides how to read it (`open()` for local,
`fsspec.open()` for cloud). One download per file regardless of how many
artifacts reference it, which is more efficient than per-artifact
materialization would be.

### `files_root` Configuration

A new field on `PipelineConfig` for Artisan-managed external file storage.

**Why a config field, not derived at point of use:** `logs/` and `images/`
are derived from `delta_root.parent` in consuming code (e.g.,
`config.delta_root.parent / "logs"`). `files_root` differs because users
may want external files on different storage than Delta (e.g., cheap bulk
NFS vs fast SSD for Delta). Making it a config field with a default
enables this override without changing the common case.

```python
class PipelineConfig(BaseModel):
    # ... existing fields ...
    files_root: Path = Field(
        default=None,
        description="Root path for Artisan-managed external files. "
        "Defaults to delta_root.parent / 'files'.",
    )

    @model_validator(mode="after")
    def _default_files_root(self) -> PipelineConfig:
        if self.files_root is None:
            object.__setattr__(self, "files_root", self.delta_root.parent / "files")
        return self
```

**Directory layout:**

```
{delta_root.parent}/              # pipeline root
    delta/                        # existing -- Delta Lake tables
    staging/                      # existing -- worker Parquet before commit
    working/                      # existing -- execution sandboxes
    logs/                         # existing -- pipeline + failure logs
    images/                       # existing -- visualization SVGs
    files/                        # NEW -- Artisan-managed external files
        {step_number}/
            workers/              # per-worker files (pre-consolidation)
            combined.silent       # example: consolidated file (domain-specific naming)
```

**Propagation:** `files_root` needs to be accessible in curators via
`artifact_store.files_root`. This requires threading the config value
through `ArtifactStore.__init__()`.

**User override:** Users who want external files on different storage (e.g.,
cheap bulk NFS vs fast SSD for Delta) pass `files_root` to
`PipelineManager.create()`:

```python
pipeline = PipelineManager.create(
    name="protein_design",
    delta_root=Path("/fast/ssd/delta"),
    staging_root=Path("/fast/ssd/staging"),
    files_root=Path("/bulk/nfs/files"),
)
```

Follows the same pattern as `working_root` -- optional with a default.

### Where Worker External Files Live

Operations running on workers produce files in their sandbox
(`working_root/.../execute/`). For external-content artifacts, these files
need to survive sandbox cleanup for downstream steps.

**Approach:** The worker writes its output file to the sandbox, then during
postprocess copies/moves it to a worker-scoped location under `files_root`.
This requires `files_root` to be accessible from workers (true on shared
NFS, which is the target environment).

```
{files_root}/
    {step_number}/
        workers/
            {execution_run_id}.silent    # per-worker file
        combined.silent                  # consolidated (after curator)
```

The `workers/` subdirectory holds per-worker files that a consolidation
curator reads from. After consolidation, these could optionally be cleaned
up (the consolidated file contains all the data).

The staging directory is not suitable -- its cleanup happens after commit,
before downstream steps run.

### Content Addressing for External Artifacts

Domain-specific external-content types should include `external_path` in
their `_finalize_content()` hash when the same logical content exists at
different locations (e.g., per-worker file vs consolidated file). This
produces distinct `artifact_id`s for the same content at different paths,
avoiding dedup conflicts at commit time.

```python
def _finalize_content(self) -> bytes | None:
    if self.content_hash is None:
        return None
    return json.dumps(
        {
            "content_hash": self.content_hash,
            "decoy_tag": self.decoy_tag,  # domain-specific
            "external_path": self.external_path,
        },
        sort_keys=True,
    ).encode("utf-8")
```

---

## Scope

| File | Change |
|------|--------|
| `src/artisan/schemas/orchestration/pipeline_config.py` | Add `files_root` field with default from `delta_root.parent / "files"` |
| `src/artisan/schemas/execution/runtime_environment.py` | Add `files_root_path: Path | None` field (workers need this to write per-worker output files to `files_root/{step}/workers/`) |
| `src/artisan/storage/core/artifact_store.py` | Add optional `files_root: Path | None = None` parameter to `__init__()`. Backward compatible — existing call sites pass nothing and get `None`. |
| `src/artisan/orchestration/pipeline_manager.py` | Pass `files_root` to ArtifactStore, accept in `create()`, propagate to RuntimeEnvironment |
| `src/artisan/orchestration/engine/step_executor.py` | Pass `files_root` when constructing ArtifactStore (two call sites) |
| `src/artisan/execution/executors/curator.py` | Pass `files_root` from `runtime_env.files_root_path` to ArtifactStore |
| `src/artisan/execution/executors/composite.py` | Pass `files_root` from `runtime_env.files_root_path` to ArtifactStore |
| `src/artisan/execution/context/builder.py` | Pass `files_root` to ArtifactStore |

`ArtifactStore.__init__` takes `files_root` as optional with default
`None`, so call sites that don't need it (interactive_filter,
ingest_pipeline_step, tests, docs) are unchanged.

No changes to the base `Artifact` class or staging/commit code. The
behavioral changes (external-content artifact types) are defined in domain
repositories.

---

## Testing

| Test file | Coverage |
|-----------|----------|
| `tests/artisan/schemas/orchestration/test_pipeline_config.py` | `files_root` default derivation from `delta_root`, explicit override, frozen model behavior |
| `tests/artisan/storage/test_artifact_store.py` | Add tests: ArtifactStore accepts and exposes `files_root`, `None` default |
| `tests/artisan/orchestration/test_pipeline_manager.py` | `files_root` threaded through `create()` to ArtifactStore |
| `tests/artisan/schemas/execution/test_runtime_environment.py` | `files_root_path` field present, propagated from PipelineConfig |

---

## Open Questions

- **File cleanup on re-run:** When a step is re-run, should
  `files_root/{step}/` be cleaned up? Old consolidated files become
  orphaned. Safest approach: don't auto-delete, provide a
  `pipeline.cleanup_files()` utility.

- **Path stability:** `external_path` stores absolute paths. On shared
  NFS/Lustre this works. If the pipeline moves, paths break. Can defer --
  absolute paths work for the initial use case (fixed cluster paths).

- **Caching contract:** If a step is fully cached, the framework expects
  external files at `files_root` to still exist. This is a new implicit
  contract -- currently all cached data lives in Delta (self-contained).
  Should cache validation check for external file existence?

- **`files_root` and different filesystems:** If `files_root` is on NFS
  and `delta_root` is on local SSD, are there atomicity concerns? The
  commit phase writes to Delta (local) and expects external files (NFS) to
  be stable.

---

## Related Docs

- `1-artifact-id-materialization.md` -- Parallel name collision fix
  (independent, but motivated by the same use case)
- `3-post-step-sugar.md` -- Post-step consolidation mechanism
- `4-silent-file-pipeline.md` -- Domain-specific implementation (protein
  design repo) that builds on this design
