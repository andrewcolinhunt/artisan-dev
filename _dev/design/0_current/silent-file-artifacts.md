# Design: External Content Artifacts and Post-Step Consolidation

**Date:** 2026-04-04
**Status:** Draft

---

## Problem

Artisan currently has two content storage strategies:

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

The motivating use case is Rosetta silent files in protein structure
pipelines (see Related Docs), but the framework changes are domain-agnostic.

### Current Ingest Flow: Conflated Concerns

The existing file ingest pattern reveals a design tension worth addressing:

```
User CSV files on disk
  -> _promote_file_paths_to_store() reads entire file bytes to hash
    -> FileRefArtifact committed to Delta (pointer: path + content_hash)
      -> IngestData curator reads file AGAIN via read_content()
        -> DataArtifact committed to Delta (content embedded as Binary)
```

FileRefArtifact serves two conflated roles:

- **Provenance record:** "this pipeline started from these user-provided
  files" (who gave us what)
- **Content pointer:** "content lives here, not in Delta" (where to read)

For the current CSV ingest use case, these roles are the same object. But
for Artisan-managed external files, only the pointer role matters -- there's
no user-provided file to track provenance for. The operation creates the
file itself.

This design separates the two concepts:

- **FileRefArtifact** stays as provenance: "user provided this file"
- **`external_path`** becomes a meaningful content pointer for artifact
  types that opt in: "read content from here"

---

## Prior Art Survey

### Artifact Type Registry (`schemas/artifact/registry.py`)

`ArtifactTypeDef` auto-registers via `__init_subclass__`. Each type provides
`key`, `table_path`, and `model`. The storage layer (staging, commit,
retrieval) is completely type-agnostic -- it dispatches through the registry.
**A new artifact type gets its own Delta table with zero storage layer
changes.** Critically, types can be defined in external packages -- they
register at import time, not at framework build time.

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

### Pipeline Submit and StepFuture

`submit()` assigns step numbers eagerly, validates, dispatches, returns
`StepFuture`. `StepFuture.output(role)` creates `OutputReference` with
`source_step=self.step_number`.

**Key insight for post_step:** If `submit()` returns the post_step's
`StepFuture`, downstream `OutputReference` objects automatically point
to the post_step. No reference rewriting needed.

### PipelineConfig (`schemas/orchestration/pipeline_config.py`)

Three configurable path roots: `delta_root`, `staging_root`, `working_root`.
Frozen Pydantic model. Sibling directories (`logs/`, `images/`) are
derived from `delta_root.parent`. Adding `files_root` follows the same
pattern.

### Composite Expanded Mode

`ExpandedCompositeContext` calls `pipeline.submit()` to create real pipeline
steps and returns a handle whose `.output()` points to the right internal
step. Proven precedent for the post_step pattern.

### Summary

| Existing code | Disposition |
|---|---|
| `ArtifactTypeDef` registry | Reuse as-is -- domain types auto-register from external packages |
| `FileRefArtifact` | Template for external-content pattern; stays as user-file provenance |
| `external_path` base field | Promote from informational to functional content pointer |
| `IngestFiles` curator pattern | Template for domain-specific consolidation curators |
| `submit()` / `StepFuture` | Extend with `post_step` desugaring |
| `PipelineConfig` | Extend with `files_root` |
| Storage layer (staging, commit) | No changes needed |

---

## Design

### Artisan vs Domain Boundary

This design spans two codebases. The framework changes are minimal and
domain-agnostic; the domain-specific code lives in the protein design
repository and uses the framework's extension points.

| Component | Where | Why |
|---|---|---|
| `files_root` on PipelineConfig | Artisan | General infrastructure for any Artisan-managed files |
| `ArtifactStore.files_root` threading | Artisan | Curators need access to managed file storage |
| `post_step` on submit/run | Artisan | General mechanism, works with any curator |
| SilentStructureArtifact | Domain repo | Rosetta-specific fields (decoy_tag, total_score, sequence) |
| ConsolidateSilentFiles curator | Domain repo | Knows silent file format, depends on `silent_tools` |
| silent_tools wrapper utilities | Domain repo | Domain dependency |

The artifact type registry is designed for exactly this split --
`ArtifactTypeDef` subclasses auto-register at import time regardless of
which package defines them. Similarly, `OperationDefinition` subclasses
(curators included) work identically whether defined in Artisan or an
external package.


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
and implements `_materialize_content()` to read from `external_path`, it is
an external-content type.

No changes to the base `Artifact` class are needed. The field already
exists, persists, and round-trips. The behavioral change is entirely in
subclass implementations of `_finalize_content()` and
`_materialize_content()`.

This means:

- **DataArtifact** (embedded): has `content` field, `external_path` remains
  informational. No behavioral change.
- **FileRefArtifact** (user pointer): has its own `path` field. No change.
- **Domain external types** (e.g. SilentStructureArtifact): no `content`
  field, `external_path` is functional. New pattern, defined in domain
  repos, auto-registered at import time.


### `files_root` Configuration

A new field on `PipelineConfig` for Artisan-managed external file storage.

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


### `post_step` Pipeline Sugar

A new parameter on `submit()`/`run()` that auto-inserts a curator step
after the main step, returning the curator's `StepFuture` to the caller.

**User-facing API:**

```python
# Domain code -- ConsolidateSilentFiles defined in protein design repo
from protein_design.operations import ConsolidateSilentFiles

step = pipeline.run(
    RunRosetta,
    inputs={"reference": prev.output("data")},
    post_step=ConsolidateSilentFiles,
)

# step.output("structures") -> consolidated artifacts
next_step = pipeline.run(
    ScoreStructures,
    inputs={"structures": step.output("structures")},
)
```

**Implementation in `submit()`:**

After dispatching the main step, check for `post_step`. If present,
recursively call `submit()` with the post_step curator, wiring the main
step's outputs as inputs. Return the post_step's `StepFuture`.

```python
def submit(
    self,
    operation,
    inputs=None,
    ...,
    post_step: type[OperationDefinition] | None = None,
) -> StepFuture:
    # ... existing validation, dispatch ...
    main_future = self._dispatch_step(...)

    if post_step is not None:
        post_inputs = {
            role: main_future.output(role)
            for role in operation.outputs
        }
        return self.submit(
            post_step,
            inputs=post_inputs,
            backend=backend,
            compact=compact,
            name=f"{step_name}__consolidate",
        )

    return main_future
```

**Step numbering:**

```
pipeline.run(RunRosetta, post_step=ConsolidateSilentFiles)
# -> step 0: RunRosetta (parallel, N silent files)
# -> step 1: ConsolidateSilentFiles (curator, 1 file)
# -> returned StepFuture has step_number=1
```

Two step numbers consumed, user interacts with the second one.

**Role matching:** post_step input roles must match the main step's output
roles. Validated by the existing `_validate_operation_overrides()` in the
recursive `submit()` call.

**Caching:** Both steps cache independently. If main step is fully cached,
consolidator runs on cached artifacts. If consolidator is also cached, both
skip. Partial cache hits on the main step work correctly -- the consolidator
always receives all artifacts (cached + fresh).

**Naming:** Main step uses user-provided `name` (or `operation.name`).
Post_step gets `f"{step_name}__consolidate"`. Both visible in results.

**Composites:** Deferred. If the main step is a composite, `submit()`
routes composites before reaching post_step logic. Composites can use
consolidation curators as explicit steps in `compose()`.

**Generality:** `post_step` accepts any `OperationDefinition` subclass, not
just consolidation curators. It's a general mechanism for "run this after
that, present as one logical step." The convention that post_step input roles
match main step output roles is enforced by existing validation.


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

The `workers/` subdirectory holds per-worker files that the consolidation
curator reads from. After consolidation, these could optionally be cleaned
up (the consolidated file contains all the data).

The staging directory is not suitable -- its cleanup happens after commit,
before the post_step curator runs.


### End-to-End Flow (Rosetta Example)

This illustrates how domain code uses the framework features. All operations
and artifact types below are defined in the protein design repository, not
in Artisan.

```
Step 0: RunRosetta (parallel, 10 workers)
    Worker 1: produces structures -> writes worker_1.silent to files_root/0/workers/
              creates SilentStructureArtifact drafts (external_path = worker file)
    Worker 2: same -> worker_2.silent
    ...
    Worker 10: same -> worker_10.silent
    Commit: 10 * M structure artifacts in Delta, each pointing to its worker file

Step 1: ConsolidateSilentFiles (auto-inserted via post_step)
    Hydrates all structure artifacts from step 0
    Cats 10 worker files -> files_root/1/combined.silent
    Re-drafts each structure with external_path = combined file
    Commit: M * 10 consolidated structure artifacts in Delta

Step 2: ScoreStructures (user-defined, references step 1 via StepFuture)
    Receives consolidated artifacts -> one silent file, all tags
    Passes file + tags to Rosetta scoring command
```

### Domain-Specific Implementation Notes

These are not part of the Artisan design but are documented here for
completeness since they motivated the framework changes.

**SilentStructureArtifact** would follow the FileRefArtifact pattern:
no `content` column, `_finalize_content()` hashes a JSON blob of
`content_hash` + `decoy_tag` + `external_path`, `_materialize_content()`
extracts the structure by tag using `silent_tools`. Registration is a
three-line `ArtifactTypeDef` subclass that auto-registers at import.

**ConsolidateSilentFiles** would follow the IngestFiles curator pattern:
hydrate inputs via `artifact_store.get_artifacts_by_type()`, cat the
underlying files (using `silent_tools`), re-draft each artifact with the
consolidated path, return `ArtifactResult`. The curator accesses
`artifact_store.files_root` to determine where to write.

**Content addressing for consolidated artifacts:** `_finalize_content()`
should include `external_path` in the hash so that consolidated artifacts
get distinct `artifact_id`s from the per-worker originals. This avoids
dedup conflicts at commit time and is semantically correct -- same structure
data, different file location.

**`silent_tools` dependency:** Conditional import with a clear error message.
MIT licensed, no Rosetta dependency. Brian Coventry (bcov) maintains it.

---

## Scope

### Artisan Framework (this repo)

#### PR 1: `files_root` configuration

| File | Change |
|------|--------|
| `src/artisan/schemas/orchestration/pipeline_config.py` | Add `files_root` field with default from `delta_root.parent / "files"` |
| `src/artisan/storage/core/artifact_store.py` | Accept and expose `files_root` |
| `src/artisan/orchestration/pipeline_manager.py` | Pass `files_root` to ArtifactStore, accept in `create()` |

#### PR 2: `post_step` pipeline sugar

| File | Change |
|------|--------|
| `src/artisan/orchestration/pipeline_manager.py` | Add `post_step` param to `submit()` and `run()` |

PRs 1 and 2 are independent of each other.

### Domain Repository (protein design)

Not part of this Artisan design, but for reference:

| Component | Description |
|-----------|-------------|
| `SilentStructureArtifact` | Artifact model + `ArtifactTypeDef` registration |
| `ConsolidateSilentFiles` | Curator operation |
| Silent tools utilities | `silent_tools` wrapper for concatenation and extraction |
| `RunRosetta` and other ops | Operations that produce SilentStructureArtifacts |

---

## Sequencing

| PR | Repo | Content | Dependencies |
|----|------|---------|--------------|
| `files_root` | Artisan | PipelineConfig, ArtifactStore threading | None |
| `post_step` | Artisan | New param on submit/run | None |
| Silent artifacts + curator | Domain | Artifact type, curator, silent_tools | Both Artisan PRs |

The two Artisan PRs can land in any order. The domain work depends on both.

---

## Testing

### Artisan Framework

| Test file | Coverage |
|-----------|----------|
| `tests/artisan/schemas/orchestration/test_pipeline_config.py` | `files_root` default derivation from `delta_root`, explicit override, frozen model behavior |
| `tests/artisan/storage/test_files_root.py` | `ArtifactStore` exposes `files_root`, directory creation |
| `tests/artisan/orchestration/test_post_step.py` | StepFuture points to post_step, step numbering, downstream OutputReference resolution, caching (both cached, partial cache), role mismatch error, post_step=None is no-op |

### Domain Repository

| Test file | Coverage |
|-----------|----------|
| `test_silent_structure_artifact.py` | Draft/finalize, to_row/from_row round-trip, POLARS_SCHEMA keys, content hashing with external_path, _materialize_content extraction |
| `test_consolidate_silent_files.py` | Single file passthrough, multi-file concat, header handling, empty input, re-drafting with updated paths |
| `test_silent_pipeline.py` (integration) | End-to-end: creator -> consolidation -> downstream consumer |

---

## Open Questions

- **File cleanup:** When a step is re-run, should `files_root/{step}/` be
  cleaned up? Old consolidated files become orphaned. Could follow the
  staging cleanup pattern (delete on successful re-commit), but the files
  are referenced by artifact rows that may still exist in Delta. Safest
  approach: don't auto-delete, provide a `pipeline.cleanup_files()` utility.

- **Path stability:** `external_path` stores absolute paths. On shared
  NFS/Lustre this works. If the pipeline moves, paths break. Could store
  relative paths (to `files_root`) and resolve at read time. Adds
  complexity but improves portability. Can defer -- absolute paths work for
  the initial use case (fixed cluster paths).

- **Worker file location:** Workers need to write output files to
  `files_root/{step}/workers/` rather than their sandbox (which gets
  cleaned up). This means `files_root` must be accessible from workers.
  On shared NFS this is fine. On local-disk backends, files would need
  to be transferred. For the initial SLURM/NFS use case, this is not an
  issue.

- **`files_root` propagation to workers:** Workers currently receive
  `RuntimeEnvironment` with `delta_root`, `staging_root`, `working_root`.
  `files_root` would need to be added to ensure workers can write to it
  during execution. Need to verify the propagation path.

---

## Related Docs

- `_dev/analysis/external-artifact-storage.md` -- Initial analysis and
  conversation with bcov about silent file format properties and tooling
