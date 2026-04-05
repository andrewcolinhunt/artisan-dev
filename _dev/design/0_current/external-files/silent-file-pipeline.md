# Design: Silent File Pipeline (Domain-Specific)

**Date:** 2026-04-04
**Status:** Draft
**Repo scope:** Protein design repository (not Artisan)

---

## Problem

Rosetta protein design tools produce structures in "silent files" -- a
domain-specific format where a single file contains many independently
addressable protein structures ("decoys"). A typical pipeline step runs
N parallel workers, each producing a separate silent file. Downstream
steps need all structures consolidated into a single file for efficient
batch processing.

Artisan's current artifact types cannot represent this:

- **DataArtifact** embeds bytes in Delta -- impractical for large structure
  files
- **FileRefArtifact** points to user-managed files -- wrong ownership model
  (the pipeline creates these files)
- No mechanism exists for consolidating per-worker files into a single
  per-step file

This design defines the domain-specific artifact type
(`SilentStructureArtifact`), consolidation curator
(`ConsolidateSilentFiles`), and end-to-end pipeline flow. It builds on three
Artisan framework features:

- `external_path` as functional content pointer (see
  `external-content-artifacts.md`)
- `files_root` configuration (see `external-content-artifacts.md`)
- `post_step` pipeline sugar (see `post-step-sugar.md`)

---

## Prior Art Survey

### Artisan Extension Points

**ArtifactTypeDef registry** (`schemas/artifact/registry.py`): Auto-registers
via `__init_subclass__` at import time. External packages define subclasses
and they appear in the registry with no Artisan changes. This is the
mechanism for `SilentStructureArtifact`.

**OperationDefinition / CuratorDefinition**: Operations and curators defined
in external packages work identically to those in Artisan. This is the
mechanism for `ConsolidateSilentFiles`.

**FileRefArtifact** (`schemas/artifact/file_ref.py`): Template for
external-content types. Key patterns to follow:
- No `content: pl.Binary` column
- `_finalize_content()` hashes a JSON blob of metadata
- `_materialize_content()` reads from disk on demand
- `POLARS_SCHEMA` defines queryable columns

**IngestFiles** (`operations/curator/`): Template for consolidation curators.
Hydrates input artifacts, processes files, returns `ArtifactResult`.

### Silent File Format

- **Text-based** (even the "binary" variant is ASCII-encoded)
- **Appendable** -- `cat a.silent b.silent > combined.silent` (with header
  handling)
- **Tag-indexed** -- every line of a structure ends with its unique decoy tag
- Each structure contains: `SEQUENCE` line, `SCORE` data, `REMARK` metadata,
  `ANNOTATED_SEQUENCE`, coordinate lines

### `silent_tools` (Brian Coventry)

Standalone Python toolkit (MIT licensed, no Rosetta dependency):
- `get_silent_index(filename)` -- dict of tag -> line numbers
- Tag-based extraction and concatenation
- Corruption detection

Key API for consolidation:

```python
silent_index = silent_tools.get_silent_index(path_to_silent)
sf = open(path_to_silent)
sys.stdout.write(silent_tools.silent_header(silent_index))
for tag in tags:
    structure = silent_tools.get_silent_structure_file_open(sf, silent_index, tag)
    sys.stdout.write("".join(structure))
```

### Summary

| Existing code | Disposition |
|---|---|
| `ArtifactTypeDef` registry | Use for SilentStructureArtifact registration |
| `FileRefArtifact` pattern | Template for external-content type |
| `IngestFiles` curator pattern | Template for ConsolidateSilentFiles |
| `silent_tools` | Wrap for concatenation and extraction |

---

## Design

### SilentStructureArtifact

An artifact type where each row in Delta represents one protein structure
within a silent file. Per-structure metadata is queryable in Delta;
coordinate data stays on disk.

```python
class SilentStructureArtifact(Artifact):
    """A protein structure stored in a Rosetta silent file.

    Each artifact represents one decoy (structure) within a silent file.
    Metadata is queryable in Delta Lake; full coordinate data is accessed
    on demand via the silent file.
    """

    POLARS_SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "artifact_id": pl.String,
        "origin_step_number": pl.Int32,
        "external_path": pl.String,
        "decoy_tag": pl.String,
        "content_hash": pl.String,
        "sequence": pl.String,
        "total_score": pl.Float64,
        "original_name": pl.String,
        "extension": pl.String,
        "metadata": pl.String,
    }

    artifact_type: str = Field(default="silent_structure", frozen=True)
    decoy_tag: str | None = Field(
        default=None,
        description="Unique structure identifier within the silent file.",
    )
    content_hash: str | None = Field(
        default=None,
        description="xxh3_128 hash of this structure's data.",
    )
    sequence: str | None = Field(
        default=None,
        description="Amino acid sequence.",
    )
    total_score: float | None = Field(
        default=None,
        description="Rosetta total energy score.",
    )
    original_name: str | None = Field(default=None)
    extension: str | None = Field(default=".silent")
```

**Registration:** Three-line `ArtifactTypeDef` subclass:

```python
class SilentStructureType(ArtifactTypeDef):
    key = "silent_structure"
    table_path = "artifacts/silent_structure"
    model = SilentStructureArtifact
```

Auto-registers at import time. Artisan's storage layer handles staging,
commit, and retrieval with no changes.

**Content hashing:**

```python
def _finalize_content(self) -> bytes | None:
    if self.content_hash is None:
        return None
    return json.dumps(
        {
            "content_hash": self.content_hash,
            "decoy_tag": self.decoy_tag,
            "external_path": self.external_path,
        },
        sort_keys=True,
    ).encode("utf-8")
```

Including `external_path` in the hash ensures per-worker artifacts get
distinct `artifact_id`s from consolidated artifacts (same structure data,
different file location).

**Materialization:**

```python
def _materialize_content(self, directory: Path) -> Path:
    """Extract this structure from the silent file by tag."""
    if self.external_path is None or self.decoy_tag is None:
        msg = "Cannot materialize: artifact not hydrated"
        raise ValueError(msg)

    import silent_tools

    silent_index = silent_tools.get_silent_index(self.external_path)
    with open(self.external_path) as sf:
        lines = silent_tools.get_silent_structure_file_open(
            sf, silent_index, self.decoy_tag,
        )

    path = directory / f"{self.decoy_tag}.silent"
    path.write_text(
        silent_tools.silent_header(silent_index) + "".join(lines),
    )
    self.materialized_path = path
    return path
```

### ConsolidateSilentFiles

A curator that concatenates per-worker silent files into a single file and
re-drafts each structure artifact with the consolidated path.

```python
class ConsolidateSilentFiles(CuratorDefinition):
    """Concatenate per-worker silent files into one per-step file."""

    name = "consolidate_silent_files"
    inputs = {"structures": InputSpec(artifact_type="silent_structure")}
    outputs = {"structures": OutputSpec(artifact_type="silent_structure")}

    def curate(self, context: CuratorContext) -> ArtifactResult:
        structures = context.input_artifacts["structures"]
        files_root = context.artifact_store.files_root
        step_dir = files_root / str(context.step_number)
        step_dir.mkdir(parents=True, exist_ok=True)

        # Collect unique worker files
        worker_files = sorted({
            a.external_path for a in structures
            if a.external_path is not None
        })

        # Cat files together (silent files are appendable with header handling)
        combined_path = step_dir / "combined.silent"
        _concatenate_silent_files(worker_files, combined_path)

        # Re-draft each structure pointing to the consolidated file
        drafts = []
        for artifact in structures:
            draft = SilentStructureArtifact.draft(
                decoy_tag=artifact.decoy_tag,
                content_hash=artifact.content_hash,
                sequence=artifact.sequence,
                total_score=artifact.total_score,
                step_number=context.step_number,
                external_path=str(combined_path),
                original_name=artifact.original_name,
            )
            drafts.append(draft)

        return ArtifactResult(
            artifacts={"structures": drafts},
            lineage=_build_one_to_one_lineage(structures, drafts),
        )
```

Uses explicit lineage (not stem inference) since the output names match
input names. This requires the curator explicit lineage bug fix
(`curator.py:150` must honor `ArtifactResult.lineage`).

### `silent_tools` Wrapper

Conditional import with a clear error message:

```python
def _concatenate_silent_files(
    input_paths: list[str],
    output_path: Path,
) -> None:
    try:
        import silent_tools
    except ImportError:
        msg = (
            "silent_tools is required for silent file operations. "
            "Install via: pip install silent-tools"
        )
        raise ImportError(msg) from None

    # Read header from first file
    first_index = silent_tools.get_silent_index(input_paths[0])
    header = silent_tools.silent_header(first_index)

    with open(output_path, "w") as out:
        out.write(header)
        for path in input_paths:
            idx = silent_tools.get_silent_index(path)
            with open(path) as sf:
                for tag in idx:
                    structure = silent_tools.get_silent_structure_file_open(
                        sf, idx, tag,
                    )
                    out.write("".join(structure))
```

### End-to-End Flow

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

**User code:**

```python
from protein_design.operations import RunRosetta, ScoreStructures
from protein_design.operations import ConsolidateSilentFiles

pipeline = PipelineManager.create(
    name="protein_design",
    delta_root=Path("/data/pipeline/delta"),
    files_root=Path("/data/pipeline/files"),
)

step = pipeline.run(
    RunRosetta,
    inputs={"reference": structures.output("data")},
    post_step=ConsolidateSilentFiles,
)

scores = pipeline.run(
    ScoreStructures,
    inputs={"structures": step.output("structures")},
)

pipeline.finalize()
```

---

## Scope

All files below are in the protein design repository.

| File | Change |
|------|--------|
| `protein_design/artifacts/silent_structure.py` | `SilentStructureArtifact` + `ArtifactTypeDef` registration |
| `protein_design/operations/consolidate_silent_files.py` | `ConsolidateSilentFiles` curator |
| `protein_design/utils/silent_tools_wrapper.py` | Concatenation and extraction utilities |
| `protein_design/operations/run_rosetta.py` | Operation that produces `SilentStructureArtifact` drafts |

---

## Testing

| Test file | Coverage |
|-----------|----------|
| `tests/test_silent_structure_artifact.py` | Draft/finalize, to_row/from_row round-trip, POLARS_SCHEMA keys, content hashing with external_path, _materialize_content extraction |
| `tests/test_consolidate_silent_files.py` | Single file passthrough, multi-file concat, header handling, empty input, re-drafting with updated paths, explicit lineage edges |
| `tests/integration/test_silent_pipeline.py` | End-to-end: creator -> consolidation -> downstream consumer |

---

## Open Questions

- **Decoy tag uniqueness across workers.** When N workers each produce
  structures, are decoy tags guaranteed unique across workers? If workers
  independently generate tags (e.g., `decoy_0001`), catting their files
  creates duplicates. Options: (a) operation responsibility -- RunRosetta
  must generate globally unique tags (e.g., prefix with worker ID),
  (b) consolidation curator detects and renames duplicates,
  (c) Artisan generates tags. Option (a) is simplest and follows the
  principle that the operation is the expert on its format.

- **Consolidated vs original artifacts.** After ConsolidateSilentFiles
  runs, Delta contains both per-worker originals (pointing to worker files)
  and consolidated versions (pointing to combined file). Are the originals
  still queryable? Useful? Should they be marked as superseded? This
  affects storage, query semantics, and provenance graph rendering.

- **Appendable vs immutable-per-step.** The analysis doc (`_dev/analysis/
  external-artifact-storage.md`) discusses silent files as "appendable"
  across steps. This design uses immutable-per-step (one consolidated file
  per step, never modified). These are different strategies. The
  immutable-per-step approach is simpler and safer for
  caching/reproducibility. Is appendability needed for any use case?

- **Error recovery during consolidation.** If ConsolidateSilentFiles fails
  halfway through catting, are the per-worker files preserved? Since worker
  files live in `files_root/{step}/workers/` and the consolidated file is
  written to a different path, partial failure should leave worker files
  intact for retry. Worth verifying.

- **`silent_tools` packaging.** Is `silent_tools` available on PyPI?
  If not, what's the installation path for users? Conditional import
  with a clear error is sufficient, but the error message should point to
  the right place.

---

## Prerequisites (Artisan Framework)

These must land before the domain work can begin:

- `external-content-artifacts.md` -- `files_root` config, `external_path`
  promotion
- `post-step-sugar.md` -- `post_step` parameter on submit/run
- `artifact-id-materialization.md` -- Parallel name collision fix
- Curator explicit lineage bug fix (`curator.py:150` must honor
  `ArtifactResult.lineage`)

---

## Related Docs

- `_dev/analysis/external-artifact-storage.md` -- Initial analysis and
  bcov conversation about silent file format
- `external-content-artifacts.md` -- Artisan framework: external file
  infrastructure
- `post-step-sugar.md` -- Artisan framework: post-step consolidation
  mechanism
- `artifact-id-materialization.md` -- Artisan framework: parallel name
  collision fix
