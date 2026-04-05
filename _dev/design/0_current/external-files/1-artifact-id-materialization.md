# Design: Artifact-ID Materialization and Lineage Name Collision Fix

**Date:** 2026-04-04
**Status:** Draft — approach decided, ready for implementation

---

## Problem

When N parallel workers run the same operation code, they naturally produce
artifacts with the same `original_name` ("output_001", "result", etc.).
`original_name` currently serves three roles:

- **Lineage matching key** — stem inference pairs outputs to inputs by name
- **Materialized filename** — `_materialize_content()` writes to
  `{original_name}{extension}`
- **Human-readable identifier** — what users see in queries

This causes two problems:

**Materialization collisions:** Two artifacts in the same role with the same
`original_name` and extension overwrite each other when materialized to the
same directory. No error, silent data loss.

**Lineage inference failure:** When a curator receives all artifacts from
parallel workers in a single execution unit, the stem index has N entries
for the same stem. `_match_by_stem_indexed` requires exactly 1 entry per
stem — multiple entries silently return `None`, producing zero lineage
edges with no error or warning.

This is not specific to any domain or artifact type. It affects any
parallel execution pattern where workers produce same-named outputs.

---

## Current Lifecycle

The creator operation lifecycle in `run_creator_lifecycle()`
(`execution/executors/creator.py`):

```
Setup
  - Create sandbox: preprocess/, execute/, postprocess/, materialized_inputs/
  - Instantiate input artifacts (hydrate from Delta Lake)
  - Materialize inputs to disk as {original_name}{extension}

Preprocess
  - Operation receives PreprocessInput with materialized artifacts
  - Operation extracts paths, prepares data

Execute
  - Operation receives ExecuteInput with execute_dir
  - Operation runs tools, writes output files to execute_dir

Postprocess
  - output_snapshot() globs execute_dir/**/* -> list[Path]
  - Operation receives PostprocessInput with file_outputs
  - Operation manually creates draft artifacts from output files
  - Operation sets original_name (typically from output filename)
  - finalize_artifacts() computes artifact_id from content hash

Lineage inference
  - capture_lineage_metadata() stem-matches output original_name
    against input original_name
  - Receives only in-memory artifact objects — no filesystem access
  - build_edges() resolves LineageMappings to SourceTargetPair records

Record (staging)
  - record_execution_success() stages artifacts + edges to Parquet
  - Artifacts are mutable up to this point

Sandbox cleanup
  - shutil.rmtree(sandbox_path)
```

Key observations:
- `output_snapshot()` already scans output files at postprocess
- The sandbox is alive through record (cleanup is after staging)
- Lineage inference has no filesystem access — works only on `original_name`
- Artifacts are mutable between finalization and staging
- The operation author decides what `original_name` is (typically from the
  output filename on disk)

---

## Stem Matching Details

`capture_lineage_metadata` (`execution/lineage/capture.py`):

- `_build_candidates_from_inputs`: collects `(original_name, artifact_id,
  role)` tuples from input artifacts in roles named by
  `OutputSpec.infer_lineage_from`
- `_build_stem_index`: groups by `strip_extensions(original_name)` into
  `dict[stem -> list[(artifact_id, role)]]`
- `_match_by_stem_indexed`: for each output artifact, strips its
  `original_name` and looks up the index. **Returns a match only when
  `len(entries) == 1`**. Multiple entries = ambiguous = `None` = no edge.
- No error, no warning. Silent lineage gap.

For creators, this rarely matters — each execution unit processes a small
batch, so name collisions within a unit are uncommon. For curators, which
receive ALL artifacts in one unit, it's a real problem.

---

## Curator Explicit Lineage Bug

Separate but related: the curator executor (`curator.py:150`) always calls
`capture_lineage_metadata()` and ignores `ArtifactResult.lineage`. The
creator executor (`creator.py:242`) correctly checks `result.lineage`
first. The documentation (`docs/concepts/operations-model.md:148`) says it
should work for curators. This is a bug.

The fix is straightforward — mirror the creator's pattern:

```python
# curator.py _handle_artifact_result(), currently line 148
if result.lineage is None:
    lineage = capture_lineage_metadata(...)
else:
    validate_lineage_integrity(result.lineage, input_artifacts, finalized)
    lineage = result.lineage
```

This should land regardless of the materialization decision.

---

## Decided Approach

Three changes that separate the three roles of `original_name`:

- **Materialize** as `{artifact_id}{extension}` — uniqueness guaranteed
- **Match** via filesystem match map — artifact_id prefix matching on
  filenames, bypasses stem matching entirely
- **Derive** human-readable name after lineage, overwrite `original_name`
  before staging — no new fields, no schema change

### Why this works

`artifact_id` is computed from content bytes only (`base.py:159-165` —
`_finalize_content()` returns `self.content`). `original_name` is not part
of the hash. This means:

- Finalization can happen before name derivation (current ordering preserved)
- Overwriting `original_name` after finalization has no effect on `artifact_id`
- The artifact-ID-based name is transient — exists only between postprocess
  and name derivation, never persisted to Delta

### Materialization

One-line change per **embedded** artifact type. Example for DataArtifact
(`data.py:94`):

```python
# Before
filename = f"{self.original_name}{self.extension or '.csv'}"

# After
filename = f"{self.artifact_id}{self.extension or '.csv'}"
```

Artifact IDs are globally unique (xxh3_128 hashes), so no collisions
regardless of how many workers produced artifacts with the same human name.

**External-content types are excluded.** Artifact types that use
`external_path` as their content pointer (e.g., SilentStructureArtifact)
skip materialization entirely — many artifacts point to the same external
file, so per-artifact materialization is wrong. These types set
`_default_hydrate = True` (metadata loaded from Delta) but use
`InputSpec(materialize=False)`. Operations read from `external_path`
directly. See `2-external-content-artifacts.md` for details.

### Filesystem Match Map

After `output_snapshot()` scans the execute directory, the framework builds
a match map from the filesystem — no dependency on `original_name`:

```python
def build_filesystem_match_map(
    materialized_artifact_ids: set[str],
    output_files: list[Path],
) -> dict[str, str]:
    """Match output files to input artifacts by artifact_id prefix.

    Args:
        materialized_artifact_ids: Set of input artifact_ids that were
            materialized to disk.
        output_files: Output files produced by the operation.

    Returns:
        Dict mapping output filename stem to input artifact_id.
    """
    match_map: dict[str, str] = {}
    for output_file in output_files:
        stem = strip_extensions(output_file.name)
        for input_id in materialized_artifact_ids:
            if stem.startswith(input_id):
                match_map[stem] = input_id
                break
    return match_map
```

This map feeds into `capture_lineage_metadata` as an alternative to stem
matching. When the match map provides a mapping, stem matching is skipped.
When the match map has no entry (operation didn't preserve the artifact_id
prefix), falls back to existing stem matching or explicit lineage.

### Name Derivation

After lineage edges are established, derive human-readable names:

```python
def derive_human_names(
    output_artifacts: dict[str, list[Artifact]],
    lineage_edges: list[SourceTargetPair],
    input_artifacts: dict[str, list[Artifact]],
    match_map: dict[str, str],
) -> None:
    """Overwrite original_name on outputs with derived human names.

    For each output matched via the filesystem match map:
    - Find the matched input's original_name (human-readable)
    - Extract the operation-applied suffix from the output name
    - Apply the suffix to the input's human name
    - Overwrite original_name on the output artifact

    Modifies artifacts in place. Must be called before staging.
    """
    # Build input_id -> original_name lookup
    input_names: dict[str, str] = {}
    for role_artifacts in input_artifacts.values():
        for art in role_artifacts:
            if art.artifact_id and hasattr(art, "original_name") and art.original_name:
                input_names[art.artifact_id] = art.original_name

    for role_artifacts in output_artifacts.values():
        for art in role_artifacts:
            output_name = getattr(art, "original_name", None)
            if output_name is None:
                continue
            input_id = match_map.get(output_name)
            if input_id is None:
                continue
            input_name = input_names.get(input_id)
            if input_name is None:
                continue

            # Extract suffix: "abc123_scored" - "abc123" = "_scored"
            suffix = output_name[len(input_id):]
            art.original_name = f"{input_name}{suffix}"
```

### Updated Lifecycle

```
Setup
  - Materialize inputs as {artifact_id}{extension}    ← CHANGED
  - Record materialized artifact_ids                   ← NEW

Preprocess / Execute / Postprocess  — unchanged

Lineage
  - Build filesystem match map                         ← NEW
  - capture_lineage_metadata (with match map fallback) ← CHANGED
  - build_edges()                                      ← unchanged

Name derivation                                        ← NEW
  - Derive human names from lineage + input names
  - Overwrite original_name on output artifacts

Record / Cleanup — unchanged
```

### Concrete Example

**Input artifacts (from prior step):**

| artifact_id | original_name | ext |
|---|---|---|
| `a1b2c3d4...` | `protein_001` | `.csv` |
| `c9d0e1f2...` | `protein_002` | `.csv` |

**Materialization:**

```
materialized_inputs/
    a1b2c3d4.csv    (was protein_001)
    c9d0e1f2.csv    (was protein_002)
```

**Operation (unchanged code) produces:**

```
execute/
    a1b2c3d4_scored.csv
    c9d0e1f2_scored.csv
```

**Postprocess — drafts created:**

| artifact_id | original_name (transient) |
|---|---|
| `x7y8z9w0...` | `a1b2c3d4_scored` |
| `p3q4r5s6...` | `c9d0e1f2_scored` |

**Filesystem match map:**

```python
{"a1b2c3d4_scored": "a1b2c3d4", "c9d0e1f2_scored": "c9d0e1f2"}
```

**Lineage edges:**

```
a1b2c3d4 → x7y8z9w0
c9d0e1f2 → p3q4r5s6
```

**Name derivation:**

| output name (before) | input_id | input name | suffix | output name (after) |
|---|---|---|---|---|
| `a1b2c3d4_scored` | `a1b2c3d4` | `protein_001` | `_scored` | `protein_001_scored` |
| `c9d0e1f2_scored` | `c9d0e1f2` | `protein_002` | `_scored` | `protein_002_scored` |

**What gets staged to Delta:**

| artifact_id | original_name |
|---|---|
| `x7y8z9w0...` | `protein_001_scored` |
| `p3q4r5s6...` | `protein_002_scored` |

### Fallback Behavior

When the match map has no entry for an output (operation didn't preserve
the artifact_id prefix in the output filename):

- Lineage falls back to existing stem matching on `original_name`
- If stem matching also fails, the operation should declare explicit lineage
  via `ArtifactResult.lineage`
- Name derivation skips the artifact — `original_name` stays as-is
  (whatever the operation set in postprocess)

This preserves backward compatibility: operations that produce unrelated
output filenames work exactly as they do today.

---

## Scope

| File | Change |
|------|--------|
| `src/artisan/schemas/artifact/data.py` | `_materialize_content()` uses `artifact_id` for filename |
| `src/artisan/schemas/artifact/metric.py` | Same |
| `src/artisan/schemas/artifact/execution_config.py` | Same |
| `src/artisan/schemas/artifact/file_ref.py` | Same (or keep path-based materialization for user-managed files) |
| `src/artisan/execution/inputs/materialization.py` | Track materialized artifact_ids, return them alongside artifacts |
| `src/artisan/execution/lineage/capture.py` | Accept filesystem match map, use it before falling back to stem matching |
| `src/artisan/execution/lineage/filesystem_match.py` | New: `build_filesystem_match_map()` |
| `src/artisan/execution/lineage/name_derivation.py` | New: `derive_human_names()` |
| `src/artisan/execution/executors/creator.py` | Wire match map and name derivation into lifecycle |
| `src/artisan/execution/executors/curator.py` | Honor `ArtifactResult.lineage` (bug fix) |

---

## Testing

| Test file | Coverage |
|-----------|----------|
| `tests/artisan/schemas/artifact/test_artifact_id_materialization.py` | All artifact types materialize with artifact_id filename; no collisions with duplicate original_name; extension preserved |
| `tests/artisan/execution/lineage/test_filesystem_match.py` | Prefix matching, no-match fallback, multiple inputs, edge cases (no suffix, extension-only) |
| `tests/artisan/execution/lineage/test_name_derivation.py` | Suffix extraction, human name derivation, unmatched outputs preserved, empty suffix case |
| `tests/artisan/execution/test_creator_lifecycle.py` | End-to-end: materialization → match map → lineage → name derivation → correct original_name in staged artifacts |
| `tests/artisan/execution/test_curator_explicit_lineage.py` | Curator honors ArtifactResult.lineage, falls back to stem inference when None |

---

## Related Docs

- `2-external-content-artifacts.md` — External content artifact pattern
- `3-post-step-sugar.md` — Post-step consolidation mechanism
- `4-silent-file-pipeline.md` — Domain-specific silent file pipeline
