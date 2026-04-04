# Design: Artifact-ID Materialization and Lineage Name Collision Fix

**Date:** 2026-04-04
**Status:** Draft (open question — needs decision before implementation)

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
1. Setup
   - Create sandbox: preprocess/, execute/, postprocess/, materialized_inputs/
   - Instantiate input artifacts (hydrate from Delta Lake)
   - Materialize inputs to disk as {original_name}{extension}

2. Preprocess
   - Operation receives PreprocessInput with materialized artifacts
   - Operation extracts paths, prepares data

3. Execute
   - Operation receives ExecuteInput with execute_dir
   - Operation runs tools, writes output files to execute_dir

4. Postprocess
   - output_snapshot() globs execute_dir/**/* -> list[Path]
   - Operation receives PostprocessInput with file_outputs
   - Operation manually creates draft artifacts from output files
   - Operation sets original_name (typically from output filename)
   - finalize_artifacts() computes artifact_id from content hash

5. Lineage inference
   - capture_lineage_metadata() stem-matches output original_name
     against input original_name
   - Receives only in-memory artifact objects — no filesystem access
   - build_edges() resolves LineageMappings to SourceTargetPair records

6. Record (staging)
   - record_execution_success() stages artifacts + edges to Parquet
   - Artifacts are mutable up to this point

7. Sandbox cleanup
   - shutil.rmtree(sandbox_path)
```

Key observations:
- `output_snapshot()` already scans output files at step 4
- The sandbox is alive through step 6 (cleanup is after staging)
- Lineage inference has no filesystem access — works only on `original_name`
- Artifacts are mutable between finalization (step 4) and staging (step 6)
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

## Proposed Approach: Artifact-ID Based Filenames

Materialize inputs using `artifact_id` as the filename instead of
`original_name`. Operations (black boxes) see and transform artifact-id
filenames. The framework uses these for unambiguous lineage matching. Human
names are derived after lineage inference.

**Materialization:**

```
Input: artifact_id=abc123def456, original_name=sample_001, ext=.csv
Materialized as: abc123def456.csv  (not sample_001.csv)
```

Artifact IDs are globally unique (xxh3_128 hashes), so no collisions
regardless of how many workers produced artifacts with the same human name.

**Operation (unchanged):**

```
Reads: abc123def456.csv
Writes: abc123def456_scored.csv  (appends suffix, as operations typically do)
```

Operation code is identical — it still reads files and derives output names
from input names. The filenames are just less readable.

**Draft creation (unchanged):**

```python
# In postprocess — same code as before
for file_path in inputs.file_outputs:
    drafts.append(DataArtifact.draft(
        content=file_path.read_bytes(),
        original_name=file_path.name,  # "abc123def456_scored.csv"
        step_number=inputs.step_number,
    ))
```

**Lineage matching (unambiguous):**

Stem matching finds `abc123def456_scored` prefix-matches `abc123def456`.
Only one entry in the index (artifact IDs are unique). Match succeeds.

**Name derivation (new step):**

After lineage inference, between steps 5 and 6 in the lifecycle:

```
Input human name:   "sample_001"
Output stem:        "abc123def456_scored"
Matched input stem: "abc123def456"
Operation suffix:   "_scored"
Derived human name: "sample_001_scored"
```

The derived name replaces or augments `original_name` on the output
artifact before staging. Artifacts are mutable at this point.

---

## Insertion Point in the Lifecycle

The name derivation step fits naturally between lineage inference and
staging:

```
4. Postprocess   — drafts created, finalized
5. Lineage       — edges established (now unambiguous via artifact-id stems)
5b. NAME DERIVE  — extract suffix, apply to input human name  ← NEW
6. Record        — stage to Parquet (with derived names)
7. Cleanup
```

No new lifecycle phase needed — it's a few lines after `build_edges()`
returns and before `record_execution_success()` is called. The sandbox is
still alive, artifacts are mutable, lineage mappings are available.

---

## Filesystem Match Map (Alternative to Stem Matching)

Instead of relying on stem matching against `original_name`, the framework
could build a match map directly from the filesystem:

```
After output_snapshot():
  - Input files:  materialized_inputs/abc123def456.csv
  - Output files: execute/abc123def456_scored.csv

Match map: {"abc123def456_scored" -> "abc123def456"} (input artifact_id)
```

This map would be passed to `capture_lineage_metadata` as an additional
argument, used instead of (or alongside) stem matching. This decouples
lineage from `original_name` entirely.

Pro: `original_name` can stay human-readable from the start — the framework
does the artifact-id matching at the filesystem level.

Con: requires threading the match map through the lifecycle, and
`capture_lineage_metadata` needs a new code path.

This is an implementation choice, not a design fork. Both approaches
(stem matching on artifact-id-based `original_name`, or filesystem match
map) achieve the same result. The filesystem match map is cleaner
conceptually but a bigger code change.

---

## Open Decision: Where Does the Human Name Live?

This is the key unresolved question. After artifact-id materialization,
`original_name` on output drafts contains artifact-id-based stems. The
human-readable name needs to be derived and stored somewhere. Options:

### Option A: Overwrite `original_name` after inference

- At draft time: `original_name = "abc123def456_scored"`
- After lineage: `original_name = "sample_001_scored"`
- At staging: Delta stores `"sample_001_scored"`

Pro: no schema change, no new fields.
Con: `original_name` changes meaning mid-lifecycle. If any code reads it
between draft creation and name derivation, it sees artifact-id noise. On
re-read from Delta, stem matching would use human names (correct for
display, but the artifact-id benefit is gone for re-runs — though this
may not matter since lineage is already established).

### Option B: Add `display_name`, keep `original_name` as matching key

- `original_name`: always artifact-id-based, stable, used for matching
- `display_name`: human-readable, derived from lineage, shown to users

Pro: clean separation, each field has one job.
Con: new field on base Artifact, new column in every Delta table, every
`POLARS_SCHEMA`/`to_row`/`from_row` updated. User-facing queries need to
use `display_name` instead of `original_name`.

### Option C: Keep `original_name` human, use artifact_id for materialization only

- Materialization: always `{artifact_id}.ext` (not driven by any field)
- `original_name`: stays human-readable, set by operation author or derived
- Lineage matching: uses filesystem match map (artifact-id from filenames),
  NOT `original_name`

Pro: `original_name` keeps its current semantics. No new fields. No mid-
lifecycle mutation.
Con: lineage inference needs a new code path (filesystem match map).
Operation authors see artifact-id filenames during execution but set
human-readable `original_name` on drafts — there's a disconnect between
what's on disk and what's in the artifact record.

### Option D: Disambiguate only when needed

- Default: materialize as `{original_name}{extension}` (current behavior)
- On collision (detected during materialization): materialize as
  `{artifact_id}__{original_name}{extension}`
- Stem matching strips the `{artifact_id}__` prefix when present

Pro: preserves readability in the common case (no collisions). Only adds
artifact-id prefix when actually needed.
Con: inconsistent filename format. Operations that hardcode filename
patterns would see different formats depending on whether collisions exist.
Harder to reason about in tests.

---

## Debugging Concern

With artifact-id filenames, scientists inspecting sandbox directories during
development see `abc123def456.csv` instead of `sample_001.csv`. This is a
real usability regression for debugging. Options D partially addresses this.
Options A and C preserve human-readable `original_name` in the artifact
record but not on disk. Option B preserves both (human name in
`display_name`, artifact-id on disk).

---

## Scope (framework changes only)

Regardless of which naming option is chosen:

| File | Change |
|------|--------|
| `src/artisan/schemas/artifact/data.py` | `_materialize_content()` uses `artifact_id` for filename |
| `src/artisan/schemas/artifact/metric.py` | Same |
| `src/artisan/schemas/artifact/execution_config.py` | Same |
| `src/artisan/schemas/artifact/file_ref.py` | Same (or keep path-based materialization) |
| `src/artisan/execution/executors/creator.py` | Name derivation step after lineage |
| `src/artisan/execution/lineage/capture.py` | Filesystem match map (if Option C) or unchanged (if Options A/B) |
| `src/artisan/execution/executors/curator.py` | Honor `ArtifactResult.lineage` (bug fix) |

If Option B: also `src/artisan/schemas/artifact/base.py` and every artifact
type's `POLARS_SCHEMA`/`to_row`/`from_row`.

---

## Related Docs

- `_dev/design/0_current/silent-file-artifacts.md` — parent design that
  motivated this work (external content artifacts, post_step, files_root)
