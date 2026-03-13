# Design: Tutorial Improvements for Release Readiness

**Date:** 2026-03-12  **Status:** Draft  **Author:** Claude + ach94

---

## Problem

The tutorial suite is strong across Getting Started, Pipeline Design, and
Execution, but has gaps and quality issues that should be addressed before
release.

### Gap: Storage layout and logging

Users will immediately encounter questions that no tutorial answers:

- Where do run directories go? Why are sandboxes temporary by default?
- Where are logs? How do I read a failure traceback?
- What are `delta_root`, `staging_root`, and `working_root`?

The three-path model is a core design decision, but the only place it's
documented is in code docstrings. Logging has a rich infrastructure
(console, file logs, per-failure logs with tracebacks) that users don't
know exists.

### Issue: Provenance tutorial is overloaded

`analysis/01-provenance-graphs.ipynb` covers four distinct skills:

- Reading macro graphs (visualization)
- Reading micro graphs (visualization)
- Backward lineage tracing (programmatic)
- Forward lineage tracing (programmatic)

The graph-reading and programmatic-tracing sections serve different
audiences at different moments. Mixing them makes the tutorial long,
makes neither topic shine, and buries the forward/backward distinction.

### Issue: Lineage code uses verbose, outdated patterns

The current provenance tutorial traces lineage with manual hop-by-hop
`ArtifactStore` calls:

```python
parents = store.get_ancestor_artifact_ids(metric_id)
parent_id = parents[0]
grandparents = store.get_ancestor_artifact_ids(parent_id)
```

Meanwhile, `ProvenanceStore` has cleaner multi-hop methods that do the
BFS internally:

```python
# Transitive — finds all ancestors in one call
ancestors = store.provenance.get_ancestor_ids(artifact_id)

# Transitive with type filter
source_data = store.provenance.get_ancestor_ids(artifact_id, ancestor_type="data")
```

The tutorial should showcase the cleanest API available.

---

## Proposed Changes

### New: Storage and Logging tutorial

**Location:** `execution/05-storage-and-logging.ipynb`
**Bumps:** existing 05→06, 06→07, 07→08

#### The three-path model

Explain the three root paths and why they're separate:

| Path | Purpose | Lifetime | Access pattern |
|------|---------|----------|----------------|
| `delta_root` | Artifact storage (Delta Lake tables) | Persistent | Read/write by framework |
| `staging_root` | Worker output staging (Parquet files) | Cleared after commit | Write-only by workers |
| `working_root` | Scratch sandboxes for operations | Cleaned after execution | Read/write by workers |

Show the directory layout after a pipeline run. Explain why each path
exists separately (isolation, cleanup semantics, parallelism safety).

#### Working directory defaults

`working_root` defaults to `tempfile.gettempdir()` (respects `$TMPDIR`).
Show what happens when you don't set it vs. when you pass an explicit
path. Explain why temp-by-default makes sense: sandboxes are ephemeral
scratch space that operations use during execution and that get cleaned
up afterward. If you need to inspect them for debugging, mention
`preserve_working`.

#### `tutorial_setup` and the `runs/` convention

Show how `tutorial_setup("name")` creates `runs/<name>/{delta,staging,working}/`
next to the calling notebook. This is a convenience for tutorials and
scripts — production pipelines typically configure paths explicitly.

#### Log locations

Map the four log destinations:

| Log | Path | When |
|-----|------|------|
| Console | stdout | Always (Rich-colored) |
| Pipeline file log | `<delta_root>/../logs/pipeline.log` | Enabled at DEBUG level |
| Failure logs | `<delta_root>/../logs/failures/step_N_op/run_id.log` | On execution failure |
| Tool output | `<sandbox>/tool_output.log` | During creator execution |

#### Controlling log level

Show `configure_logging(level="DEBUG")` and the automatic setup via
`PipelineManager`. Demonstrate toggling between INFO and DEBUG to see
the difference in output verbosity.

#### Inspecting failures in practice

Run a pipeline where an operation deliberately raises an exception. Use
`inspect_pipeline` to see the failed step status. Navigate to and read
the failure log on disk. Show: the traceback, the run ID correlation,
how to find which artifact failed.

### Rewrite: Provenance Graphs (visualization only)

**Location:** `analysis/01-provenance-graphs.ipynb` (rewrite in place)

Strip all programmatic lineage tracing. Keep:

- **Macro graph** — what it shows, how to read it, the element legend
- **Micro graph** — what it shows, how to read it, the element legend
- **Step snapshots** — `build_micro_graph(delta_root, max_step=N)` to
  watch the graph grow incrementally
- **Which graph for which question** — summary table

The pipeline setup and graph interpretation sections from the current
tutorial are good — they just need to exist without the code-tracing
sections pulling focus.

### New: Lineage Tracing tutorial

**Location:** `analysis/02-lineage-tracing.ipynb`

Focused tutorial on answering lineage questions programmatically. Clean
separation of backward and forward as distinct tools for distinct
questions.

#### Backward: "Where did this come from?"

Start from a final artifact (e.g., a refined dataset from the last step).
Use `store.provenance.get_ancestor_ids(artifact_id)` for full transitive
ancestry in one call. Show the complete chain from output back to source.

Contrast with `store.provenance.get_direct_ancestors(artifact_id)` for
one-hop-only queries when you just need the immediate parent.

#### Forward: "What was derived from this?"

Start from a source dataset. Use
`store.provenance.get_descendant_ids(artifact_id, descendant_type="metric")`
to find all downstream metrics. Frame as impact analysis: "if this source
is bad, what results are affected?"

Contrast with `store.provenance.get_direct_descendants(source_ids)` for
one-hop queries.

#### Bulk traversal with DataFrames

Introduce `walk_forward` / `walk_backward` from `artisan.provenance` for
batch operations across many artifacts. These operate on Polars DataFrames
and are the right tool when you need to trace lineage for hundreds of
artifacts at once.

#### Practical example

"An artifact looks wrong. Trace it back to the source, find the sibling
artifacts at each step, check if the issue is isolated or systematic."
This ties together backward tracing, forward tracing, and the inspect
helpers from earlier tutorials.

#### Which tool for which question

| Question | Method | Returns |
|----------|--------|---------|
| Immediate parent | `provenance.get_direct_ancestors(id)` | `list[str]` |
| Full ancestry | `provenance.get_ancestor_ids(id)` | `set[str]` |
| Full ancestry (typed) | `provenance.get_ancestor_ids(id, ancestor_type="data")` | `set[str]` |
| Immediate children | `provenance.get_direct_descendants(ids)` | `dict[str, list[str]]` |
| Full descendants | `provenance.get_descendant_ids(id)` | `set[str]` |
| Full descendants (typed) | `provenance.get_descendant_ids(id, descendant_type="metric")` | `set[str]` |
| Bulk backward (DataFrame) | `walk_backward(candidates, targets, edges)` | `DataFrame` |
| Bulk forward (DataFrame) | `walk_forward(sources, edges, target_type)` | `DataFrame` |

### Renumbering

| Current | New |
|---------|-----|
| `execution/05-step-overrides.ipynb` | `execution/06-step-overrides.ipynb` |
| `execution/06-slurm-execution.ipynb` | `execution/07-slurm-execution.ipynb` |
| `execution/07-pipeline-cancellation.ipynb` | `execution/08-pipeline-cancellation.ipynb` |
| `analysis/02-interactive-filter.ipynb` | `analysis/03-interactive-filter.ipynb` |
| `analysis/03-timing-analysis.ipynb` | `analysis/04-timing-analysis.ipynb` |

### Index update

`tutorials/index.md` updated to reflect new entries and renumbering.
Cross-references within tutorials updated.

---

## Scope and Non-Goals

**In scope:**

- One new execution tutorial (storage and logging)
- One rewrite (provenance graphs → visualization only)
- One new analysis tutorial (lineage tracing)
- File renumbering and index updates
- Cross-reference fixes in affected tutorials

**Not in scope:**

- New concepts pages (the tutorials should be self-contained)
- API changes (we document what exists)
- Changes to Getting Started, Pipeline Design, or Writing Operations
- Tutorial ordering beyond the existing section-level recommendation

---

## Implementation Order

- Renumber existing files (mechanical, no content changes)
- Write `execution/05-storage-and-logging.ipynb`
- Rewrite `analysis/01-provenance-graphs.ipynb`
- Write `analysis/02-lineage-tracing.ipynb`
- Update `tutorials/index.md`
- Fix cross-references across all affected tutorials
- Run docs build to verify
