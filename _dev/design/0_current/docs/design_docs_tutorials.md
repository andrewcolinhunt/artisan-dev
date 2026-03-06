# Design Doc: Artisan Tutorials Documentation Improvements

## Overview

The `docs/artisan/tutorials/` directory contains 12 tutorial files (1 index + 11
Jupyter notebooks) organized into three sections: Getting Started (4), Pipeline
Patterns (5), and Working with Results (2). The tutorials teach the Artisan
pipeline framework through progressively complex examples using the built-in
example operations (DataGenerator, DataTransformer, MetricCalculator, etc.).

**Overall assessment:** The tutorials are structurally sound and cover a wide range
of patterns. The code is functional and the provenance graph visualizations are a
real strength. However, there are several systemic issues: (1) multiple notebooks
import from the `pipelines` domain package, violating the artisan/pipelines
separation; (2) narrative text between code cells is often too sparse, leaving the
reader to infer what is happening; (3) several notebooks lack learning objectives,
prerequisites, or summary sections; and (4) some concepts are used before being
introduced. The Pipeline Patterns notebooks (01-05) are particularly affected by
thin narrative — they read more like a pattern catalog than guided tutorials.

---

## File-by-File Analysis

### `docs/artisan/tutorials/index.md`

- **Current state**: Landing page listing all 12 tutorials across three sections.
- **Pipelines leakage**: None.
- **Jargon issues**: "14 reusable pipeline topologies" is mentioned in cross-references from other notebooks but not reflected here. The descriptions are clear.
- **Narrative quality**: Functional. Provides one-liner descriptions and links.
- **Structure issues**:
  - No introductory paragraph explaining who these tutorials are for or what background is assumed.
  - No "recommended reading order" or visual learning path.
  - No indication of difficulty level or time estimates per tutorial.
- **Recommended changes**:
  1. Add a 2-3 sentence introduction: who is this for, what will they learn, what's assumed.
  2. Add time estimates (already present in individual notebooks — surface them here).
  3. Add a short "Suggested path" callout: "New to Artisan? Start with First Pipeline, then Exploring Results..."
  4. Consider adding a visual learning path diagram (even ASCII art).

---

### `docs/artisan/tutorials/getting-started/01-first-pipeline.ipynb`

- **Current state**: Builds and runs a 7-step pipeline (IngestData, DataGenerator, Merge, DataTransformer, MetricCalculator, Filter, DataTransformer). Shows inspect_pipeline, inspect_metrics, and macro graph.
- **Pipelines leakage**: **None.** All imports are from `artisan.*`. This is clean.
- **Jargon issues**:
  - "Delta Lake tables" is mentioned without explaining what Delta Lake is or why it matters. A new user unfamiliar with Delta Lake will be confused.
  - "Provenance graph" is used in the header cell without definition.
  - "OutputReference" and "lazy" are mentioned in Key Takeaways but never explained in the body.
  - "artifacts" — the term is introduced in the diagram cell ("Each step produces **artifacts**") but deserves a one-sentence definition.
  - "execution units" appears in log output but is never explained.
- **Narrative quality**: Good overall flow. The diagram at the top is excellent for orientation. However, the jump from "here's the pipeline code" to "here's inspect_pipeline" is abrupt — there is no text explaining what `pipeline.finalize()` returns or why we call it. The `inspect_metrics` section has a malformed markdown cell (backtick instead of markdown header).
- **Structure issues**:
  - Has prerequisites, time estimate, GPU note — good.
  - Missing explicit "Learning objectives" (what will you know after this?).
  - Missing a "What just happened?" narrative section between the pipeline run and the inspection.
  - The code is wrapped in a `run_first_pipeline()` function, which is unusual for a tutorial. Most tutorials run code inline. The function adds an unnecessary layer of abstraction for a first tutorial.
- **Recommended changes**:
  1. Add a "What you'll learn" bullet list at the top.
  2. Unwrap the pipeline code from the function — run inline for clarity.
  3. Add a brief "What is an artifact?" callout before the pipeline diagram.
  4. Add a 2-3 sentence "What just happened?" section between the run and the inspection.
  5. Fix the malformed markdown cell for `inspect_metrics` (currently starts with a backtick).
  6. Add a brief explanation of Delta Lake on first mention (e.g., "Delta Lake — a versioned, columnar storage format that Artisan uses to persist all pipeline data").
  7. Explain `pipeline.finalize()` — what it does and what it returns.

---

### `docs/artisan/tutorials/getting-started/02-exploring-results.ipynb`

- **Current state**: Teaches inspection at every level: pipeline overview, step details, data content, metrics, lineage queries, run listing, and provenance graphs.
- **Pipelines leakage**: **None.** All imports are from `artisan.*`.
- **Jargon issues**:
  - "DataArtifact" and "Polars DataFrame" are used without introduction.
  - "lineage" is used before being defined.
  - `ArtifactStore` is introduced without context about what it represents.
- **Narrative quality**: Good progressive structure (pipeline → step → artifact → lineage). The markdown cells between code cells are too terse — most are single sentences. The lineage query section is the most complex and would benefit from a diagram or more explanation of what "ancestor" means in this context.
- **Structure issues**:
  - Has learning objectives ("What you'll learn"), prerequisites, time, GPU note — good.
  - Missing a summary bridging from inspection to the next conceptual step.
  - The Key Takeaways section is a good reference but reads like a cheat sheet rather than a conclusion.
- **Recommended changes**:
  1. Add 1-2 sentences of context before each inspection call explaining what question it answers.
  2. Add a brief explanation of Polars on first use.
  3. Expand the lineage query section with a text explanation of what "ancestor" means.
  4. Add a note about when to use programmatic queries vs. visualization.

---

### `docs/artisan/tutorials/getting-started/03-run-vs-submit.ipynb`

- **Current state**: Comprehensive comparison of `run()` vs `submit()`, mixing both, step caching, timing analysis, and resume/list runs.
- **Pipelines leakage**: **None.** All imports are from `artisan.*`.
- **Jargon issues**:
  - "StepResult", "StepFuture", "OutputReference" are introduced but the comparison table at the top may overwhelm a new reader before they see the first example.
  - "step_spec_id" is mentioned in the caching section without explaining what a spec ID is or how it's computed.
  - "deterministic" is used without clarification.
- **Narrative quality**: Excellent structure. The numbered sections (1-5) provide a clear progression. The comparison table at the top is useful as a reference. The mixing section nicely ties together the two approaches. The caching section effectively shows the speed benefit but could explain *when* cache is invalidated.
- **Structure issues**:
  - Good: has prerequisites, time, GPU note, Key Takeaways, Next Steps.
  - The output shows `PipelineTimings.plot_steps()` but there's no explanation of how to interpret the plot.
  - Section 5 (Resume) is thin — only shows the API calls but doesn't explain the use case well.
- **Recommended changes**:
  1. Move the comparison table lower — show `run()` first, then `submit()`, then the table as a recap.
  2. Add 1-2 sentences explaining when cache is invalidated (e.g., params change, inputs change).
  3. Add interpretation guidance for the timing plot.
  4. Expand the Resume section with a realistic scenario (e.g., "your notebook kernel crashed mid-pipeline").

---

### `docs/artisan/tutorials/getting-started/04-slurm-execution.ipynb`

- **Current state**: Shows transitioning a pipeline from local to SLURM execution, resource configuration, batching, and debugging tips.
- **Pipelines leakage**: **YES.**
  - `from pipelines.utils import configure_logging` — imports from the domain package.
- **Jargon issues**:
  - "HPC cluster" is used without expansion (High-Performance Computing).
  - "execution unit" is used throughout but never formally defined.
  - "artifacts_per_unit" and "units_per_worker" are mentioned without clear definition.
  - "partition", "gres" are SLURM terms that may not be familiar to all users.
- **Narrative quality**: Good structure with the table showing which steps run where. The commented-out code examples for resources and batching are a reasonable approach for a notebook that can't actually run on SLURM during docs build. However, the `EXECUTE = True` guard pattern makes the notebook hard to follow — a reader has to mentally filter out the `if EXECUTE:` wrappers.
- **Structure issues**:
  - Has prerequisites, time, GPU note, note about SLURM requirement — good.
  - Missing explicit learning objectives.
  - The debugging section is useful but feels tacked on.
- **Recommended changes**:
  1. **Fix pipelines leakage**: Replace `from pipelines.utils import configure_logging` with the artisan equivalent (or remove if `configure_logging` can be moved to artisan).
  2. Add a "What you'll learn" section.
  3. Expand "HPC" on first use.
  4. Define "execution unit" clearly when it first appears.
  5. Consider removing the `if EXECUTE:` guards and instead using the `:::note` to indicate the notebook requires SLURM — the guards add visual noise.
  6. Add a brief explanation of common SLURM terms (partition, gres, time_limit) for non-HPC users.

---

### `docs/artisan/tutorials/pipeline-patterns/01-sources-and-chains.ipynb`

- **Current state**: Three patterns: generative source, external file ingest, and simple chain. Each has a code block, macro graph, and micro provenance stepper.
- **Pipelines leakage**: **YES.**
  - `from pipelines.utils import configure_logging` — imports from the domain package.
- **Jargon issues**:
  - "FileRefArtifact" and "DataArtifact" are used without prior introduction.
  - The graph legend (boxes, arrows) is well-done — a strength.
- **Narrative quality**: Thin. Each pattern has a "When to use" callout (good) but the explanatory text between code and graph is nearly absent. After showing the code, the notebook immediately shows the graph with no discussion of what happened, what the output means, or what to notice in the graph.
- **Structure issues**:
  - Missing learning objectives, prerequisites, time estimate.
  - Missing a summary/next-steps section at the end.
  - The `make_env()` helper function is repeated in every Pipeline Patterns notebook — this is fine for independence but could be extracted to a shared utility.
  - No conclusion tying the patterns together.
- **Recommended changes**:
  1. **Fix pipelines leakage**: Replace `from pipelines.utils import configure_logging`.
  2. Add header with learning objectives, prerequisites, time.
  3. Add 2-3 sentences after each graph explaining what to notice (e.g., "Notice that IngestData produces both FileRefArtifact and DataArtifact nodes — the file reference preserves the original path while the data artifact contains the loaded content").
  4. Add a summary section comparing the three patterns.
  5. Add next-steps links.

---

### `docs/artisan/tutorials/pipeline-patterns/02-branching-and-merging.ipynb`

- **Current state**: Four patterns: execution fan-out, data fan-out (variants), merge, and multi-merge.
- **Pipelines leakage**: **YES.**
  - `from pipelines.utils import configure_logging` — imports from the domain package.
- **Jargon issues**:
  - "passthrough semantics" is used for Merge without explaining what that means (no new artifacts created, just routing).
  - "variants" parameter is used without explaining the concept.
- **Narrative quality**: Same issue as 01 — thin narrative between code and graphs. The patterns themselves are well-chosen and build logically. The "When to use" callouts are helpful.
- **Structure issues**:
  - Missing header section (objectives, prereqs, time).
  - Missing summary/next-steps.
  - Pattern 4B (Multi-Merge) has no markdown explanation at all between code and graph.
- **Recommended changes**:
  1. **Fix pipelines leakage**: Replace `from pipelines.utils import configure_logging`.
  2. Add header and footer sections.
  3. Add graph interpretation text after each visualization.
  4. Explain "passthrough semantics" when Merge is first introduced.
  5. Explain the `variants` parameter concept before showing pattern 3B.

---

### `docs/artisan/tutorials/pipeline-patterns/03-metrics-and-filtering.ipynb`

- **Current state**: Three patterns: single-metric filter, multi-metric filter, and empty filter (graceful skip). Includes FilterDiagnostics.
- **Pipelines leakage**: **YES.**
  - `from pipelines.utils import configure_logging` — imports from the domain package.
- **Jargon issues**:
  - "auto-discovers associated metrics via provenance" — this is a complex concept introduced without explanation of the mechanism.
  - "passthrough" input role — used repeatedly but the distinction between passthrough and other roles is not clear.
  - "role.field" syntax for multi-metric filter — introduced well with explanation.
  - "AND-semantics" — clear to programmers but could trip up domain scientists.
- **Narrative quality**: Best of the Pipeline Patterns notebooks. The multi-metric filter section (5B) has good explanatory text. The empty filter section (5C) is a nice touch showing graceful degradation. The FilterDiagnostics integration (summary + plot) is valuable.
- **Structure issues**:
  - Missing header (objectives, prereqs, time).
  - Missing summary/next-steps.
  - The inspect_metrics and FilterDiagnostics calls are well-placed but could use interpretation text.
- **Recommended changes**:
  1. **Fix pipelines leakage**: Replace `from pipelines.utils import configure_logging`.
  2. Add header and footer sections.
  3. Explain how Filter auto-discovers metrics (brief summary of the provenance walk).
  4. Add interpretation text after the FilterDiagnostics outputs.
  5. Add a brief note about operator options (gt, ge, lt, le, eq).

---

### `docs/artisan/tutorials/pipeline-patterns/04-multi-input-operations.ipynb`

- **Current state**: Four patterns: config+execute, parameter sweep, ingest+transform, multi-source metrics.
- **Pipelines leakage**: **YES.**
  - `from pipelines.utils import configure_logging` — imports from the domain package.
- **Jargon issues**:
  - "group_by=LINEAGE" and "group_by=CROSS_PRODUCT" — introduced without a general explanation of the group_by concept. These are critical framework concepts.
  - "$artifact references" in ExecutionConfigArtifact — mentioned but not explained.
  - "1:N lineage pairing" — assumes understanding of lineage mechanics.
- **Narrative quality**: The most concept-dense notebook. Pattern 6A (Config+Execute) introduces the most complex topic in the series but lacks sufficient explanation. The note at the end of 6B about LINEAGE vs CROSS_PRODUCT is critical information buried as a `::: note` — this should be front and center.
- **Structure issues**:
  - Missing header (objectives, prereqs, time).
  - Missing summary/next-steps.
  - The 6B note about LINEAGE only matching one config is a significant gotcha that's buried.
  - Patterns 7A and 7B are simpler than 6A/6B and feel like they belong in an earlier notebook.
- **Recommended changes**:
  1. **Fix pipelines leakage**: Replace `from pipelines.utils import configure_logging`.
  2. Add header and footer sections.
  3. Add a dedicated "Understanding group_by" section before pattern 6A, explaining LINEAGE, CROSS_PRODUCT, and when to use each.
  4. Move the LINEAGE vs CROSS_PRODUCT note from a buried admonition to a prominent explanation section.
  5. Explain "$artifact" references in config templates.
  6. Consider reordering: 7A and 7B first (simpler), then 6A and 6B (more complex).

---

### `docs/artisan/tutorials/pipeline-patterns/05-advanced-patterns.ipynb`

- **Current state**: Four patterns: batching, diamond DAG, output-to-output lineage, and iterative refinement (cycling).
- **Pipelines leakage**: **YES.**
  - `from pipelines.utils import configure_logging` — imports from the domain package.
  - Pattern 10A mentions "Used in production by domain operations (e.g., RFD3, AF3)" — direct reference to domain-specific operations.
- **Jargon issues**:
  - "artifacts_per_unit" — used with only a brief explanation. The relationship between artifacts, execution units, and SLURM jobs is not diagrammed.
  - "infer_lineage_from" — an advanced OutputSpec config mentioned in 10A without context.
  - "OutputSpec" — referenced but not linked to documentation.
- **Narrative quality**: Pattern 11A (Iterative Refinement) is the highlight — it clearly demonstrates a real-world pattern with a Python for loop. Pattern 9A (Diamond DAG) nicely combines earlier patterns. Pattern 8A (Batching) would benefit from a diagram showing how artifacts map to execution units. Pattern 10A (Output Lineage) is the most abstract and could use more explanation.
- **Structure issues**:
  - Missing header (objectives, prereqs, time).
  - Missing summary/next-steps.
  - The patterns vary widely in complexity — 8A is simple, 10A is framework-internal.
- **Recommended changes**:
  1. **Fix pipelines leakage**: Replace `from pipelines.utils import configure_logging`.
  2. **Remove domain references**: Replace "Used in production by domain operations (e.g., RFD3, AF3)" with "Used by domain extension operations that produce multiple correlated output types."
  3. Add header and footer sections.
  4. Add a diagram for batching showing N artifacts mapped to K execution units.
  5. Add more context for output-to-output lineage — explain when and why you would use it.
  6. Add graph interpretation text after each visualization.

---

### `docs/artisan/tutorials/working-with-results/01-provenance-graphs.ipynb`

- **Current state**: Shows macro graph, micro provenance stepper, and raw Delta Lake table queries.
- **Pipelines leakage**: **None.** All imports are from `artisan.*`.
- **Jargon issues**:
  - "provenance" itself is never defined — it's assumed the reader knows the term.
  - "artifact_edges" table — low-level detail that appears without context.
- **Narrative quality**: Good. The "Reading the Macro Graph" and "Reading the Micro Graph" sections are well-written and provide the interpretation guidance that the Pipeline Patterns notebooks lack. The raw table queries section is useful for power users.
- **Structure issues**:
  - Has prerequisites, time, GPU note — good.
  - Missing explicit learning objectives.
  - Key Takeaways section is missing (has Next Steps but no summary).
- **Recommended changes**:
  1. Add a "What you'll learn" section.
  2. Add a brief definition of "provenance" in the opening paragraph (e.g., "Provenance — the record of where each artifact came from, what produced it, and what it was derived from").
  3. Add a Key Takeaways section before Next Steps.
  4. Add more context for the raw table query (what columns exist, what each means).

---

### `docs/artisan/tutorials/working-with-results/02-filter-diagnostics.ipynb`

- **Current state**: Demonstrates FilterDiagnostics: summary, funnel, DataFrame, and distribution plots.
- **Pipelines leakage**: **YES.**
  - `from pipelines.utils import configure_logging` — imports from the domain package.
- **Jargon issues**:
  - "funnel" is used as a concept without explanation (cumulative AND from left to right).
  - The import path `from artisan.operations.examples.data_generator_with_metrics import DataGeneratorWithMetrics` is unusually specific — other notebooks import from the package level.
- **Narrative quality**: Adequate but thin. Each section (summary, funnel, dataframe, plots) is introduced with a single sentence. The notebook would benefit from showing example output inline and explaining how to interpret the results. A "now what?" section — what do you do when a criterion is too strict? — would be valuable.
- **Structure issues**:
  - Has prerequisites, time, GPU note — good.
  - Missing learning objectives.
  - Missing a "How to use this information" section.
  - The Next Steps link to "Interface Metrics" is a domain tutorial (not artisan).
- **Recommended changes**:
  1. **Fix pipelines leakage**: Replace `from pipelines.utils import configure_logging`.
  2. **Fix domain reference in Next Steps**: Remove or replace the link to "Interface Metrics" (a domain tutorial) with an artisan-only link.
  3. Add learning objectives.
  4. Add interpretation guidance after each diagnostic output.
  5. Add a "Tuning your filter" section showing the workflow: run diagnostics → identify bottleneck → adjust criteria → re-run.
  6. Fix the unusually specific import path for DataGeneratorWithMetrics.

---

## Learning Path Assessment

The current ordering is mostly logical:

```
Getting Started:        01 → 02 → 03 → 04    (build → inspect → execution modes → SLURM)
Pipeline Patterns:      01 → 02 → 03 → 04 → 05   (sources → branching → filtering → multi-input → advanced)
Working with Results:   01 → 02               (graphs → filter diagnostics)
```

**Issues:**

1. **Gap between Getting Started and Pipeline Patterns.** Tutorial 01 (First
   Pipeline) builds a 7-step pipeline but the Pipeline Patterns tutorials start
   from scratch with simple patterns. There's no bridge explaining that "now we'll
   break down each pattern individually."

2. **Working with Results is partially redundant.** The First Pipeline and
   Exploring Results notebooks already show `build_macro_graph`, `inspect_pipeline`,
   and `inspect_metrics`. The Provenance Graphs tutorial covers similar ground.
   Consider cross-referencing more explicitly to avoid repetition.

3. **Pipeline Patterns 04 (Multi-Input) has misplaced patterns.** Patterns 7A
   (Ingest+Transform) and 7B (Multi-Source Metrics) are simpler than 6A/6B
   (Config+Execute) and could be moved earlier.

4. **Pipeline Patterns 03 (Metrics and Filtering) introduces FilterDiagnostics**
   but Working with Results 02 is a full tutorial on the same tool. The overlap
   is not acknowledged.

5. **No tutorial covers writing custom operations.** The Getting Started sequence
   uses built-in example operations but never shows how to create your own. This
   is arguably the most important next step for a real user.

**Recommended ordering:** Keep the current structure but add bridges:
- Add a "where to go from here" section at the end of Getting Started 04.
- Add introductory text to Pipeline Patterns 01 saying "Now that you've built a
  full pipeline, let's understand each pattern in isolation."
- Cross-reference the FilterDiagnostics overlap explicitly.

---

## Cross-Cutting Issues

### 1. `pipelines.utils.configure_logging` leakage (HIGH priority)

**Affected notebooks:** 04-slurm-execution, 01-sources-and-chains, 02-branching-and-merging, 03-metrics-and-filtering, 04-multi-input-operations, 05-advanced-patterns, 02-filter-diagnostics.

**Seven out of eleven notebooks** import `from pipelines.utils import configure_logging`. This is a domain package import in artisan tutorials. Either:
- Move `configure_logging` to `artisan.utils` (preferred), or
- Remove the import and rely on default logging, or
- Replace with a standard `logging.basicConfig()` call.

### 2. Thin narrative in Pipeline Patterns (MEDIUM priority)

All five Pipeline Patterns notebooks follow the same template: pattern description → code → macro graph → micro stepper → (next pattern). The code runs and the graphs render, but there is almost no text explaining what the output means, what to notice in the graph, or how the pattern connects to real-world use cases. These read like a reference catalog, not tutorials.

**Fix:** Add 2-4 sentences of interpretation after each graph. Point out specific features: "Notice that Merge produces no new artifact nodes..." or "The orange lineage arrows show how each variant traces back to the same parent dataset."

### 3. Missing standard sections (MEDIUM priority)

The Pipeline Patterns notebooks (01-05) all lack:
- Learning objectives
- Prerequisites
- Time estimates
- Summary/Key Takeaways
- Next Steps links

The Getting Started and Working with Results notebooks have most of these. Standardize across all tutorials.

### 4. Inconsistent helper pattern (LOW priority)

- Getting Started notebooks use `tutorial_setup()` from `artisan.utils`.
- Pipeline Patterns notebooks use `get_caller_dir()` + `make_env()` with manual `shutil.rmtree()`.

This inconsistency is confusing — a reader who does Getting Started first will expect `tutorial_setup()` everywhere. Consider standardizing on `tutorial_setup()`.

### 5. Excessive log output (LOW priority)

Many notebooks show extensive Prefect/artisan log output in cell outputs. While useful for debugging, this makes the notebooks visually noisy when read as documentation. Consider either:
- Suppressing log output for docs builds, or
- Adding a note explaining that the log output is normal and what to look for.

---

## Missing Tutorials

### 1. Writing Custom Operations (HIGH priority)

- **Title:** "Writing Your First Operation"
- **Learning objective:** Create a custom OperationDefinition with input/output specs, params, and a run method.
- **Where it fits:** New section between Getting Started and Pipeline Patterns, or as Getting Started 05.
- **Rationale:** This is the single most important missing tutorial. Every real user will need to write operations. The current tutorials only use built-in examples.

### 2. Exporting Results (MEDIUM priority)

- **Title:** "Exporting Pipeline Data"
- **Learning objective:** Export artifacts from Delta Lake to standard formats (CSV, Parquet, files on disk) for downstream use.
- **Where it fits:** Working with Results 03.
- **Rationale:** Users will need to get data out of the Artisan storage system. Currently no tutorial covers this.

### 3. Error Handling and Debugging (MEDIUM priority)

- **Title:** "When Things Go Wrong"
- **Learning objective:** Diagnose and recover from operation failures, partial pipeline runs, and bad inputs.
- **Where it fits:** Getting Started 05 or a new "Troubleshooting" section.
- **Rationale:** Real pipelines fail. Showing how to inspect errors, use `preserve_working`, read error metadata, and resume after failure would be extremely valuable.

### 4. Understanding Artifacts and Schemas (LOW priority)

- **Title:** "Artifacts and Data Models"
- **Learning objective:** Understand the artifact type hierarchy (DataArtifact, MetricArtifact, FileRefArtifact, ExecutionConfigArtifact), how they're stored, and how to query them.
- **Where it fits:** Between Getting Started and Pipeline Patterns, or as a Concepts companion notebook.
- **Rationale:** Artifacts are the central data model but are never formally introduced. Users encounter DataArtifact, MetricArtifact, FileRefArtifact in output but don't know the taxonomy.

### 5. Pipeline Configuration and Best Practices (LOW priority)

- **Title:** "Configuring Pipelines for Production"
- **Learning objective:** Set up delta_root, staging_root, working_root for production use; understand naming conventions; configure logging; set up for team collaboration.
- **Where it fits:** After SLURM Execution or as a new "Production" section.
- **Rationale:** The tutorials use temporary directories. Real users need guidance on directory structure, naming, and configuration for persistent use.

---

## Priority

### High Impact

| Item | Scope | Effort |
|------|-------|--------|
| Fix `pipelines.utils.configure_logging` leakage (7 notebooks) | All Pipeline Patterns + SLURM + Filter Diagnostics | Small (find-and-replace or move function) |
| Remove domain reference in 05-advanced-patterns (RFD3, AF3 mention) | 1 notebook | Trivial |
| Fix domain link in 02-filter-diagnostics Next Steps | 1 notebook | Trivial |
| Write "Writing Your First Operation" tutorial | New notebook | Large |

### Medium Impact

| Item | Scope | Effort |
|------|-------|--------|
| Add narrative interpretation text to Pipeline Patterns (01-05) | 5 notebooks | Medium (2-4 sentences per pattern, ~20 additions total) |
| Add standard sections (objectives, prereqs, time, summary) to Pipeline Patterns | 5 notebooks | Medium |
| Add "What is Delta Lake?" and "What is provenance?" callouts | 01-first-pipeline, 01-provenance-graphs | Small |
| Fix malformed markdown cell in 01-first-pipeline | 1 notebook | Trivial |
| Expand group_by explanation in 04-multi-input-operations | 1 notebook | Small |
| Write "Exporting Pipeline Data" tutorial | New notebook | Medium |
| Write "When Things Go Wrong" tutorial | New notebook | Medium |

### Low Impact

| Item | Scope | Effort |
|------|-------|--------|
| Standardize helper pattern (tutorial_setup vs make_env) | 5 notebooks | Small |
| Add time estimates and difficulty to index.md | 1 file | Trivial |
| Reduce log noise in cell outputs | All notebooks | Medium (re-run with suppressed logging) |
| Unwrap function in 01-first-pipeline | 1 notebook | Small |
| Write "Artifacts and Data Models" tutorial | New notebook | Medium |
| Write "Configuring for Production" tutorial | New notebook | Medium |
