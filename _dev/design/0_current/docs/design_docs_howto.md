# Design Doc: Artisan How-to Guides Documentation Improvements

## Overview

The `docs/artisan/how-to-guides/` directory contains six how-to guides and an
index page. The guides cover operation authoring, artifact types, execution
configuration, provenance inspection, and result export. Overall quality is
uneven: some guides (configuring-execution, inspecting-provenance) are
well-structured and genuinely useful, while others suffer from significant
pipelines-layer leakage, inconsistent task-orientation, and missing content.

The most critical issue is that three of the six guides import or reference the
`pipelines` domain package directly, which violates the artisan/pipelines layer
boundary. The second major issue is that several guides read more like reference
documentation or design docs than task-oriented how-to guides (missing
prerequisites, unclear goals, no "you should now see X" verification steps).

---

## File-by-File Analysis

### index.md

- **Current state**: Clean index page with three sections (Operations,
  Configuration, Results), each listing 2-3 guides with one-line descriptions.
- **Pipelines leakage**: None.
- **Jargon issues**: None. Descriptions are clear.
- **Content issues**: The page is minimal but functional. It does not explain
  what how-to guides are (vs. tutorials or concepts) or suggest which guide to
  start with.
- **Task-orientation**: N/A (index page).
- **Recommended changes**:
  1. Add a one-sentence explanation of the how-to guide format ("These are
     goal-oriented recipes. Each guide assumes you have a working pipeline and
     focuses on a specific task.").
  2. As new guides are added, consider grouping more granularly or adding a
     "Start here" callout for the most common entry points.

---

### configuring-execution.md

- **Current state**: Comprehensive guide covering compute backends, resource
  configuration, batching, containers, working root, and failure policies. Good
  tables, code examples, and explanations.
- **Pipelines leakage**: None. All imports are from `artisan`. Examples use
  generic `MyOp` placeholders.
- **Jargon issues**:
  - "execution unit (work package)" -- the parenthetical helps, but the concept
    of execution units vs. workers vs. tasks deserves a one-line definition
    earlier in the guide.
  - "predicate pushdown" -- mentioned in passing (not in this file, but in
    exporting-results); not an issue here.
  - "staging_root" -- referenced in the Working Root section without
    introduction. A reader who hasn't read the storage concepts page won't know
    what this is.
- **Content issues**:
  - The guide opens with "Key types" and "Key files" pointing at source paths.
    This is reference-doc style, not how-to style. A user doesn't need to know
    `src/artisan/schemas/operation_config/` to configure execution.
  - No "Prerequisites" section.
  - No "Result" or verification step ("Run your pipeline and check the SLURM
    logs to confirm GPU allocation").
  - The `CommandSpec` section is thorough but mixes "how to override container
    settings at the step level" (how-to) with "how to declare command specs in
    an operation" (belongs in writing-creator-operations or a reference page).
- **Task-orientation**: Mostly good. Each section answers "how to do X" but the
  guide tries to cover too many tasks in one page. It could be split into
  focused guides (e.g., "How to run on SLURM", "How to configure batching",
  "How to use containers").
- **Recommended changes**:
  1. Add a Prerequisites section.
  2. Remove "Key files" source paths -- replace with links to API reference if
     needed.
  3. Add brief verification steps after key configurations.
  4. Consider splitting into 2-3 smaller, more focused guides.

---

### creating-artifact-types.md

- **Current state**: The longest guide (700 lines). Walks through creating a
  `SequenceArtifact` end-to-end, including model code, registry, exports, tests,
  and usage in operations.
- **Pipelines leakage**: **Severe.**
  - Line 3: "A practical guide for adding new artifact types to the `pipelines`
    framework." -- Should say "Artisan framework."
  - Lines 17-19: The artifact type table lists `structure` and
    `structure_annotation` as `pipelines`-layer types. These are domain artifacts
    and should either be removed or labeled as "example domain extension" only.
  - Line 68: `ArtifactTypeDef` import path references
    `pipelines.schemas.artifact.registry` -- should be
    `artisan.schemas.artifact.registry`.
  - Lines 429-430: "Atom selections are stored in a separate
    `StructureAnnotation` artifact, not embedded in `StructureArtifact`." --
    Pure domain concern; remove entirely.
  - Lines 622-672: The "Example: Using in Operations" section references
    `ArtifactTypes.STRUCTURE`, `StructureArtifact`, and shows a
    `StructureArtifact -> SequenceExtractor -> SequenceArtifact` diagram. This
    is domain-specific and should use generic artisan types (data, metric) or
    the SequenceArtifact being built in the guide itself.
  - Line 661-671: "Relationship to StructureArtifact" subsection -- entirely
    domain-specific; remove.
- **Jargon issues**:
  - "content-addressed data nodes in the bipartite provenance graph" (line 9) --
    extremely dense. Needs unpacking or simplification. A user creating an
    artifact type doesn't need to understand graph theory.
  - "ID-only mode" -- used but never fully explained until the Hydration Modes
    section deep in the guide.
  - "xxh3_128" -- implementation detail; just say "content hash."
- **Content issues**:
  - The guide is far too long. The full `SequenceArtifact` implementation
    (lines 109-317) is ~200 lines of Python code, much of it boilerplate that
    could be summarized with a shorter example and a link to a full reference
    implementation.
  - Line 365: Contains an inline TODO ("TODO: why are we referencing old code.
    simplify.") -- this should have been resolved before publishing.
  - The "What it replaces" table (lines 367-378) describes the old system.
    Users don't care about what the old system looked like. Remove this
    migration context.
  - The test section (lines 469-599) is very thorough but arguably too detailed
    for a how-to guide. The full test file could be linked rather than inlined.
  - No Prerequisites section.
  - No "Result" verification step.
- **Task-orientation**: Partially good -- it has a clear checklist and step-by-
  step flow. But the volume of code and internal-architecture exposition drowns
  the task-oriented structure.
- **Recommended changes**:
  1. **Fix all pipelines references** -- replace `pipelines` with `artisan`,
     remove domain artifact examples, remove StructureArtifact references.
  2. Remove the inline TODO on line 365.
  3. Remove the "What it replaces" migration table.
  4. Shorten the model code example -- show the essential pattern (POLARS_SCHEMA,
     to_row, from_row, draft, finalize, materialize_to) with a simpler example
     (e.g., a `DocumentArtifact` with text content), and link to the full
     implementation for reference.
  5. Move the full test suite to a collapsible section or a separate file.
  6. Simplify the opening paragraph -- replace graph-theory jargon with
     practical language.
  7. Add Prerequisites and a verification step.
  8. Shorten from ~700 lines to ~300-400 lines.

---

### exporting-results.md

- **Current state**: Short, practical guide (~145 lines) covering Delta table
  reads, metric parsing, provenance joins, CSV/Parquet export, step filtering,
  and lazy scanning.
- **Pipelines leakage**: **Moderate.**
  - Lines 80-82: References `export_structures_with_metrics()` as a
    "domain-specific" function and links to
    `../../pipelines/how-to-guides/exporting-structures.md`. The cross-reference
    itself is fine (it's clearly labeled as domain-specific), but the phrasing
    in the body text should make it clearer this is an optional domain extension,
    not something the reader needs.
  - Lines 141-142: Cross-reference to the pipelines export guide is acceptable
    but should be clearly framed as "If you use the pipelines domain layer."
- **Jargon issues**:
  - "predicate pushdown" (line 125) -- not explained. Add a brief parenthetical
    ("filtering happens at the storage level, so only matching rows are loaded
    into memory").
  - "Delta Lake tables" -- assumed knowledge throughout. A brief note on what
    Delta Lake is (or a link to the concepts page) at the top would help.
- **Content issues**:
  - No Prerequisites section (should state: completed pipeline run, `polars`
    installed).
  - The metric parsing section (lines 36-49) uses a Python loop to parse JSON.
    This is functional but not idiomatic Polars. A `pl.col("content").map_elements(json.loads)`
    or `pl.col("content").str.json_decode()` approach would be more natural.
  - No mention of how to export file-type artifacts (e.g., getting actual files
    out, not just tabular data).
  - Missing a "Result" section showing what the output CSV/Parquet looks like.
- **Task-orientation**: Good structure. Each section is a clear mini-task. But
  the guide as a whole lacks a framing goal statement ("By the end of this
  guide, you will be able to export any pipeline data to CSV or Parquet for
  analysis in external tools.").
- **Recommended changes**:
  1. Soften the pipelines cross-reference -- frame as optional domain extension.
  2. Add a Prerequisites section.
  3. Add a brief note explaining Delta Lake or linking to concepts.
  4. Add a section on exporting file-type artifacts (materializing to disk).
  5. Add a framing goal statement at the top.
  6. Explain "predicate pushdown" briefly.

---

### inspecting-provenance.md

- **Current state**: Well-structured guide (~170 lines) covering macro graphs,
  micro graphs/stepper, inspect helpers, programmatic queries (backward/forward
  trace, metadata), and raw table queries.
- **Pipelines leakage**: None. All imports are from `artisan`. Examples use
  generic artifact IDs.
- **Jargon issues**:
  - "dual provenance system" (line 5) -- might confuse readers. The guide
    explains it implicitly through macro vs. micro graphs, but a one-liner
    ("Artisan tracks provenance at two levels: pipeline-level topology and
    artifact-level derivation") would help.
  - "bipartite" is not used here (good), but the graph element table (line 45)
    mentions "Blue boxes: Artifacts (structures, metrics, configs)" -- the
    parenthetical "structures" leaks a domain concept. Replace with
    "(data, metrics, configs)" or just "(various types)".
- **Content issues**:
  - The "Inspect Helpers" section is the most practical part and should be
    promoted higher (or at least called out as "Start here").
  - The `get_all_ancestors()` manual walk function (lines 96-105) is useful but
    should note whether there's a built-in utility for this (or if one is
    planned).
  - No Prerequisites section.
  - No example output -- showing what `inspect_pipeline()` returns (even as a
    text table) would make the guide much more concrete.
- **Task-orientation**: Good overall. Each section answers a clear "how to"
  question. The progression from visual (graphs) to tabular (helpers) to
  programmatic (queries) to raw (tables) is logical.
- **Recommended changes**:
  1. Fix the minor "structures" reference in the graph legend table.
  2. Add a Prerequisites section.
  3. Add example output for `inspect_pipeline()` and `inspect_step()`.
  4. Promote Inspect Helpers to first position (most users start here).
  5. Add a one-liner explaining "dual provenance."

---

### writing-creator-operations.md

- **Current state**: Comprehensive guide (~300 lines) covering a minimal
  example, metadata/roles, inputs/outputs, parameters, the three-phase
  lifecycle, common patterns, and common pitfalls.
- **Pipelines leakage**: **Moderate.**
  - Lines 104-105: `InputRole.STRUCTURE = "structure"` and related output roles
    like `RELAXED`, `SCORES` -- these imply molecular structure processing,
    which is a domain concern. Should use generic roles (e.g., `DATASET`,
    `REPORT`, `SUMMARY`).
  - Lines 187-194: The postprocess example uses `StructureArtifact.draft()` and
    filters for `.pdb`, `.cif` extensions. This is an explicit domain reference.
    Should use `DataArtifact` or a generic example.
  - Lines 226-234: The memory outputs example uses `rmsd` and `energy` as metric
    names -- these are molecular simulation terms. Use generic metric names like
    `accuracy`, `processing_time`.
  - The imports at line 25 (`DataArtifact`) are fine, but the later examples
    switch to `StructureArtifact` without importing it, creating inconsistency.
- **Jargon issues**:
  - "three-phase lifecycle" -- explained well through the guide, but never
    explicitly defined in one place. A brief summary diagram at the top would
    help.
  - "content-addressed" -- used in passing, never defined.
  - "infer_lineage_from" -- central concept but the explanation is terse. The
    table on lines 114-118 is helpful but could use more prose.
- **Content issues**:
  - The minimal working example is good but long (~50 lines). It tries to show
    everything at once. Consider a truly minimal version (10-15 lines) first,
    then expand.
  - The `preprocess` section says "required for operations with inputs" but the
    heading says "(required for operations with inputs)" -- the conditions under
    which preprocess can be skipped should be clearer.
  - `execute` says "(required)" but generative operations with no file I/O may
    not need it in the traditional sense. Clarify.
  - The common patterns section is excellent and covers real use cases well.
  - No Prerequisites section (the guide does reference prerequisites in line 6-8
    but not in a structured format).
  - The pitfalls table is very useful.
- **Task-orientation**: Mostly good. The step-by-step structure (Steps 1-4) is
  clear. The common patterns section extends well beyond the basic how-to.
- **Recommended changes**:
  1. **Remove all domain references**: Replace `StructureArtifact` with
     `DataArtifact`, replace `STRUCTURE`/`RELAXED` roles with generic names,
     replace `rmsd`/`energy` with generic metrics.
  2. Start with a shorter minimal example, then build up.
  3. Add a lifecycle diagram (preprocess -> execute -> postprocess).
  4. Add a structured Prerequisites section.
  5. Clarify when `preprocess` and `execute` are truly required vs. optional.

---

### writing-curator-operations.md

- **Current state**: Guide (~265 lines) covering curator vs. creator comparison,
  return types, anatomy of a curator, reference implementations (Filter, Merge,
  IngestStructure), and testing.
- **Pipelines leakage**: **Severe.**
  - Lines 54-67: The `ArtifactResult` example imports `StructureArtifact` from
    `pipelines.schemas` and creates structure drafts. This is a direct
    pipelines import in what should be an artisan-only guide.
  - Lines 207-221: The `IngestStructure` section is entirely domain-specific.
    `IngestStructure` is a pipelines-layer operation. This section should either
    be removed or replaced with a generic `IngestFiles` example showing how to
    subclass the abstract base for any artifact type.
  - Lines 231-255: The testing section uses `StructureArtifact` from
    `pipelines.schemas`. Should use `DataArtifact` from `artisan.schemas`.
  - Line 266: Cross-reference to `pipeline-construction.md` -- this file may not
    exist (not in the index). Broken link risk.
- **Jargon issues**:
  - "CuratorExecuteInput" -- the type name is clear but the relationship to
    `ExecuteInput` (creator) is never explained. A note saying "this is the
    curator equivalent of the creator's three input types" would help.
  - "runtime_defined_inputs" -- mentioned in the class variable table but not
    well explained. When would a user set this to `True` vs. `False`?
  - "independent_input_streams" -- listed in the table but never demonstrated
    in code.
- **Content issues**:
  - The guide conflates "how to write a custom curator" with "reference docs for
    built-in curators." The Filter and Merge sections are usage guides for
    existing operations, not guides for writing new ones. These belong in a
    separate "How to filter artifacts" or "How to merge streams" guide, or in
    a tutorial.
  - The "Anatomy of a Curator Operation" section is the actual how-to content
    and should be the primary focus.
  - No Prerequisites section.
  - No verification/result step.
  - The testing section is good but uses domain types.
  - The `MyFilter` example (lines 87-117) uses `self._passes(artifact.artifact_id)`
    which is never defined. The reader has no idea what filtering logic goes
    inside. A concrete example (e.g., filtering by metadata field) would be more
    useful.
- **Task-orientation**: Weak. The guide title says "writing curator operations"
  but much of the content describes how to USE existing curators. The actual
  writing guidance (the Anatomy section) is one of five major sections.
- **Recommended changes**:
  1. **Remove all pipelines imports and domain references.** Replace
     `StructureArtifact` with `DataArtifact`, remove `IngestStructure` section.
  2. Refocus the guide on writing custom curators. Move built-in curator usage
     (Filter, Merge) to a separate guide or tutorial.
  3. Replace the `self._passes()` placeholder with a concrete filtering example
     (e.g., filter by metadata field value, or filter by artifact count).
  4. Add Prerequisites and verification steps.
  5. Explain `runtime_defined_inputs` and `independent_input_streams` with
     concrete use cases.
  6. Fix the potentially broken cross-reference to `pipeline-construction.md`.

---

## Cross-Cutting Issues

### 1. Inconsistent Pipelines/Artisan Boundary

Three of six guides contain direct `pipelines` imports or domain-specific
references (`StructureArtifact`, `IngestStructure`, molecular file formats like
`.pdb`/`.cif`, molecular metrics like `rmsd`/`energy`). This is the highest-
priority fix. Artisan documentation must be domain-agnostic.

**Affected files**: `creating-artifact-types.md`, `writing-creator-operations.md`,
`writing-curator-operations.md`.

### 2. Missing Prerequisites Sections

None of the six guides has a proper Prerequisites section (structured, with
links). `writing-creator-operations.md` has an informal one-liner. All guides
should have:
```
**Prerequisites:**
- A working Artisan installation ([Getting Started](../getting-started/...))
- Familiarity with [core concepts](../concepts/...)
```

### 3. Missing Verification Steps

No guide tells the reader how to verify their work. Each should end with a
"Verify it works" section (run a test, check output, inspect a table).

### 4. "Key files" Source Paths

`configuring-execution.md`, `inspecting-provenance.md`, and
`writing-curator-operations.md` all list source file paths at the top. This is
reference-doc convention, not how-to convention. Users following a how-to guide
don't need to know where the framework source lives. Remove these or move them
to a "Further reading" section at the bottom.

### 5. Inconsistent Code Example Style

Some guides use generic placeholders (`MyOp`, `MyFilter`), others use domain-
specific names (`AlignOp`, `SequenceExtractor`). All should use generic,
descriptive names that don't imply a specific scientific domain.

### 6. No Consistent Structure Template

The guides don't follow a consistent template. A standard template would improve
both authoring and reading:

```
# Title (How to [do X])
Brief goal statement.

**Prerequisites:** ...

## Steps
### Step 1: ...
### Step 2: ...

## Verify
How to confirm it worked.

## Common Issues
Troubleshooting table.

## Cross-References
```

---

## Missing Guides

### "How to Build a Pipeline" (pipeline-construction.md)

- **Goal**: Walk through constructing a multi-step pipeline with sources,
  steps, and result access.
- **Who needs it**: Every Artisan user. This is the most fundamental task.
- **Note**: `writing-curator-operations.md` already references
  `pipeline-construction.md` in its cross-references, suggesting it was planned
  but never written.

### "How to Filter Artifacts"

- **Goal**: Use the built-in `Filter` operation to select artifacts by metric
  criteria.
- **Who needs it**: Any user who needs to narrow results between pipeline steps.
- **Note**: Currently buried inside `writing-curator-operations.md` as a
  "reference implementation," but it's a common user task that deserves its own
  focused guide.

### "How to Merge Artifact Streams"

- **Goal**: Combine artifacts from multiple pipeline steps into a single stream
  using `Merge`.
- **Who needs it**: Users building pipelines with branching/merging patterns.
- **Note**: Same issue as Filter -- currently embedded in the curator guide.

### "How to Ingest External Data"

- **Goal**: Bring external files or data into a pipeline using `IngestFiles`
  (or a domain-specific subclass).
- **Who needs it**: Every user who starts with data from outside the framework.
- **Note**: The `IngestStructure` reference in the curator guide is domain-
  specific. A generic `IngestFiles` guide would serve the artisan layer.

### "How to Debug a Failed Step"

- **Goal**: Diagnose why a pipeline step failed -- inspect logs, check sandbox
  contents, use `preserve_working`, read error metadata.
- **Who needs it**: Every user, eventually.
- **Note**: `configuring-execution.md` mentions `preserve_working` but doesn't
  explain the debugging workflow.

### "How to Use Caching"

- **Goal**: Understand and control artifact caching (cache hits, invalidation,
  forced re-runs).
- **Who needs it**: Users running pipelines iteratively during development.
- **Note**: No guide mentions caching at all, but it's a core framework feature.

### "How to Write Integration Tests for Operations"

- **Goal**: Test an operation end-to-end in a real (local) pipeline, not just
  unit-test the methods.
- **Who needs it**: Operation authors who want confidence beyond unit tests.
- **Note**: `creating-artifact-types.md` mentions integration tests but doesn't
  provide guidance.

---

## Priority

### High (fix immediately -- layer boundary violations and structural issues)

| Issue | File(s) | Impact |
|-------|---------|--------|
| Remove all `pipelines` imports and domain references | creating-artifact-types, writing-creator-operations, writing-curator-operations | Layer boundary violation; confuses users about what's artisan vs. domain |
| Remove inline TODO | creating-artifact-types | Published TODO is unprofessional |
| Remove "What it replaces" migration table | creating-artifact-types | Irrelevant to users; internal migration context |
| Fix potentially broken `pipeline-construction.md` link | writing-curator-operations | Broken cross-reference |

### Medium (improve quality and usability)

| Issue | File(s) | Impact |
|-------|---------|--------|
| Add Prerequisites sections | All 6 guides | Users lack context on what they need before starting |
| Add verification/result steps | All 6 guides | Users can't confirm they did it right |
| Refocus curator guide on writing, not using | writing-curator-operations | Guide doesn't match its title |
| Remove "Key files" source paths from headers | configuring-execution, inspecting-provenance, writing-curator-operations | Wrong convention for how-to guides |
| Shorten creating-artifact-types from ~700 to ~350 lines | creating-artifact-types | Too long; intimidating for a how-to |
| Add example output for inspect helpers | inspecting-provenance | Readers can't visualize what they'll see |
| Write "How to Build a Pipeline" guide | New file | Most fundamental task is undocumented |
| Write "How to Filter Artifacts" guide | New file | Common task extracted from curator guide |

### Low (polish and completeness)

| Issue | File(s) | Impact |
|-------|---------|--------|
| Standardize code example naming (no domain terms) | All guides | Consistency |
| Adopt a consistent guide template | All guides | Uniformity across the section |
| Explain "predicate pushdown" briefly | exporting-results | Minor jargon issue |
| Add lifecycle diagram to creator guide | writing-creator-operations | Visual learners benefit |
| Write "How to Debug a Failed Step" guide | New file | Useful but not blocking |
| Write "How to Use Caching" guide | New file | Useful but not blocking |
| Write "How to Merge Artifact Streams" guide | New file | Useful but not blocking |
| Write "How to Ingest External Data" guide | New file | Useful but not blocking |
| Write "How to Write Integration Tests" guide | New file | Useful but not blocking |
