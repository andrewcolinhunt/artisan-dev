# Design Doc: Artisan Documentation Overhaul — Coordination Plan

## Goal

Make the `docs/artisan/` documentation clean, clear, domain-agnostic, and
accessible. Artisan is a standalone pipeline framework. Its docs must stand on
their own without referencing the `pipelines` domain package, specific domain
operations (AlphaFold, MPNN, RFDiffusion), or domain artifacts
(StructureArtifact, StructureAnnotation). Domain extensions may be mentioned
generically as "plugins" or "domain layers."

This document coordinates work across the four section-specific design docs:

- `design_docs_concepts.md`
- `design_docs_howto.md`
- `design_docs_reference.md`
- `design_docs_tutorials.md`

---

## Principles

1. **One pass per concern** — group related changes across sections into single
   work phases so we don't revisit the same files repeatedly.
2. **Dependency order** — some changes produce content that others consume
   (e.g., a glossary must exist before pages can link to it).
3. **Parallel where possible** — changes within a section are independent across
   sections, so section-level work can run in parallel.
4. **Ship incrementally** — each phase produces a committable, self-consistent
   improvement. No phase depends on a future phase being complete.
5. **Respect Diataxis boundaries** — every edit must land in the correct
   quadrant. Never add explanatory theory to a tutorial, teaching narrative to a
   how-to, procedural instructions to a concept page, or design rationale to a
   reference page. When in doubt, link across quadrants instead of inlining.

---

## Diataxis Boundary Rules

All documentation work must respect these quadrant boundaries. This table is
the arbiter when deciding where content belongs.

| Quadrant | Purpose | Contains | Does NOT contain |
|----------|---------|----------|-----------------|
| **Tutorials** | Learning-oriented | Step-by-step doing, minimal context, "follow me" narrative, links to concepts for the "why" | Theory, design rationale, exhaustive option lists, reference tables |
| **How-to Guides** | Task-oriented | Focused recipes: prerequisites, numbered steps, verification, links to concepts for background and reference for API details | Teaching narrative, conceptual explanations, lifecycle diagrams, lengthy background |
| **Concepts** | Understanding-oriented | Why things exist, design trade-offs, mental models, motivation, architectural context | Procedures, exact API signatures, step-by-step instructions, "how to do X" |
| **Reference** | Information-oriented | Exact signatures, field tables, types, error messages, algorithms, glossary | Rationale, tutorials, task workflows, "why" explanations |

### Cross-quadrant linking pattern

Each page should link to its counterparts in other quadrants:

- **Tutorials** → concepts ("to understand why, see…") and how-to ("for more on
  this technique, see…")
- **How-to guides** → concepts (for background) and reference (for API details)
- **Concepts** → tutorials (for hands-on practice) and reference (for specifics)
- **Reference** → how-to (for usage patterns) and concepts (for context)

This linking replaces content duplication across quadrants. If you find yourself
wanting to inline an explanation in a tutorial or a procedure in a concept page,
write a link instead.

---

## Phase 0: Structural Prerequisites

Work that unblocks everything else. Do this first.

### 0A. Move `configure_logging` to artisan

**Problem**: 7 of 11 tutorial notebooks import
`from pipelines.utils import configure_logging`. This is the single most
widespread pipelines leakage.

**Action**: Determine whether `configure_logging` already exists in
`artisan.utils` or can be trivially moved/aliased. If it can, move it. If not,
replace tutorial imports with `logging.basicConfig()`. This unblocks all
tutorial cleanup.

**Scope**: `src/artisan/utils/`, `src/pipelines/utils/`, 7 notebooks.

### 0B. Decide: merge or sharpen design-goals vs design-principles

**Problem**: These two concept pages have ~60% content overlap. Every downstream
edit (removing leakage, reducing redundancy) is wasted effort if we later
restructure them.

**Decision needed**: Option A (merge into one "Design Rationale" page) or
Option B (sharpen: goals = why/vision/comparisons, principles =
decision/rationale/implications). Recommended: **Option B**.

**Action**: Make the structural decision and execute the split/merge before
touching either page for other fixes.

**Scope**: `concepts/design-goals.md`, `concepts/design-principles.md`.

### 0C. Merge reference/operations-model.md into operation-definition.md

**Problem**: Near-duplicate reference pages. Consolidating first avoids doing
pipelines-leakage and jargon fixes twice.

**Action**: Merge all unique content from `operations-model.md` into
`operation-definition.md`. Delete `operations-model.md`. Update all
cross-references.

**Scope**: `reference/operations-model.md`, `reference/operation-definition.md`,
all files that link to either.

### 0D. Fix reference/index.md

**Problem**: `operation-definition.md` and `pipeline-manager.md` are not listed,
making them undiscoverable.

**Action**: Add both pages. Reorganize sections if needed.

**Scope**: `reference/index.md`.

### 0E. Establish Diataxis boundary rules

**Problem**: Without explicit rules for what content belongs in which quadrant,
every contributor (human or AI) makes ad-hoc decisions that drift over time.
Several existing pages violate Diataxis boundaries (concepts contain procedures,
how-to guides contain teaching, tutorials contain theory).

**Action**: Add a `docs/artisan/CONTRIBUTING-DOCS.md` (or equivalent) containing
the Diataxis boundary table and cross-linking pattern from the "Diataxis
Boundary Rules" section above. Every agent working on docs must read this before
editing.

**Scope**: New file. Referenced by all phases.

---

## Phase 1: Domain Decontamination

Systematic removal of all `pipelines` references across all four sections.
This is the highest-impact change and can be done as a single focused sweep.

### Rules

| Pattern | Replacement |
|---------|-------------|
| `from pipelines.…` imports | `from artisan.…` or remove |
| `StructureArtifact` | `DataArtifact`, `MetricArtifact`, or generic "a domain plugin might register `IMAGE`" |
| `StructureAnnotation` | Remove or generic "annotation artifacts" |
| `IngestStructure` | `IngestFiles` or `IngestData` |
| AlphaFold3, RFDiffusion, MPNN, AF3, RFD3 | "domain operation", "Op A", "Plugin" |
| `.pdb`, `.cif` file extensions | `.csv`, `.dat`, `.json` |
| `rmsd`, `energy` (domain metrics) | `accuracy`, `score`, `processing_time` |
| "protein design", "molecular" | "computational research workflows" |
| `PIPELINES_PREFECT_SERVER` env var | Note as known naming issue (can't rename without breaking) |
| "Pipelines framework" | "Artisan framework" |

### Per-section scope

| Section | Files affected | Severity |
|---------|---------------|----------|
| **Concepts** | 8 of 9 content pages. Worst: `design-principles.md` §7 (124 lines of pure domain content — **delete entire section**, move to `docs/pipelines/`) | High |
| **How-to** | 3 of 6 guides: `creating-artifact-types`, `writing-creator-operations`, `writing-curator-operations` | High |
| **Reference** | 5 of 8 pages: `api-quickstart`, `operation-definition`, `provenance-schemas`, `storage-schemas`, `pipeline-manager` | Medium |
| **Tutorials** | 7 of 11 notebooks (configure_logging), plus `05-advanced-patterns` (RFD3/AF3 mention), `02-filter-diagnostics` (domain Next Steps link) | High (but mostly handled by Phase 0A) |

### Special cases

- **`design-principles.md` Section 7** ("Semantic Labels and Structure
  Identity"): 124 lines of purely domain-specific content. **Delete from
  artisan docs entirely.** Move to `docs/pipelines/concepts/semantic-labels.md`.
- **`design-principles.md` Section 8** ("Documentation Structure"): Meta-docs
  about doc conventions. **Move to** `docs/contributing/` or delete.
- **`creating-artifact-types.md`**: Contains inline TODO and "What it replaces"
  migration table. Delete both.
- **`writing-curator-operations.md`**: `IngestStructure` section is entirely
  domain. Replace with generic `IngestFiles` subclassing example.

---

## Phase 2: Content Deduplication

Establish one authoritative page per topic. Replace duplicate explanations with
1-2 sentence summaries + cross-references.

### Topic ownership

| Topic | Authoritative page | Pages that currently duplicate it |
|-------|--------------------|----------------------------------|
| Artifact types, content addressing, draft/finalize | `concepts/artifacts-and-content-addressing.md` | design-goals, design-principles, storage-and-delta-lake |
| Two operation types, three-phase lifecycle, specs | `concepts/operations-model.md` | design-goals, design-principles, architecture-overview, execution-flow |
| Dual provenance, lineage, edges | `concepts/provenance-system.md` | design-goals, design-principles |
| Delta Lake, tables, staging, commit | `concepts/storage-and-delta-lake.md` | design-principles, execution-flow |
| Dispatch-execute-commit flow, caching | `concepts/execution-flow.md` | design-principles, architecture-overview |
| `infer_lineage_from` config | `reference/operation-definition.md` | ~~reference/operations-model.md~~ (deleted in 0C), reference/provenance-schemas.md |
| Built-in artifact types table | `reference/api-quickstart.md` | reference/operation-definition.md, reference/provenance-schemas.md |

### Action pattern

For each topic:
1. Ensure the authoritative page has the complete, canonical explanation.
2. In other pages, replace the duplicated content with a brief summary (1-2
   sentences) and a cross-reference: `See [Topic](../path) for details.`
3. Keep only what's unique to each page's perspective (e.g., execution-flow can
   mention the three phases in context without re-explaining them).

---

## Phase 3: Jargon and Clarity Pass

Add definitions, parentheticals, and links for recurring technical terms.
Can be done per-section in parallel.

### Glossary approach

A glossary is reference material (information-oriented, structured by the thing
being described). It belongs in the reference quadrant, not concepts.

Create `docs/artisan/reference/glossary.md` with short definitions for:

| Term | Definition |
|------|-----------|
| Artifact | A versioned, content-addressed data object produced or consumed by a pipeline step |
| Content addressing | Generating IDs from content hashes so identical data always has the same ID |
| Delta Lake | A versioned columnar storage format built on Parquet files |
| Parquet | A columnar file format optimized for analytical queries |
| Provenance | The record of where each artifact came from and what produced it |
| Execution unit | A batch of artifacts processed together in a single worker invocation |
| NFS | Network File System — shared storage accessible from multiple machines |
| ACID | Atomicity, Consistency, Isolation, Durability — properties of reliable database transactions |
| Hydration | Loading artifact content from storage (vs. loading only metadata/IDs) |
| Stem matching | Matching artifacts by comparing filename stems after stripping extensions |

### Per-page action

On each page, on first use of a glossary term, add either:
- A parenthetical: `"Delta Lake (a versioned columnar storage format)"`
- A link: `"[Delta Lake](../reference/glossary.md#delta-lake)"`

### Section-specific jargon fixes

**Concepts**: SIMD metaphor, bipartite, denormalized, optimistic concurrency,
partition pruning, close-to-open consistency, fsync, W3C PROV, orphan artifact

**How-to**: content-addressed, execution unit, staging_root, predicate pushdown,
ID-only mode, passthrough semantics

**Reference**: stem-match, co-input edges, orphan output, shard path, compact,
ArgStyle, facade→namespace

**Tutorials**: Delta Lake, provenance, artifacts, execution units, group_by,
passthrough semantics, variants

---

## Phase 4: Structural Improvements

Per-section quality improvements that are independent of each other.

### 4A. Concepts

Concepts pages explain *why* things exist and *what problem they solve*. They
must not contain procedures ("to do X, call Y") or exact API signatures (that's
reference). When a concepts page needs to reference an API, link to the
reference page.

- Add "Why This Exists" motivation paragraph to each page — frame as the
  problem being solved and the design trade-off, NOT as user instructions.
  Good: "The provenance system exists so that any artifact can be traced back
  to its origin." Bad: "To trace an artifact, call `get_ancestors()`."
- Add multi-input pairing diagram to `operations-model.md` (conceptual diagram
  showing the mental model, not API signatures)
- Shorten `design-principles.md` (after §7 and §8 removal) by cross-referencing
  detailed pages
- Move exact validation rules and error messages from `concepts/operations-model.md`
  to `reference/operation-definition.md` — keep only a high-level summary in
  concepts
- Update `index.md` grouping if needed
- Minimize internal file path references (use "Key files" callouts sparingly)
- Add cross-quadrant links: each concepts page should link to relevant tutorials
  ("see it in practice") and reference pages ("exact API details")

### 4B. How-to Guides

How-to guides are *recipes*. They assume the reader already understands the
concepts and just needs step-by-step directions to accomplish a specific task.
They must not teach theory or include diagrams that explain the model — link to
concepts for that. They must not list exact field tables — link to reference.

- Add Prerequisites section to all 6 guides (with links to relevant concepts
  pages for background knowledge)
- Add Verification/Result step to all 6 guides
- Remove "Key files" source paths from headers (3 guides) — this is reference
  convention, not how-to
- Restructure `creating-artifact-types.md` as a focused recipe (~300-350 lines):
  numbered steps, minimal explanation, link to concepts for the "why" and
  reference for exact field schemas. Currently reads as a tutorial (700 lines
  of walkthrough with teaching narrative) — strip the teaching, keep the recipe.
- Refocus `writing-curator-operations.md` on writing custom curators (extract
  built-in usage to separate how-to guides: "How to Filter Artifacts", "How to
  Merge Streams")
- In `writing-creator-operations.md`, replace inline lifecycle explanation with
  a link to `concepts/operations-model.md` where the lifecycle diagram lives.
  The how-to should say: "The three lifecycle methods are called in order (see
  [Operations Model](../concepts/operations-model.md)). Here's how to implement
  each one:" — then show code.
- Adopt consistent guide template across all guides:
  ```
  # How to [verb] [noun]
  Brief goal statement (one sentence).

  **Prerequisites**: [links to concepts pages]

  ## Steps
  ### 1. [verb]...
  ### 2. [verb]...

  ## Verify
  How to confirm it worked.

  ## Troubleshooting
  Common issues table.

  ## See Also
  Links to concepts (background), reference (API details), tutorials (practice).
  ```
- Fix broken `pipeline-construction.md` cross-reference

### 4C. Reference

Reference pages document *what exists*: exact signatures, field tables, types,
error messages, algorithms. They must not explain *why* (that's concepts) or
*how to use* in a workflow (that's how-to). Explanatory prose currently in
reference pages should be moved to concepts or deleted.

- Document missing types: `StagingResult`, `RuntimeEnvironment`,
  `FailurePolicy`, `ArgStyle`
- Add minimal code snippets (1-2 per page) showing import/instantiation — these
  are *usage examples*, not tutorials. Keep to 3-5 lines each.
- Remove explanatory "why" prose from reference pages (e.g., "why three phases"
  in `operations-model.md`, caching rationale in `pipeline-manager.md`) — link
  to concepts instead
- Add consistent "See Also" footer to all pages with links to concepts
  (background), how-to (usage patterns), and tutorials (practice)
- Slim `api-quickstart.md` to true cheat sheet: one-line summary per type +
  link to the canonical reference page. Remove duplicated field tables.
- Document `ArtifactStore` constructor fully (parameters, defaults, errors)
- Clarify `StepFuture.result()` error behavior (raises? returns failed result?)
- Move the glossary here: `reference/glossary.md`

### 4D. Tutorials

Tutorials teach by *doing*. The reader follows along, builds something, and
learns through the experience. Tutorials must NOT stop to explain theory — when
a concept needs background, provide a brief contextual sentence and link to the
concepts page. The goal is forward momentum: every cell should move the reader
closer to a working result.

- Add standard header/footer to all Pipeline Patterns notebooks (objectives,
  prereqs, time, summary, next steps)
- Add graph interpretation text to Pipeline Patterns — but keep it observational,
  not theoretical. Good: "Notice that Merge produces no new artifact nodes — it
  just routes existing artifacts into a single stream." Bad: "Merge uses
  passthrough semantics, which means..." (that's a concept — link to it).
  2-4 sentences per pattern, ~20 additions total.
- In `04-multi-input-operations`, replace the proposed "Understanding group_by"
  explanatory section with a contextual sentence + link: "We'll use
  `group_by=LINEAGE` to pair each config with the artifacts it produced. For
  the full mental model, see [Operations Model: Multi-Input Pairing](
  ../concepts/operations-model.md#multi-input-pairing)." The theory belongs in
  concepts, not in a tutorial.
- Same pattern for Delta Lake, provenance, and other terms that appear in
  tutorials: one contextual sentence + link to concepts, NOT an inline
  explanation. Good: "Artisan persists all pipeline data in
  [Delta Lake](../reference/glossary.md#delta-lake) tables." Bad: "Delta Lake
  is a versioned columnar storage format built on Parquet files that
  provides..."
- Fix malformed markdown cell in `01-first-pipeline`
- Unwrap pipeline code from function in `01-first-pipeline`
- Standardize helper pattern (`tutorial_setup()` everywhere)
- Add bridge text between Getting Started and Pipeline Patterns
- Acknowledge FilterDiagnostics overlap between sections
- Update tutorials/index.md with time estimates and suggested reading order

### 4E. Cross-Quadrant Linking Pass

After the per-section work in 4A-4D, do a final pass to ensure every page links
to its counterparts in other quadrants. This is what makes Diataxis work — the
four quadrants form a web, not silos.

For each page, add a "See Also" or equivalent section at the bottom:

| Page quadrant | Should link to |
|---------------|---------------|
| Each concepts page | 1-2 relevant tutorials, the corresponding reference page |
| Each how-to guide | The prerequisite concepts page(s), the relevant reference page(s) |
| Each reference page | The corresponding concepts page, relevant how-to guides |
| Each tutorial | The concepts page for deeper understanding, relevant how-to guides for specific tasks |

This pass also catches orphan pages (no inbound links) and dead ends (no
outbound links).

---

## Phase 5: New Content

Content that doesn't exist yet. Lower priority than fixing existing content.
Each item is independent.

### Missing How-to Guides (ordered by priority)

1. **How to Build a Pipeline** — the most fundamental task, currently a broken
   link reference. Covers `PipelineManager.create()`, adding steps, `run()`,
   `finalize()`.
2. **How to Filter Artifacts** — extract from curator guide into standalone
   guide using the built-in `Filter` operation.
3. **How to Debug a Failed Step** — inspect logs, sandbox, `preserve_working`,
   error metadata, resume.
4. **How to Use Caching** — cache hits, invalidation, forced re-runs.
5. **How to Merge Artifact Streams** — standalone guide for `Merge`.
6. **How to Ingest External Data** — `IngestFiles` / `IngestData` usage.

### Missing Tutorials (ordered by priority)

1. **Writing Your First Operation** — the single biggest gap. Create a custom
   `OperationDefinition` end-to-end. Could be Getting Started 05 or a new
   section.
2. **Exporting Pipeline Data** — get data out of Delta Lake to CSV/Parquet/
   files. Working with Results 03.
3. **When Things Go Wrong** — diagnose failures, partial runs, recovery.

### Missing Reference Pages (ordered by priority)

1. **ArtifactTypeDef Registry** — core extensibility with zero docs.
2. **Built-in Operations** — `Filter`, `Merge`, `IngestData`, etc. with
   parameter docs and examples.
3. **Visualization API** — `ProvenanceGraph`, `FilterDiagnostics`.
4. **Error Types** — all framework exceptions, when raised, how to handle.

---

## Execution Strategy

### Parallelism

Phases 0A-0E are prerequisites but are independent of each other — run in
parallel.

Phase 1 (decontamination) can start per-section as soon as the relevant Phase 0
work is done:
- Tutorials can start after 0A
- Concepts can start after 0B
- Reference can start after 0C + 0D
- How-to can start immediately (no Phase 0 dependency)
- All sections: 0E (boundary rules doc) should be done before Phase 4

Phases 2-4 are per-section and can run in parallel across sections. Phase 4
specifically requires 0E (the Diataxis boundary rules must exist before
structural improvements begin).

Phase 5 (new content) can start anytime — it's additive.

### Suggested Work Order

```
Sprint 1: Phase 0 (structural prereqs, including 0E boundary rules) + Phase 1 (decontamination)
Sprint 2: Phase 2 (dedup) + Phase 3 (jargon/glossary in reference/glossary.md)
Sprint 3: Phase 4 (structural improvements per section, guided by 0E rules) + Phase 4E (cross-linking)
Sprint 4: Phase 5 (new content, each piece filed in the correct quadrant from the start)
```

Each sprint produces shippable improvements. No sprint depends on a future
sprint.

### Per-Sprint Agent Allocation

For sprint work, one agent per section (concepts, howto, reference, tutorials)
can operate in parallel, each following the phase instructions for their section.
A fifth agent handles cross-cutting work (glossary, cross-references, index
pages).

---

## Success Criteria

### Domain independence
- [ ] Zero imports from `pipelines` in any `docs/artisan/` file
- [ ] Zero mentions of StructureArtifact, AlphaFold, MPNN, RFDiffusion, or
      domain-specific terms in artisan docs

### Diataxis compliance
- [ ] No explanatory theory in tutorials (only contextual sentences + links)
- [ ] No teaching narrative in how-to guides (only recipe steps + links)
- [ ] No procedures or API signatures in concepts pages (only mental models +
      links)
- [ ] No design rationale in reference pages (only specifications + links)
- [ ] Every page has cross-quadrant links ("See Also" section)
- [ ] `docs/artisan/CONTRIBUTING-DOCS.md` exists with boundary rules

### Content quality
- [ ] Each topic has exactly one authoritative page (no duplication)
- [ ] Every technical term is defined on first use or linked to the glossary
- [ ] `reference/glossary.md` exists with all recurring terms
- [ ] All how-to guides follow the standard template (prereqs, steps, verify)
- [ ] All tutorials have learning objectives, prerequisites, summaries

### Structural
- [ ] All reference pages are listed in `reference/index.md`
- [ ] `reference/operations-model.md` is deleted (merged into
      `operation-definition.md`)
- [ ] `design-principles.md` §7 and §8 are removed from artisan docs
- [ ] Docs build passes: `pixi run -e docs docs-build`

---

## Related Design Docs

| Doc | Scope |
|-----|-------|
| `design_docs_concepts.md` | File-by-file analysis of concepts/ |
| `design_docs_howto.md` | File-by-file analysis of how-to-guides/ |
| `design_docs_reference.md` | File-by-file analysis of reference/ |
| `design_docs_tutorials.md` | File-by-file analysis of tutorials/ |
