# Plan: Artisan Documentation Overhaul

## Context

The `docs/artisan/` documentation has pervasive `pipelines` domain leakage (imports, StructureArtifact references, domain-specific examples), content duplication across pages, undefined jargon, missing structural elements (prerequisites, glossary, cross-links), and several missing guides/tutorials. A comprehensive set of design docs at `_dev/design/0_current/docs/` details every issue file-by-file across 4 Diataxis sections.

This plan executes the **full overhaul** in a single effort by combining all phases (decontamination, deduplication, jargon, structural improvements) into **single per-file passes** organized by section, rather than making 5 separate passes over the same files.

---

## Batch 1: Cross-cutting prerequisites (independent, can run in parallel)

### 1A: Create `docs/artisan/CONTRIBUTING-DOCS.md` (NEW)
- Diataxis boundary rules table (what belongs in each quadrant)
- Cross-quadrant linking pattern
- Domain decontamination replacement table
- How-to guide template + tutorial standard structure

### 1B: Merge `reference/operations-model.md` into `reference/operation-definition.md`
- Migrate unique content: MaterializationBehavior subsection, LineageMapping model, PassthroughResult model
- Delete `reference/operations-model.md`
- Update cross-references in `concepts/operations-model.md` and `reference/provenance-schemas.md`

### 1C: Fix `reference/index.md`
- Add entries for `operation-definition.md` and `pipeline-manager.md`
- Restructure into 5 sections: API, Operations, Orchestration, Provenance & Execution, Storage

### 1D: Extract `design-principles.md` Sections 7 & 8
- Move Section 7 ("Semantic Labels", ~124 lines) to `docs/pipelines/concepts/semantic-labels.md` (NEW)
- Fold Section 8 ("Documentation Structure", ~36 lines) content into CONTRIBUTING-DOCS.md
- Update summary invariants list

### 1E: Fix `configure_logging` imports in 7 notebooks
- Replace `from pipelines.utils import configure_logging` with `from artisan.utils import configure_logging`
- Files: `getting-started/04-slurm-execution.ipynb`, `pipeline-patterns/01-05.ipynb`, `working-with-results/02-filter-diagnostics.ipynb`

### 1F: Create `reference/glossary.md` (NEW)
- 10 terms with anchor headings: Artifact, Content addressing, Delta Lake, Parquet, Provenance, Execution unit, NFS, ACID, Hydration, Stem matching

**Files created**: 3 | **Files modified**: 12 | **Files deleted**: 1

---

## Batch 2: Section-level full passes (parallel across sections)

Each file gets ONE comprehensive pass applying all remaining changes: domain decontamination, deduplication, jargon fixes, structural improvements.

### 2A: Concepts (10 files)

| File | Key changes |
|------|------------|
| `index.md` | Add orientation sentence |
| `architecture-overview.md` | Remove/minimize Coding Conventions section; add bipartite definition; reduce data flow overlap with execution-flow |
| `operations-model.md` | Replace `StructureArtifact.draft()` → `DataArtifact.draft()`; add multi-input pairing diagram; move exact validation rules to reference; add "Why This Exists" |
| `artifacts-and-content-addressing.md` | Replace pipelines domain extension examples with generic (IMAGE, DOCUMENT); remove cross-ref to deleted §7; add glossary links; add "Why This Exists" |
| `provenance-system.md` | Replace `.pdb`/docking/ligand examples with generic computational examples; add "Why This Exists"; add "What You Can Do" paragraph |
| `execution-flow.md` | Add NFS/Parquet/sharding/fsync parentheticals; shorten "Why This Architecture" section; add "Why This Exists" |
| `storage-and-delta-lake.md` | Replace `pipelines` domain layer references; add ACID/partition-pruning definitions; add "Why This Exists" |
| `error-handling.md` | "Pipelines framework" → "Artisan framework"; `.pdb` → `.csv`; add "Why This Exists" |
| `design-goals.md` | Replace "protein design"/AF3/MPNN with generic terms; sharpen scope to goals/vision/comparisons ONLY; replace re-explanations with cross-refs to authoritative pages |
| `design-principles.md` | Replace AlphaFold3/MPNN/RFDiffusion/IngestStructure with generic terms; reduce overlap by cross-referencing detailed pages; ensure Decision/Rationale/Implications format |

### 2B: How-to Guides (7 files)

All guides get: Prerequisites section, Verification step, consistent template, "Key files" source paths removed from headers.

| File | Key changes |
|------|------------|
| `index.md` | Add format explanation sentence |
| `configuring-execution.md` | Add Prerequisites/Verify; remove "Key files" source paths |
| `creating-artifact-types.md` | **Major restructure** 700→~350 lines: fix `pipelines.schemas` imports; remove StructureArtifact refs, inline TODO, migration table; strip teaching narrative; add Prerequisites/Verify |
| `exporting-results.md` | Soften pipelines cross-reference; add Prerequisites/Verify; explain "predicate pushdown" |
| `inspecting-provenance.md` | Fix "structures" in graph legend; add Prerequisites/Verify; promote Inspect Helpers |
| `writing-creator-operations.md` | Replace StructureArtifact→DataArtifact, .pdb/.cif→.csv, rmsd/energy→accuracy/score; replace inline lifecycle explanation with link to concepts; add Prerequisites/Verify |
| `writing-curator-operations.md` | Replace StructureArtifact→DataArtifact; replace IngestStructure section with generic IngestFiles example; fix broken `pipeline-construction.md` cross-ref; add Prerequisites/Verify |

### 2C: Reference (6 remaining files)

All pages get: domain decontamination, minimal code snippets, explanatory prose moved to concepts, consistent "See Also" footer.

| File | Key changes |
|------|------------|
| `api-quickstart.md` | Remove StructureArtifact/StructureAnnotation refs; rename "Facade"→"Extensible Namespace"; slim duplicated field tables to summary+link; add code snippets |
| `operation-definition.md` | Remove `"structure" from pipelines` ref; define stem-match/orphan-output; document ArgStyle enum; add skeleton operation example; add See Also |
| `pipeline-manager.md` | Document FailurePolicy; clarify StepFuture.result() error behavior; slim caching section; add Prefect context note; add See Also |
| `provenance-schemas.md` | Remove STRUCTURE/pipelines refs; define denormalized/co-input/group_id; consolidate infer_lineage_from to link; add See Also |
| `execution-internals.md` | Document StagingResult + RuntimeEnvironment fields; expand NFS/shard jargon; add See Also |
| `storage-schemas.md` | Replace pipelines domain example; document ArtifactStore constructor; add content-addressed dedup definition; add See Also |

### 2D: Tutorials (12 files)

All Pipeline Patterns notebooks get: standard header (objectives, prereqs, time), graph interpretation text (2-4 sentences per pattern), summary/next-steps footer.

| File | Key changes |
|------|------------|
| `tutorials/index.md` | Add intro paragraph, time estimates, reading order suggestion |
| `getting-started/01-first-pipeline.ipynb` | Fix malformed markdown cell; unwrap pipeline code from function; add "What you'll learn" + "What just happened?" narrative; add glossary links |
| `getting-started/02-exploring-results.ipynb` | Add richer narrative between cells; add glossary links |
| `getting-started/03-run-vs-submit.ipynb` | Minor: add glossary links, interpretation for timing plot |
| `getting-started/04-slurm-execution.ipynb` | Add "What you'll learn"; expand HPC; define execution unit |
| `pipeline-patterns/01-sources-and-chains.ipynb` | Add header/footer; add graph interpretation text |
| `pipeline-patterns/02-branching-and-merging.ipynb` | Add header/footer; add graph interpretation text; explain passthrough semantics |
| `pipeline-patterns/03-metrics-and-filtering.ipynb` | Add header/footer; add filter diagnostics interpretation |
| `pipeline-patterns/04-multi-input-operations.ipynb` | Replace explanatory group_by section with contextual sentence + link to concepts |
| `pipeline-patterns/05-advanced-patterns.ipynb` | Remove RFD3/AF3 domain references; add header/footer |
| `working-with-results/01-provenance-graphs.ipynb` | Add "What you'll learn"; define provenance in opening |
| `working-with-results/02-filter-diagnostics.ipynb` | Fix domain-specific Next Steps link; add interpretation guidance |

---

## Batch 3: Cross-quadrant linking pass

After all per-file passes, sweep all ~38 files to ensure:
- Every concepts page → 1-2 tutorials + relevant reference page
- Every how-to guide → prerequisite concepts + relevant reference
- Every reference page → corresponding concepts + relevant how-to
- Every tutorial → concepts for understanding + how-to for techniques
- No orphan pages or dead ends

---

## Batch 4: New content (Priority 1 only)

| Item | File | Type |
|------|------|------|
| How to Build a Pipeline | `how-to-guides/building-a-pipeline.md` (NEW) | How-to guide (~150 lines) |
| Writing Your First Operation | `getting-started/05-writing-an-operation.ipynb` (NEW) | Tutorial notebook |
| Built-in Operations Reference | `reference/built-in-operations.md` (NEW) | Reference page |

Priority 2-3 items deferred to follow-up work.

---

## Batch 5: myst.yml + validation

- Update `docs/myst.yml`:
  - Remove `artisan/reference/operations-model.md`
  - Add `artisan/reference/glossary.md`
  - Add `pipelines/concepts/semantic-labels.md`
  - Add new Batch 4 files
- Run: `~/.pixi/bin/pixi run -e docs docs-build`
- Fix any build errors

---

## Verification

1. `~/.pixi/bin/pixi run -e docs docs-build` passes
2. Zero `from pipelines` imports in any `docs/artisan/` file
3. Zero mentions of StructureArtifact, AlphaFold, MPNN, RFDiffusion in artisan docs
4. Every page has cross-quadrant links (See Also section)
5. `reference/glossary.md` and `CONTRIBUTING-DOCS.md` exist
6. `reference/index.md` lists all reference pages
7. `reference/operations-model.md` is deleted
