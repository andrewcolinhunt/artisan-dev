# Design Doc: Artisan Reference Documentation Improvements

## Overview

The `docs/artisan/reference/` directory contains eight files: an index page, an
API quickstart, and six topic-specific reference pages (execution internals,
operation definition, operations model, pipeline manager, provenance schemas,
storage schemas). Overall, the reference documentation is strong on
field-table completeness and source-file traceability. However, there are
several systemic issues:

1. **Pipelines leakage** -- multiple files reference the `pipelines` domain
   package, `StructureArtifact`, or domain-specific operations by name,
   violating the principle that Artisan is domain-agnostic.
2. **Significant overlap between concepts/ and reference/** -- the
   `operations-model.md` file exists in both directories with heavily
   duplicated content. The boundary between "explanation" and "specification" is
   not consistently maintained.
3. **Missing reference pages** -- several major API surfaces (visualization,
   ArtifactTypeDef registry, GroupByStrategy pairing, utilities, Prefect
   integration) have no reference documentation.
4. **No runnable examples** -- reference pages document signatures and field
   tables thoroughly but lack executable code snippets showing common usage
   patterns.
5. **Index page is incomplete** -- it lists only four of the six topic pages,
   omitting `operation-definition.md` and `pipeline-manager.md`.

---

## File-by-File Analysis

### index.md

- **Current state**: Short landing page with four links organized into three
  sections (API, Operations, Provenance and Execution, Storage).
- **Pipelines leakage**: None.
- **Jargon issues**: None -- appropriately brief.
- **Content issues**:
  - Missing two reference pages: `operation-definition.md` and
    `pipeline-manager.md` are not listed. This means users navigating via the
    index will never discover these pages.
  - No introductory guidance on how to use the reference section (e.g., "These
    pages document API signatures and schemas. For conceptual explanations, see
    Concepts.").
- **Overlap with concepts/**: None.
- **Recommended changes**:
  1. Add entries for `operation-definition.md` and `pipeline-manager.md`.
  2. Restructure into five sections: API, Operations, Orchestration, Provenance
     and Execution, Storage -- to accommodate the pipeline manager.
  3. Add a one-sentence orientation paragraph differentiating reference from
     concepts and how-to guides.
  4. As new reference pages are added (see "Missing Reference Pages" below),
     add them to the index.

---

### api-quickstart.md

- **Current state**: A broad overview page covering module layout, enums,
  artifact models, specification models, provenance models, execution entry
  points, storage, helper functions, example operations, and curator operations.
  Functions as a compact "cheat sheet."
- **Pipelines leakage**:
  - **Line 5-6**: "For domain-specific types (StructureArtifact,
    StructureAnnotation, domain operations), see [Pipelines API
    Reference](...)" -- explicitly names `StructureArtifact` and
    `StructureAnnotation`. Replace with: "For domain-specific types, see
    your domain layer's API reference."
  - **Line 316**: "`IngestStructure` (pipelines)" -- names a domain-specific
    operation. Replace with: "domain-specific ingest operations are provided
    by extension layers."
  - **Line 198-199** (ArtifactTypes): "Domain-registered types (e.g.,
    `"structure"` from `pipelines`) are added at import time via the
    `ArtifactTypeDef` registry." -- Replace with: "Domain layers can register
    additional types at import time via the `ArtifactTypeDef` registry." Remove
    the `"structure"` example or use a hypothetical like `"image"`.
- **Jargon issues**:
  - "Facade" (line 28) used for `ArtifactTypes` -- this is non-standard. The
    class is a namespace/registry pattern. Use "namespace" or "extensible
    registry."
  - "Draft/finalize pattern" (line 43) is introduced but not linked to any
    deeper explanation. Add a parenthetical or link to the concepts page on
    artifacts.
- **Content issues**:
  - The page tries to be both a "quick reference" and a comprehensive API
    listing. It duplicates significant content from `operation-definition.md`
    (InputSpec, OutputSpec, result models, ExecutionUnit, execution entry
    points). The quickstart should be a concise cheat sheet with links to
    dedicated pages, not a second copy of the field tables.
  - Example operations table (lines 296-303) and curator operations table
    (lines 308-316) are useful but could link to the actual source or a
    dedicated "built-in operations" reference page.
  - No code examples showing actual usage (e.g., "how to import and use
    `ArtifactStore`").
- **Overlap with concepts/**: Minimal -- the page is API-focused. But the
  "execution entry points" section partially overlaps with
  `execution-internals.md`.
- **Recommended changes**:
  1. Remove or genericize all three instances of pipelines leakage.
  2. Rename `ArtifactTypes` kind from "Facade" to "Extensible Namespace."
  3. Slim down the duplicated field tables (InputSpec, OutputSpec, result
     models, ExecutionUnit) to a summary line + link to the canonical reference
     page (`operation-definition.md` or `execution-internals.md`).
  4. Add 2-3 short code snippets showing common import patterns.
  5. Add a "See also" callout box linking to the dedicated reference pages.

---

### execution-internals.md

- **Current state**: Thorough internals reference covering ExecutionUnit,
  ExecutionContext, cache key computation, run ID generation, cache lookup,
  sandbox structure, staging structure, commit order, input resolution,
  two-level batching, dispatch backends, and a key files table. Well-organized
  with clear field tables and algorithm descriptions.
- **Pipelines leakage**: None. This is a clean artisan-only document.
- **Jargon issues**:
  - "NFS sync" (line 126) -- the term NFS is used without expansion. Not all
    readers will know this stands for Network File System or why it matters.
    Add a brief parenthetical: "(Network File System -- ensures durability on
    shared storage)."
  - "Shard path" (in the key files table and helper functions) -- the concept
    of sharding is not explained. Add a one-sentence note: "Shard paths
    distribute files across subdirectories using hash prefixes to avoid
    filesystem bottlenecks."
- **Content issues**:
  - The "Key Files" table (lines 199-219) lists 18 source files. This is
    useful for developers but could benefit from grouping or a brief
    description of each file's role (some entries have only a generic
    "purpose" like "Lineage inference and capture").
  - `StagingResult` is not documented anywhere but is the return type of both
    `run_creator_flow` and `run_curator_flow`. Its fields should be documented.
  - `RuntimeEnvironment` is a parameter to both flow functions but is not
    documented. Its fields and purpose should be listed.
  - The "Dispatch Backends" table mentions "SlurmTaskRunner" but does not link
    to any reference for it (this is the `prefect_submitit` package).
- **Overlap with concepts/**: The two-level batching explanation overlaps with
  `concepts/execution-flow.md`. The reference version is appropriately terse
  (ASCII diagram with parameters), but could explicitly link to the concepts
  page for the "why."
- **Recommended changes**:
  1. Document `StagingResult` fields (return type of the executor flows).
  2. Document `RuntimeEnvironment` fields (parameter to both flows).
  3. Expand NFS and shard path jargon with brief parentheticals.
  4. Add a link from the dispatch backends section to the Prefect/SLURM
     integration docs or a new reference page.
  5. Group the key files table by subsystem (execution, staging, lineage,
     storage, schemas).

---

### operation-definition.md

- **Current state**: The most complete reference page. Covers
  OperationDefinition class hierarchy, ClassVars, instance fields, role enum
  requirements, lifecycle methods, input models, result models, ExecutionConfig,
  ResourceConfig, CommandSpec hierarchy, and the operation registry. Field
  tables are comprehensive and well-structured.
- **Pipelines leakage**:
  - **Line 65 (postprocess description in concepts/operations-model.md that
    this page cross-references)**: The concepts page mentions
    `StructureArtifact.draft()` as an example. While this page itself does not
    contain the leak, the cross-reference leads to one. Not directly
    actionable here, but note for the concepts page cleanup.
  - **Line 198-199 (ArtifactTypes section)**: "Domain-registered types (e.g.,
    `"structure"` from `pipelines`) are added at import time via the
    `ArtifactTypeDef` registry." Same leak as in `api-quickstart.md`. Replace
    with generic language.
- **Jargon issues**:
  - "Stem-match" (lines 47, 48, 87) -- used repeatedly without definition.
    Add a brief note on first use: "(match by comparing filename stems after
    stripping extensions; see Provenance Schemas Reference for the full
    algorithm)."
  - "Orphan output" (line 49) -- unclear to newcomers. Define as "an output
    artifact with no parent provenance edges."
- **Content issues**:
  - The `CommandSpec` hierarchy section (lines 326-346) documents subclasses
    but does not explain when to use each one or provide a minimal example.
  - The `ArgStyle` enum is referenced but never defined. Document its values.
  - No example of a complete operation definition (even a minimal 10-line
    skeleton would be valuable for a reference page).
  - `PassthroughResult` is documented well but could benefit from a note on
    when to return it vs. `ArtifactResult` from `execute_curator`.
- **Overlap with concepts/**: The lifecycle methods section (lines 62-97) and
  validation rules section (lines 113-126) are nearly identical to content in
  `concepts/operations-model.md`. The reference page should focus on exact
  signatures, types, and error messages. The concepts page should explain the
  design rationale. Currently both do a mix of both.
- **Recommended changes**:
  1. Remove the `"structure"` from `pipelines` reference in the ArtifactTypes
     section.
  2. Define "stem-match" and "orphan output" on first use.
  3. Document the `ArgStyle` enum values.
  4. Add a minimal skeleton operation example (creator and curator).
  5. Add guidance on `CommandSpec` subclass selection.
  6. De-duplicate lifecycle method explanations -- keep exact signatures and
     error messages here, move "why" content to concepts.

---

### operations-model.md (reference)

- **Current state**: Field tables for InputSpec, OutputSpec,
  OperationDefinition ClassVars, ExecutionConfig, subclass validation rules,
  lifecycle methods summary table, result models (ArtifactResult,
  PassthroughResult, LineageMapping), and the operation registry. Significant
  overlap with `operation-definition.md`.
- **Pipelines leakage**: None directly. But it references
  `concepts/operations-model.md` which contains `StructureArtifact.draft()`.
- **Jargon issues**:
  - Same "stem-match" issue as `operation-definition.md`.
  - "Orphan output" undefined.
- **Content issues**:
  - **Major overlap with operation-definition.md**: InputSpec fields, OutputSpec
    fields, `infer_lineage_from` patterns, OperationDefinition ClassVars,
    ExecutionConfig fields, validation rules, lifecycle methods, result models,
    and the operation registry are all documented in both files. This is
    the single largest content issue in the reference section.
  - The two pages serve nearly identical purposes. `operations-model.md`
    appears to be the original, and `operation-definition.md` was added later
    with more detail. They should be merged.
- **Overlap with concepts/**: Subclass validation rules (lines 113-126) are
  listed identically in `concepts/operations-model.md` (lines 137-145). The
  `infer_lineage_from` patterns table appears in three places: this file,
  `operation-definition.md`, and `provenance-schemas.md`.
- **Recommended changes**:
  1. **Merge into `operation-definition.md`** -- consolidate all operations
     model reference content into the single `operation-definition.md` page.
     Delete `operations-model.md` from reference/ and update all cross-links.
  2. If keeping both, clearly differentiate: one for the spec system (InputSpec,
     OutputSpec, lineage config), the other for the base class API. But merging
     is strongly preferred.

---

### pipeline-manager.md

- **Current state**: Well-structured reference page covering
  `PipelineManager.create()`, `PipelineManager.resume()`,
  `PipelineManager.list_runs()`, `run()`/`submit()`, input patterns,
  `finalize()`, container protocol, `StepResult`, `StepFuture`,
  `OutputReference`, `PipelineConfig`, and caching. Good field tables and
  signature documentation.
- **Pipelines leakage**:
  - **Line 48**: "`PIPELINES_PREFECT_SERVER` env var" -- this environment
    variable name embeds the word "pipelines" (the domain package). If this is
    the actual env var name, it should be noted as a known naming issue or
    documented with context. If it can be renamed, flag for renaming.
- **Jargon issues**:
  - "Compact" parameter (line 134): described as "Run Delta Lake compaction
    after commit" -- readers may not know what compaction means. Add a brief
    parenthetical: "(merges small Parquet files for query performance)."
  - "Prefect server resolution order" (line 47) -- presumes familiarity with
    Prefect. Add a brief note: "Artisan uses Prefect for task orchestration.
    A running Prefect server is required for pipeline execution."
- **Content issues**:
  - The `submit()` signature (lines 113-116) says "Same parameters as run()"
    but does not list them. This is fine for brevity but should explicitly state
    "See `run()` above for full parameter documentation."
  - `FailurePolicy` is used but never fully defined. Document it as a literal
    type: `Literal["continue", "fail_fast"]` with behavior descriptions.
  - The caching section (lines 303-315) duplicates content from
    `execution-internals.md`. Keep a one-sentence summary here with a link.
  - No example pipeline script showing `create()` -> `run()` -> `finalize()`
    flow.
  - `StepFuture` does not document what happens when `result()` is called on a
    failed step (does it raise? return a failed `StepResult`?).
  - **Not listed in index.md** -- users cannot discover this page via the
    landing page.
- **Overlap with concepts/**: The caching section overlaps with execution flow
  concepts. Minimal concern if kept brief.
- **Recommended changes**:
  1. Add this page to `index.md`.
  2. Document `FailurePolicy` as a literal type with behavior descriptions.
  3. Clarify `StepFuture.result()` error behavior.
  4. Add a minimal end-to-end pipeline example (5-10 lines).
  5. Slim down the caching section to a summary + link.
  6. Add a note about the `PIPELINES_PREFECT_SERVER` env var naming.
  7. Add brief context about Prefect dependency.

---

### provenance-schemas.md

- **Current state**: Documents ExecutionRecord, ExecutionEdge,
  ArtifactProvenanceEdge, LineageMapping, `infer_lineage_from` configuration,
  filename stem matching algorithm, built-in artifact types, and storage tables.
  Strong on algorithm details (stem matching with digit boundary protection is
  well-explained).
- **Pipelines leakage**:
  - **Line 157-158**: "Additional types (e.g., `STRUCTURE`) are registered
    dynamically by the domain layer (`pipelines`) via `ArtifactTypeDef`."
    Replace with generic language: "Additional types can be registered by
    domain layers via `ArtifactTypeDef`."
- **Jargon issues**:
  - "Denormalized type" (lines 55-56) -- database term that may confuse
    non-database readers. Add parenthetical: "(stored redundantly on the edge
    for query convenience, avoiding joins to the artifact index)."
  - "Co-input edges" (line 59) -- not defined. Explain: "edges that link
    multiple parent artifacts which were jointly required to produce a single
    output."
  - "group_id" semantics (line 72) -- described as "deterministic hash linking
    jointly-necessary co-input edges" but does not explain how the hash is
    computed or what "jointly-necessary" means concretely.
- **Content issues**:
  - The `infer_lineage_from` configuration table (lines 76-95) is the third
    copy of this table (also in `operation-definition.md` and
    `operations-model.md`). Consolidate into one canonical location and link.
  - Built-in artifact types table (lines 146-161) duplicates what is in
    `api-quickstart.md` and `operation-definition.md`. Pick one canonical
    location.
  - Storage tables section (lines 166-176) duplicates `storage-schemas.md`.
    Remove from here or slim to a summary + link.
  - Missing: how `group_id` is computed (hash algorithm and inputs).
  - Missing: how provenance edges are queried (e.g., the ArtifactStore methods
    that read these tables).
- **Overlap with concepts/**: The stem matching algorithm section partially
  overlaps with `concepts/provenance-system.md`. However, the reference version
  is appropriately focused on the algorithm mechanics rather than design
  rationale, so this is acceptable overlap.
- **Recommended changes**:
  1. Remove the `pipelines` and `STRUCTURE` reference.
  2. Define "denormalized," "co-input edges," and expand `group_id` semantics.
  3. Consolidate `infer_lineage_from` into one canonical page (recommended:
     `operation-definition.md`) and link from here.
  4. Remove or slim the built-in artifact types and storage tables sections to
     avoid triple-duplication.
  5. Document `group_id` computation.

---

### storage-schemas.md

- **Current state**: The cleanest and most focused reference page. Documents
  all framework Delta Lake tables (executions, execution_edges, artifact_edges,
  index, steps), all built-in artifact tables (metrics, configs, data,
  file_refs), domain artifact table extensibility, ArtifactStore methods, and
  DeltaCommitter behavior.
- **Pipelines leakage**:
  - **Lines 171-174**: "Example from the `pipelines` domain layer:
    `artifacts/structures` -- registered by `StructureTypeDef`,
    `artifacts/structure_annotations` -- registered by
    `StructureAnnotationTypeDef`." Replace with generic language: "For example,
    a domain layer might register `artifacts/images` via a custom
    `ArtifactTypeDef`."
- **Jargon issues**:
  - "Content-addressed deduplication" (line 206) -- briefly define for
    newcomers: "(artifacts with identical content produce the same ID, so
    duplicates are automatically skipped during commit)."
- **Content issues**:
  - ArtifactStore method table (lines 182-192) is well-structured but lacks
    information on error behavior (what does `get_artifact` return when the
    artifact does not exist? Answer: `None`, but this is only clear from the
    return type, not from a description).
  - Missing: `ArtifactStore` constructor documentation (what arguments does it
    take beyond `base_path`?).
  - Missing: How to read/query Delta tables directly (e.g., using Polars
    or DeltaTable).
  - The framework tables section duplicates content from
    `provenance-schemas.md`. This is acceptable since storage-schemas.md is the
    canonical location for column schemas.
- **Overlap with concepts/**: Minimal. The concepts page on storage explains
  "why Delta Lake" while this page documents the actual schemas.
- **Recommended changes**:
  1. Replace the `pipelines` domain example with a generic hypothetical.
  2. Add error behavior notes to ArtifactStore methods.
  3. Document ArtifactStore constructor fully.
  4. Define "content-addressed deduplication" briefly.
  5. Optionally add a short section on direct Delta table access patterns.

---

## Cross-Cutting Issues

### 1. Triple-duplication of `infer_lineage_from`

The `infer_lineage_from` configuration table appears in three reference files:
`operation-definition.md`, `operations-model.md`, and
`provenance-schemas.md`. This creates a maintenance burden and risk of
divergence. **Recommendation**: designate `operation-definition.md` as the
canonical location (since `infer_lineage_from` is a field on `OutputSpec`).
Other pages should have a one-line summary with a cross-link.

### 2. Duplicate built-in artifact types

The built-in artifact types table appears in `api-quickstart.md`,
`operation-definition.md`, and `provenance-schemas.md`.
**Recommendation**: designate `api-quickstart.md` as the canonical location
(it is the "types and imports" cheat sheet) and link from others.

### 3. Operations model duplication

`operations-model.md` and `operation-definition.md` cover the same content
with slightly different framing. **Recommendation**: merge into
`operation-definition.md` and redirect/delete `operations-model.md`.

### 4. Pipelines leakage pattern

Five of the eight files contain references to the `pipelines` domain package.
The leaks follow two patterns:
- **Explicit domain type names**: `StructureArtifact`, `StructureAnnotation`,
  `IngestStructure`, `StructureTypeDef`, `StructureAnnotationTypeDef`
- **Explicit package references**: "the domain layer (`pipelines`)", "from
  `pipelines`"

All should be replaced with generic language like "domain layers," "extension
layers," or hypothetical examples like `"image"` artifacts.

### 5. No runnable code examples

None of the reference pages contain executable Python code snippets showing
common usage. Reference docs can be dry, but even a single 5-line example per
page dramatically improves usability. Key examples needed:
- Creating and using an `ArtifactStore`
- Defining a minimal creator operation
- Defining a minimal curator operation
- Running a 3-step pipeline
- Querying provenance edges

### 6. Inconsistent cross-referencing

Some pages link to their concepts/ counterparts and some do not.
`api-quickstart.md` has no cross-references section. `operations-model.md`
links to `concepts/operations-model.md` but not to
`operation-definition.md`. All reference pages should have a consistent
"Cross-References" section at the bottom.

---

## Concepts vs Reference Boundary

The intended boundary:

| Aspect | Concepts/ | Reference/ |
|--------|-----------|------------|
| Purpose | Explain *why* -- design rationale, mental models, tradeoffs | Document *what* -- exact signatures, field tables, error messages, algorithms |
| Audience | Users learning the system | Users looking up specifics while building |
| Tone | Narrative, explanatory | Terse, tabular, precise |
| Examples | Conceptual diagrams, workflows | API signatures, minimal code snippets |

**Current violations**:
- `concepts/operations-model.md` contains the subclass validation rules list
  (this is reference-grade content -- exact error messages and rule numbers).
  Move to reference, keep a high-level summary in concepts.
- `reference/operations-model.md` contains explanatory paragraphs about "why
  three phases" and "why this separation matters" (this is concepts-grade
  content). These belong in concepts only.
- `reference/execution-internals.md` is well-bounded -- it documents algorithms
  and structures without extensive rationale. Good model to follow.
- `reference/pipeline-manager.md` is mostly well-bounded but the caching
  section drifts into explanation.

**Recommended actions**:
1. In `concepts/operations-model.md`: remove exact validation rule numbers and
   error messages, link to reference for details.
2. In `reference/operations-model.md` (if kept): remove explanatory prose,
   keep only field tables and signatures.
3. Use `execution-internals.md` and `storage-schemas.md` as the template for
   reference page style.

---

## Missing Reference Pages

### 1. Visualization Reference
- **What it would cover**: `ProvenanceGraph` and `MacroProvenanceGraph` APIs
  (constructor, render methods, configuration options), `FilterDiagnostics`
  API, timing utilities. These are user-facing APIs with no reference docs.
- **Priority**: Medium

### 2. ArtifactTypeDef Registry Reference
- **What it would cover**: How to register custom artifact types, the
  `ArtifactTypeDef` class fields, the registration lifecycle, how registered
  types interact with `ArtifactTypes`, table creation behavior. Currently
  mentioned in passing but never documented.
- **Priority**: High (extensibility is a core framework feature)

### 3. Utilities Reference
- **What it would cover**: `compute_artifact_id()`, `shard_path()`,
  `strip_extensions()`, `find_executable()`, logging configuration. Currently
  scattered across `api-quickstart.md` and `execution-internals.md`.
- **Priority**: Low (these are internal helpers, but some are used by
  operation authors)

### 4. Error Types Reference
- **What it would cover**: All framework exception types
  (`PrefectServerNotFound`, `PrefectServerUnreachable`, `ValidationError`
  patterns), when they are raised, and how to handle them.
- **Priority**: Medium

### 5. Built-in Operations Reference
- **What it would cover**: Dedicated page for `Filter`, `Merge`, `IngestData`,
  `IngestPipelineStep`, `DataGenerator`, `DataTransformer`, etc. Currently
  summarized in tables in `api-quickstart.md` but without parameter docs,
  usage examples, or behavioral details.
- **Priority**: Medium-High (these are the operations users interact with
  most frequently)

---

## Priority

### High Impact
1. **Merge `operations-model.md` into `operation-definition.md`** -- eliminates
   the largest source of duplication and confusion. Single canonical page for
   all operations API reference.
2. **Fix all pipelines leakage** (5 files) -- quick wins that enforce the
   domain-agnostic principle. Estimated effort: 30 minutes.
3. **Add `operation-definition.md` and `pipeline-manager.md` to index.md** --
   two reference pages are currently undiscoverable from the landing page.
4. **Create ArtifactTypeDef Registry Reference page** -- extensibility is a
   core feature with zero reference documentation.

### Medium Impact
5. **Consolidate `infer_lineage_from` to one canonical location** -- reduces
   triple-duplication across three files.
6. **Document missing types** (`StagingResult`, `RuntimeEnvironment`,
   `FailurePolicy`, `ArgStyle`) -- these are referenced but never defined.
7. **Add minimal code examples** to each reference page (1-2 per page).
8. **Create Built-in Operations Reference page** -- users need parameter docs
   for Filter, Merge, Ingest, etc.
9. **Define jargon on first use** across all pages (stem-match, co-input,
   denormalized, orphan, NFS, shard, compact).

### Low Impact
10. **Create Visualization Reference page** -- useful but less critical.
11. **Create Utilities Reference page** -- mostly internal helpers.
12. **Create Error Types Reference page** -- helpful for debugging.
13. **Standardize cross-references section** across all pages.
14. **Slim `api-quickstart.md`** to a true cheat sheet with links to detailed
    pages, reducing redundancy.
