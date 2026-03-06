# Design Doc: Artisan Concepts Documentation Improvements

## Overview

The `docs/artisan/concepts/` section contains 10 files (1 index + 9 content
pages) covering architecture, operations, provenance, storage, execution,
artifacts, error handling, and two "design rationale" pages. Overall the content
is substantial and well-written, but several recurring problems reduce its
effectiveness:

1. **Pipelines leakage** -- multiple pages reference the `pipelines` domain
   package, domain-specific operations (AlphaFold3, RFDiffusion, ProteinMPNN,
   MPNN), and domain artifacts (`StructureArtifact`, `StructureAnnotation`).
   These references violate the principle that Artisan docs should be
   domain-agnostic.

2. **Redundancy** -- design-goals.md and design-principles.md have significant
   overlap (content addressing, dual provenance, operations model, execution
   architecture, storage). Several topics are explained three or four times
   across different pages.

3. **Section 7 of design-principles.md** -- an entire section on "Semantic
   Labels and Structure Identity" is purely domain-specific (`StructureArtifact`,
   atom selections, `atomworks`, residue numbering). It does not belong in the
   framework concepts docs at all.

4. **Inconsistent audience calibration** -- some pages are written for framework
   contributors (referencing specific file paths, internal function names) while
   the stated audience is developers who want to _use_ the framework.

---

## File-by-File Analysis

### index.md

- **Current state**: Clean table of contents with three groups: System
  Architecture, Data and Tracking, Design Rationale. Links to all 9 content
  pages.
- **Pipelines leakage**: None.
- **Jargon issues**: None. Clear and concise.
- **Content issues**: The grouping is reasonable but could be improved. "Data and
  Tracking" groups provenance, execution flow, and storage -- execution flow is
  more about system behavior than "data and tracking."
- **Recommended changes**:
  - Consider renaming "Data and Tracking" to "Execution and Persistence" or
    splitting execution flow into the "System Architecture" group.
  - If design-goals and design-principles are merged (see below), update links
    accordingly.

---

### architecture-overview.md

- **Current state**: System diagram, package map, data flow description,
  dependency direction, bipartite architecture, coding conventions reference.
  Well-structured and informative.
- **Pipelines leakage**: None. Uses generic examples (DataGenerator,
  DataTransformer) throughout.
- **Jargon issues**:
  - "SIMD" used without explanation in the data flow section (line: "build output
    artifacts with content-addressed IDs"). While SIMD is explained elsewhere
    (design-principles.md), it should either be defined here or linked.
  - "Bipartite architecture" -- a good label but could benefit from a one-line
    definition for readers unfamiliar with the term.
  - `prefect_submitit` appears in the package map with no explanation of what
    Prefect or submitit are.
- **Content issues**:
  - The "Coding Conventions" section at the bottom feels out of place in an
    architecture overview. It is a brief summary that adds little value when
    there is already a link to the full coding conventions page.
  - The data flow section duplicates content from execution-flow.md (the
    three-phase model is described in both places).
- **Recommended changes**:
  - Remove or minimize the "Coding Conventions" section. A single sentence with
    a link suffices.
  - In the data flow section, keep it at a summary level and add an explicit
    "For details, see Execution Flow" callout, rather than re-explaining each
    sub-step.
  - Add a brief glossary note for "bipartite" when it first appears.
  - Add a one-line description of Prefect and submitit in the package map, or
    link to the "Why Prefect" section in design-goals.md.

---

### artifacts-and-content-addressing.md

- **Current state**: Explains what artifacts are, the four built-in types,
  draft/finalize lifecycle, content addressing implications, base fields, and
  hydration modes. Good standalone page.
- **Pipelines leakage**:
  - Line 46-47: "the `pipelines` domain layer registers `STRUCTURE` for
    molecular structure data and `STRUCTURE_ANNOTATION` for associated
    annotations." -- Direct reference to the pipelines domain package and its
    specific artifact types.
  - Lines 158-161: "the `pipelines` domain layer adds `atom_selections` to
    `StructureArtifact` for semantic labels." -- Same problem.
  - Line 164: Cross-reference to "Design Principles SS7: Semantic Labels" -- this
    section is itself domain-specific.
- **Jargon issues**:
  - `xxh3_128` -- mentioned without explaining it is a fast non-cryptographic
    hash. A reader unfamiliar with hash functions may not know what this is.
  - "Delta Lake" used throughout without a definition or link on first use (the
    storage page explains it, but this page does not link on first mention).
  - "Hydration modes" -- the term "hydrate" is framework-specific jargon. A
    brief definition ("loading full content vs. loading only metadata") would
    help.
- **Content issues**:
  - The "How Artifacts Flow Through Pipelines" diagram uses `DataGenerator` and
    `DataTransformer` which are good generic examples, but the section title says
    "Pipelines" which could confuse the boundary between artisan and the
    pipelines package. Consider "How Artifacts Flow Through Steps" or "...Through
    a Pipeline."
  - The "Domain-Specific Artifact Extensions" subsection is useful but the
    examples are entirely pipelines-specific. Replace with generic examples
    (e.g., "a domain layer might add a `format` field to a specialized artifact
    type").
- **Recommended changes**:
  - Replace all `pipelines`-specific examples with generic domain extension
    examples. Instead of "registers STRUCTURE for molecular structure data," use
    something like "a domain plugin might register `IMAGE` for image data or
    `DOCUMENT` for document artifacts."
  - Remove the cross-reference to Design Principles SS7 (Semantic Labels).
  - Add a one-line definition of xxh3_128 on first use.
  - Add a link to the storage page on first mention of Delta Lake.
  - Clarify "hydrate" terminology.

---

### design-goals.md

- **Current state**: Problem statement, core design vision (six pillars),
  strategic productivity goals, comparisons to other workflow systems, technical
  objectives, architecture summary diagram, "Why Pixi" and "Why Prefect"
  sections, and a goals-to-decisions mapping table. Comprehensive.
- **Pipelines leakage**:
  - Line 10: "especially in protein design" -- frames the problem statement
    around a specific domain.
  - Lines 34-35: "The protein design operations (AlphaFold3, RFDiffusion,
    ProteinMPNN) are the flagship use case" -- names specific domain operations.
  - Line 164: Architecture summary diagram includes `AF3`, `MPNN` as example
    operations alongside `Curator` and `Yours`.
- **Jargon issues**:
  - "ACID transactions" -- used without definition. While common in database
    circles, the target audience (researchers using the framework) may not know
    the term.
  - "Partition pruning" -- database optimization term used without explanation.
  - "Optimistic concurrency" -- referenced in passing without explanation.
  - "Content-addressed" -- the fundamental concept is used throughout but never
    defined on this page (it is defined on the artifacts page).
- **Content issues**:
  - Significant overlap with design-principles.md. Both pages cover: content
    addressing, dual provenance, operation model, execution architecture, storage
    decisions. The distinction between "goals" (why) and "principles" (how) is
    not consistently maintained -- design-goals.md includes technical details
    (cache key formula, architecture diagram) that belong in principles.
  - The "Strategic Productivity Goals" section includes aspirational language
    ("Enable autonomous design," "pave the way for future capabilities") that
    reads more like a product roadmap than a concepts page.
  - "Why Pixi" and "Why Prefect" sections could be their own page or moved to a
    "Technology Choices" section to keep design-goals focused.
- **Recommended changes**:
  - HIGH PRIORITY: Merge with design-principles.md or clearly delineate scope.
    Proposed split: design-goals.md covers the "why" (problem, vision, strategic
    goals, comparisons) and design-principles.md covers the "how" (specific
    decisions, rationale, implications). Currently both cover both.
  - Replace "protein design" framing in the problem statement with generic
    examples ("computational research workflows" is already there -- just remove
    the parenthetical).
  - Replace AF3/MPNN in the architecture diagram with generic placeholders like
    `Op A`, `Op B`, or `Plugin`.
  - Remove or shorten the "Strategic Productivity Goals" section -- it reads like
    marketing copy rather than technical documentation.
  - Move "Why Pixi" and "Why Prefect" to a separate "Technology Choices"
    appendix or a dedicated page.
  - Add brief definitions for ACID, partition pruning on first use, or link to
    the storage page.

---

### design-principles.md

- **Current state**: The longest page (590 lines). Covers content addressing,
  dual provenance, operation model, provenance system details, execution
  architecture, storage/IO, orchestration, semantic labels, and documentation
  structure. Uses a consistent Decision/Rationale/Implications format
  throughout.
- **Pipelines leakage**:
  - Line 63-65: "Operations for AlphaFold3, RFDiffusion, and ProteinMPNN are
    shipped as reference implementations" -- names domain-specific operations.
  - Lines 106: "Curator operations (Filter, Merge, IngestStructure)" --
    `IngestStructure` is a domain-specific operation from the pipelines package.
  - Lines 414-536: ALL of Section 7 ("Semantic Labels and Structure Identity") is
    entirely domain-specific. It references `StructureArtifact`,
    `atom_selections`, `atomworks`, residue numbering, chain IDs, PDB-specific
    concepts (OG, NE2, OD1 atoms), and specific pipeline operations (MPNN, RFD3,
    AF3). This is the most severe leakage in the entire concepts section.
  - Line 483: Table references "MPNN (redesigns sequence at fixed positions)"
    and "RFD3 (renumbers residues), AF3 (may remap ligand chains)."
  - Line 589: Summary invariant #9: "Labels are atom-level anchors" -- this is
    domain-specific and should not be in framework-level invariants.
- **Jargon issues**:
  - "SIMD" (line 48) -- used as a metaphor for batch processing. While
    technically apt, it is jargon from CPU architecture that may confuse the
    audience.
  - "Denormalized" (line 190) -- database term used without explanation.
  - "Foreign key dependencies" (in commit order section) -- assumes database
    knowledge.
  - "Close-to-open consistency" (line 267) -- NFS-specific term that most
    readers will not know.
  - "W3C PROV" -- referenced but not explained (it is explained on the
    provenance page, but not linked here on first use).
- **Content issues**:
  - At 590 lines, the page is too long. It tries to be both a principles
    document AND a detailed technical reference.
  - Section 7 (Semantic Labels) is 124 lines of purely domain-specific content.
    It should be moved entirely to the pipelines domain docs.
  - Section 8 (Documentation Structure) is meta-documentation about the docs
    themselves. While useful, it belongs in a contributing guide, not in
    framework concepts.
  - Heavy overlap with: design-goals.md (pillars 1-5), operations-model.md
    (section 2), provenance-system.md (sections 1 and 3), execution-flow.md
    (section 4), storage-and-delta-lake.md (section 5).
- **Recommended changes**:
  - HIGH PRIORITY: Remove Section 7 (Semantic Labels) entirely. Move it to
    `docs/pipelines/concepts/` or a domain-specific design principles page.
  - HIGH PRIORITY: Remove Section 8 (Documentation Structure) and move to a
    contributing guide (e.g., `docs/contributing/documentation-style.md`).
  - Replace all domain-specific operation names with generic examples.
  - Replace "IngestStructure" with a generic curator example.
  - Reduce overlap with other pages. For principles that are fully explained on
    their own dedicated page, keep only the Decision statement and a
    cross-reference. The full Rationale/Implications can live on the dedicated
    page.
  - Add a brief explanation or link for "SIMD" on first use.
  - Consider breaking the remaining content into a more focused page that covers
    only the top-level design decisions with brief rationale, linking to
    detailed pages for each topic.

---

### error-handling.md

- **Current state**: Explains the "errors are data" philosophy with 8 numbered
  principles and a complete error flow trace. Well-structured, clear, and
  actionable.
- **Pipelines leakage**:
  - Line 2: "How the Pipelines framework handles failures" -- says "Pipelines
    framework" instead of "Artisan framework."
  - Line 73: `FileNotFoundError: /data/input.pdb` -- the `.pdb` extension is
    domain-specific. Use a generic extension like `.csv` or `.dat`.
- **Jargon issues**:
  - "StagingResult" -- framework-internal type used without explanation. While
    the error flow trace shows it in context, a brief definition on first use
    would help.
  - "XCom" -- mentioned nowhere on this page, but worth noting the page is
    otherwise jargon-free and accessible.
- **Content issues**:
  - This is one of the best-written pages in the section. The principles are
    clear, well-motivated, and the error flow trace is an excellent concrete
    example.
  - Minor: the `fail_fast` section could benefit from a brief example of when a
    user would enable it.
- **Recommended changes**:
  - LOW PRIORITY: Fix "Pipelines framework" -> "Artisan framework" in line 2.
  - LOW PRIORITY: Replace `.pdb` with a generic file extension in the error
    example.
  - Optional: Add a 1-2 line example scenario for when `fail_fast` is useful.

---

### execution-flow.md

- **Current state**: Detailed walkthrough of dispatch, execute, and commit
  phases. Includes creator vs curator comparison table. Clear and well-organized.
- **Pipelines leakage**: None. Uses generic terminology throughout.
- **Jargon issues**:
  - "NFS close-to-open consistency" -- used without explanation. A parenthetical
    "(ensuring files written by one machine are visible to another)" would help.
  - "Parquet files" -- used without explaining what Parquet is. A brief "(a
    columnar file format)" note on first use would help.
  - "Sharded staging directories" -- "sharded" is not explained.
  - `fsync()` -- system call used without context. Most framework users will not
    know what this does.
- **Content issues**:
  - Good standalone page. The three-phase structure mirrors the architecture
    overview's data flow section, but with more detail -- this is appropriate.
  - The "Why This Architecture" section at the end repeats content from the
    earlier sections. It could be shortened to a brief summary or removed.
- **Recommended changes**:
  - Add brief parenthetical explanations for NFS, Parquet, sharding, and fsync
    on first use.
  - Shorten the "Why This Architecture" section to avoid repeating what was just
    explained above.

---

### operations-model.md

- **Current state**: Explains two operation types, three-phase lifecycle, spec
  system, configuration, behavioral flags, and multi-input pairing. Thorough.
- **Pipelines leakage**:
  - Line 65: "`StructureArtifact.draft()`" -- domain-specific artifact type used
    as an example in the postprocess description.
- **Jargon issues**:
  - "SIMD" -- used implicitly via "batch processing" but not named. The page
    avoids the term, which is fine.
  - "ClassVar" -- Python typing term used without explanation. Readers
    unfamiliar with Python class variables may not understand.
  - "StrEnum" -- same issue.
  - "Pydantic fields" -- assumes familiarity with Pydantic.
- **Content issues**:
  - The spec system section is detailed and useful.
  - "Behavioral Flags" section (`runtime_defined_inputs`,
    `hydrate_inputs`, `independent_input_streams`) is fairly advanced and could
    benefit from concrete examples showing when you would set each flag.
  - The "Multi-Input Pairing" section is good but brief. A small diagram showing
    how LINEAGE vs ZIP vs CROSS_PRODUCT pair inputs would significantly improve
    understanding.
  - Overlap with design-principles.md sections 2.1-2.6 -- both pages explain the
    two operation types, three-phase lifecycle, and spec system.
- **Recommended changes**:
  - Replace `StructureArtifact.draft()` with `MetricArtifact.draft()` or another
    built-in artifact type.
  - Add brief examples for each behavioral flag.
  - Add a visual diagram for multi-input pairing strategies.
  - Add brief explanations or links for ClassVar, StrEnum, and Pydantic on
    first use (or add a "Prerequisites" note at the top).
  - Reduce overlap with design-principles.md by keeping the detailed "how" here
    and referencing principles for the "why."

---

### provenance-system.md

- **Current state**: Explains dual provenance, W3C PROV alignment, why artifact
  provenance cannot be derived, dual execution identity, lineage declaration,
  filename stem matching, edge types, co-input edges, and key design decisions.
  Comprehensive.
- **Pipelines leakage**:
  - Line 129: "A structure named `sample_001.pdb` that gets relaxed produces
    `sample_001_relaxed.pdb`" -- while `pdb` is a common extension, the
    "structure" and "relaxed" language is domain-suggestive. Consider a more
    generic example.
  - Lines 165-166: "Docking -- Requires ligand + receptor" -- domain-specific
    example for co-input edges.
- **Jargon issues**:
  - "W3C PROV" -- mentioned and mapped, but no link to the actual standard. A
    link to the W3C PROV overview would help curious readers.
  - "wasDerivedFrom", "wasGeneratedBy" -- W3C PROV terms used without
    explanation beyond the mapping table.
  - "Denormalized artifact types on edges" -- assumes database normalization
    knowledge.
  - "Orphan artifact" -- framework-specific term used without definition.
- **Content issues**:
  - The page is well-organized and covers the topic thoroughly.
  - The "Why Artifact Provenance Cannot Be Derived" section is excellent -- it
    clearly motivates a non-obvious design decision.
  - The "Key Design Decisions" table at the end is a good summary but partially
    duplicates the design-principles.md provenance sections.
  - Missing: how to actually query or visualize provenance. A brief "what you
    can do with it" section (even just links) would help readers understand why
    this system matters to them as users.
- **Recommended changes**:
  - Replace domain-specific examples (PDB files, docking, ligand/receptor) with
    generic computational examples (e.g., "image processing: requires both the
    image and the filter kernel" or "comparison: requires both inputs to compute
    a difference score").
  - Add a link to the W3C PROV specification.
  - Define "orphan artifact" on first use.
  - Add a brief "What You Can Do With Provenance" paragraph or section linking
    to visualization/query tools.

---

### storage-and-delta-lake.md

- **Current state**: Explains why Delta Lake, table layout, content addressing,
  ArtifactStore, staging-commit pattern, and reading with Polars. Focused and
  practical.
- **Pipelines leakage**:
  - Lines 62-63: "e.g., `structure`, `structure_annotation` from the `pipelines`
    domain layer" -- direct reference to domain-specific types and the pipelines
    package.
- **Jargon issues**:
  - "ACID transactions" -- used in the first bullet without definition.
  - "Partition pruning" -- used without explanation.
  - "Optimistic concurrency conflicts" -- used without explanation.
  - "Parquet" -- used extensively without definition.
  - "NFS close-to-open consistency" -- same issue as other pages.
  - "deltalake-rs" -- library name used without explanation.
- **Content issues**:
  - The "Reading Tables with Polars" section is a nice practical touch.
  - Missing: how to set up or initialize a Delta Lake store. Where does
    `delta_root` come from? How is it created? This is probably a how-to guide
    topic, but a brief pointer would help.
  - The "Table Layout" section is useful but the tree diagram could include brief
    descriptions of what each table stores (some have them, some do not).
- **Recommended changes**:
  - Replace `pipelines` domain layer references with generic examples (e.g., "a
    domain plugin might register `image` or `document` types").
  - Add brief definitions for ACID, Parquet, partition pruning, and optimistic
    concurrency on first use. These terms appear across multiple pages -- a
    shared glossary would be ideal (see Cross-Cutting Issues).
  - Add a brief note about where `delta_root` comes from or link to the
    relevant getting-started page.

---

## Cross-Cutting Issues

### 1. Pipelines Domain Leakage (HIGH)

Every page except execution-flow.md and index.md has at least one reference to
the `pipelines` package, domain-specific operations (AlphaFold3, RFDiffusion,
ProteinMPNN/MPNN), or domain artifacts (StructureArtifact, StructureAnnotation).
The worst offender is design-principles.md Section 7 (124 lines of purely
domain-specific content).

**Action**: Systematic pass through all files to:
- Remove or generalize all `pipelines` references
- Replace domain operations with generic names (`Op A`, `Plugin`, etc.)
- Replace `StructureArtifact` with `MetricArtifact` or hypothetical generic
  examples
- Move Section 7 of design-principles.md to pipelines domain docs

### 2. Redundancy Between Pages (HIGH)

The following topics are explained in detail on multiple pages:

| Topic | Pages |
|-------|-------|
| Two operation types (creator/curator) | design-principles, operations-model, execution-flow, architecture-overview |
| Three-phase lifecycle | design-principles, operations-model, execution-flow, architecture-overview |
| Content addressing | design-principles, design-goals, artifacts-and-content-addressing, storage-and-delta-lake |
| Dual provenance | design-principles, design-goals, provenance-system |
| Staging-commit pattern | design-principles, execution-flow, storage-and-delta-lake |
| Cache keys | design-principles, design-goals, execution-flow, artifacts-and-content-addressing |

Each topic should have ONE authoritative page with full detail. Other pages
should provide a 1-2 sentence summary and cross-reference.

**Proposed ownership:**

| Topic | Authoritative Page |
|-------|-------------------|
| Artifact types, content addressing, draft/finalize | artifacts-and-content-addressing.md |
| Two operation types, three-phase lifecycle, specs | operations-model.md |
| Dual provenance, lineage, edges | provenance-system.md |
| Delta Lake, tables, staging, commit | storage-and-delta-lake.md |
| Dispatch-execute-commit flow, caching | execution-flow.md |
| High-level rationale, comparisons, vision | design-goals.md |
| Specific design decisions and invariants | design-principles.md |

### 3. design-goals.md vs design-principles.md Overlap (HIGH)

These two pages cover much of the same ground with slightly different framing.
design-goals.md explains WHY decisions were made (motivation, comparisons) but
also includes HOW details (architecture diagram, cache key formula).
design-principles.md explains HOW decisions work (Decision/Rationale/
Implications) but also includes WHY motivation.

**Options:**
- **Option A (Merge)**: Combine into a single "Design Rationale" page. Pros:
  eliminates redundancy. Cons: creates a very long page.
- **Option B (Sharpen boundaries)**: design-goals.md covers only problem
  statement, vision, comparisons, and technology choices (Pixi, Prefect).
  design-principles.md covers only Decision/Rationale/Implications for each
  technical choice, with cross-references to detailed pages. Remove overlapping
  content from both.
- **Recommended**: Option B. Both pages have value but need clearer scope.

### 4. Missing Glossary (MEDIUM)

Terms like ACID, Parquet, Delta Lake, NFS, partition pruning, optimistic
concurrency, content addressing, SIMD, W3C PROV, hydration, and fsync are used
across multiple pages without consistent definitions. Options:

- Add a glossary page to the concepts section
- Define each term on first use within each page (with brief parenthetical)
- Both (glossary page + parenthetical definitions)

**Recommended**: Add parenthetical definitions on first use within each page AND
create a shared glossary page that all concept pages can link to.

### 5. Inconsistent Audience (MEDIUM)

Some pages reference internal file paths (`src/artisan/orchestration/`),
function names (`compute_execution_spec_id()`, `capture_lineage_metadata()`),
and internal types (`StagingResult`, `LineageMapping`). These are useful for
contributors but may confuse users who just want to understand the concepts.

**Recommended**: For concept pages, minimize internal function/file references.
Use "Key files" callouts (as architecture-overview.md does) or move detailed
function references to the corresponding reference pages. The "Key files" pattern
at the top of a page is acceptable as an orientation aid.

### 6. Missing "What Can I Do With This?" Framing (LOW)

Several pages explain mechanisms in detail but never connect them to user-facing
benefits or actions. For example:
- provenance-system.md explains the dual system but never says "you can use this
  to trace a bad result back to its source"
- storage-and-delta-lake.md explains table layout but never says "you can query
  this to find all metrics from step 3"
- artifacts-and-content-addressing.md explains hashing but never says "this
  means if you re-run the same pipeline, cached results are used automatically"

Adding a brief "Why This Matters to You" paragraph to each page would
significantly improve reader engagement.

---

## Recommended Page Structure

The current 9-page structure is reasonable. Recommended adjustments:

### Keep (with modifications)
1. **architecture-overview.md** -- Reduce data flow detail, fix audience
2. **artifacts-and-content-addressing.md** -- Remove pipelines leakage, add
   glossary terms
3. **operations-model.md** -- Add examples, diagrams, remove pipelines leakage
4. **provenance-system.md** -- Replace domain examples, add "why it matters"
5. **execution-flow.md** -- Add jargon definitions, reduce "Why This
   Architecture" repetition
6. **storage-and-delta-lake.md** -- Remove pipelines leakage, add definitions
7. **error-handling.md** -- Fix "Pipelines" reference, minor polish

### Restructure
8. **design-goals.md** -- Narrow scope to: problem, vision, comparisons,
   technology choices. Remove technical implementation details.
9. **design-principles.md** -- Remove Section 7 (Semantic Labels) and Section 8
   (Documentation Structure). Reduce overlap with dedicated topic pages. Keep
   only the Decision/Rationale/Implications that are NOT covered on other pages.

### Add
10. **glossary.md** (new) -- Shared definitions for recurring technical terms
    (ACID, Parquet, Delta Lake, content addressing, NFS, partition pruning, etc.)

### Move
- design-principles.md Section 7 (Semantic Labels) -> `docs/pipelines/concepts/`
  or `docs/pipelines/concepts/semantic-labels.md`
- design-principles.md Section 8 (Documentation Structure) ->
  `docs/contributing/documentation-style.md`

---

## Priority

### High Impact
1. **Remove pipelines domain leakage** -- systematic pass through all 9 pages.
   Largest single improvement for doc quality and framework independence.
2. **Remove/move Section 7 from design-principles.md** -- 124 lines of purely
   domain-specific content in a framework concepts page.
3. **Reduce redundancy** -- establish authoritative pages for each topic and
   replace duplicated explanations with cross-references.
4. **Sharpen design-goals vs design-principles boundary** -- eliminate the
   overlap between these two pages.

### Medium Impact
5. **Add glossary** -- either a dedicated page or consistent parenthetical
   definitions for recurring jargon.
6. **Add "Why This Matters" framing** -- brief user-benefit paragraph on each
   page.
7. **Move Section 8 (docs structure)** from design-principles.md to
   contributing guide.
8. **Add diagrams to operations-model.md** -- multi-input pairing visualization.

### Low Impact
9. **Fix minor jargon** -- parenthetical definitions for NFS, Parquet, fsync,
   etc.
10. **Polish error-handling.md** -- fix "Pipelines" reference, genericize `.pdb`
    example.
11. **Add examples to behavioral flags** in operations-model.md.
12. **Improve index.md grouping** -- minor reorganization of section headers.
