# Evaluation: silent-file-artifacts.md

**Date:** 2026-04-04
**Evaluating:** `_dev/design/0_current/silent-file-artifacts.md`

---

## Overall Assessment

The doc is well-researched — every code claim checks out against the codebase
(line numbers, behavior descriptions, gap analysis). The prior art survey is
substantive. The problem statement is clear and well-motivated.

The core issue: **it's a monolith covering five distinct concerns** with
different audiences, different repos, and different dependency chains. This
makes it hard to review, implement incrementally, or hand off pieces to
different contributors.

---

## Proposed Split

### Artisan Framework Docs (this repo)

**Doc A: `external-content-artifacts.md`** — External content artifact pattern

Core framework extension: `external_path` as functional content pointer, two
categories of external files (user-managed vs Artisan-managed), `files_root`
configuration on PipelineConfig, directory layout, ArtifactStore threading.

This is the foundational piece — it establishes the pattern that domain-specific
artifact types build on.

Scope: PRs 3 from the original doc (`files_root` configuration), plus the
conceptual framework (two categories of files, `external_path` promotion).

**Doc B: `post-step-sugar.md`** — `post_step` parameter on submit/run

Self-contained mechanism: auto-insert a curator step after the main step,
return the curator's StepFuture. Step numbering, caching behavior, role
matching, naming convention.

Independent of the external content work — useful for any "run this after that"
pattern. Has its own open questions (composites interaction, user mental model
with hidden step numbers).

Scope: PR 4 from the original doc.

**Doc C: `artifact-id-materialization.md`** — Already exists

This doc already exists as a standalone design. The silent-file-artifacts doc
duplicates its content (the "Artifact-ID Materialization and Name Derivation"
section). The standalone doc is more thorough (has four options instead of
the parent's summary of one approach, has the lifecycle analysis, has the
debugging concern section).

Action: **Remove the duplicate section from silent-file-artifacts.md.** Add a
cross-reference. The standalone doc is the canonical source.

**No separate doc needed:** Curator explicit lineage bug fix

This is a straightforward bug fix (mirror creator's pattern in curator
executor). Already documented in both the monolith and the
artifact-id-materialization doc. Doesn't need its own design doc — it's a
single-PR fix with obvious implementation.

### Domain-Specific Doc (protein design repo)

**Doc D: `silent-file-pipeline.md`** — Silent file artifacts and consolidation

Everything Rosetta/protein-specific: SilentStructureArtifact definition,
ConsolidateSilentFiles curator, silent_tools wrapper utilities, end-to-end
flow example, content addressing for consolidated artifacts.

This doc lives in the protein design repo, not artisan. It references the
artisan design docs (external-content-artifacts, post-step-sugar) as
prerequisites but doesn't duplicate their content.

Includes the conversation with bcov and the format properties section from
the analysis doc — these are domain context that belongs with the domain code.

---

## Dependency Graph Between Split Docs

```
artifact-id-materialization (existing, independent)
                \
external-content-artifacts -----> silent-file-pipeline (domain repo)
                /                         |
post-step-sugar (independent) -----------/
                                          |
curator lineage bug fix (independent) ---/
```

The four artisan pieces are independent of each other. The domain doc depends
on all of them.

---

## Unanswered Questions

### Already identified in the doc (carried forward)

These are listed in the doc's Open Questions section. They're real and need
decisions:

- **`original_name` field semantics** — Options A/B/C/D in the
  artifact-id-materialization doc. Blocks implementation.
- **Name derivation edge cases** — Prepend/infix modifications fall back to
  explicit lineage. Is this acceptable?
- **File cleanup** — Should `files_root/{step}/` be cleaned on re-run?
- **Path stability** — Absolute paths break if pipeline moves.
- **Worker file location** — Need to verify `RuntimeEnvironment` propagation
  for `files_root`.

### New questions not addressed in the doc

**Tag uniqueness across workers.** When N workers each produce a silent file
with structures, are decoy tags guaranteed unique across workers? The bcov
conversation says "decoy tags must be unique within a file." If workers
independently generate tags, catting their files together could create
duplicates. Who enforces uniqueness — the operation, the consolidation
curator, or the framework? This is a correctness issue, not a nice-to-have.

**Consolidated vs original artifacts.** After ConsolidateSilentFiles runs,
there are two sets of artifacts in Delta: the per-worker originals (pointing
to `files_root/{step}/workers/worker_N.silent`) and the consolidated versions
(pointing to `files_root/{step}/combined.silent`). Are the originals still
queryable? Are they useful? Should they be marked as superseded? This affects
storage, query semantics, and provenance graph rendering.

**Caching + external files contract.** If a step is fully cached and its
results are reused, the framework expects the external files at `files_root`
to still exist. This is a new implicit contract — currently all cached data
lives in Delta (self-contained). What happens if someone deletes
`files_root/` but Delta is intact? Should cache validation check for external
file existence?

**Appendable vs immutable-per-step.** The analysis doc discusses silent files
as "appendable" (a step can append structures to an existing file across
multiple pipeline steps). The design doc uses "cat workers into combined"
(one consolidated file per step, immutable after creation). These are
different strategies. Which is it? The immutable-per-step approach is simpler
and safer for caching/reproducibility, but the analysis doc seemed to explore
appendability as a feature. This should be explicitly decided and documented.

**Post-step and the user's mental model.** `post_step` consumes two step
numbers but presents as one logical step. How does this affect:
  - Pipeline results display (does the user see both steps or just the
    consolidated one?)
  - Provenance graph visualization (extra node for the consolidation step?)
  - `StepFuture` numbering (if user does `step.step_number`, they get the
    post_step number, not the main step number — is this surprising?)
  - Error messages (if the consolidation fails, does the error reference the
    hidden step number?)

**`files_root` and different filesystems.** If `files_root` is on NFS and
`delta_root` is on local SSD, are there atomicity concerns? The commit phase
writes to Delta (local) and expects external files (NFS) to be stable. If an
NFS write is delayed or fails, Delta metadata could reference a file that
doesn't fully exist yet. Is there a sync point?

**Error recovery during consolidation.** If ConsolidateSilentFiles fails
halfway through catting worker files, are the per-worker files preserved?
The doc says worker files live in `files_root/{step}/workers/` and the
consolidated file goes to `files_root/{step}/combined.silent`. If the
consolidation step fails, the main step's worker files should remain intact
for retry. Is this guaranteed by the framework, or does the curator need to
handle this explicitly?

**Post-step interaction with composites.** The doc says "Deferred. Composites
can use consolidation curators as explicit steps in `compose()`." But this
means composites that produce external files have a different API than
non-composite steps (explicit consolidation step vs `post_step` sugar). Is
this acceptable asymmetry, or does `post_step` need to work with composites
eventually?

**`files_root` accessibility from SLURM workers.** The doc assumes shared NFS
makes `files_root` accessible from all workers. But the SLURM backend can
target different partitions/clusters. If workers land on nodes without the NFS
mount, file writes fail silently or with opaque OS errors. Should the
framework validate NFS accessibility at dispatch time, or is this purely a
deployment concern?

---

## Overlap and Redundancy with Existing Docs

**`artifact-id-materialization.md`** — Significant overlap. The
silent-file-artifacts doc's "Artifact-ID Materialization and Name Derivation"
section (lines 202-268) summarizes content that's covered more thoroughly in
the standalone doc. The standalone doc has four options; the monolith presents
one. The standalone doc has the full lifecycle analysis; the monolith has a
brief version. **Resolution: remove the duplicate section, add a
cross-reference.**

**`external-artifact-storage.md` (analysis)** — The analysis doc contains
the bcov conversation and format properties that motivated the design. The
design doc's "Domain-Specific Implementation Notes" section partially
duplicates this. **Resolution: the domain design doc (in protein design repo)
should reference the analysis doc for background. Don't duplicate the bcov
conversation in the design doc.**

---

## Quality Assessment

| Criterion | Rating | Notes |
|-----------|--------|-------|
| Code claims accuracy | Strong | All claims verified against codebase |
| Prior art survey | Strong | Thorough, references specific files and line numbers |
| Problem statement | Strong | Clear motivation, concrete examples |
| Design completeness | Mixed | Framework pieces are solid; domain pieces are sketchy ("would follow the pattern") |
| Open questions | Weak | Listed questions are good but missing several critical ones (tag uniqueness, cache contract, consolidated vs original artifacts) |
| Scope/sequencing | Strong | Clear PR breakdown, dependency analysis |
| Testability | Adequate | Test plan exists but domain tests are vague |
| Separation of concerns | Weak | Monolith mixing framework and domain; duplicate content with existing docs |

---

## Recommended Next Steps

- Split into the four docs described above (three artisan, one domain)
- Resolve the `original_name` semantics question (Options A-D in
  artifact-id-materialization.md) — this blocks implementation of the
  largest PR
- Answer the tag uniqueness question — this is a correctness issue for the
  domain work
- Decide appendable vs immutable-per-step for silent files
- The curator explicit lineage bug fix is independent — can land immediately
