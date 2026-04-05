# Design: External Content Artifacts and Post-Step Consolidation

**Date:** 2026-04-04
**Status:** Superseded — split into separate design docs

---

## Split Docs

This monolith design has been broken into focused documents:

**Artisan framework (this repo):**

- `external-content-artifacts.md` — `external_path` as functional content
  pointer, `files_root` configuration, two categories of external files
- `post-step-sugar.md` — `post_step` parameter on submit/run for
  auto-inserting consolidation steps
- `artifact-id-materialization.md` — Parallel name collision fix via
  artifact-ID-based filenames (already existed as standalone doc)

**Domain-specific (protein design repo):**

- `silent-file-pipeline.md` — SilentStructureArtifact, ConsolidateSilentFiles
  curator, silent_tools wrapper, end-to-end flow

**Standalone bug fix (no design doc needed):**

- Curator explicit lineage bug — `curator.py:150` must honor
  `ArtifactResult.lineage` (mirror creator executor pattern at
  `creator.py:242`)

---

## Evaluation

See `evaluation.md` for the analysis that motivated this split,
including unanswered questions identified during review.
