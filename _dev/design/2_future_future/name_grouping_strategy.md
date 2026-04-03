# Design: `GroupByStrategy.NAME` — Name-Based Input Stream Pairing

**Date:** 2026-03-31
**Status:** Draft

---

## Summary

Add `GroupByStrategy.NAME` to match artifacts across input roles by their
`original_name` stem. This enables joint processing of independently-produced
data streams that share naming conventions but have no provenance ancestry.
The existing co-input edge machinery creates lineage edges automatically when
paired artifacts flow through a creator operation — no dedicated curator op
needed.

---

## Motivation

Two artifact streams produced by independent ingest operations share no
provenance ancestry. Both contain artifacts named after the same entities
(e.g., `sample_001.pdb` and `sample_001.csv`) but were ingested separately.

A downstream operation needs to process them together — one dataset paired
with its corresponding metadata by name.

Today's strategies fail:

- **LINEAGE** — requires common ancestors. Independent ingests have none.
- **ZIP** — requires identical cardinality and relies on positional order.
  Only works by accident if both streams happen to sort the same way.
- **CROSS_PRODUCT** — produces N*M combinations. Only N diagonal pairings
  are meaningful.

The workaround is a curator op that creates lineage edges between the two
streams so that LINEAGE can match downstream. But this adds a pipeline step
whose sole purpose is to "trick" the framework. A native NAME strategy is
cleaner.

---

## Current Architecture

### Grouping phase (pre-execution)

`group_inputs()` at `grouping.py:47` receives `dict[str, list[str]]`
(role → artifact IDs) and dispatches to a strategy-specific matcher:

- `_match_zip()` — positional pairing
- `_match_cross_product()` — cartesian product
- `_match_by_ancestry()` — provenance graph walk (requires `artifact_store`)

Each matcher returns `list[dict[str, str]]` (matched sets), which
`_matched_sets_to_aligned()` converts to aligned inputs + `group_ids`.

A parallel entry point, `match_inputs_to_primary()` at `grouping.py:87`,
does anchor-based pairing (one primary role matched independently against
N other roles). Currently supports LINEAGE only.

### How group_ids create lineage edges

`group_ids` flow through batching → `ExecutionUnit` → execution →
`capture_lineage_metadata()` at `capture.py:16`. When `group_by` is set and
an output stem-matches a primary input at index *i*, co-input edges are
created from ALL other input roles at index *i* to that output, all sharing
the `group_id`. These become `ArtifactProvenanceEdge` rows in the
`artifact_edges` Delta table.

This means: **any strategy that produces `group_ids` gets lineage edges for
free** through the existing machinery. NAME just needs to produce correct
aligned inputs and group_ids — no changes to `capture.py` required.

### Where `original_name` lives

`original_name` is stored on artifact content tables (DataArtifact,
MetricArtifact, FileRefArtifact, ExecutionConfigArtifact) but NOT in
`artifact_index`. Loading names requires scanning content tables by type.

A pattern for this exists in `visualization/graph/micro.py:84-110`, which
iterates all `ArtifactTypeDef` entries and scans each content table for
`(artifact_id, original_name)`.

---

## Design

### New enum value

Add `NAME = "name"` to `GroupByStrategy` in `schemas/enums.py`.

### Name-loading helper

Add `load_original_names(artifact_ids)` to `ArtifactStore`. Implementation:

- Call `load_type_map(artifact_ids)` to get `artifact_id → type` from the
  artifact index (single scan).
- Group IDs by type, then scan only the needed content tables for
  `(artifact_id, original_name)`.
- Return `dict[str, str]` mapping artifact_id → original_name.

This avoids scanning every content table and follows the `micro.py` pattern.

### `_match_by_name()` in grouping.py

Following the `_match_by_ancestry()` pattern:

- Collect all artifact IDs across all roles.
- Call `artifact_store.load_original_names(all_ids)`.
- Apply `strip_extensions()` to each name to get the stem.
- Validate uniqueness per role via `validate_stem_match_uniqueness()`.
- Build a stem → artifact_id index per role.
- For each stem present in ALL roles, emit a matched set.
- Stems missing from any role are skipped (logged at WARNING).

### Key decisions

**Exact stem match only.** The lineage capture phase uses prefix matching as
a fallback for output-to-input pairing (where an operation may append
suffixes). For NAME strategy, both sides are inputs at the same conceptual
level — prefix matching is dangerous (`sample_1` would match `sample_10`).
Exact match is sufficient and predictable.

**N-role support.** LINEAGE is restricted to exactly 2 roles. NAME has no
such limitation — stems from 3+ roles are intersected naturally. If a stem
exists in all N roles, it produces an N-way match.

**Unique stems per role required.** Duplicate `original_name` values within
a role make matching ambiguous. `validate_stem_match_uniqueness()` already
exists for this purpose. NAME strategy calls it per role before matching,
raising `ValueError` on duplicates.

### Wiring

- Add `NAME` branch in `group_inputs()` dispatch (alongside ZIP,
  CROSS_PRODUCT, LINEAGE).
- Add `NAME` support in `match_inputs_to_primary()`. The primary role's
  stems become the target set; each other role is independently matched.
  This lifts the current LINEAGE-only restriction for `NAME`.

### No changes to lineage capture

`capture_lineage_metadata()` already handles `group_by` and `group_ids`
generically. When a creator op uses `group_by=NAME`, the grouping phase
produces `group_ids`, and co-input edges are created through the existing
path. No changes to `capture.py`, `builder.py`, or `enrich.py`.

---

## Alternative: Dedicated Curator Op

A "LinkByName" curator would receive two input streams, match by name, and
create `ArtifactProvenanceEdge` rows without producing new artifacts.

**Why this is insufficient as the only solution:**

- Neither `ArtifactResult` nor `PassthroughResult` supports "create only
  provenance edges." `ArtifactResult` requires new draft artifacts.
  `PassthroughResult` creates only execution edges (input/output direction),
  not artifact provenance edges.
- A new `EdgeResult` type would be needed — a larger change to the curator
  executor for a narrow use case.

**Why it might still be useful later:**

- For establishing persistent provenance links that survive across pipeline
  runs without requiring the downstream operation to exist yet.
- For "link then branch" patterns where multiple downstream steps should all
  benefit from the linkage.

**Assessment:** NAME strategy alone solves the immediate problem. A curator
op for explicit edge creation is a separate, orthogonal feature.

---

## Edge Cases

**Artifacts without `original_name`** — silently skipped during matching.
Logged at DEBUG level.

**Unmatched stems** — stems present in some roles but not all produce no
matched set. Logged at WARNING (same pattern as LINEAGE unmatched artifacts).

**Case sensitivity** — `strip_extensions()` does not lowercase. Stems
`Sample_001` and `sample_001` do not match. This is correct — filenames are
case-sensitive on Linux.

**Empty roles** — follow existing pattern, return empty results.

---

## Scope

| File | Change |
|------|--------|
| `schemas/enums.py` | Add `NAME = "name"` to `GroupByStrategy` |
| `storage/core/artifact_store.py` | Add `load_original_names()` method |
| `execution/inputs/grouping.py` | Add `_match_by_name()`, wire into `group_inputs()` and `match_inputs_to_primary()` |
| `tests/artisan/execution/test_grouping.py` | Unit tests for NAME strategy |

---

## Open Questions

- **Should `original_name` move to `artifact_index`?** If NAME strategy
  becomes widely used, the multi-table content scan becomes a hot path.
  Adding `original_name` to the index would make it a single scan, but
  requires a schema migration.

- **Should prefix matching be opt-in?** A `strict=False` parameter could
  enable the digit-boundary-protected prefix fallback from `capture.py`.
  Not needed initially but worth considering if exact match proves too
  rigid in practice.

- **Performance at scale.** The content table scans are bounded by the
  number of artifact types (currently 4). Each scan is filtered by
  `artifact_id.is_in(ids)`, so it scales with input size, not table size.
  Should be fine for typical pipelines.
