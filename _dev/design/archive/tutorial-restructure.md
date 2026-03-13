# Design: Tutorial Restructure

**Date:** 2026-03-11  **Status:** Draft  **Author:** Claude + ach94

---

## Summary

Restructure the 18 existing tutorials from four groups into five groups that
cleanly separate concerns: DAG topology, execution behavior, result inspection,
and framework extension. Slim "Getting Started" to a two-tutorial on-ramp,
absorb execution-related tutorials into a unified "Execution" group, rename
groups for clarity, and add a new "Writing a Composite" tutorial (19 total).

---

## Problem

The current four-group structure conflates what you're building (topology) with
how you run it (execution) with how you inspect it (analysis):

- **"First Steps" mixes concerns.** It bundles the core on-ramp (first
  pipeline, exploring results) with deployment (SLURM), execution model (run vs
  submit), and framework extension (writing an operation). A user who just wants
  to build pipelines hits SLURM before they've learned any patterns.

- **"Pipeline Patterns" contains non-patterns.** "Error Visibility" is runtime
  behavior (what happens when filters produce nothing), not a topology pattern.
  "Advanced Patterns" is a grab bag: diamond DAGs (pattern), batching
  (execution concern), and iterative refinement (pattern) in one tutorial.

- **"Working with Results" includes execution behavior.** "Resume and Caching"
  is about re-running pipelines and cache policy — an execution concern, not
  result inspection.

- **"Writing an Operation" is stranded.** It lives in First Steps but teaches a
  fundamentally different skill (extending the framework vs using it). There is
  no equivalent tutorial for writing a composite.

- **No home for SLURM/Cloud.** SLURM sits awkwardly in First Steps. When a
  cloud backend tutorial is added, there's no natural group for it.

---

## Current Structure

```
First Steps (5)
├── First Pipeline
├── Exploring Results
├── Run vs Submit
├── SLURM Execution
└── Writing an Operation

Pipeline Patterns (7)
├── Sources and Sequencing
├── Branching and Merging
├── Metrics and Filtering
├── Multi-Input Operations
├── Advanced Patterns          ← grab bag (diamonds + batching + iteration)
├── Error Visibility           ← runtime behavior, not a pattern
└── Composable Operations

Working with Results (4)
├── Provenance Graphs
├── Resume and Caching         ← execution concern
├── Interactive Filter
└── Timing Analysis

Execution and Tuning (2)
├── Batching and Performance
└── Step Overrides
```

---

## Proposed Structure

```
Getting Started (2)
├── First Pipeline
└── Exploring Results

Pipeline Design (6)
├── Sources and Sequencing
├── Branching and Merging
├── Metrics and Filtering
├── Multi-Input Operations
├── Diamonds and Iteration     ← "Advanced Patterns" minus batching
└── Composable Operations

Execution (6)
├── Run vs Submit              ← moved from Getting Started
├── Resume and Caching         ← moved from Working with Results
├── Batching and Performance   ← moved from Execution and Tuning
├── Error Visibility           ← moved from Pipeline Patterns
├── Step Overrides             ← moved from Execution and Tuning
└── SLURM Execution            ← moved from Getting Started

Analysis (3)
├── Provenance Graphs
├── Interactive Filter
└── Timing Analysis

Writing Operations (2)
├── Writing an Operation       ← moved from Getting Started
└── Writing a Composite        ← new tutorial
```

---

## What Changes

### Getting Started (5 → 2)

Remove everything that isn't the core on-ramp. A new user should be able to
build a pipeline and inspect the results in ~30 minutes, then choose where to
go next.

**Removed:**
- Run vs Submit → Execution
- SLURM Execution → Execution
- Writing an Operation → Writing Operations

### Pipeline Design (7 → 6, renamed from "Pipeline Patterns")

Only DAG topology patterns. Rename the group — "Design" signals that these
tutorials are about *designing* pipeline structure, not runtime behavior.

**Removed:**
- Error Visibility → Execution (runtime behavior, not topology)

**Modified:**
- "Advanced Patterns" → "Diamonds and Iteration" — remove the batching content
  (moved to Execution/Batching), keep diamond DAGs and iterative refinement.
  The title becomes specific and descriptive.

### Execution (2 → 6, renamed from "Execution and Tuning")

Unified group for everything about *how pipelines run*. Drop "and Tuning" —
"Execution" is sufficient and cleaner.

**Absorbed:**
- Run vs Submit (from Getting Started) — core execution model
- Resume and Caching (from Working with Results) — re-running and cache policy
- Error Visibility (from Pipeline Patterns) — runtime skip behavior

**Ordering rationale:**
- Run vs Submit first (foundational execution model)
- Resume and Caching second (what happens on re-runs)
- Batching and Performance third (tuning execution)
- Error Visibility fourth (runtime edge cases)
- Step Overrides fifth (all override knobs)
- SLURM Execution last (specific backend, requires cluster access)

This ordering also provides a natural home for a future Cloud Execution
tutorial after SLURM.

### Analysis (4 → 3, renamed from "Working with Results")

Shorter, clearer name. Only tutorials about inspecting pipeline output.

**Removed:**
- Resume and Caching → Execution

### Writing Operations (new group, 2 tutorials)

Framework extension tutorials. These teach a different skill — authoring new
operation types rather than using existing ones.

- **Writing an Operation** — existing tutorial, moved from Getting Started
- **Writing a Composite** — new tutorial covering CompositeDefinition authoring
  (the existing Composable Operations tutorial teaches *using* composites; this
  teaches *building* them)

---

## Tutorial Dependency Graph

```
Getting Started
  First Pipeline ──────────────────────────────┐
  └── Exploring Results                        │
                                               │
Pipeline Design                                │
  Sources and Sequencing ◄─────────────────────┤
  ├── Branching and Merging                    │
  │   ├── Multi-Input Operations               │
  │   └──┐                                    │
  │      ├── Diamonds and Iteration            │
  ├── Metrics and Filtering                    │
  └── Composable Operations                    │
                                               │
Execution                                      │
  Run vs Submit ◄──────────────────────────────┤
  ├── Resume and Caching                       │
  ├── Batching and Performance ◄── Diamonds    │
  ├── Error Visibility ◄── Metrics and Filt.   │
  ├── Step Overrides ◄── Batching              │
  └── SLURM Execution ◄── Run vs Submit        │
                                               │
Analysis                                       │
  Provenance Graphs ◄── Exploring Results      │
  Interactive Filter ◄── Metrics and Filt.     │
  Timing Analysis ◄───────────────────────────┘
                                               │
Writing Operations                             │
  Writing an Operation ◄───────────────────────┘
  └── Writing a Composite ◄── Composable Ops
```

---

## Open Questions

- **"Pipeline Design" vs "Composing Pipelines" vs "Patterns"?** Current
  proposal uses "Pipeline Design." "Patterns" is shorter but vague.
  "Composing Pipelines" is descriptive but longer.

- **"Writing Operations" vs "Extending Artisan" vs "Custom Operations"?**
  Current proposal uses "Writing Operations" for directness.

- **"Diamonds and Iteration"** — is this name clear enough? Alternatives:
  "Complex Topologies", "Composed Patterns", "Loops and Diamonds."

- **Writing a Composite scope.** Should this tutorial start from scratch, or
  assume the user has completed "Composable Operations" (which teaches usage)?
  Proposed: prerequisite on Composable Operations, so the tutorial focuses
  purely on authoring.

---

## File Changes

### Moves (rename numbered prefixes)

| Current path | New path |
|---|---|
| `getting-started/03-run-vs-submit.ipynb` | `execution/01-run-vs-submit.ipynb` |
| `getting-started/04-slurm-execution.ipynb` | `execution/06-slurm-execution.ipynb` |
| `getting-started/05-writing-an-operation.ipynb` | `writing-operations/01-writing-an-operation.ipynb` |
| `working-with-results/02-resume-and-caching.ipynb` | `execution/02-resume-and-caching.ipynb` |
| `pipeline-patterns/06-error-visibility.ipynb` | `execution/04-error-visibility.ipynb` |
| `execution-and-tuning/01-batching-and-performance.ipynb` | `execution/03-batching-and-performance.ipynb` |
| `execution-and-tuning/02-step-overrides.ipynb` | `execution/05-step-overrides.ipynb` |

### Renames (directory and/or numbered prefix updates)

| Current | New |
|---|---|
| `working-with-results/01-provenance-graphs.ipynb` | `analysis/01-provenance-graphs.ipynb` |
| `working-with-results/03-interactive-filter.ipynb` | `analysis/02-interactive-filter.ipynb` |
| `working-with-results/04-timing-analysis.ipynb` | `analysis/03-timing-analysis.ipynb` |
| `pipeline-patterns/01-sources-and-sequencing.ipynb` | `pipeline-design/01-sources-and-sequencing.ipynb` |
| `pipeline-patterns/02-branching-and-merging.ipynb` | `pipeline-design/02-branching-and-merging.ipynb` |
| `pipeline-patterns/03-metrics-and-filtering.ipynb` | `pipeline-design/03-metrics-and-filtering.ipynb` |
| `pipeline-patterns/04-multi-input-operations.ipynb` | `pipeline-design/04-multi-input-operations.ipynb` |
| `pipeline-patterns/05-advanced-patterns.ipynb` | `pipeline-design/05-diamonds-and-iteration.ipynb` |
| `pipeline-patterns/07-composable-operations.ipynb` | `pipeline-design/06-composable-operations.ipynb` |

### Directory changes

| Action | Path |
|---|---|
| Rename | `pipeline-patterns/` → `pipeline-design/` |
| Rename | `working-with-results/` → `analysis/` |
| Rename | `execution-and-tuning/` → `execution/` |
| Create | `writing-operations/` |
| Keep   | `getting-started/` (2 files remain, name matches group) |

### New files

| Path | Description |
|---|---|
| `writing-operations/02-writing-a-composite.ipynb` | New tutorial |

### Updated files

| File | Changes needed |
|---|---|
| `docs/myst.yml` | Update TOC with new groups and paths |
| `docs/tutorials/index.md` | Rewrite group headings and listings |
| `pipeline-patterns/05-advanced-patterns.ipynb` | Remove batching content, rename to `pipeline-design/05-diamonds-and-iteration.ipynb` |
| All moved notebooks | Update internal cross-references |

---

## Migration Checklist

- [ ] Create new directories (`execution/`, `writing-operations/`, `analysis/`)
- [ ] Move and rename files per tables above
- [ ] Split batching content out of "Advanced Patterns"
- [ ] Rename "Advanced Patterns" to "Diamonds and Iteration"
- [ ] Update `docs/myst.yml` TOC
- [ ] Update `docs/tutorials/index.md`
- [ ] Update cross-references in all moved notebooks
- [ ] Write "Writing a Composite" tutorial
- [ ] Update `CLAUDE.md` architecture section
- [ ] Build docs and verify all links resolve
