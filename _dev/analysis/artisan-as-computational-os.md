# Analysis: Artisan as an Operating System for Computational Work

**Date:** 2026-04-02  **Status:** Active  **Author:** Claude + ach94

---

## The Problem

Scientific and analytical workflows accumulate hidden complexity. Scripts embed
undocumented assumptions. Results are hard to reproduce. Scaling a working
prototype requires rewriting it. And when a researcher moves on, so does the
knowledge of how anything actually worked.

The deeper issue is that most research infrastructure conflates two things that
should be separate: the *thinking* — hypotheses, interpretations, judgments —
and the *execution* — what ran, on what inputs, producing what outputs. When
those are tangled together, the work is fragile, opaque, and hard to build on.

---

## What Artisan Does

Artisan is a Python framework for building computational pipelines with
automatic provenance tracking. Its goal is to remove the friction of doing
computational work so that researchers can ask questions, perform analyses, and
iterate faster — without managing the bookkeeping themselves.

Every operation in Artisan declares an explicit contract: typed inputs, typed
outputs, and parameters. The framework uses that contract to wire computations
together, cache results, track what produced what, and store outputs as
content-addressed artifacts. The computation inside each operation is a black
box — wrap whatever tools you already use.

The key design choices:

- **Explicit over implicit.** There is no hidden knowledge in the system. What
  an operation does, what it takes, and what it produces are all declared up
  front. Tribal knowledge stays with the user; the system stays honest.

- **Freedom to compose.** Researchers can design any DAG they want — any graph
  of operations, any topology. The framework imposes no fixed pipeline shape.
  Intermediate outputs can be kept or discarded per node. Scaling can go wide
  (parallel independent runs) or deep (sequential dependent refinement), or
  both.

- **Integrate once, use everywhere.** When someone wraps a new tool as an
  Artisan operation, that integration — with its provenance tracking, caching,
  and composability — becomes available to everyone. The library of operations
  is the team's accumulated, reusable computational knowledge.

- **Queryable history.** Artifacts, metrics, and provenance live in a single
  store, accessible as dataframes. No log parsing, no directory archaeology.
  The full record of what was tried is always available.

---

## The Operating System Analogy

An operating system doesn't run programs — it creates the conditions in which
programs can run reliably. It manages resources, isolates processes, provides a
filesystem, and gives programs a stable interface to the hardware underneath.
The programs don't need to know how any of that works.

Artisan fills the same role for computational work:

| OS concept           | Artisan equivalent                                      |
| -------------------- | ------------------------------------------------------- |
| Process isolation    | Worker sandboxing — each execution gets its own directory, never touches shared state |
| Filesystem           | Delta Lake artifact store — content-addressed, ACID-transactional, queryable |
| Process scheduling   | Orchestrator dispatch — batches work, routes to backends (local, SLURM, cloud) |
| System calls         | Operation contract — declared inputs, outputs, params; the interface between user code and framework |
| Shared libraries     | Operation registry — integrate a tool once, reuse it across any pipeline |
| Audit log            | Dual provenance — execution log (what happened) and artifact graph (what derived from what) |
| Virtual memory       | Content-addressed caching — same inputs and params, instant cache hit; the system never does work it's already done |

The analogy clarifies what Artisan is *not*. It is not a workflow language. It
is not a job scheduler. It is not a storage engine. It is the layer that sits
between the researcher's intent and the computational infrastructure, handling
the concerns that every pipeline eventually needs but no individual pipeline
wants to build.

---

## Domains Beyond Protein Design

Artisan was developed for computational protein design, but the problem shape
it addresses — a population of candidates flowing through a multi-stage funnel
of transformation, evaluation, filtering, and iteration — appears across many
fields.

The strongest fits share these properties:

| Property                          | Why Artisan helps                                  |
| --------------------------------- | -------------------------------------------------- |
| Expensive computation             | Caching avoids redundant work                      |
| Many candidates processed in batch| Batched dispatch to worker pools or clusters        |
| Iterative parameter exploration   | Cache + provenance = "what have I already tried"    |
| Multi-stage with branching/merging| DAG model with filter, merge, and composite support |
| Traceability requirements         | Automatic provenance satisfies audit/reproducibility needs |
| Team-shared infrastructure        | Integrate once, compose freely                     |

### Natural fits

**Drug discovery / computational chemistry.** Generate candidate molecules,
dock, predict ADMET properties, score, select leads. Same funnel shape as
protein design. Regulatory submissions (FDA, EMA) increasingly want
computational audit trails showing how candidates were selected — the
provenance graph is that audit trail.

**Materials discovery.** Generate crystal structures or alloy compositions, run
DFT calculations, predict properties, filter. DFT is expensive (hours per
calculation on HPC), so caching is valuable. The field is moving toward
high-throughput computational screening, which is exactly
batch-of-artifacts-through-operations.

**Genomics and variant analysis.** Sample QC, alignment, variant calling,
annotation, filtering, cohort analysis. Existing tools (Nextflow, Snakemake)
are file-centric workflow managers. Artisan's differentiator is artifact-level
provenance — tracing a specific variant call back through the exact alignment,
reference, and caller version that produced it. Clinical genomics has strict
traceability requirements.

**ML training data curation.** Collect raw data, clean, deduplicate, filter,
annotate, score quality, assemble datasets. The curator operations (Filter,
Merge, Ingest) map directly. Content addressing means you know exactly which
data points went into which training set. This matters for data governance and
model auditing under emerging regulation (EU AI Act).

**Climate and earth science modeling.** Data ingestion, downscaling, simulation,
post-processing, analysis. Long-running simulations on HPC clusters, parameter
sensitivity studies, and a strong reproducibility culture — results back policy
decisions.

**Computational fluid dynamics / engineering simulation.** Mesh generation,
simulation, post-processing, analysis, with parameter sweeps across geometries
or boundary conditions. Each simulation is expensive. Engineers iterate on
designs. Caching means they don't re-simulate configurations already explored.

**Synthetic biology / metabolic engineering.** Design metabolic pathways, run
flux balance analysis, score strains, rank candidates for wet lab. Same
generate-score-filter funnel, at the pathway level instead of the sequence
level.

### Less obvious fits

**Financial risk modeling.** Scenario generation, Monte Carlo simulation, risk
metric computation, aggregation, reporting. Banks are required by regulators
(Basel III, SOX) to demonstrate full audit trails for risk calculations. The
provenance graph is literally what auditors ask for.

**Digital pathology.** Tile whole-slide images, extract features, classify,
aggregate, produce diagnostic scores. Batch-heavy, HPC-relevant, and clinical
validation requires knowing exactly which image tiles contributed to which
diagnosis.

**Semiconductor design space exploration.** Generate circuit or layout
candidates, simulate, evaluate performance/power/area, filter, optimize. IP
documentation requires traceability.

### Anti-patterns

Artisan is not a good fit when:

- **Streaming / real-time.** Artisan is batch-oriented. Sub-second latency is
  not the goal.
- **Single-item, single-step.** If the workflow is "run one thing once," the
  framework overhead isn't justified.
- **Pure ETL with no branching.** If the pipeline is a straight line with no
  filtering, merging, or parameter variation, simpler tools suffice.
- **Provenance doesn't matter.** If nobody will ever ask "where did this output
  come from," a core value proposition is unused.

---

## How Agents Fit In

Artisan is not an agent framework. It does not reason. It does not hypothesize.
What it does is create the conditions in which reasoning can happen more
productively — the same way an operating system does not write programs but
creates the conditions in which programs can run reliably.

### The problem with agents today

Agentic systems are capable reasoners but poor record-keepers. Their
understanding of their own work is typically trapped in ephemeral context: once
a session ends, the record of what was tried, what succeeded, and what was
learned disappears. This makes iterative computational exploration — the
*hypothesize, compute, analyze, repeat* loop that defines scientific work —
fragile and hard to scale.

When an agent calls a tool today, the interaction is fire-and-forget: string
in, string out, no persistent record beyond the conversation, no deduplication,
no way to query "what have I already tried," no recovery if the agent crashes
halfway through. The agent's memory of its own work is whatever fits in the
context window.

### What Artisan provides

Artisan gives an agent a durable, structured, queryable substrate for
computational work:

- **The agent never loses track of what it's done.** The provenance graph is a
  complete, queryable history of every computation, every parameter choice,
  every intermediate result. The agent doesn't need to remember — it queries
  the store.

- **The agent never re-runs what it's already tried.** Content-addressed
  caching means if the agent (or a previous agent run) already computed
  something with identical inputs and parameters, it gets the cached result
  instantly. Redundant work is structurally impossible.

- **The agent can recover from failures.** Long-running agentic loops fail.
  When the agent restarts, `pipeline.resume()` picks up from the last
  committed step. No manual checkpointing, no lost work.

- **The agent can scale computation.** An agent that needs to evaluate 1,000
  candidates shouldn't do them one at a time through serial tool calls. It
  calls `pipeline.run()` once and gets back 1,000 results.

- **Multiple agents can share state safely.** Two agents working on related
  problems can read from the same Delta Lake store. ACID transactions prevent
  corruption. Content addressing means independent agents that produce the
  same result don't create duplicates.

- **The agent can navigate the system directly.** Because the system is
  explicit — no implicit knowledge, no hidden state — the operation contracts
  describe what tools exist and what they do, and the artifact store describes
  what has already been computed.

### The division of labor

The `pipeline.run()` API is already incremental: call it, get results, inspect
them, decide what to run next. This is a natural agent loop with the reasoning
step removed:

```python
# The agent decides what to generate
step0 = pipeline.run(Generate, params=agent_chosen_params)

# The agent analyzes results
results = query_metrics(step0)

# The agent reasons about what to do next
if agent_decides_to_filter:
    step1 = pipeline.run(Filter, inputs=..., params=agent_chosen_criteria)
else:
    step1 = pipeline.run(Transform, inputs=..., params=agent_chosen_params)
```

The agent makes decisions between steps. Artisan handles everything within a
step. The agent supplies judgment. Artisan supplies memory, reproducibility,
and structure. Neither does the other's job.

### Contrast with agentic tool use

| Concern                | Typical agent tool call       | Agent + Artisan                         |
| ---------------------- | ----------------------------- | --------------------------------------- |
| Input/output typing    | Strings                       | Typed, validated artifacts              |
| Execution record       | Ephemeral conversation context| Persistent Delta Lake tables            |
| Batching               | One call, one result          | One call, thousands of results          |
| Caching                | Call it again, run it again   | Content-addressed, never recompute      |
| Provenance             | None                          | Automatic input-to-output lineage       |
| Post-hoc analysis      | Scroll through chat logs      | Structured queries with Polars          |
| Crash recovery         | Start over                    | Resume from last committed step         |
| Multi-agent sharing    | Copy-paste between sessions   | Shared ACID-transactional store         |

---

## Summary

Artisan separates thinking from execution. It takes the concerns that every
computational workflow eventually needs — reproducibility, caching, provenance,
scaling, structured storage — and makes them the framework's problem instead of
the researcher's problem.

This separation is what makes it useful both for human researchers who want to
focus on their science and for AI agents that need a durable substrate for
iterative computational exploration. In both cases, the value proposition is
the same: the system remembers what was done, never does it twice, and makes
the full history available for whatever comes next.
