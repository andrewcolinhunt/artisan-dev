# Execution Flow

When you call `pipeline.run()`, a cascade of coordinated work happens between
the orchestrator and workers before results appear in Delta Lake. Understanding
this flow explains why cache hits are free, why partial failures never corrupt
your data, why lineage must be captured during execution rather than after, and
where to look when something goes wrong.

This page walks through the lifecycle of a pipeline step and the design
decisions that shape each phase.

---

## Three phases, two roles

Each pipeline step flows through three phases split across two runtime roles:

```
  Orchestrator                    Workers                     Orchestrator
┌────────────────────┐  ┌──────────────────────────┐  ┌────────────────────────┐
│      DISPATCH      │  │        EXECUTE           │  │        COMMIT          │
│                    │  │                          │  │                        │
│  Resolve inputs    │  │  Set up sandbox          │  │  Collect staged files   │
│  Compute cache key │  │  Materialize inputs      │  │  Deduplicate           │
│  Check cache       │──│  Run operation lifecycle │──│  Write to Delta Lake   │
│  Batch + dispatch  │  │  Capture lineage         │  │  Return StepResult     │
│                    │  │  Stage to Parquet        │  │                        │
└────────────────────┘  └──────────────────────────┘  └────────────────────────┘
```

The orchestrator has a global view of the pipeline and exclusive write access
to Delta Lake. Workers run in isolation, possibly on remote cluster nodes, with
no shared mutable state. The staging directory is the contract between them.
For the rationale behind this split, see
[Architecture Overview](architecture-overview.md#the-orchestrator-worker-split).

---

## Dispatch: preparing work

The orchestrator's job is to figure out what needs to run and whether it needs
to run at all.

### Input resolution

When a step receives an `OutputReference` (the lazy pointer returned by
`output("step_name", "role")`), the orchestrator resolves it to concrete artifact IDs
by querying execution edges in Delta Lake. The result is a sorted, deduplicated
list of artifact IDs per role.

**Why sort?** Determinism. Cache keys derive from artifact IDs, so the same set
of inputs must always produce the same key regardless of the order workers
completed in the prior step.

### Multi-input pairing

Operations consuming multiple input roles need their artifacts aligned. The
orchestrator [pairs inputs](operations-model.md#pairing-strategies) according
to the operation's `group_by` strategy before batching, so batch boundaries
respect paired groups.

### Two-level caching

The framework checks two caches before any computation runs. Each level exists
because it skips different amounts of work.

**Step-level cache** checks first. The `step_spec_id` hashes the operation
name, step number, upstream step spec IDs, parameters, and command overrides.
A hit here skips the entire step -- no input resolution, no batching, no worker
dispatch. The orchestrator returns a pre-resolved `StepResult` immediately.

**Execution-level cache** checks per batch. The `execution_spec_id` hashes the
operation name, sorted input artifact IDs, parameters, and command overrides.
A hit skips that batch while other batches in the same step may still execute.

```
pipeline.run(operation=MyOp, inputs={"data": output("prev_step", "data")})
    │
    ▼
step_spec_id = hash(op_name | step_number | upstream_spec_ids | params)
    │
    ├── HIT:  return cached StepResult (skip everything)
    │
    └── MISS: resolve inputs → batch → per-batch:
                  │
                  execution_spec_id = hash(op_name | sorted_artifact_ids | params)
                      │
                      ├── HIT:  skip this batch
                      └── MISS: dispatch to worker
```

**Why two levels?** Step-level caching is fast (one Delta table scan, no input
resolution) but coarse -- it only hits when the exact same step runs in the
exact same pipeline position with the exact same upstream results. Execution-level
caching is finer-grained -- it catches reuse even when the pipeline structure
changes, as long as the specific inputs and parameters match. Neither can
replace the other.

**Why both keys are deterministic:** Artifact IDs are content hashes. Same
content, same ID, same cache key. No false hits (any input change invalidates),
no false misses (identical computation always matches). No manual invalidation
needed.

### Batching and dispatch

Inputs that survive the cache check are partitioned into `ExecutionUnit`
objects -- the sealed packages that travel to workers.

**Level 1 batching** (`artifacts_per_unit`) controls how many artifacts each
unit processes. An ML inference operation might set this to 1 (one structure per
GPU job). A metrics calculation might set it to 100 (batch for efficiency).

**Level 2 batching** (`units_per_worker`) controls how many units a single SLURM
job processes. This adapts to cluster characteristics without changing the
operation.

Each `ExecutionUnit` carries the fully configured operation instance (not a class
reference), the batch of artifact IDs, the cache key, the step number, and any
group IDs from pairing. Workers need nothing else to execute.

---

## Execute: running operations

Workers receive `ExecutionUnit` objects and run the operation lifecycle. The
creator and curator paths diverge here because they optimize for different
workloads.

### Creator operations: the sandbox lifecycle

Each creator execution gets an isolated sandbox on disk:

```
{working_root}/{step}_{op_name}/{shard}/{execution_run_id}/
    materialized_inputs/    # Input artifacts written to disk
    preprocess/             # Preprocess working directory
    execute/                # Execute output directory
    postprocess/            # Postprocess working directory
```

The `{shard}` directories (two levels of hex prefix from the run ID) distribute
thousands of sandboxes across the filesystem, avoiding inode contention on HPC
shared filesystems.

The framework runs the [three-phase creator lifecycle](operations-model.md#the-creator-lifecycle)
within this sandbox, with two additional runtime steps:

**Materialize inputs** (before preprocess). Artifact content is written to disk
in the `materialized_inputs/` directory. Config artifacts are materialized last
because they may contain `$artifact` references that resolve to paths of other
materialized artifacts. Operations can request format conversion at this stage
(e.g., materializing a JSON record as CSV) via the `materialize_as` field
on `InputSpec`.

**Finalize** (after postprocess). The framework computes content-addressed IDs
for all draft artifacts (`artifact_id = xxh3_128(content)`). After this point,
artifacts have their permanent identity.

### Curator operations: the lightweight path

[Curators](operations-model.md#the-curator-lifecycle) skip the sandbox entirely
— no materialization, no three-phase lifecycle, no worker dispatch. A single
`execute_curator` call runs on the orchestrator with artifacts in memory. This
eliminates overhead that would add latency with no benefit for metadata-only
operations like Filter and Merge.

---

## Lineage capture

After postprocess, the framework captures artifact provenance — which specific
input produced which specific output. This happens during execution because
the context needed for matching (filename stems, pairing order, declarations)
is lost once execution completes.

The framework uses [filename stem matching](provenance-system.md#the-algorithm)
to infer which input produced which output. Each output's lineage source is
declared via [`infer_lineage_from`](operations-model.md#output-specs) on
`OutputSpec`, which the framework validates at class definition time.

---

## Staging: the contract between workers and orchestrator

Workers never write to Delta Lake. Instead, each worker writes Parquet files to
an isolated staging directory — one file per table type, with `executions.parquet`
written last as a sentinel. The orchestrator collects these after all workers
complete and commits them atomically.

This [staging-commit pattern](storage-and-delta-lake.md#the-staging-commit-pattern)
eliminates write conflicts, ensures atomic visibility, and tolerates worker
failures. See the storage page for the full directory layout, sharding strategy,
and NFS consistency handling.

---

## Commit: atomic persistence

After all workers complete, the orchestrator collects staged Parquet files and
commits them to Delta Lake. Tables are committed in a
[specific order](storage-and-delta-lake.md#commit-ordering) (content before
index before provenance before execution records) so that partial failures
leave recoverable state rather than broken references.

During commit, content-addressed
[deduplication](storage-and-delta-lake.md#deduplication-during-commit) drops
artifacts that already exist in storage. After commit, optional compaction
merges small Parquet files into larger ones for better read performance.

---

## Error handling across phases

Errors are caught at different boundaries depending on where they occur, with a
consistent principle: preserve as much information as possible and fail as
early as possible.

| Where | What happens | What's preserved |
|-------|-------------|-----------------|
| Validation (before dispatch) | Raised immediately in `submit()` | Nothing dispatched, no step recorded |
| Execute phase (worker) | Caught, failure staged | Input edges + failure record in staging |
| Postprocess/lineage (worker) | Caught, failure staged | Same as execute failure |
| Dispatch infrastructure | Caught in step executor | Error recorded in step metadata |
| Commit (orchestrator) | Per-table error logging | Successfully committed tables preserved |

The `failure_policy` controls what happens when some batches fail within a step:

- **`continue`** (default): The step succeeds if any batches succeeded. Failed
  batches are recorded but do not block downstream steps from consuming the
  successful results.
- **`fail_fast`**: Any batch failure immediately stops the step and raises an
  error.

**Why default to continue?** In large pipeline runs (thousands of artifacts),
occasional failures are expected -- a single malformed input should not discard
thousands of successful results. The failure records are always preserved for
diagnosis.

---

## Key design decisions

| Decision | Rationale |
|----------|-----------|
| Two-level caching (step + execution) | Step-level is fast but coarse; execution-level catches fine-grained reuse |
| Two-level batching (artifacts per unit + units per worker) | Separates logical batching from cluster adaptation |
| Default continue-on-failure | Large runs expect occasional failures; successful results should not be discarded |

---

## Cross-references

- [Architecture Overview](architecture-overview.md) -- System structure, five
  layers, and the orchestrator-worker mental model
- [Operations Model](operations-model.md) -- Two operation types, three-phase
  lifecycle, spec system
- [Provenance System](provenance-system.md) -- Dual provenance, stem matching
  algorithm, co-input edges
- [Storage and Delta Lake](storage-and-delta-lake.md) -- Table layout, the
  staging-commit pattern, querying with Polars
- [Design Principles](design-principles.md) -- Foundational rationale for
  content addressing, scale transparency, fail-fast validation
- [First Pipeline Tutorial](../tutorials/getting-started/01-first-pipeline.ipynb) -- See the execution flow in action
- [SLURM Execution Tutorial](../tutorials/getting-started/04-slurm-execution.ipynb) -- Run operations on a SLURM cluster
