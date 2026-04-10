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
│  Resolve inputs    │  │  Set up sandbox          │  │  Verify staging files  │
│  Pair multi-inputs │  │  Materialize inputs      │  │  Capture worker logs   │
│  Compute cache key │──│  Run operation lifecycle │──│  Collect staged files  │
│  Check cache       │  │  Capture lineage         │  │  Deduplicate           │
│  Batch + dispatch  │  │  Stage to Parquet        │  │  Write to Delta Lake   │
│                    │  │                          │  │  Compact tables        │
│                    │  │                          │  │  Return StepResult     │
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
`step.output("role")` on a `StepResult` or `StepFuture`), the orchestrator
resolves it to concrete artifact IDs by querying execution edges in Delta Lake.
The result is a sorted, deduplicated list of artifact IDs per role.

**Why sort?** Determinism. Cache keys derive from artifact IDs, so the same set
of inputs must always produce the same key regardless of the order workers
completed in the prior step.

**Empty input handling.** When every input role resolves to zero artifact IDs,
the step is skipped entirely. The orchestrator records a "skipped" result
(with the reason `empty_inputs`) in the steps table so that downstream steps
and resume logic know the step was attempted but produced no work.

### Multi-input pairing

Operations consuming multiple input roles need their artifacts aligned. The
orchestrator [pairs inputs](operations-model.md#pairing-strategies) according
to the operation's `group_by` strategy before batching, so batch boundaries
respect paired groups.

Three strategies are available:

- **ZIP** -- positional pairing (first with first, second with second). All
  roles must have the same length.
- **LINEAGE** -- provenance-aware matching. Artifacts sharing common ancestry
  in the provenance graph are paired together. Requires exactly two input roles.
- **CROSS_PRODUCT** -- all combinations of artifacts across roles. Useful when
  every combination is meaningful.

For operations with a primary input role (such as Filter), a variant called
anchor-based matching pairs each primary artifact independently against every
other role, retaining only complete matches.

### Two-level caching

The framework checks two caches before any computation runs. Each level exists
because it skips different amounts of work.

**Step-level cache** checks first. The `step_spec_id` hashes the operation
name, step number, upstream step spec IDs and roles (from `OutputReference`
pointers), parameters, and config overrides (environment, tool). A hit here skips the
entire step -- no input resolution, no batching, no worker dispatch. The
orchestrator returns a previously recorded `StepResult` from the steps Delta
table.

**Execution-level cache** checks per batch. The `execution_spec_id` hashes the
operation name, sorted input artifact IDs across all roles, parameters, and
config overrides. A hit skips that batch while other batches in the same step
may still execute.

```
prev = pipeline.run(operation=PrevOp, ...)
pipeline.run(operation=MyOp, inputs={"data": prev.output("data")})
    │
    ▼
step_spec_id = hash(op_name | step_number | upstream_spec_ids_and_roles | params | config)
    │
    ├── HIT:  return cached StepResult (skip everything)
    │
    └── MISS: resolve inputs → pair → batch → per-batch:
                  │
                  execution_spec_id = hash(op_name | sorted_artifact_ids | params | config)
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
creator, curator, and composite paths diverge here because they optimize for
different workloads.

### Creator operations: the sandbox lifecycle

Each creator execution gets an isolated sandbox on disk:

```
{working_root}/{N}_{op_name}/{ab}/{cd}/{execution_run_id}/
    materialized_inputs/    # Input artifacts written to disk
    preprocess/             # Preprocess working directory
    execute/                # Execute output directory
    postprocess/            # Postprocess working directory
    tool_output.log         # Captured tool stdout/stderr
```

The `{ab}/{cd}` directories (first four characters of the run ID, split into
two levels) distribute sandboxes across the filesystem, avoiding inode
contention on HPC shared filesystems.

The framework runs the [three-phase creator lifecycle](operations-model.md#the-creator-lifecycle)
within this sandbox, with two additional runtime steps:

**Materialize inputs** (before preprocess). Artifact content is written to disk
in the `materialized_inputs/` directory. Config artifacts are materialized last
because they may contain `$artifact` references that resolve to paths of other
materialized artifacts. Operations can request format conversion at this stage
(e.g., materializing with a different file extension) via the `materialize_as`
field on `InputSpec`.

**Finalize** (after postprocess). The framework computes content-addressed IDs
for all draft artifacts (`artifact_id = xxh3_128(content)`). After this point,
artifacts have their permanent identity.

After finalization, the sandbox is cleaned up unless `preserve_working` is set
in the pipeline configuration.

### Curator operations: the lightweight path

[Curators](operations-model.md#the-curator-lifecycle) skip the sandbox entirely
-- no materialization, no three-phase lifecycle, no remote worker dispatch. The
orchestrator spawns a local subprocess (via `ProcessPoolExecutor` with the
`spawn` context) for memory isolation and runs the curator flow with input
DataFrames rather than on-disk artifacts. This eliminates the overhead of
sandboxing and remote dispatch that would add latency with no benefit for
metadata-only operations like Filter and Merge.

**Why a subprocess?** Curator operations can load large DataFrames into memory.
Running them in a subprocess means the operating system reclaims all memory when
the subprocess exits, preventing gradual memory growth in the orchestrator. If
the subprocess is killed (typically by the OOM killer), the framework detects
the broken process pool, captures diagnostic information (peak RSS, system
memory), and stages a failure record rather than crashing the pipeline.

### Composite execution: collapsed and expanded modes

When multiple creator operations are composed into a
[composite](operations-model.md), they can execute in two modes.

In **collapsed** mode, the composite runs within a single worker process.
Each `ctx.run()` call executes its operation eagerly through the standard
creator lifecycle (`run_creator_lifecycle`), and artifacts are passed
in-memory to subsequent operations via `ArtifactSource` objects, avoiding
Delta Lake round-trips.

In **expanded** mode, each `ctx.run()` call delegates to the parent pipeline
as a separate step, giving each internal operation its own dispatch-execute-commit
cycle with full parallelism and independent failure handling.

In both modes, the `intermediates` policy controls what happens to artifacts
produced by non-final operations:

- **DISCARD** (default) -- only the final operation's artifacts are committed.
  Shortcut provenance edges link the composite's initial inputs directly to
  its final outputs.
- **PERSIST** -- intermediate artifacts and their internal provenance edges are
  also committed, preserving the full composite lineage.
- **EXPOSE** -- like PERSIST, but provenance edges for intermediates are marked
  with `step_boundary=True` so they appear in step-level provenance queries.

For the full conceptual model of composites, see
[Composites and Composition](composites-and-composition.md).

### Compute routing

The execute() call is the routing boundary for compute targets. Everything
before it (sandbox setup, input materialization) and after it (lineage capture,
staging to Parquet) runs on the worker. Only the execute() call itself can be
routed to a remote target.

```
  Workers
┌──────────────────────────┐
│        EXECUTE           │
│                          │
│  Set up sandbox          │
│  Materialize inputs      │
│  ┌────────────────────┐  │
│  │  execute() ────────│──│──→ [Compute target]
│  │  (routing boundary)│  │     Local | Modal
│  └────────────────────┘  │
│  Capture lineage         │
│  Stage to Parquet        │
└──────────────────────────┘
```

Compute routing is orthogonal to the dispatch backend. A local worker can
route execute() to Modal; a SLURM worker can also route execute() to Modal.
The dispatch backend controls where the worker process runs. The compute
target controls where execute() runs inside that worker.

When compute is `"local"` (the default), execute() runs as a direct call
inside the worker process -- today's behavior. When compute is `"modal"`,
the framework serializes the operation and inputs via cloudpickle, ships
them to a Modal container, runs execute() there, and returns the results.
File-based operations additionally transport sandbox files (up to 50 MB per
direction).

---

## Lineage capture

After postprocess, the framework captures artifact provenance -- which specific
input produced which specific output. This happens during execution because
the context needed for matching (filename stems, pairing order, declarations)
is lost once execution completes.

The framework uses [filename stem matching](provenance-system.md#the-algorithm)
to infer which input produced which output. Each output's lineage source is
declared via [`infer_lineage_from`](operations-model.md#output-specs) on
`OutputSpec`, which the framework validates at class definition time.

When multi-input pairing is active (`group_by` is set), the framework creates
co-input edges from all paired input roles at the matched index. Each co-input
edge carries a `group_id` that links it to the rest of its paired group.

---

## Staging: the contract between workers and orchestrator

Workers never write to Delta Lake. Instead, each worker writes Parquet files to
an isolated staging directory -- one file per table type, with `executions.parquet`
written last as a sentinel. The orchestrator collects these after all workers
complete and commits them atomically.

This [staging-commit pattern](storage-and-delta-lake.md#the-staging-commit-pattern)
eliminates write conflicts, ensures atomic visibility, and tolerates worker
failures. See the storage page for the full directory layout, sharding strategy,
and NFS consistency handling.

### Staging verification

On distributed filesystems (NFS), directory attribute caching can delay
visibility of files written by SLURM workers. Before committing, the
orchestrator polls for `executions.parquet` sentinel files using
close-to-open consistency checks (`open()` + `read()` rather than `stat()`)
with exponential backoff. This verification runs only when the backend reports
a shared filesystem; local backends skip it entirely.

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

### Worker log capture

For SLURM backends, worker stdout/stderr is captured after dispatch completes
and patched into the `executions.parquet` staging files before commit. Failed
executions also get human-readable log files written to a per-step directory
under `logs/failures/`. This happens on a best-effort basis -- missing logs
never block the commit.

---

## Step tracking

The orchestrator records each step's lifecycle in the steps Delta table. A
"running" row is written before dispatch. After completion, it is updated to
"completed", "skipped", "cancelled", or "failed" with timing and count metadata.
This table serves three purposes:

- **Step-level caching** -- the `check_cache` query scans this table for a
  completed step matching the same `step_spec_id`.
- **Resume** -- `load_completed_steps` returns all completed or skipped steps
  from a prior run so the pipeline can skip them on restart.
- **Observability** -- the table records `pipeline_run_id`, operation class,
  parameters, compute backend, and timing for every step ever executed.

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
| Subprocess OOM (curator) | Broken pool detected | Synthetic failure record staged with diagnostics |

**Double-fault protection.** If staging a failure record itself fails, the error
is folded into the `StagingResult` so the caller always gets a value. The
original error and the staging error are combined into a single message.

**Failure logs.** Every failed execution writes a human-readable log file
containing the run ID, operation name, step number, backend, timestamp, full
traceback, and (when available) tool output. These live in
`logs/failures/step_{N}_{op_name}/` alongside the Delta tables.

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

## Cancellation

The framework supports cooperative cancellation through `pipeline.cancel()` or
signal handling (SIGINT/SIGTERM). Cancellation is checked between step phases
-- a step that is mid-execution completes its current phase before stopping,
so no partial writes occur.

### Cancel checkpoints

The cancel event is checked at multiple gates:

| Checkpoint | Effect |
|-----------|--------|
| Before step dispatch | Step skipped with `skip_reason="cancelled"` |
| After waiting for predecessors | Step skipped before any work begins |
| Between execute and commit phases | Step returns a cancelled result |
| Inside curator subprocess polling | Curator stops waiting for results |

### Signal escalation

When running from a terminal, the framework installs signal handlers on the
first dispatched step. These implement a three-press escalation:

| Press | Effect |
|-------|--------|
| First Ctrl+C | Graceful cancellation -- current step drains, remaining steps skip |
| Second Ctrl+C | Restores Python's default signal handlers |
| Third Ctrl+C | Raises `KeyboardInterrupt`, force-killing the process |

Worker child processes ignore SIGINT (via `SIG_IGN` in the process pool
initializer), so only the orchestrator handles the signal. In Jupyter
notebooks, signal handlers are not installed -- use `pipeline.cancel()`
directly.

### SLURM cancellation

On SLURM, the dispatch handle calls `scancel --name` automatically when
cancellation is triggered, killing in-flight jobs by their SLURM job name.
This works even before job IDs are returned, because the job name is set
at submission time. No manual `scancel` is needed.

### Cache interaction

Cancelled steps are recorded with `status="cancelled"` in the steps Delta
table. They are excluded from cache lookups, so re-running the same pipeline
re-executes cancelled steps while completed steps load from cache.

---

## Key design decisions

| Decision | Rationale |
|----------|-----------|
| Two-level caching (step + execution) | Step-level is fast but coarse; execution-level catches fine-grained reuse |
| Two-level batching (artifacts per unit + units per worker) | Separates logical batching from cluster adaptation |
| Curator subprocess isolation | Prevents memory leaks from accumulating in the long-lived orchestrator process |
| Staging verification with close-to-open consistency | NFS attribute caching can hide files; `stat()` is not sufficient |
| Default continue-on-failure | Large runs expect occasional failures; successful results should not be discarded |
| Composite collapsed mode | Avoids Delta Lake round-trips for tightly coupled operations |
| Steps Delta table | Enables step-level caching, resume, and observability without additional infrastructure |

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
- [SLURM Execution Tutorial](../tutorials/execution/07-slurm-execution.ipynb) -- Run operations on a SLURM cluster
- [Pipeline Cancellation Tutorial](../tutorials/execution/08-pipeline-cancellation.ipynb) -- Cooperative cancellation in action
- [Compute Routing Tutorial](../tutorials/execution/13-compute-routing.ipynb) -- Route execute() to local or remote compute targets
