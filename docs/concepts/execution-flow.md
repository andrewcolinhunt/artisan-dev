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
Upstream outputs are sorted and deduplicated because they represent a set.
Explicit caller-provided lists retain their order and duplicates because those
may control ZIP pairing or batching.

Resolution is only the first part of input preparation. The orchestrator also
loads each ID's concrete type, rejects missing or contradictory index entries,
hydrates enough content to validate stored identities, verifies external bytes,
and applies the operation's grouping strategy. Both cache levels and worker
dispatch consume this same prepared snapshot.

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

**Step-level cache** hashes the operation name, step number, the complete
prepared input snapshot, parameters, and effective execution configuration. A
hit skips batching and worker dispatch. The orchestrator returns a previously
recorded outcome under a fresh step-run ID owned by the current pipeline run.
It also links that current step to the source's actual executions; it never
reuses the source step-run ID.

**Execution-level cache** checks per batch. The `execution_spec_id` hashes the
same kind of concrete input occurrences, sliced to that batch, plus the
operation, parameters, and effective configuration. A hit skips that batch
while other batches in the same step may still execute. The current step
records a link to every reused execution, so all-cached and mixed steps expose
the same complete output set as fully executed steps.

Each input occurrence includes its role, group ID, role-local position,
artifact type, and artifact ID. Mapping insertion order does not matter, but
item order, pairing, role assignment, type, and multiplicity do.

```
prev = pipeline.run(operation=PrevOp, ...)
pipeline.run(operation=MyOp, inputs={"data": prev.output("data")})
    │
    ▼
resolve -> type-check -> verify -> group
    |
    v
step_spec_id = hash(op_name | step_number | concrete_typed_occurrences | params | config)
    │
    ├── HIT:  link current step to source executions; return cached outcome
    │
    └── MISS: batch → per-batch:
                  │
                  execution_spec_id = hash(op_name | batch_typed_occurrences | params | config)
                      │
                      ├── HIT:  link current step to cached execution
                      └── MISS: dispatch to worker
```

**Why two levels?** Step-level caching is coarse and can skip the whole step
after preparation. Execution-level caching is finer-grained: it catches reuse
when only some batches match. Both share one input truth, so they cannot disagree
because one hashed symbolic references while the other hashed concrete data.

Every submitted step has a fresh UUID-based attempt ID, regardless of whether
it executes, is cached, is skipped, is cancelled, or fails. Step numbers remain
the logical sequence within a pipeline run. Cache reuse changes neither: the
current step owns its attempt ID and refers directly to existing execution IDs.

**Why both keys are deterministic:** Artifact IDs identify typed semantic
content, and the occurrence sequence captures invocation semantics. The same
prepared invocation produces the same keys without depending on worker
completion order or storage URI.

### Batching and dispatch

Inputs that survive the cache check are partitioned into `ExecutionUnit`
objects -- the sealed packages that travel to workers.

**Level 1 batching** (`artifacts_per_unit`) controls how many artifacts each
unit processes. An ML inference operation might set this to 1 (one structure per
GPU job). A metrics calculation might set it to 100 (batch for efficiency).

**Level 2 batching** (`units_per_worker`) controls how many units one worker
processes sequentially. Core applies this policy before dispatch, so local and
external runners use identical packing semantics.

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

**Finalize** (after postprocess). The framework computes a versioned ID from
each draft's registered type, canonical content, and semantic metadata. It then
protects durable fields and saves an identity snapshot for later integrity
checks.

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

### Composite execution

When multiple creator operations are composed into a
[composite](operations-model.md), running the composite expands it into
real pipeline steps. Each `ctx.run()` call delegates to the parent
pipeline as its own step, giving each internal operation its own
dispatch-execute-commit cycle with full parallelism and independent
failure handling.

There is no separate composite runtime. A composite is a named grouping
over ordinary steps, so every internal operation participates in the same
caching, provenance, and cancellation machinery as any other step.

For the full conceptual model of composites, see
[Composites and Composition](composites-and-composition.md).

### Compute routing

The execute phase is the routing boundary for compute targets. Everything
before it (sandbox setup, input materialization) and after it (lineage capture,
staging to Parquet) runs on the worker. Only the execute phase itself can be
routed to a remote target.

```
  Workers
┌──────────────────────────┐
│        EXECUTE           │
│                          │
│  Set up sandbox          │
│  Materialize inputs      │
│  ┌────────────────────┐  │
│  │  execute ──────────│──│──→ [Compute target]
│  │  (routing boundary)│  │     Local | Modal
│  └────────────────────┘  │
│  Capture lineage         │
│  Stage to Parquet        │
└──────────────────────────┘
```

Compute routing is orthogonal to the step runner. A local worker can
route the execute phase to Modal; a SLURM worker can also route the execute phase to Modal.
The step runner controls where the worker process runs. The compute
target controls where the execute phase runs inside that worker.

When compute is `"local"` (the default), the execute phase runs as a direct call
inside the worker process -- today's behavior. When compute is `"modal"`,
the execute phase becomes an HTTP client of the operation's deployed tool endpoint:
it submits the op's params + input files, polls until the tool finishes,
and downloads the output files back into the execute dir. Only command ops
can route to modal -- an op with a `ToolSpec` + `execute_command()`, or one
that sets `execute_as_tool=True`. Inline transfers are bounded at 100 MB per
direction; `s3://` inputs pass by reference.

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
with exponential backoff. This verification runs only when the step runner
reports a shared filesystem; local step runners skip it entirely.

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

Runner providers can attach worker stdout/stderr to each `UnitResult` before
collection completes. Artisan then patches those logs into the
`executions.parquet` staging files before commit. Failed executions also get
human-readable log files written to a per-step directory under `logs/failures/`.
This happens on a best-effort basis -- missing logs never block the commit.

---

## Step tracking

The orchestrator records each step attempt as immutable snapshots in the steps
Delta table. Once API-shape validation accepts a submission, a fresh
`step_run_id` is assigned and `pending` is written before hashing or other
operational work. Execution writes `running`; the attempt then reaches exactly
one terminal status: `succeeded`, `partial`, `failed`, `cancelled`, or `skipped`.
Terminal snapshots never transition again.

The valid lifecycle edges are:

```text
pending -> running -> succeeded | partial | failed | cancelled
       \-> skipped
       \-> cancelled
```

This table serves three purposes:

- **Step-level caching** -- the `check_cache` query scans this table for a
  `succeeded` attempt matching the same `step_spec_id`, or a `partial` attempt
  when `CachePolicy.STEP_COMPLETED` is selected.
- **Resume** -- `load_resumable_steps` restores `succeeded`, `partial`, and
  `skipped` steps. It refuses a run with an unresolved `pending` or `running`
  attempt rather than guessing an outcome.
- **Observability** -- the table records `pipeline_run_id`, operation class,
  parameters, step runner, lifecycle sequence, and timing for every accepted
  attempt.

---

## Error handling across phases

Errors are caught at different boundaries depending on where they occur, with a
consistent principle: preserve as much information as possible and fail as
early as possible.

| Where | What happens | What's preserved |
|-------|-------------|-----------------|
| API-shape validation | Raised immediately in `submit()` | Nothing dispatched, no attempt accepted |
| Operational preparation after acceptance | Attempt becomes `failed` | Fresh attempt ID + error |
| Execute phase (worker) | Caught, failure staged | Input edges + failure record in staging |
| Postprocess/lineage (worker) | Caught, failure staged | Same as execute failure |
| Dispatch infrastructure | Caught in step executor | Failed step + single `error` diagnostic |
| Commit (orchestrator) | Attempt becomes `failed` | Error plus any physical records already written |
| Subprocess OOM (curator) | Broken pool detected | Synthetic failure record staged with diagnostics |

**Double-fault protection.** If staging a failure record itself fails, the error
is folded into the `StagingResult` so the caller always gets a value. The
original error and the staging error are combined into a single message.

**Failure logs.** Every failed execution writes a human-readable log file
containing the run ID, operation name, step number, step runner, timestamp, full
traceback, and (when available) tool output. These live in
`logs/failures/YYYYMMDD/YYYYMMDDTHHMMSSffffffZ_executionID.log` alongside
local Delta tables, named by the source execution's UTC start time. Cloud stores
keep these files under the local working root.

The `failure_policy` controls what happens when some batches fail within a step:

- **`continue`** (default): A known mixture becomes `partial`. Failed batches
  are recorded, and downstream steps can consume the successful results. If
  every batch fails, the step becomes `failed`.
- **`fail_fast`**: Any observed batch failure makes the step `failed` and its
  outputs unavailable. Work that already finished remains recorded for audit.

**Why default to continue?** In large pipeline runs (thousands of artifacts),
occasional failures are expected -- a single malformed input should not discard
thousands of successful results. The failure records are always preserved for
diagnosis.

---

## Cancellation

The framework supports cooperative cancellation through `pipeline.cancel()` or
signal handling (SIGINT/SIGTERM). Cancellation evidence progresses separately
from lifecycle state: `requested` must become `confirmed`, `rejected`, or
`unknown`. Only confirmed evidence can produce `status="cancelled"`. Rejected
work finishes naturally; an unknown outcome fails closed as `failed`.

### Cancel checkpoints

The cancel event is checked at multiple gates:

| Checkpoint | Effect |
|-----------|--------|
| Before step dispatch | Pending attempt records requested → confirmed → cancelled |
| After waiting for predecessors | Queued attempt records requested → confirmed → cancelled |
| During provider work | Provider acknowledgement determines cancelled, failed, or natural completion |
| Inside curator subprocess polling | Local process exit supplies confirmation before cancelled |

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

### Provider cancellation

Each lifecycle router owns the exact futures, jobs, or processes it submits.
Cancellation targets those handles only. The optional Submitit provider cancels
its submitted job IDs rather than issuing a broad name-based scheduler query.

### Cache interaction

Cancelled steps are recorded with `status="cancelled"` in the steps Delta
table. They are excluded from cache lookups, so re-running the same pipeline
re-executes them. `succeeded` steps load from cache; `partial` steps also qualify
when `CachePolicy.STEP_COMPLETED` is selected.

---

## Key design decisions

| Decision | Rationale |
|----------|-----------|
| Two-level caching (step + execution) | Step-level is fast but coarse; execution-level catches fine-grained reuse |
| Two-level batching (artifacts per unit + units per worker) | Separates logical batching from cluster adaptation |
| Curator subprocess isolation | Prevents memory leaks from accumulating in the long-lived orchestrator process |
| Staging verification with close-to-open consistency | NFS attribute caching can hide files; `stat()` is not sufficient |
| Default continue-on-failure | Large runs expect occasional failures; successful results should not be discarded |
| Composites expand into real steps | A composite is a named grouping; each internal operation is an ordinary step, so caching, provenance, and cancellation need no parallel path |
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
- [Glossary](../reference/glossary.md) -- Definitions for the terms used
  throughout this page
- [First Pipeline Tutorial](../tutorials/01-getting-started/01-first-pipeline.ipynb) -- See the execution flow in action
- `artisan-submitit` documentation -- Run operations on a SLURM cluster
- [Pipeline Cancellation Tutorial](../tutorials/05-errors-and-control/03-pipeline-cancellation.ipynb) -- Cooperative cancellation in action
- [Compute Routing Tutorial](../tutorials/07-compute-backends/01-compute-routing.ipynb) -- Route the execute phase to local or remote compute targets
