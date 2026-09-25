# Execution Flow

When you call `pipeline.run()`, a cascade of coordinated work happens between
the orchestrator and workers before results appear in Delta Lake. Understanding
this flow explains what work a cache hit skips, when results become visible,
why lineage is captured during execution, and where to look when something goes
wrong.

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
│                    │  │                          │  │  Complete commit       │
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

Four strategies are available:

- **ZIP** -- positional pairing (first with first, second with second). All
  roles must have the same length.
- **LINEAGE** -- matches a candidate to its nearest target ancestor along
  directed provenance edges. Sharing a common ancestor is insufficient.
  Ordinary pairing requires exactly two input roles.
- **CROSS_PRODUCT** -- all combinations of artifacts across roles. Useful when
  every combination is meaningful.
- **NAME** -- exact filename-stem matching across roles, for corresponding
  inputs that do not have directed ancestry.

See [Pairing strategies](operations-model.md#pairing-strategies) for ancestor
and sibling examples, unmatched inputs, and ambiguity handling.

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

Workers receive `ExecutionUnit` objects and run the operation lifecycle.
Creators and curators use different paths. Composites expand into ordinary
steps before worker execution.

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
[composite](composites-and-composition.md), running the composite expands it into
real pipeline steps. Each `ctx.run()` call delegates to the parent
pipeline as its own step, giving each internal operation its own
dispatch-execute-commit cycle and independent failure handling. Steps execute
serially in the current manager; each creator step can parallelize its batches.

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

After postprocess, Artisan validates the operation’s explicit `ArtifactResult`
declarations against each output’s `derives_from` contract. Every emitted role
must have a lineage entry; every derived occurrence must name its required
parents. Root roles explicitly contain an empty list.

Finalization preserves output order and human names. The framework resolves
role-local output indices to artifact IDs, looks up known types, and records
only the declared edges. Group labels represent the declared unique parent
sets; input pairing never supplies extra parents. Config materialization still
resolves embedded artifact references to paths, while config ancestry comes
from the producing operation’s declarations.

See [Provenance System](provenance-system.md) for exact references, joint parent
groups, and optional operation-called matching helpers.

---

## Staging: the contract between workers and orchestrator

Workers never write to Delta Lake. Instead, each worker writes Parquet files to
an isolated staging directory -- one file per table type, with `executions.parquet`
written last as a sentinel. The orchestrator collects these after all workers
complete and prepares a logical commit with the terminal step snapshot.

This [staging-commit pattern](storage-and-delta-lake.md#the-staging-commit-pattern)
isolates worker writes and preserves evidence for verification and recovery.
See the storage page for the full directory layout, sharding strategy, and NFS
consistency handling.

### Staging verification

On distributed filesystems (NFS), directory attribute caching can delay
visibility of files written by SLURM workers. Before committing, the
orchestrator polls for `executions.parquet` sentinel files using
close-to-open consistency checks (`open()` + `read()` rather than `stat()`)
with exponential backoff. This verification runs only when the step runner
reports a shared filesystem; local step runners skip it entirely.

---

## Commit: making results visible

The orchestrator seals an immutable plan containing staged worker evidence and
the terminal step snapshot. It writes the planned table effects and then marks
the logical commit complete after verifying those effects. Supported readers
use that completion record for visibility; raw Delta reads can show partial
physical writes.
The authoritative sequence is in
[Commit ordering](storage-and-delta-lake.md#commit-ordering).

Commit verification reuses identical artifact content and rejects conflicting
rows under the same ID. Optional compaction merges small Parquet files after
commit. Neither deduplication nor compaction changes run ownership: new attempts
and cache reuse retain their own recorded membership.

### Worker log capture

Runner providers can attach worker stdout/stderr to each `UnitResult` before
collection completes. Artisan stores those diagnostics separately under
`_artisan/worker_logs/<execution_run_id>.log` in the Delta root, keeping sealed
staging immutable. Use
[`inspect_worker_log()`](../how-to-guides/debugging-executions.md#inspect-provider-logs)
to read an exact execution's log, including uncommitted attempts. Failed
executions also get human-readable log files grouped by source execution start
date under `logs/failures/YYYYMMDD/` (UTC). Log delivery is best effort; missing
logs do not block commitment or decide recovery eligibility.

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

**Recording failures.** Before an execution is sealed, failure-recording errors
are combined with the original error in the returned diagnostic. An error after
seal publication propagates without rewriting the immutable staging evidence.

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
first dispatched step when running on the main thread:

| Press | Effect |
|-------|--------|
| First Ctrl+C | Requests cooperative cancellation; pending work records confirmed cancellation before dispatch |
| Second Ctrl+C | Restores the signal handlers that were installed before Artisan's handlers |
| Later Ctrl+C | Uses the restored handler; Python's usual SIGINT handler raises `KeyboardInterrupt` |

Worker child processes ignore SIGINT (via `SIG_IGN` in the process pool
initializer), so only the orchestrator handles the signal. Signal-handler
installation is skipped off the main thread. In notebooks, use
`pipeline.cancel()` directly instead of relying on interrupt handling.
Cancellation does not guarantee that the current execute phase
finishes: a local runner may terminate workers after its cooperative grace
period. The recorded acknowledgement determines the terminal status.

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
- [Provenance System](provenance-system.md) -- Dual provenance, explicit declarations,
  joint parent groups
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
