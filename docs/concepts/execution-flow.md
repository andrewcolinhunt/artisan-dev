# Execution Flow

`pipeline.run()` prepares inputs, reuses eligible cached work, dispatches the
remaining execution units, and commits their results. Workers stage isolated
files; the orchestrator decides which results become visible to readers and
downstream steps.

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

The orchestrator coordinates the pipeline and owns Delta Lake writes. Workers
run in isolation, possibly on remote cluster nodes, and return staged files.
For the rationale behind this split, see
[Architecture Overview](architecture-overview.md#the-orchestrator-worker-split).

---

## Dispatch: preparing work

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
dispatch consume this same prepared snapshot. Modal deployment overrides are
also checked before either cache lookup, so a cache hit cannot hide an invalid
infrastructure change.

**Empty input handling.** When every input role resolves to zero artifact IDs,
the step is skipped entirely. The orchestrator records a "skipped" result
(with the reason `empty_inputs`) in the steps table so that downstream steps
and resume logic know the step was attempted but produced no work.

### Multi-input pairing

The orchestrator pairs multiple input roles before batching, so batch
boundaries respect paired groups. The operation's `group_by` selects `ZIP`,
`LINEAGE`, `CROSS_PRODUCT`, or `NAME`; see
[Pairing strategies](operations-model.md#pairing-strategies) for their matching
rules, unmatched inputs, and ambiguity handling.

For operations with a primary input role (such as Filter), a variant called
anchor-based matching pairs each primary artifact independently against every
other role, retaining only complete matches.

### Two-level caching

Artisan can reuse a whole step or individual execution batches.

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

Every submitted step has a fresh UUID-based attempt ID, regardless of whether
it executes, is cached, is skipped, is cancelled, or fails. Step numbers remain
the logical sequence within a pipeline run. Cache reuse changes neither: the
current step owns its attempt ID and refers directly to existing execution IDs.

The same prepared invocation produces the same keys regardless of worker
completion order or storage URI.

### Batching and dispatch

Inputs that need execution are partitioned into `ExecutionUnit` objects for
workers.

**Level 1 batching** (`artifacts_per_unit`) controls how many artifacts each
unit processes. An ML inference operation might set this to 1 (one structure per
GPU job). A metrics calculation might set it to 100 (batch for efficiency).

**Level 2 batching** (`units_per_worker`) controls how many units one worker
processes sequentially. Core applies this policy before dispatch, so local and
external runners use identical packing semantics.

Each `ExecutionUnit` carries the configured operation instance, prepared inputs,
cache key, step identity, and pairing information. Workers also receive a
`RuntimeEnvironment` with storage settings and the trusted application modules
to load. The worker environment must contain the operation's dependencies.

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

A non-materialized config can instead request typed references with
`resolve_references=True`. Its `$artifact` markers remain intact, while
`inputs.referenced_artifacts(config)` supplies verified dependency objects and
local files to preprocess and postprocess. The operation packages any dependencies
needed remotely into its declared execute inputs. See
[Read typed config dependencies](../how-to-guides/writing-creator-operations.md#read-typed-config-dependencies).

**Finalize** (after postprocess). The framework computes a versioned ID from
each draft's registered type, canonical content, and semantic metadata. It then
protects durable fields and saves an identity snapshot for later integrity
checks.

After normal completion, the sandbox is cleaned up unless `preserve_working`
is set. Failed uploads and uncertain command cleanup retain it for diagnosis.

### Curator operations: the lightweight path

[Curators](operations-model.md#the-curator-lifecycle) receive input DataFrames
and store access in a local spawned subprocess. They do not use the creator's
sandbox, materialization, or three-phase lifecycle. Process isolation lets the
operating system reclaim their memory after execution.

A killed curator process produces a failed step with available diagnostics.
Artisan attempts to persist a synthetic execution failure, and any managed
commands still require cleanup before their files are safe to collect.

### Composite execution

A [composite](composites-and-composition.md) expands into ordinary steps, each
with its own dispatch-execute-commit cycle. The current manager executes steps
serially; each creator step can parallelize its batches.

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

The step runner places the worker process; the compute provider places its
execute phase. Both local and SLURM workers can route execute to Modal.

When compute is `"local"` (the default), an ordinary `execute_function` runs
inside the lifecycle worker. Command operations, including `execute_as_tool`,
use a managed subprocess on that worker's host. When compute is `"modal"`,
the execute phase becomes an HTTP client of the operation's deployed tool endpoint:
it submits the op's params + input files, polls until the tool finishes,
and downloads the output files back into the execute dir. Only command ops
can route to modal -- an op with a `ToolSpec` + `execute_command()`, or one
that sets `execute_as_tool=True`. Inline transfers are bounded at 100 MB per
direction. Eligible externally stored file inputs pass by reference after
policy and integrity checks. Other inputs, including selected appendable
records, travel inline. See
[Tool endpoint file transport](../how-to-guides/deploying-tool-endpoints.md#choose-how-files-travel).

---

## Lineage capture

After postprocess, Artisan validates `ArtifactResult.lineage` against each
output's `derives_from` contract. It finalizes drafts in their original order,
resolves output indices to artifact IDs, and records the declared edges and
joint parent groups. Input pairing and config materialization add no parents.
See [Provenance System](provenance-system.md) for the declaration contract.

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
Terminal snapshots never transition again. When a logical step has several
attempts, current-result readers select its newest attempt before checking
whether outputs are available. A newer failure does not reveal an older
successful attempt's outputs. Select an exact `step_run_id` when inspecting
historical results.

The valid lifecycle edges are:

```text
pending -> running -> succeeded | partial | failed | cancelled
       \-> skipped | cancelled
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

An error's phase determines where Artisan records it:

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

**Failure logs.** Artisan attempts to write a human-readable log for each
failed execution, containing the run ID, operation name, step number, step
runner, timestamp, traceback, and available tool output. These live in
`logs/failures/YYYYMMDD/YYYYMMDDTHHMMSSffffffZ_executionID.log` alongside
local Delta tables, named by the source execution's UTC start time. Cloud stores
keep these files under the local working root.

The default `continue` policy makes a mix of successes and failures `partial`;
`fail_fast` makes an observed failure fail the step. See
[failure policies](error-handling.md#you-control-the-response) for output
availability and retained evidence.

---

## Cancellation

Request cancellation with `pipeline.cancel()` or the installed SIGINT/SIGTERM
handlers. The request starts cleanup; it does not by itself prove that work
stopped. Artisan records cancellation evidence separately from the step's
lifecycle status:

| Evidence | Meaning | Step outcome |
| --- | --- | --- |
| `requested` | Cleanup is still being evaluated. | No terminal cancellation outcome yet. |
| `confirmed` | Work never started, or required worker and command cleanup was proved. | `cancelled` |
| `rejected` | Work completed before cancellation could take effect. | Its natural outcome. |
| `unknown` | Artisan cannot prove the required cleanup. | `failed`, with no accepted outputs. |

### Cancel checkpoints

Before dispatch, or after waiting for a predecessor, Artisan can confirm that a
pending attempt never started. During local creator or curator work, it must
establish both worker exit and managed command cleanup. Killing the worker alone
is insufficient.

On Linux and macOS, an independent supervisor owns native commands before they
start. Cleanup requests a graceful stop, then forces termination within a shared
bounded deadline. Registered nested commands remain covered even after their
parent worker exits. Built-in Docker additionally requires removal of the exact
owned container. Arbitrarily detached work and external daemons are outside this
contract; see [managed command limits](../how-to-guides/configuring-execution.md#keep-managed-commands-in-the-foreground).

Modal cancellation targets an exact call and requests termination of its worker
container. An accepted provider request still leaves the outcome `unknown`;
Artisan suppresses output download and ingestion for that call. Local process
cleanup does not supply missing remote evidence.

### Signal escalation

The manager installs signal handlers from the main thread when work begins:

| Press | Effect |
| --- | --- |
| First Ctrl+C | Requests cancellation. |
| Second Ctrl+C | Restores the signal handlers that preceded Artisan's handlers. |
| Later Ctrl+C | Uses the restored handler; Python's usual handler raises `KeyboardInterrupt`. |

Native worker processes ignore SIGINT so the orchestrator handles the request.
Handler installation is skipped outside the main thread. In notebooks, call
`pipeline.cancel()` directly. Cleanup can terminate workers before their execute
phase finishes; use recorded evidence to determine the outcome.

### Recovery and cache reuse

Cancelled steps cannot provide whole-step cache hits. Their uncommitted staging
is retained, and independently completed execution units can still be recovered
and reused by a new run. Unknown cleanup does not make incomplete work eligible
for recovery. See [Crash recovery](storage-and-delta-lake.md#storage-crash-recovery).

---

## Cross-references

- [First Pipeline Tutorial](../tutorials/01-getting-started/01-first-pipeline.ipynb) -- See the execution flow in action
- [Pipeline Cancellation Tutorial](../tutorials/05-errors-and-control/03-pipeline-cancellation.ipynb) -- Cooperative cancellation in action
- [Compute Routing Tutorial](../tutorials/07-compute-backends/01-compute-routing.ipynb) -- Route the execute phase to local or remote compute targets
