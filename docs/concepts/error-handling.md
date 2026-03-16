# Error Handling

Pipeline computations fail. Operations crash, files go missing, cluster nodes
die. How the framework handles those failures determines whether you lose hours
of work or get a clear report of what went wrong.

This page explains how Artisan contains, records, and reports failures -- and
the design thinking behind each choice.

---

## The core idea

**Errors are data, not control flow.**

When something fails -- an operation crashes, a file is missing, a SLURM node
dies -- the framework converts that failure into a structured record and passes
it forward as data. A pipeline processing 1,000 items where 3 fail completes
with 997 successes and 3 failure records, not a stack trace.

This is different from most execution frameworks, which abort on the first
uncaught exception. Artisan treats failure as an expected outcome -- one that
should be captured with the same care as success.

---

## Containment: never crash the many for the one

The most important rule: a single item failure must never destroy other items'
results. This applies at every level of the system.

| Level | What happens on failure |
|-------|------------------------|
| Within a step | One execution unit fails; other units' results are still committed |
| Within a pipeline | A step has partial failures; downstream steps receive whatever succeeded |

Pipeline work is expensive. Throwing away 997 successful results because 3
failed is wasteful. And failure patterns are often informative -- seeing *which*
inputs fail helps diagnose the problem.

The only exception is the explicit `fail_fast` policy, where you have decided
that *any* failure should stop execution (more on this [below](#you-control-the-response)).

---

## Layered error boundaries

Exceptions are caught and converted to structured data at each architectural
boundary. An exception never crosses two boundaries.

```
  ┌─────────────────────────────────────────────────────────────────┐
  │  Layer 1: Worker                                                │
  │  Catches: operation errors, staging errors, validation errors   │
  │  Returns: StagingResult(success=False, error="...")             │
  ├─────────────────────────────────────────────────────────────────┤
  │  Layer 2: Dispatch                                              │
  │  Catches: anything that escaped Layer 1, future failures        │
  │  Returns: {"success": False, "error": "...", "item_count": N}   │
  ├─────────────────────────────────────────────────────────────────┤
  │  Layer 3: Step executor                                         │
  │  Catches: dispatch crashes, commit failures                     │
  │  Returns: StepResult(failed_count=N)                            │
  ├─────────────────────────────────────────────────────────────────┤
  │  Layer 4: Pipeline manager                                      │
  │  Catches: step executor exceptions, submit-time validation      │
  │  Returns: StepResult (always -- the pipeline never crashes)     │
  └─────────────────────────────────────────────────────────────────┘
```

**Why this redundancy?** In the normal case, Layer 1 catches everything and
the outer layers see only structured results. But if Layer 1 has a bug, Layer 2
catches the leak. If Layer 2 has a bug, Layer 3 catches it. Each boundary is
independently responsible for never letting an unstructured exception escape.

**Why catch early?** The layer closest to the failure has the most context. A
worker knows which operation, which inputs, which execution run. The dispatch
layer only knows which unit. The orchestrator only knows which step. Catching
at the source preserves rich diagnostics.

---

## How a failure flows through the system

Here is the complete journey of a single failure, from the moment an operation
raises an exception to the moment you see the result:

```
1. operation.execute() raises ValueError("invalid input format")
       │
2. Worker catches the exception
   │   Formats error with full traceback via format_error()
   │   Stages failure record to Parquet (success=False)
   │   Writes human-readable failure log to disk
   │   Returns StagingResult(success=False, error="ValueError: ...")
       │
3. Dispatch converts StagingResult to dict
   │   {"success": False, "error": "ValueError: ...", "item_count": 1}
       │
4. Step executor aggregates all worker results
   │   succeeded=9, failed=1
   │   Commits staged data (successes AND failure records) to Delta Lake
       │
5. StepResult(success=False, succeeded_count=9, failed_count=1)
       │
6. You see: "Step completed with 1 failure out of 10"
   You query: executions table → find error message → know exactly what failed
```

The original exception message -- `ValueError: invalid input format` -- survives
the entire journey unchanged. Every layer that touches the error preserves the
original type name and message. No layer replaces it with a generic string.

---

## The structured result types

Three data types carry error information through the system, one per scope:

### StagingResult (single execution)

The return type from both creator and curator flows. Never raised as an
exception -- exceptions are caught and converted into this type at the worker
boundary.

```
StagingResult
├── success: bool
├── error: str | None          # "ValueError: invalid input format"
├── staging_path: Path | None  # where staged Parquet files live
├── execution_run_id: str | None
└── artifact_ids: list[str]    # empty on failure
```

### Result dict (dispatch boundary)

A plain dict that the dispatch layer produces from `StagingResult`. The step
executor feeds these dicts to `aggregate_results()` (which reads `success`
and `item_count`) and `extract_execution_run_ids()` (which collects the run
IDs for commit).

```
{"success": bool, "error": str | None, "item_count": int, "execution_run_ids": [...]}
```

### StepResult (step aggregate)

The final, immutable record for a step. Counts successes and failures across
all workers.

```
StepResult
├── success: bool
├── succeeded_count: int
├── failed_count: int
├── total_count: int
├── duration_seconds: float | None
└── metadata: dict             # may contain "dispatch_error" or "commit_error"
```

---

## Recording: every attempt is persisted

Every execution attempt -- success or failure -- produces a record in Delta
Lake. When you ask "why did this fail?", the answer is queryable.

Failed executions are persisted with the same schema as successes. The
`executions` table row includes:

- `success: False`
- `error: "ValueError: invalid input format"`
- `execution_run_id`, `execution_spec_id`
- Timestamps, source worker ID
- Input artifact IDs (via the `execution_edges` table, joined on `execution_run_id`)

At the step level, the `steps` table records `status="failed"` with the error
string, or `status="completed"` with `failed_count` and `succeeded_count`
for partial failures. Infrastructure-level problems are captured separately
in `dispatch_error` and `commit_error` columns.

### Failure logs

Beyond the structured records in Delta Lake, the framework writes
human-readable failure log files for each failed execution. These logs include
the execution run ID, operation name, step number, compute backend, timestamp,
and full error traceback. When running on SLURM, worker stderr is appended
to the log after job completion. These files provide a quick diagnostic path
without needing to query Delta Lake.

### The double-fault handler

What if writing the failure record itself fails (disk full, permission error)?
The recorder has a fallback: it catches the staging exception, combines both
error messages, and returns a `StagingResult` with the combined error. The
error message still flows upward through the return value so the orchestrator
can count it. The execution record may be missing from Delta Lake, but the
failure is never silently swallowed.

---

(you-control-the-response)=
## You control the response

The framework distinguishes two failure policies. You choose which one applies.

| Policy | Behavior | When to use |
|--------|----------|-------------|
| `continue` (default) | Collect all results, count successes and failures, keep going | Most pipelines -- partial results are valuable |
| `fail_fast` | Abort on the first failure | When partial results are meaningless, or failures indicate a systemic problem |

With `continue`, a step that processes 1,000 items with 3 failures completes
normally. The 997 successes are committed. Downstream steps receive whatever
succeeded. You inspect the `StepResult` and the executions table to diagnose
the 3 failures.

With `fail_fast`, the first failure raises a `RuntimeError` that propagates
through the dispatch and step layers. The pipeline manager catches it and
returns a failed `StepResult`. Work already completed by other workers is
still committed -- only remaining work is aborted.

The policy can be set at two levels:

- **Pipeline default** -- applies to all steps unless overridden
- **Per-step override** -- applies to a single `run()` or `submit()` call

---

## Failures and caching

The framework's caching system interacts with failures through the cache
policy. Two policies control whether a step with partial failures qualifies
as a cache hit on re-run:

| Cache policy | Behavior |
|--------------|----------|
| `all_succeeded` (default) | Cache hit only when the step had zero execution failures |
| `step_completed` | Cache hit for any completed step, regardless of failure count |

Both policies block caching when infrastructure errors occurred (dispatch
or commit failures). The distinction matters when you re-run a pipeline
after fixing a bug: with `all_succeeded`, the step re-executes so the
previously-failed items get another chance. With `step_completed`, the
step is skipped because it already ran to completion, even though some
items failed.

---

## Empty inputs and pipeline stopping

When a step receives no input artifacts -- because an upstream step produced
nothing, or a filter removed all items -- the step is skipped rather than
executed. This is not treated as a failure; it is recorded with
`status="skipped"` and `skip_reason="empty_inputs"`.

Skipping propagates forward: once a step is skipped due to empty inputs, all
subsequent steps in the pipeline are also skipped, since they depend on the
outputs of the skipped step. The pipeline records each skipped step and
completes normally, giving you visibility into where the data ran out.

---

## Validation errors: fail before work begins

Some errors are caught before any execution starts. When you call `run()` or
`submit()`, the pipeline manager validates your inputs immediately:

- Unrecognized parameter keys
- Invalid resource, execution, environment, or tool configuration keys
- Input roles that do not match the operation's declared inputs
- Missing required input roles
- Input type mismatches

These raise `ValueError` at call time, before any dispatching or worker
allocation. This is intentional: configuration mistakes should fail fast and
loud, not silently produce wrong results or waste compute.

---

## Subprocess and composite error handling

Two execution modes have their own error containment strategies.

### Curator operations in subprocesses

Curator operations run in a spawned subprocess for memory isolation. If the
subprocess is killed (out-of-memory, SLURM timeout), the framework catches the
`BrokenProcessPool` exception, generates a synthetic execution run ID, records
the failure to Delta Lake with the error details, and returns a failed
`StepResult`. The killed process does not take down the orchestrator.

### Composite operations

In **collapsed** mode, a failure in any internal `ctx.run()` call aborts the
entire composite. Artifacts are passed in-memory between operations, so there
is no meaningful partial result to preserve. The composite executor catches the
failure and returns a `StagingResult` with the error from the failing operation.
At the step level, the composite failure is counted as a single failed item.

In **expanded** mode, each internal operation runs as its own pipeline step
and fails independently with standard step-level error handling.

For the full composites model, see
[Composites and Composition](composites-and-composition.md).

---

## Crash recovery

If the orchestrator process crashes mid-pipeline (power failure, `kill -9`),
staged Parquet files from completed workers may be left on disk without having
been committed to Delta Lake. On the next pipeline initialization, the
framework detects these orphaned staging files and commits them. This is
controlled by the `recover_staging` option (enabled by default) and ensures
that work completed before the crash is not lost.

---

## Severity, not category

The framework does not distinguish between *kinds* of failures for control
flow. Operation bugs, missing files, network errors, disk full, SLURM
timeouts -- all are handled identically: catch, record, report, continue.

The distinction that matters is **severity at the pipeline level**, not the
error category. Four severity levels emerge naturally from the layered
architecture:

| Severity | Meaning | How it manifests |
|----------|---------|------------------|
| Item failure | One input could not be processed | `StagingResult(success=False)` |
| Step partial failure | Some items in a step failed | `StepResult(failed_count=N, succeeded_count=M)` |
| Step total failure | All items in a step failed | `StepResult(succeeded_count=0)` |
| Infrastructure failure | Dispatch or commit itself crashed | `StepResult` with error in `metadata` |

You decide what severity warrants action. The framework gives you the data
to make that decision.

---

## Design summary

| Principle | What it means |
|-----------|---------------|
| Errors are data | Failures become structured records, not stack traces |
| Contain at the boundary | Each layer catches exceptions and returns structured results |
| Preserve the message | Original error type and message survive end-to-end |
| Record everything | Every attempt (success or failure) is persisted to Delta Lake |
| Defense in depth | Four nested safety nets, each independently responsible |
| Return, don't raise | Functions return results; exceptions are for programming errors |
| Continue by default | Partial results are preserved; `fail_fast` is opt-in |

---

## Cross-references

- [Error Handling in Practice tutorial](../tutorials/execution/04-error-visibility.ipynb) --
  Runtime failures, failure logs, and FailurePolicy in action
- [Pipeline Cancellation tutorial](../tutorials/execution/08-pipeline-cancellation.ipynb) --
  Cooperative cancellation, signal handling, and cancelled step metadata
- [Resume and Caching tutorial](../tutorials/execution/02-resume-and-caching.ipynb) --
  How caching interacts with failures during re-runs
- [Execution Flow](execution-flow.md) -- Dispatch, execute, commit lifecycle
  where error boundaries live
- [Design Principles](design-principles.md) -- Foundational design decisions
- [Architecture Overview](architecture-overview.md) -- Layer boundaries and
  the orchestrator-worker split
- [Coding Conventions: Error Handling](../contributing/coding-conventions.md#error-handling) --
  Implementation patterns and code examples
