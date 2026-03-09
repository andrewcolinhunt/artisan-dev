# Error Handling

Pipeline computations fail. Operations crash, files go missing, cluster nodes
die. How the framework handles those failures determines whether you lose hours
of work or get a clear report of what went wrong.

This page explains how Artisan contains, records, and reports failures --
and the design thinking behind each choice.

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
| Within a batch | One item fails; remaining items in the batch still execute |
| Within a step | One worker fails; other workers' results are still committed |
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
  │  Catches: everything, including fail_fast aborts                │
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
   │   Formats error: "ValueError: invalid input format"
   │   Writes failure record to staging parquet (success=False)
   │   Returns StagingResult(success=False, error="ValueError: invalid input format")
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
├── staging_path: Path | None  # where staged parquet files live
├── execution_run_id: str | None
└── artifact_ids: list[str]    # empty on failure
```

### Result dict (dispatch boundary)

A plain dict that the dispatch layer produces from `StagingResult`. The step
executor feeds these dicts to `aggregate_results()` (which reads `success`,
`item_count`, and `error`) and `extract_execution_run_ids()` (which collects
the run IDs for commit).

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
- Input artifact IDs, timestamps, worker ID

At the step level, the `steps` table records `status="failed"` with the error
string.

### The double-fault handler

What if writing the failure record itself fails (disk full, permission error)?
The recorder has a fallback: it catches the staging exception, combines both
error messages, and returns a `StagingResult` with the combined error. The
error message still flows upward through the return value so the orchestrator
can count it. The execution record may be missing from Delta Lake, but the
failure is never silently swallowed.

---

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

## Cross-References

- [Execution Flow](execution-flow.md) -- Dispatch, execute, commit lifecycle
  where error boundaries live
- [Design Principles](design-principles.md) -- Foundational design decisions
- [Architecture Overview](architecture-overview.md) -- Layer boundaries and
  the orchestrator-worker split
- [Coding Conventions: Error Handling](../../contributing/coding-conventions.md#error-handling) --
  Implementation patterns and code examples
