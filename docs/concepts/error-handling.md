# Error Handling

Artisan records execution failures alongside successful work. A step's terminal
status tells you whether downstream work can use its outputs; execution records
and failure logs explain what failed.

Most pipelines keep useful partial results with the `continue` policy.
`fail_fast` instead makes any observed item failure fail the step, while
preserving the failed execution and sibling work that already finished.

---

## Error boundaries

Failures have different outcomes depending on when they occur:

| Boundary | Outcome |
|----------|---------|
| API-shape validation before acceptance | Raises to the caller; no step attempt has been accepted |
| Operational preparation after acceptance | Records a failed step attempt with a diagnostic |
| Worker execution | Stages success or failure evidence for each execution |
| Runner and dispatch | Returns unit results, including provider failures and available worker logs |
| Step completion | Aggregates results under the failure policy and persists the terminal state |
| Commit failure | Reports failure and retains incomplete commit evidence for inspection or repair |

For example, under `continue`, nine successful executions and one failed
execution produce a `partial` step. The successful output references remain
usable, and the failed execution remains inspectable. If all executions fail,
the step is `failed` and exposes no outputs.

Recording can itself fail. If a worker cannot stage its failure record, it
returns the original error together with the staging error. That preserves a
diagnostic for the orchestrator, although the execution row may be absent.
Interrupted commits require the explicit recovery process described below.

---

## The structured result types

Three data types carry error information through the system, one per scope.
Each is a return value, separate from an exception. Use
[Python API lookup](../reference/python-api.md) for the public step and runner
result models; `StagingResult` is an internal worker model.

### StagingResult (single execution)

The outcome of one creator or curator execution. Produced at the worker
boundary, where exceptions are caught and converted into this type rather than
raised. On failure it carries the error message; on success it carries the
staging path -- where the run's Parquet files live -- and the IDs of the
artifacts produced.

### UnitResult (dispatch boundary)

A frozen record the dispatch layer produces from each `StagingResult`. It
captures whether the unit succeeded, the error message when it did not, how
many items it processed, and the execution run IDs to commit. Runner providers
can also attach a captured worker log, so stderr from a killed job survives
back to the orchestrator. The step executor reads these records to tally
successes and failures and to collect run IDs for the commit.

### StepResult (step aggregate)

The final, immutable record for a step. It counts successes and failures across
all workers and reports the step's authoritative status and duration.
Infrastructure problems -- a dispatch or commit that itself crashed -- make the
step `failed` and appear in its single `error` field rather than as item
failures.

---

## Structured failure identity

`StepResult.status` answers whether the step completed successfully, partially,
or unsuccessfully. Structured error details answer a different question: what
failed and what action might help.

Framework exceptions derived from `artisan.errors.ArtisanError` carry a stable
`code` and an `ArtisanErrorEnvelope`. The envelope combines a readable message
with available context such as the operation, an offending field, suggestions,
and a `recovery_hint`. Automation can branch on the code rather than parse an
error string. The hint suggests a next action; it does not retry or repair work.

Store, artifact-integrity, persistence, lineage, and execution-contract errors
use this hierarchy. Python callers can catch `ArtisanError` at an appropriate
boundary and inspect its `code` or serialize it with `to_dict()`. See
[Python API](../reference/python-api.md) for the exception classes, codes, and
current envelope definition.

When a failed execution carries an Artisan error, its persisted execution
record includes an `error_envelope`. `inspect_failures()` exposes structured
code, recovery hint, offending field, and suggestions alongside the error and
log location. An ordinary exception such as `ValueError` may have no envelope;
its structured columns are then null, while its error text remains available.
Not every result object carries an envelope, and a step-level infrastructure
failure need not have a failed execution row.

### Failure evidence and logs

Execution records identify the attempt, operation configuration, timestamps,
and success or failure. Execution edges retain its input IDs. The terminal
step snapshot records the aggregate state and counts. Supported readers expose
this evidence through the
[logical commit boundary](storage-and-delta-lake.md#commit-ordering).

Human-readable failure logs include the execution identity and traceback, plus
worker output when the runner captures it. Logs are grouped by the source
execution's UTC start date. Reusing an execution in another run preserves its
original execution and log identity.

See [Inspect Pipeline Results and Provenance](../how-to-guides/inspecting-provenance.md)
for selecting a run and reading failures, or
[Debug Executions](../how-to-guides/debugging-executions.md) for diagnostic replay.

---

(you-control-the-response)=
## You control the response

The framework distinguishes two failure policies. You choose which one applies.

| Policy | Behavior | When to use |
|--------|----------|-------------|
| `continue` (default) | Collect all results, count successes and failures, keep going | Most pipelines -- partial results are valuable |
| `fail_fast` | Fail the step on an observed failure; preserve finished work for audit | When partial results are meaningless, or failures indicate a systemic problem |

With `continue`, a step that processes 1,000 items with 3 failures becomes
`partial`. The 997 successes are committed and remain available to downstream
steps. If every item fails, the step becomes `failed` and exposes no output
references. You inspect the `StepResult` and the executions table to diagnose
the failures.

With `fail_fast`, any observed item failure makes the step `failed`, so its
outputs are unavailable to downstream steps. The failed execution and any
sibling work that already finished are persisted for audit.

Set the policy as a default or override:

- **Pipeline default** -- applies to all steps unless overridden
- **Composite default** -- applies to children unless they override it
- **Per-step override** -- applies to a single `run()` or `submit()` call

---

## Failures and caching

The framework's caching system interacts with failures through the cache
policy. Two policies control whether a step with partial failures qualifies
as a cache hit on re-run:

| Cache policy | Behavior |
|--------------|----------|
| `all_succeeded` (default) | Cache hit only for a `succeeded` attempt |
| `step_completed` | Cache hit for a `succeeded` or `partial` attempt |

Failed, cancelled, and skipped attempts never qualify. The distinction matters
when you re-run a pipeline after fixing a bug: with `all_succeeded`, a partial
step re-executes so failed items get another chance. With `step_completed`, its
accepted successful subset is reused with `disposition="cache_hit"`.

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
- Invalid resource, batching, environment, or tool configuration keys
- Input roles that do not match the operation's declared inputs
- Missing required input roles
- Input type mismatches

Call-shape errors raise at call time, before accepting a step attempt. Some
checks require reading stored inputs: identity, content, and external-location
verification happen during operational preparation after acceptance. A failure
there records a failed attempt. See [Python API](../reference/python-api.md)
for the exceptions documented by each entry point.

---

## Subprocess and composite error handling

Two execution modes have their own error containment strategies.

### Curator operations in subprocesses

Curator operations run in a spawned subprocess for memory isolation. If the
subprocess is killed (for example, by the operating system's OOM killer), the
framework catches the `BrokenProcessPool` exception, generates a synthetic
execution run ID, records the failure to Delta Lake with the error details, and
returns a failed `StepResult`. The killed process does not take down the
orchestrator.

### Composite operations

A composite expands into real pipeline steps: each internal `ctx.run()`
runs as its own step and fails independently with standard step-level
error handling. A failing internal step is recorded and counted like any
other step failure; downstream steps that depend on its output receive
empty inputs and skip.

For the full composites model, see
[Composites and Composition](composites-and-composition.md).

---

## Crash recovery

If the orchestrator process crashes mid-pipeline (power failure, `kill -9`),
an immutable logical commit may remain planned with only some physical effects.
Normal startup does not guess how to recover it. Run `artisan store repair`
with the Delta and staging roots to report the evidence, then use `--apply` to
replay a validated plan or `--abandon ID --reason ...` to explicitly abandon
one unrecoverable plan.

---

## Deciding how to respond

The failure policy controls whether a step can expose a successful subset.
Error codes and recovery hints provide diagnostic detail for deciding what to
do next. A malformed input, a broken tool invocation, and a store-integrity
failure can all leave a failed step, but they require different corrective
actions.

Inspect the terminal step state first, then execution failures where present.
Use the preserved evidence to correct inputs or configuration, retry applicable
work, or inspect the store before attempting repair.

---

## Cross-references

- [Error Handling in Practice tutorial](../tutorials/05-errors-and-control/02-error-visibility.ipynb) --
  Runtime failures, failure logs, and FailurePolicy in action
- [Pipeline Cancellation tutorial](../tutorials/05-errors-and-control/03-pipeline-cancellation.ipynb) --
  Cooperative cancellation, signal handling, and cancelled step metadata
- [Resume and Caching tutorial](../tutorials/03-caching/01-resume-and-caching.ipynb) --
  How caching interacts with failures during re-runs
- [Python API](../reference/python-api.md) -- Result models, structured errors,
  and inspection entry points
- [Execution Flow](execution-flow.md) -- Dispatch, execute, commit lifecycle
  where error boundaries live
- [Design Principles](design-principles.md) -- Foundational design decisions
- [Architecture Overview](architecture-overview.md) -- Layer boundaries and
  the orchestrator-worker split
- [Coding Conventions: Error Handling](../contributing/coding-conventions.md#error-handling) --
  Implementation patterns and code examples
