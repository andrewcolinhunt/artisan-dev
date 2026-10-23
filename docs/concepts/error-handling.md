# Error Handling

Artisan records execution failures alongside successful work. A step's terminal
status tells you whether downstream work can use its outputs; execution records
and failure logs explain what failed.

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

Recording can itself fail. If a worker cannot stage its failure record, it
returns the original error together with the staging error. That preserves a
diagnostic for the orchestrator, although the execution row may be absent.
Interrupted commits retain evidence for the recovery process described below.

---

## The structured result types

Execution, dispatch, and step results each carry error information. Use
[Python API lookup](../reference/python-api.md) for the public step and runner
result models; `StagingResult` is an internal worker model.

### StagingResult (single execution)

The worker's result for one creator or curator execution. It carries an error
message on failure, or the staged Parquet path and output artifact IDs on success.

### UnitResult (dispatch boundary)

A frozen dispatch result with the unit's outcome, error, item count, and
execution run IDs to commit. Runner providers can attach captured worker logs,
including stderr from a killed job. The step executor aggregates these results.

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

Choose a failure policy based on whether partial results are usable:

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

Cache policy controls whether a partial step qualifies for whole-step reuse:

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

When a step's input roles contain no artifacts, Artisan records
`status="skipped"` and `skip_reason="empty_inputs"`. This can happen after a
filter removes every item or an upstream step produces nothing.

When the manager detects empty inputs before dispatch, it stops accepting work
for that pipeline. Later submissions, including independent steps, are skipped
with `skip_reason="pipeline_stopped"`. A source operation with no input contract
can still start a pipeline normally.

---

## Validation errors: fail before work begins

Some errors are caught before any execution starts. When you call `run()` or
`submit()`, the pipeline manager validates your inputs immediately:

- Unrecognized parameter keys
- Unrecognized resource, batching, environment, or tool configuration keys
- Input roles that do not match the operation's declared inputs
- Missing required input roles
- Declared upstream output types that do not match the consuming input role

These call-shape errors raise before accepting a step attempt. Validation of
values in recognized parameter fields happens when Artisan constructs the
operation during preparation. If that fails, `run()` returns a failed
`StepResult`; it does not necessarily raise the model's validation exception.
Construct the operation's `Params` directly when you want to validate values
before submission.

Checks that read stored inputs, such as identity, content, external locations,
and input pairing, also happen during preparation after acceptance. Their
failures become failed attempts. Empty-input handling can skip a step before
those checks run. See [Python API](../reference/python-api.md) for each entry
point's documented exceptions.

---

## Subprocess and composite error handling

### Curator operations in subprocesses

Curator operations run in a spawned subprocess for memory isolation. If it is
killed, for example by the operating system's OOM killer, Artisan detects the
broken process pool and attempts to record a synthetic execution failure.
Managed commands still require cleanup. If that cleanup cannot be confirmed,
the step fails and retains its workspace instead of collecting outputs.

### Composite operations

A composite's children use ordinary step error handling. A failed child exposes
no outputs, so dependent steps receive empty inputs and skip.

For the full composites model, see
[Composites and Composition](composites-and-composition.md).

---

## Cancellation and uncertain cleanup

Cancellation is a request until Artisan can establish what happened to the
worker and its managed commands. Confirmed cleanup produces a `cancelled` step;
unknown cleanup produces a `failed` step without accepted outputs. Work that
already completed can reject cancellation and keep its natural result.

For example, a successful request to cancel a Modal call does not prove that its
container stopped. Artisan records `unknown` and suppresses output download and
ingestion for that call. Check the provider before reusing external files. See
[Cancellation](execution-flow.md#cancellation) for native, container, and remote
limits.

## Crash recovery

After interruption, startup retries eligible recorded commits and recovers
validated finished execution units before cache lookup. Cancellation and failure
retain uncommitted staging; recovery leaves the original step status unchanged.
Incomplete evidence stays in place, while corruption or ownership conflicts
block startup.

Confirm that the old orchestrator has stopped before reopening its roots. See
[Crash recovery](storage-and-delta-lake.md#storage-crash-recovery) for the independent
recovery and preservation flags, and
[explicit repair](../how-to-guides/configuring-execution.md#recovering-from-crashes)
for inspecting evidence or applying recovery separately.

---

## Deciding how to respond

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
- [Coding Conventions: Error Handling](../contributing/coding-conventions.md#error-handling) --
  Implementation patterns and code examples
