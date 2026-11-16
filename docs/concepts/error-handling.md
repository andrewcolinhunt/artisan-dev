# Error Handling

Artisan records execution failures alongside successful work. A step's terminal
status tells you whether downstream work can use its outputs; execution records
and failure logs explain what failed.

---

## Error boundaries

Failures have different outcomes depending on when they occur:

| Boundary | Outcome |
|----------|---------|
| Invalid call or declared input contract | Raises to the caller before a pending step is recorded |
| Input or prepared-software resolution | Records a failed step attempt with a diagnostic |
| Scientific execution | Fails the complete execution unit; successful sibling units follow the outer failure policy |
| Runner and dispatch | Records an infrastructure failure and the available ownership and worker evidence |
| Step completion | Selects accepted results under the failure policy and persists the terminal state |
| Commit failure | Retains the selected commit evidence for inspection or repair |

Recording can itself fail. A failed step need not have a corresponding scientific
execution record when no trustworthy worker result is available. Diagnostics
describe that gap; they do not manufacture a successful or failed scientific
execution. Interrupted commits retain evidence for the recovery process below.

---

## Read the step result

`pipeline.run()` and `StepFuture.result()` return a `StepResult`. It reports the
step's status, duration, error, and counts of execution units. One unit may
process several input artifacts and produce several outputs, so those counts
are not artifact counts. A composite invocation is one complete unit.

Use the result's output references only when its status makes outputs available.
Infrastructure failures are reported as failed steps, even when individual
workers finished computation. See [Python API](../reference/python-api.md) for
the public result and provider contracts.

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

When a recorded failure carries an Artisan error, `inspect_failures()` exposes
structured code, recovery hint, offending field, and suggestions alongside the error and
log location. An ordinary exception such as `ValueError` may have no envelope;
its structured columns are then null, while its error text remains available.
Not every result object carries an envelope, and a step-level infrastructure
failure need not have a failed execution row.

### Failure evidence and logs

Selected execution evidence identifies the attempt, operation configuration,
inputs, and outcome. The terminal step records the aggregate state and counts.
Supported readers expose this evidence through the
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

With `continue`, a step with 1,000 execution units and 3 scientific failures
becomes `partial`. The 997 successful units' selected outputs remain available
to downstream steps. If every unit fails, the step becomes `failed` and exposes
no output references. Use `StepResult` and `inspect_failures()` to diagnose it.

With `fail_fast`, any observed unit failure makes the step `failed`, so its
outputs are unavailable to downstream steps. The failed execution and any
sibling work that already finished are persisted for audit.

Set the policy as a default or override:

- **Pipeline default** -- applies to all steps unless overridden
- **Per-step override** -- applies to a single `run()` or `submit()` call

A composite uses its outer submission's policy. Children have no separate
failure policy, persisted step, or retry boundary.

---

## Failures and caching

Cache policy controls whether a partial step qualifies for whole-step reuse:

| Cache policy | Behavior |
|--------------|----------|
| `all_succeeded` (default) | Cache hit only for a `succeeded` attempt |
| `step_completed` | Cache hit for a `succeeded` or `partial` attempt |

Failed, cancelled, and skipped attempts never qualify. The distinction matters
when you re-run a pipeline after fixing a bug: with `all_succeeded`, a partial
step reuses eligible successful units and executes the remaining units. With
`step_completed`, its accepted successful subset is reused with
`disposition="cache_hit"`.

---

## Empty inputs and pipeline stopping

When a supplied input boundary is empty, Artisan records
`status="skipped"` and `skip_reason="empty_inputs"`. This can happen after a
filter removes every item or an upstream step produces nothing.

After validating that empty boundary, the manager stops subsequent work
for that pipeline. Later submissions, including independent steps, are skipped
with `skip_reason="pipeline_stopped"`. A source operation with no input contract
can still start a pipeline normally. A valid nonempty boundary that produces
zero pairing groups skips that step without stopping the pipeline.

---

## Validation errors: fail before work begins

Some errors are caught before any execution starts. When you call `run()` or
`submit()`, the pipeline manager validates your inputs immediately:

- Unrecognized parameter keys
- Unrecognized resource, batching, or tool configuration keys
- Input roles that do not match the operation's declared inputs
- Missing required input roles
- Declared upstream output types that do not match the consuming input role

Invalid parameter values also raise before a pending step is recorded. You can
construct the operation's `Params` directly to validate those values separately.

Checks that read stored inputs, such as identity, content, external locations,
and input pairing, also happen during preparation after acceptance. Their
failures become failed attempts. An empty stream does not bypass its declared
role/type checks or the validation of its source. See
[Python API](../reference/python-api.md) for each entry point's documented exceptions.

---

## Subprocess and composite error handling

### Curator operations in subprocesses

The selected runner places a curator in its own worker. A killed worker, for
example after an out-of-memory failure, can leave only infrastructure diagnostics
and no authored result. Managed commands still require cleanup. If cleanup
cannot be confirmed, the step fails without accepting outputs and preserves
the available workspace and evidence.

### Composite operations

A child failure fails its entire composite execution unit. Internal outputs are
not separately committed, and later children do not run. Other units in the
outer step follow its failure policy. For an independent intermediate checkpoint,
submit separate operations or composites as pipeline steps.

For the full composites model, see
[Composites and Composition](composites-and-composition.md).

---

## Cancellation and uncertain cleanup

Cancellation is a request until Artisan can establish what happened to the
worker and its managed commands. Confirmed cleanup produces a `cancelled` step;
unknown cleanup produces a `failed` step without accepted outputs. Work that
already completed can reject cancellation and keep its natural result.

For example, a successful request to cancel a Modal call does not prove that its
container stopped. Artisan records `unknown` and prevents result acceptance for
that call. Check the provider before reusing external files. See
[Cancellation](execution-flow.md#cancellation) for native, container, and remote
limits.

## Crash recovery

Resume can finish publication of results the controller already selected before
the interruption. Creating a new pipeline does not adopt orphan staging, and
repair cannot turn an unclaimed worker result into success after cancellation.
Incomplete evidence stays in place; unresolved ownership or integrity conflicts
prevent unsafe continuation.

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
