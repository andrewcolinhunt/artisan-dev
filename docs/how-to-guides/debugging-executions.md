# Debug a Recorded Execution

Replay one recorded execution with fresh IDs, verbose local worker output, and
retained working files.

**Prerequisites:** A current-format store and an execution ID from
[inspecting failures](configuring-execution.md#inspect-commands-from-an-execution).
See [Execution Flow](../concepts/execution-flow.md) for lifecycle terminology.

---

## Minimal working example

Use the execution's existing store and separate diagnostic destinations:

```python
from artisan.orchestration import replay_execution
from artisan.orchestration.runner_api import RuntimeEnvironment

result = replay_execution(
    execution_run_id,
    runtime=RuntimeEnvironment(
        delta_root="runs/delta",
        staging_root="debug/staging",
        working_root="debug/work",
        files_root="debug/files",
        failure_logs_root="debug/logs",
    ),
)
print(result.step_result.status)
print(result.execution_run_id)
print(result.diagnostic_roots)
```

Artisan adds a fresh UUID directory beneath each supplied destination. It
retains the original step number, ordered input occurrences, repeated inputs,
pairing groups, and captured associated artifacts. It appends a fresh pipeline,
step, and execution attempt through the normal commit lifecycle. Original rows
remain unchanged; diagnostic attempts never supply either execution or step
cache hits.

---

## Step 1: Select an execution ID

Use `execution_run_id` from the executions table or failure report:

```bash
artisan failures --delta-root runs/delta --json
```

A pipeline ID or step ID cannot identify one execution unit. For composites,
select a child's execution ID. Manual interactive selections have no executable
unit and report unavailable replay evidence.

## Step 2: Choose diagnostic destinations

The local CLI supplies working, staging, files, and log defaults:

```bash
artisan execution replay EXECUTION_RUN_ID \
  --delta-root runs/delta --debug-root debug/case-17 --json
```

For cloud stores, supply staging and files URIs with the store's protocol:

```bash
artisan execution replay EXECUTION_RUN_ID \
  --delta-root s3://example/runs/delta --debug-root debug/case-17 \
  --staging-root s3://example/debug/staging \
  --files-root s3://example/debug/files --json
```

Working and failure-log roots are local paths. Storage credentials use current
environment discovery. The Python API accepts a fresh `StorageConfig` through
`RuntimeEnvironment.storage`. Diagnostic destinations cannot overlap the Delta
root or original sandbox.

Local lifecycle workers enable DEBUG logging and tool streaming. Driver logging
remains caller-owned. Endpoint workers retain their server logging configuration;
the returned diagnostic copy contains their full tool log and job-owned input and
output files. Remote copies appear beneath `remote-debug/artifact_<index>/` in
the local sandbox, before a remote tool failure is raised. Endpoint containers
still clean up their temporary directories.

## Step 3: Supply omitted configuration explicitly

Replay snapshots omit explicit environment values, credential parameters, secret
fields, and credential-bearing URLs. Map each reported JSON pointer to a current
environment variable name:

```bash
artisan execution replay EXECUTION_RUN_ID \
  --delta-root runs/delta --debug-root debug/case-17 \
  --supply /environments/local/env/TOKEN=SERVICE_TOKEN --json
```

Python uses `replacement_env={"/params/token": "SERVICE_TOKEN"}`. Values are read
on the caller before validating the concrete operation. Valid JSON decodes to
its scalar, object, or array value; other input remains a string. JSON-quote a
string such as `"null"` when it must remain text. Suggested variable names are
hints; every slot requires an explicit mapping. This argument accepts only
recorded redacted slots.

Known replacement values are omitted from new framework diagnostic rows and
error envelopes. Raw tool output and operation-produced files can contain what
the operation writes; preserved files are outside that filtering guarantee.

## Step 4: Handle changed code and dependencies

Install the recorded operation and dependencies. Replay checks the concrete
operation's name, version, module digest, configuration fields, and behavior.
Use `allow_code_change=True` or `--allow-code-change` to acknowledge detected
code differences. Notebook classes can be supplied through
`operation_class=MyOperation`; unverifiable code also needs explicit allowance.
The worker checks against the selected driver code, even with this allowance.

An external lifecycle runner requires its configured Python `RunnerBase`
instance. Pass `step_runner="local"` or `--local-runner` to move that lifecycle
to a local worker. Compute routing remains part of the recorded operation, so a
local lifecycle can still invoke the original endpoint. Endpoints require
diagnostic-capable deployments and a compatible identity handshake.

---

## Verify

Check both the execution outcome and evidence delivery:

```python
from artisan.orchestration import StepStatus

assert result.step_result.status is StepStatus.SUCCEEDED
assert result.diagnostic_status == "complete"
print(result.diagnostic_errors)
print(result.reproducibility_notes)
```

The CLI exits zero only when execution succeeds and requested evidence delivery
is complete. A failed operation can still commit a new execution ID and preserve
useful files. Cancellation follows ordinary staging-discard semantics and may
return `execution_run_id=None`; remaining working files are best effort. Commit
failures raise a structured persistence error with diagnostic context.

Curators read the current committed store. Explicit run-scoping parameters are
retained, but arbitrary store queries, external files, network reads, randomness,
and mutable images are not historical snapshots. Replay verifies tracked
artifact content and freezes framework-discovered associations; it does not
promise identical output for external state that changed.

---

## Cross-references

- [Execution Flow](../concepts/execution-flow.md) — dispatch, execution, and commit
- [Configuring Execution](configuring-execution.md) — command inspection and preservation
- [Glossary](../reference/glossary.md) — execution and artifact terminology
