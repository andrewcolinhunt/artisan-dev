# Debug a Recorded Execution

Replay one recorded execution with fresh IDs, verbose local worker output, and
retained working files.

**Prerequisites:** A current-format store and an execution ID from
[inspecting failures](inspecting-provenance.md#diagnose-failures-and-timing).
See [Execution Flow](../concepts/execution-flow.md) for lifecycle terminology.

---

## Replay an execution

Supply an `execution_run_id` from the store and separate diagnostic destinations:

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

Artisan creates UUID directories beneath the diagnostic destinations and appends
new pipeline, step, and execution attempts. Replay preserves the original step
number, ordered and repeated inputs, pairing groups, and captured associations.
Original records remain unchanged; diagnostic attempts cannot supply cache hits.

---

## Select an execution ID

Use `execution_run_id` from the executions table or failure report:

```bash
artisan failures --delta-root runs/delta --json
```

A pipeline ID or step ID cannot identify one execution unit. For composites,
select a child's execution ID. Manual interactive selections have no executable
unit and report unavailable replay evidence.

## Choose diagnostic destinations

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

Local lifecycle workers enable DEBUG logging and tool streaming; you control
driver logging. Endpoint workers keep their server logging configuration. Once
managed command cleanup is confirmed, their full tool log and job-owned files are
copied beneath `remote-debug/artifact_<index>/` in the local sandbox, before any
remote tool failure is raised. If cleanup is uncertain, diagnostic capture is
unavailable. The endpoint retains its workspace without reading, uploading, or
deleting files.

## Supply omitted configuration explicitly

Replay snapshots omit unclassified environment values, credential parameters,
secret fields, and credential-bearing URLs. Map each reported JSON pointer
(the path to an omitted value) to a current environment variable name:

```bash
artisan execution replay EXECUTION_RUN_ID \
  --delta-root runs/delta --debug-root debug/case-17 \
  --supply /environments/local/env/TOKEN=SERVICE_TOKEN --json
```

In Python, pass `replacement_env={"/params/token": "SERVICE_TOKEN"}`. The caller
reads values before operation validation, decoding valid JSON and retaining
other values as strings. JSON-quote text such as `"null"` to keep it a string.
Explicitly map each omitted slot; suggested variable names are hints. Only
recorded redacted slots accept replacements.

Known replacement values are omitted from new framework diagnostic rows and
error envelopes. Raw tool output and operation-produced files can contain what
the operation writes; preserved files are outside that filtering guarantee.

The same policy applies to ordinary attempts' new parameter and override records,
execution settings, literal input locations, errors, and authored result metadata,
including pending records, cache reuse, and interactive-filter commits. Redaction
affects diagnostic copies. Operations receive the original values; cache identity
and artifact content remain unchanged.

### Declare ordinary environment configuration

Declare an environment value public only when it is ordinary configuration that
may appear in persisted replay evidence and diagnostics:

```python
from artisan.schemas import DockerEnvironmentSpec

environment = DockerEnvironmentSpec(
    image="example/tool:fixed",
    env={"APPLICATION_ROOT": "/opt/application"},
    public_env=("APPLICATION_ROOT",),
)
```

`public_env` names entries in the same object's `env` mapping. It is available
on local, Docker, Apptainer, Pixi, and Modal compute configurations. Public values
remain unchanged in replay configuration, including aliases in mount keys, and
need no replacement. Environment values still affect cache identity; the
annotation itself and its ordering do not.

The default empty tuple protects every explicit env value. Credential names and
credential-bearing URLs cannot be declared public. Typed secrets, known secret
aliases, ambient credentials, and explicit replay replacements remain protected.
A sensitive value inside a dictionary key still makes replay capture unavailable.
Before sealing an execution record, Artisan checks copied command and replay
evidence against secrets known at that point. If command flags or result metadata
reveal a previously public value in the captured replay configuration, that
snapshot becomes unavailable instead of retaining the value.

Earlier pending records cannot use sensitivity discovered after their write,
including values created only by parameter default factories. Existing records
are not rewritten. Declare credentials through typed secret fields, recognized
credential names, or protected environment entries before submission; public
declarations require trusted configuration authors.

Historical dynamic-curator cache entries with unredacted result metadata can
fail reuse with `terminal metadata differs from curator execution`. Run the
affected operation with `skip_cache=True` to create a protected result before
reusing it. The original records remain unchanged.

Modal `secrets`, `image_registry_secret`, and `auth_secret` remain provider object
names or variable prefixes. Replay preserves these references and requires current
runtime access; it never stores their resolved credentials. Rotation behind an
unchanged reference is an external reproducibility limitation.

Fresh replay snapshots retain replacement sensitivity even when a replacement
also appears in a public field. Explicit replacements compute a new effective
execution identity and run a fresh diagnostic attempt with cache skipping.

## Handle changed code and dependencies

Install the recorded operation and dependencies. For application operations,
supply the trusted registration modules again through
`RuntimeEnvironment.application_modules` or CLI `--load-module`. Recorded module
names do not authorize imports. See
[application loading](configuring-execution.md#load-application-definitions-in-every-process).

Replay checks the operation's name, version, module digest, configuration fields,
and behavior. Use `allow_code_change=True` or `--allow-code-change` to acknowledge
code differences. Notebook classes can be supplied through
`operation_class=MyOperation`; unverifiable code also needs explicit allowance.
The worker checks against the selected driver code, even with this allowance.

An unavailable historical snapshot stays unavailable. Older ready snapshots
that lack the new `public_env` fields fail exact configuration validation;
`allow_code_change` does not authorize silently adding defaults. Keep such
evidence paired with its historical environment, or create a new ordinary
execution under the current configuration. Existing stores are not rewritten.

An external lifecycle runner requires its configured Python `RunnerBase`
instance. Pass `step_runner="local"` or `--local-runner` to move that lifecycle
to a local worker. Compute routing remains part of the recorded operation, so a
local lifecycle can still invoke the original endpoint. Endpoints require
diagnostic-capable deployments and a compatible identity handshake.

---

(verify-replay)=
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
useful files. Cancellation retains uncommitted staging and may return
`execution_run_id=None`; remaining working files are best effort. Diagnostic
staging is excluded from ordinary startup recovery and cache reuse. Commit
failures raise a structured persistence error with diagnostic context.

Unconfirmed command cleanup means Artisan could not prove work stopped or result
collection finished. It preserves the original error or interruption, skips output
collection, and retains the workspace. Missing logs or diagnostic archives in
this case do not establish that no command ran.
Check the affected tool or external provider before reusing its files. Host
process exit alone does not prove that external work stopped. Built-in
Docker cleanup additionally requires removal of the exact registered container;
other external environments and Modal have separate limits. See
[managed command limits](configuring-execution.md#keep-managed-commands-in-the-foreground).

Curators read the current committed store. Explicit run-scoping parameters are
retained, but arbitrary store queries, external files, network reads, randomness,
and mutable images are not historical snapshots. Replay verifies tracked
artifact content and freezes framework-discovered associations; it does not
promise identical output for external state that changed.

---

## Inspect provider logs

Provider logs can arrive after an execution has finished or after its staging
has been cleaned. Read the exact execution's log independently:

```python
from artisan.visualization import inspect_worker_log

print(inspect_worker_log(delta_root, execution_run_id))
```

Logs are stored under `_artisan/worker_logs/<execution_run_id>.log` in the Delta
root and can also be read for uncommitted or failed executions. The reader falls
back to an embedded committed worker log when available. Missing logs do not
establish an execution outcome; use persisted lifecycle and execution records
for that. Log delivery is best effort and does not change recovery eligibility.

## Inspect commands from an execution

After an execution commits, read the commands it attempted:

```python
from artisan.visualization import inspect_commands

recording = inspect_commands(delta_root, execution_run_id)
for command in recording["commands"]:
    print(
        command["invocation"], command["sequence"], command["argv"], command["outcome"]
    )
```

`requested_argv` contains the requested arguments; `argv` includes the actual
Local, Pixi, Docker, or Apptainer wrapper. Each record includes the worker's
working directory, declared tool, environment identity, outcome, and exit code.
Python-only executions have a complete, empty command list.

Read `invocation` as input dispatch order and `sequence` as command order within
that invocation. Endpoint calls can run concurrently; their positions do not
represent a shared clock or completion order. Recording covers `run_command`
and Artisan's invocation helpers, including preprocessing and postprocessing.
Commands from operation threads share the invocation's recording; join those
threads before returning from the phase. Direct `subprocess` calls are outside
this capture scope.

### Check whether evidence is complete

Check `status` before interpreting an empty list. `missing_invocations` explains
transport failures, cancellation, and missing or invalid endpoint evidence.
`unavailable_reason="worker_evidence_unavailable"` means the orchestrator had no
worker evidence when it synthesized a failure. Neither case proves that the
worker launched no command. Endpoints without recording support require redeployment.

The entire recording is capped at 1 MiB. When it reaches that limit, Artisan
keeps an ordered prefix and reports `omitted_commands` and
`omitted_missing_invocations`; later, shorter entries do not skip the cutoff.
Omissions make the recording partial. Inspection reads committed execution
rows; it never reconstructs missing evidence from logs.

### Supply credentials safely

Prefer environment variables or recognizable credential flags such as `--token`
and `--password`. For opaque values, pass `sensitive_values` to `run_command`:

```python
from artisan.utils import run_command

result = run_command(
    environment,
    ["tool", "--custom-option", credential],
    cwd=work_dir,
    sensitive_values=(credential,),
)
```

Recording removes explicit environment values, inherited credential values,
recognized credential arguments, and URL capabilities from diagnostic copies.
Ephemeral command-owner authentication values are also protected, including
aliases and values otherwise declared public. They are never restored from a
replay snapshot.
`redacted_fields` identifies affected fields; `required_environment` lists known
variable names that may require fresh credentials. Subprocess arguments and
successful return values keep their original values. Raw tool output files and
live stdout are not secret-filtered; arbitrary unknown positional secrets cannot
be inferred reliably.

Stored arguments can contain placeholders, and worker paths can disappear. Use
command evidence for diagnosis; use execution replay to reconstruct an operation.

---

## Cross-references

- [Execution Flow](../concepts/execution-flow.md) — dispatch, execution, and commit
- [Configuring Execution](configuring-execution.md) — command inspection and preservation
- [Glossary](../reference/glossary.md) — execution and artifact terminology
