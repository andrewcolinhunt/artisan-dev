# Debug a Recorded Execution

Replay one recorded execution with fresh diagnostic IDs and retained working
files. The original result stays unchanged, and diagnostic attempts cannot supply
ordinary cache hits.

**Prerequisites:** A supported store, an execution ID from
[failure inspection](inspecting-provenance.md#diagnose-failures-and-timing), and
the operation definitions installed in the selected worker environment.

## Replay an execution

Supply the original store binding and the definition you authorize Artisan to
execute. This fragment assumes `execution_run_id` names a recorded `DataGenerator`
execution:

```python
from pathlib import Path

from artisan.operations.examples import DataGenerator
from artisan.orchestration import replay_execution

result = replay_execution(
    execution_run_id,
    delta_root="runs/delta",
    operation=DataGenerator,
    working_root=Path("debug/work").resolve(),
    staging_root="debug/staging",
)
print(result.step_result.status)
print(result.execution_run_id)
print(result.diagnostic_roots)
```

For a composite, pass its class as `operation` and its selected child definitions
through `definitions=(...)`. Supply trusted application registration modules
through `application_modules` when your artifact types need them. Stored module
names never authorize imports.

Artisan creates fresh working and staging children and records a new diagnostic
run in the same store. Replay preserves the captured input occurrences, ordering,
pairing, and explicit association context. Missing or inconsistent source evidence
fails before scientific execution.

## Select an execution ID

Use `execution_run_id` from an execution record or failure report:

```bash
artisan failures --delta-root runs/delta --json
```

A pipeline or step ID does not identify one unit. For a composite, select its
outer execution ID; internal calls are part of that same recorded boundary.

## Choose diagnostic destinations

The CLI requires an explicit importable definition:

```bash
artisan execution replay EXECUTION_RUN_ID \
  --operation artisan.operations.examples:DataGenerator \
  --delta-root runs/delta --debug-root debug/case-17 --json
```

Use repeatable `--definition MODULE:CLASS` for composite children and
`--load-module MODULE` for trusted registrations. `--debug-root` chooses the
parent for fresh local working files. `--staging-root` must use the source store's
configured protocol. `--files-root` names the source's managed artifact files,
not a separate debug files directory. Nonlocal stores require it explicitly.

Use `--storage-config path/to/storage.json` for an explicit `StorageConfig` JSON
file. The Python API accepts `storage=StorageConfig(...)`. Credentials remain
caller-supplied runtime configuration. Diagnostic destinations must not overlap
the store or an existing source sandbox.

## Supply omitted configuration explicitly

Replay evidence omits private configuration. Restore reported runtime slots with
`--runtime KEY=ENVIRONMENT_VARIABLE`. For a curator configuration slot, use
`--supply /params/token=SERVICE_TOKEN`. The CLI reads the named environment variable;
configuration replacements decode JSON when valid and otherwise remain strings.
JSON-quote text such as `"null"` when it must remain a string.

The Python API accepts actual values through `runtime_replacements` and, for
curators, `configuration_replacements`. For example, continuing with a curator
whose evidence reports the corresponding omitted slot:

```python
import os

result = replay_execution(
    execution_run_id,
    delta_root="runs/delta",
    operation=MyCurator,
    configuration_replacements={"/params/token": os.environ["SERVICE_TOKEN"]},
)
```

Only recorded omitted slots accept replacements. Scientific parameters cannot
carry private credentials; use the runner's runtime configuration. Replacement
sensitivity carries into new framework diagnostic records. Raw tool output and
operation-produced files can contain whatever the operation writes and are
outside that filtering guarantee.

## Handle changed code and dependencies

Install the selected operation and dependencies before replay. For an external
provider, pass its configured runner instance as `step_runner`; recorded provider
names cannot reconstruct credentials or deployment configuration. The CLI's
`--local-runner` explicitly selects local execution. Prepare any required tool
environments beforehand.

Replay checks operation identity and selected software. An observed local worker
also captures its inherited environment, which can differ in a fresh process.
`allow_code_change=True` or `--allow-code-change` explicitly permits selecting
changed code or runtime software for diagnosis. It
does not authorize stored imports, invent missing snapshots, or bypass input and
store integrity checks. Worker entry still verifies the selected installed code.

(verify-replay)=
## Verify

Check both execution and diagnostic coverage:

```python
from artisan.orchestration import StepStatus

assert result.step_result.status is StepStatus.SUCCEEDED
assert result.diagnostic_status == "complete"
print(result.diagnostic_errors)
print(result.reproducibility_notes)
```

A failed operation can still have a new execution ID and useful evidence. The
execution ID may be absent when work never produced an accepted record.
Diagnostic coverage is reported separately as complete, incomplete, or unavailable;
missing diagnostics do not establish that no command ran.

Unconfirmed cleanup retains ownership and evidence. Do not treat a provider
acknowledgement or process exit alone as proof that all external work stopped.
See [managed command ownership](configuring-execution.md#keep-managed-commands-in-the-foreground).

Curators can read current accepted store state. Their arbitrary queries, external
files, network reads, and randomness are not historical snapshots. Replay verifies
captured framework context; it cannot promise identical output when external
state changes.

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

evidence = inspect_commands(delta_root, execution_run_id)
print("Coverage:", evidence["status"])
groups = [
    (invocation["call_path"], invocation["recording"])
    for invocation in evidence["invocations"]
]
if evidence["recording"] is not None:
    groups.append(([], evidence["recording"]))  # A curator's direct recording.
for call_path, recording in groups:
    for command in recording["commands"]:
        print(call_path, command["sequence"], command["argv"], command["outcome"])
```

`requested_argv` contains the requested arguments; `argv` includes the actual
Local, Pixi, Docker, or Apptainer wrapper. Each record includes the worker's
working directory, declared tool, environment identity, outcome, and exit code.
An observed Python-only invocation has a complete, empty command list.

Scientific executions group commands by invocation and composite call path;
curators have a direct `recording`. Read command `invocation` and `sequence`
within their recording, not as indices shared across groups. Commands in separate
execution units can overlap; their positions do not represent a shared clock or
completion order. Recording covers `run_command`
and Artisan's invocation helpers, including preprocessing and postprocessing.
Commands from operation threads share the invocation's recording; join those
threads before returning from the phase. Direct `subprocess` calls are outside
this capture scope.

### Check whether evidence is complete

Check the top-level `status` before interpreting an empty list. A null
`omitted_invocations` means the execution's diagnostics are unavailable; a
positive count means some invocation groups were omitted. Within each recording,
`missing_invocations`, `omitted_commands`, and `omitted_missing_invocations`
describe incomplete evidence. An unavailable recording does not prove that the
worker launched no command.

Command recordings and execution diagnostics have size limits. Truncation keeps
an ordered prefix and makes coverage partial. Inspection reads the selected
execution's evidence; it never reconstructs missing commands from logs.

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
