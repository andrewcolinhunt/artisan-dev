# Design: Execution Log Taxonomy

**Status:** Draft
**Scope:** `EXECUTIONS_SCHEMA`, `parquet_writer.py`, `recorder.py`, `creator.py`, `dispatch.py`

---

## Problem

Pipeline executions produce diagnostic output at multiple layers of the stack.
We need to persist this output so users can debug failures without reproducing
them. The question is: how do we categorize and name the log columns?

The naive approach — one column per source (subprocess log, Python stdout, SLURM
job log) — creates an inconsistent experience: which column you check depends on
the compute backend and operation type. The user shouldn't have to think about
*where* to look.

---

## Execution Stack (Actual)

The execution model differs by operation type and compute backend. Understanding
the actual process/thread boundaries is critical to getting capture right.

### Creator operations

```
User process (notebook / script)
  └── PipelineManager.submit()                    [main thread]
       └── step_executor._execute_creator_step()  [background thread, ThreadPoolExecutor(1)]
            └── dispatch_to_workers()              [same thread]
                 └── Prefect @flow                 [same thread]
                      └── execute_unit_task.map()
                           │
                      ┌────┴────────────────────────────────────┐
                      │ LOCAL                                    │ SLURM
                      │ ThreadPoolTaskRunner(max_workers=N)      │ SlurmTaskRunner
                      │ Threads in user process                  │ Separate processes on compute nodes
                      │                                          │
                      ▼                                          ▼
                 execute_unit_task()                        execute_unit_task()
                   └── run_creator_flow()                    └── run_creator_flow()
                        ├── create_sandbox()                      ├── create_sandbox()
                        ├── materialize_inputs()                  ├── materialize_inputs()
                        ├── operation.preprocess()                ├── operation.preprocess()
                        ├── operation.execute()                   ├── operation.execute()
                        │    └── run_external_command() [opt]     │    └── run_external_command() [opt]
                        ├── operation.postprocess()               ├── operation.postprocess()
                        ├── stage Parquet                         ├── stage Parquet (to shared NFS)
                        └── cleanup sandbox                       └── cleanup sandbox
```

**Key difference:** LOCAL workers are **threads** sharing the user process's
`sys.stdout`. SLURM workers are **separate processes** with their own stdout
captured by submitit log files.

### Curator operations

Curators **never go through dispatch**. They run directly in the step executor
thread:

```
step_executor._execute_curator_step()  [background thread]
  └── run_curator_flow()               [same thread, no Prefect, no worker pool]
       └── operation logic (filter/merge/ingest)
       └── stage Parquet
```

No worker process. No thread pool. No Prefect task. The "worker" concept does
not apply to curators at all.

### Output produced at each layer

| Layer | What it produces | LOCAL | SLURM |
|-------|-----------------|-------|-------|
| **PipelineManager** | Artisan logger: step lifecycle | Console (if configured) | Console (if configured) |
| **step_executor** | Artisan logger: dispatch/cache/timing | Console (if configured) | Console (if configured) |
| **dispatch** | Artisan logger: coordination | Console (if configured) | Console (if configured) |
| **execute_unit_task** | Artisan logger: error catch | Console (if configured) | **Silently dropped** (no handler on node) |
| **run_creator_flow** | Artisan logger: setup/timing | Console (if configured) | **Silently dropped** |
| **operation.execute()** | `print()`, Python logging | User's terminal | submitit log files |
| **run_external_command** | Subprocess stdout/stderr | `log_path` file | `log_path` file (on shared NFS) |
| **SLURM job wrapper** | Prefect runner, env setup, submitit | N/A | submitit log files |

**Critical finding:** Artisan `logging.getLogger("artisan.*")` calls inside
workers are **silently dropped on SLURM nodes** because no one calls
`configure_logging()` there. Only `print()` and subprocess output reach the
submitit log files.

---

## Key Insight: Categorize by Question, Not by Source

The user debugging a failure asks three questions:

1. **"What was the error?"** → Exception traceback
2. **"What was the operation doing?"** → All output from the operation
3. **"Was it an infrastructure issue?"** → Worker/SLURM-level output

These questions don't change based on compute backend or operation type. The
columns should map to these questions, not to the technical source of the data.

---

## Design: Three Columns

```python
EXECUTIONS_SCHEMA = {
    ...
    "error": pl.String,          # Exception traceback (format_error)
    "execution_log": pl.String,  # All output from operation.execute()
    "worker_log": pl.String,     # Worker process output (SLURM only)
    ...
}
```

### `error` — Why did it fail?

The exception traceback, formatted by `format_error()`. Full traceback truncated
to 50 lines from the tail. Only populated on failure.

### `execution_log` — What was the operation doing?

**All output produced during `operation.execute()`**, regardless of whether it
came from Python code or an external command. This is the single place to look
for operation-level diagnostics.

The goal: always captured by the same mechanism, always in the same column,
regardless of backend. See [Open Question 1](#open-question-1) for the
thread-safety constraint that complicates this.

### `worker_log` — What was the infrastructure doing?

The worker process output *outside* the operation. For SLURM, this is the job's
stdout/stderr (captured via `future.logs()`). For local and curator runs, this
is `NULL` — not because we failed to capture it, but because **there is no
separate worker process**.

| Context | `worker_log` value | Why |
|---------|-------------------|-----|
| Creator (SLURM) | submitit log content | Separate process with captured stdout |
| Creator (LOCAL) | `NULL` | Thread in user process — no separate stdout |
| Curator (any) | `NULL` | Runs directly in orchestrator — no worker at all |

---

## Why Not Two Columns?

We considered keeping the external command output separate from Python stdout
(i.e., `tool_log` + `worker_log`). This fails because:

1. **Pure Python operations get nothing.** A metrics calculation that prints
   diagnostic output before failing would have `tool_log = NULL` (no subprocess)
   and `worker_log = NULL` (local run). The only diagnostic is the traceback in
   `error`. This is the most common debugging scenario for local development.

2. **The column you check depends on context.** For AF3 failures you'd look at
   `tool_log`. For metrics failures you'd look at `worker_log` (on SLURM) or
   nowhere (on local). Inconsistent UX.

3. **The boundary between "tool" and "Python" is arbitrary.** An operation might
   do significant Python work before calling an external command. Where does that
   output go? The answer should be: the same place as everything else.

---

## Naming Decision

`execution_log` was chosen over alternatives:

| Name | Why not |
|------|---------|
| `tool_output` | Implies external command only; excludes Python operations |
| `tool_log` | Same problem — "tool" means subprocess |
| `command_log` | Even more specific to subprocess |
| `operation_log` | Reasonable, but `operation` in the codebase refers to the class definition, not a single run |
| `process_log` | Ambiguous — which process? |
| `execution_log` | **Matches `execution_run_id`, `execution_spec_id` naming. Describes a single execution's output.** |

`worker_log` is correct as-is — it describes the worker process, consistent with
`source_worker` column.

---

## Open Questions

### Open Question 1: Thread-safe stdout capture for LOCAL {#open-question-1}

The design calls for redirecting `sys.stdout`/`sys.stderr` during
`operation.execute()` to capture Python output. This works for SLURM (separate
process), but **is not thread-safe for LOCAL** where multiple threads share
`sys.stdout` via `ThreadPoolTaskRunner(max_workers=N)`.

**Why it matters:** Without solving this, `execution_log` for pure Python
operations on LOCAL would only contain `log_path` content (external command
output), which is `NULL` for operations that don't call `run_external_command()`.
This violates the "consistent across backends" goal.

**Options:**

| Option | Mechanism | Captures Python stdout? | Thread-safe? | Tradeoffs |
|--------|-----------|------------------------|-------------|-----------|
| **A. Accept asymmetry** | Only capture `log_path` content for LOCAL | No | Yes | Simple. LOCAL stdout goes to terminal (visible but not persisted). SLURM captures everything. Breaks the consistency promise for pure Python operations. |
| **B. `os.dup2` fd redirect** | Redirect file descriptors 1 and 2 to a file around `execute()` | Yes (all threads) | Process-wide | Captures everything but contaminates concurrent threads. Would need `max_workers=1` to be safe, killing parallelism. |
| **C. Per-execution file logger** | Pass a file handle via `ExecuteInput`; operations write to it instead of `print()` | Only if operations opt in | Yes | Requires operation code changes. Non-intrusive for existing operations but they'd need updating. Framework `print()` still lost. |
| **D. Redirect only for SLURM** | `sys.stdout` redirect in SLURM process; no redirect for LOCAL | SLURM only | Yes | Clean for SLURM. LOCAL gets only `log_path` content. Honest about the limitation. |
| **E. Capture at `run_creator_flow` level** | Redirect stdout for the entire creator flow, not just execute() | Yes | Same thread-safety issue as B | Same problem — threads share stdout. |

**Current leaning:** Option A or D — accept that LOCAL Python stdout is visible
in the terminal but not persisted to `execution_log`. The `log_path` file
(external command output) is captured for both backends. The consistency gap only
affects pure Python operations on LOCAL, which is the development/debugging
context where terminal output is most useful anyway.

### Open Question 2: Should we fix the SLURM logging gap?

Artisan `logger.*` calls inside workers are silently dropped on SLURM nodes
because `configure_logging()` is never called there. This means:

- `logger.info("Creator setup failed for %s", run_id)` → lost
- `logger.debug("Execution %s timings: %s", run_id, timings)` → lost
- `print("Processing structure...")` → captured in submitit log files

**Should we call `configure_logging()` inside `run_task_in_slurm()`?** This
would route artisan logger output to stdout on the SLURM node, which submitit
captures. The output would then appear in `worker_log`.

**Arguments for:** More complete SLURM worker output. Framework diagnostic
messages (timing, setup errors) would be preserved.

**Arguments against:** Adds noise to `worker_log`. Framework logging is rarely
the root cause of user-facing failures. Could be done independently of this
design.

### Open Question 3: Curator log columns

Curators never go through dispatch and have no worker concept. Both
`execution_log` and `worker_log` would be `NULL` for curator executions.

Is this acceptable? Curators do metadata-only work (filter, merge, ingest) and
rarely produce diagnostic output. Their failures are typically schema mismatches
or empty inputs, which are fully described by the `error` column.

If we want curator output captured, we'd need to add capture to
`run_curator_flow()` directly, which runs in the step executor thread (same
thread-safety concern as LOCAL creators, though curators are single-threaded
so it would be safe).

---

## Overlap Between `execution_log` and `worker_log`

For SLURM runs with stdout redirection (if we solve Open Question 1 for SLURM):
Python stdout during `execute()` would be redirected to the `execution_log`
capture file, so it would NOT appear in the SLURM job stdout. This cleanly
separates the two:

- `execution_log`: What the operation said (redirected, not in SLURM stdout)
- `worker_log`: What the infrastructure said (everything else in SLURM stdout)

Without stdout redirection (Option A/D): Python stdout during `execute()` goes
to the SLURM job stdout, so it appears in `worker_log`. The `execution_log`
would only contain `log_path` content. There is partial overlap — the tool's
subprocess output goes to `log_path` (→ `execution_log`) and is also echoed to
stdout by `_run_with_streaming()`'s `print()` calls (→ `worker_log`).

For `_run_with_streaming` specifically: subprocess output is written to the
`log_path` file directly AND echoed to stdout via `print()`. So the external
command output appears in both `execution_log` (from the file) and `worker_log`
(from the print echo in SLURM stdout). This duplication is acceptable — they
serve different access patterns.

---

## Summary

| Column | Question | Populated | Backend-dependent? |
|--------|----------|-----------|-------------------|
| `error` | Why did it fail? | On failure | No |
| `execution_log` | What was it doing? | See Open Question 1 | Goal: no |
| `worker_log` | Infrastructure issue? | SLURM creators only | Yes (by nature) |

The user always checks `execution_log` first. `worker_log` is supplementary
for SLURM-specific infrastructure debugging. The open questions determine how
complete `execution_log` is for pure Python operations on LOCAL.
