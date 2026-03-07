# Design: Subprocess Cleanup on Ctrl+C

**Status:** Proposed
**Date:** 2026-03-06
**Scope:** `src/artisan/utils/external_tools.py`

---

## Problem

When the user presses Ctrl+C during a pipeline run, child processes spawned by
`subprocess.Popen` (apptainer containers running MPNN, Rosetta, etc.) become
orphans and keep running indefinitely. The user must manually find and kill them.

This happens because:

1. `_run_with_streaming` iterates over `process.stdout` with no `try/finally`
   cleanup — if `KeyboardInterrupt` hits, the `Popen` child is never terminated.
2. No `process_group` or `start_new_session` is set, so the child inherits the
   parent's process group and `killpg` cannot target it.
3. The non-streaming path (`subprocess.run`) has the same issue.
4. There is **zero** signal handling, process group management, or interrupt
   cleanup anywhere in the artisan codebase.

### Orphan Process Chain

```
Ctrl+C
  → PipelineManager: KeyboardInterrupt propagates, finalize() never called
    → StepExecutor: Prefect ProcessPoolTaskRunner workers continue
      → run_external_command: Popen child never terminated
        → apptainer container: fully orphaned (re-parented to PID 1)
```

---

## Approach

Fix at the subprocess layer only (`external_tools.py`). This is the single
chokepoint — every external command in pipelines goes through
`run_external_command`. No changes needed at higher layers.

---

## Design

### 1. `_kill_process_group` helper

Sends SIGTERM to the child's process group, waits up to 3 seconds for graceful
shutdown, then escalates to SIGKILL. Handles the race where the process has
already exited.

```python
def _kill_process_group(process: subprocess.Popen, timeout: float = 3.0) -> None:
    """Kill a subprocess and its entire process group.

    Args:
        process: Subprocess to kill (must have been started with process_group=0).
        timeout: Seconds to wait after SIGTERM before escalating to SIGKILL.
    """
    import os
    import signal

    try:
        pgid = os.getpgid(process.pid)
        os.killpg(pgid, signal.SIGTERM)
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(pgid, signal.SIGKILL)
            process.wait()
    except ProcessLookupError:
        pass
```

### 2. Modify `_run_with_streaming`

Two changes:

- **Add `process_group=0`** to `Popen` — gives the child its own process group
  so `os.killpg` can target the entire tree (container + its children).
- **Wrap stdout loop** in `try/except BaseException` that calls
  `_kill_process_group` before re-raising.

```python
process = subprocess.Popen(
    cmd.parts,
    cwd=cwd,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
    bufsize=1,
    env=env,
    process_group=0,  # own process group for clean killpg
)

try:
    for line in process.stdout:
        # ... existing timeout/logging/print logic ...
    returncode = process.wait()
except BaseException:
    _kill_process_group(process)
    raise
```

The timeout path also switches from `process.kill()` to
`_kill_process_group(process)` to kill the full tree.

### 3. Add `_run_captured` helper

Replace `subprocess.run()` in the non-streaming path with a `Popen`-based
equivalent that has the same `process_group=0` + cleanup pattern:

```python
def _run_captured(cmd, cwd, timeout, env):
    process = subprocess.Popen(
        cmd.parts, cwd=cwd,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, env=env, process_group=0,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except BaseException:
        _kill_process_group(process)
        raise
    return subprocess.CompletedProcess(
        args=cmd.parts, returncode=process.returncode,
        stdout=stdout, stderr=stderr,
    )
```

### 4. Wire into `run_external_command`

Replace `subprocess.run(...)` with `_run_captured(cmd, cwd, timeout, env)`.

---

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Where to fix | `external_tools.py` only | Single chokepoint for all external commands |
| Process isolation | `process_group=0` | More precise than `start_new_session=True` — creates new process group without detaching from controlling terminal. Avoids TTY issues. Available in Python 3.11+ (we require 3.12). |
| Signal sequence | SIGTERM → 3s wait → SIGKILL | Apptainer needs SIGTERM for clean overlay unmount. 3s is short enough to not frustrate the user. |
| Exception type | `except BaseException` | `KeyboardInterrupt` is a `BaseException`, not `Exception` |
| Higher-layer changes | None | Fixing the subprocess layer is sufficient — all external commands funnel through `run_external_command` |

---

## Edge Cases

- **Process already exited:** `os.getpgid` raises `ProcessLookupError` → caught and ignored.
- **Partial group exit:** `os.killpg` sends signal to all processes in group; already-exited ones are skipped by the kernel.
- **Nested containers:** Apptainer's child processes (container init, user process) share the same process group — `killpg` reaches them all.
- **`TimeoutExpired` from `process.communicate`:** Flows through `except BaseException` → cleanup → re-raise → caught by existing `except subprocess.TimeoutExpired` in `run_external_command`.

---

## Tests

**File:** `tests/artisan/utils/test_external_tools.py`

| Test | Verifies |
|------|----------|
| `test_kill_process_group_sigterm_sufficient` | Only SIGTERM sent when process exits promptly |
| `test_kill_process_group_escalates_to_sigkill` | SIGKILL sent after SIGTERM timeout |
| `test_kill_process_group_already_dead` | `ProcessLookupError` swallowed |
| `test_streaming_popen_uses_process_group` | `process_group=0` passed to Popen |
| `test_streaming_interrupt_kills_group` | KeyboardInterrupt triggers cleanup |
| `test_streaming_timeout_kills_group` | Timeout triggers `_kill_process_group` instead of `process.kill` |
| `test_captured_interrupt_kills_group` | Same for non-streaming path |

---

## Verification

```bash
# Unit tests
~/.pixi/bin/pixi run -e dev test-unit -- tests/artisan/utils/test_external_tools.py

# Manual: run a pipeline, Ctrl+C during an external command step,
# verify no orphan processes:
ps aux | grep apptainer
```

---

## Files Modified

| File | Change |
|------|--------|
| `src/artisan/utils/external_tools.py` | Add `_kill_process_group`, `_run_captured`; modify `_run_with_streaming` and `run_external_command` |
| `tests/artisan/utils/test_external_tools.py` | Add `TestProcessCleanup` (7 tests) |
