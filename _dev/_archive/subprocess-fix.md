# Design: Fix curator subprocess re-importing user scripts

**Status:** Proposed
**Author:** Andrew Hunt
**Date:** 2026-03-27
**Issue:** User pipeline scripts are re-executed when `pipeline.run()` spawns curator subprocesses

---

## Problem

When a user runs a pipeline script, calling `pipeline.run()` on a curator
operation (e.g., `IngestStructure`, `Filter`) causes the user's entire script
to be re-executed in a child process. Any module-level code with side
effects runs again: argument parsing, file deletion, file creation, print
statements, etc.

This manifests as:

- Race conditions (`shutil.rmtree` deleting the output directory mid-run)
- Duplicate stdout output ("SCRIPT BEGIN" printed twice)
- Silent data corruption (input files overwritten during execution)
- Ingest failures (`0/1 succeeded`) cascading to pipeline-wide failure

The user must move all side-effecting code inside `if __name__ == "__main__"`
to avoid this. This requirement is counterintuitive and undocumented.

### Reproducer

```python
#!/usr/bin/env python
import shutil
from pathlib import Path

print("THIS RUNS TWICE")  # <-- visible in stdout

RUNS_DIR = Path("/tmp/my_pipeline")
if RUNS_DIR.exists():
    shutil.rmtree(RUNS_DIR)  # <-- deletes output mid-run

def main():
    pipeline = PipelineManager.create(name="example", delta_root=RUNS_DIR / "delta", ...)
    pipeline.run(operation=IngestStructure, inputs=[...], backend=Backend.LOCAL)
    # ^-- spawns subprocess that re-executes this entire file

if __name__ == "__main__":
    main()
```

### Who is affected

Any user who writes pipeline scripts with module-level side effects. This is
the natural way to write Python scripts and should not be a footgun.

---

## Root cause

`_run_curator_in_subprocess` in `step_executor.py` uses
`multiprocessing.get_context("spawn")` with `ProcessPoolExecutor`:

```python
def _run_curator_in_subprocess(unit, runtime_env, cancel_event=None):
    ctx = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=1, mp_context=ctx) as pool:
        future = pool.submit(run_curator_flow, unit, runtime_env, 0)
        ...
```

The `spawn` start method creates a fresh Python interpreter for each worker.
During bootstrap, CPython's `multiprocessing/spawn.py:prepare()` reads
`sys.modules['__main__'].__file__` from the parent process and re-imports
that file in the child via `runpy.run_path()`. This re-executes all
module-level code in the user's script.

The re-import is unconditional -- it happens regardless of whether the target
function or its arguments reference `__main__`. In our case, the function
(`run_curator_flow`) and arguments (`ExecutionUnit`, `RuntimeEnvironment`) are
entirely from installed artisan packages. The child process does not need the
user's module namespace at all.

### Scope

Two code paths trigger this:

| Path | Trigger | File |
|---|---|---|
| Curator subprocess | Any curator op (ingest, filter, merge) | `step_executor.py:_run_curator_in_subprocess` |
| Creator local backend | Creator ops on `Backend.LOCAL` | `local.py:SIGINTSafeProcessPoolTaskRunner` |

The curator path is the primary issue (always hits this). The creator path is
masked because curators typically run first and fail first.

### Why the subprocess exists

The subprocess provides two things:

1. **Memory isolation**: Curator memory is freed when the subprocess exits.
   Curators can process thousands of artifacts in a single `ExecutionUnit`,
   potentially allocating gigabytes.

2. **OOM containment**: If a curator exhausts memory, the OOM killer
   terminates the subprocess, not the orchestrator. The orchestrator catches
   `BrokenProcessPool` and reports a diagnostic message
   (`_format_subprocess_kill_error`) with peak RSS and system memory stats.

Both of these are valuable and must be preserved.

---

## Proposed solution: `subprocess.Popen` with artisan worker entry point

Replace `ProcessPoolExecutor` + `multiprocessing.spawn` with
`subprocess.Popen` launching a dedicated artisan worker module. The child
process's `__main__` becomes the artisan worker, not the user's script.

### Why this approach

`multiprocessing.spawn` is designed for parallelizing work within a single
program -- it assumes workers need the caller's module namespace.
`subprocess.Popen` is designed for launching separate programs in isolated
processes. We want isolation, not parallelism. Using the right tool means we
don't need to fight its assumptions.

This is the same pattern used by Celery, Dask, and Airflow for process
isolation. It also matches artisan's existing creator dispatch pattern, where
`_save_units` serializes `ExecutionUnit` objects to pickle files on disk.

### Design

#### New module: `artisan/orchestration/engine/_curator_worker.py`

A minimal entry point that loads serialized inputs, runs the curator, and
writes the result:

```
python -m artisan.orchestration.engine._curator_worker \
    <unit_path> <env_path> <result_path>
```

The worker:

1. Loads `ExecutionUnit` and `RuntimeEnvironment` from pickle files
2. Calls `run_curator_flow(unit, runtime_env, worker_id=0)`
3. On success: writes `StagingResult` to `result_path`
4. On failure: writes a `WorkerError(exception, traceback)` to `result_path`
5. Exits (memory freed by OS)

```python
"""Curator subprocess worker.

Runs a curator flow in an isolated process without importing the
user's __main__ module. Invoked by _run_curator_in_subprocess via
subprocess.Popen.
"""
from __future__ import annotations

import pickle
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path


@dataclass
class WorkerError:
    """Serializable error wrapper for cross-process exception propagation."""

    error_type: str
    message: str
    traceback: str


def main() -> None:
    unit_path, env_path, result_path = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])

    with open(unit_path, "rb") as f:
        unit = pickle.load(f)
    with open(env_path, "rb") as f:
        runtime_env = pickle.load(f)

    from artisan.execution.executors.curator import run_curator_flow

    try:
        result = run_curator_flow(unit, runtime_env, 0)
    except Exception as exc:
        result = WorkerError(
            error_type=type(exc).__name__,
            message=str(exc),
            traceback=traceback.format_exc(),
        )

    with open(result_path, "wb") as f:
        pickle.dump(result, f, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    main()
```

#### Updated: `_run_curator_in_subprocess`

```python
def _run_curator_in_subprocess(
    unit: ExecutionUnit,
    runtime_env: RuntimeEnvironment,
    cancel_event: threading.Event | None = None,
) -> StagingResult:
    """Run curator flow in an isolated subprocess.

    Uses subprocess.Popen with a dedicated worker entry point to avoid
    re-importing the user's __main__ module (which multiprocessing.spawn
    does unconditionally).
    """
    tmp_dir = Path(tempfile.mkdtemp(prefix="artisan_curator_"))
    unit_path = tmp_dir / "unit.pkl"
    env_path = tmp_dir / "env.pkl"
    result_path = tmp_dir / "result.pkl"

    try:
        with open(unit_path, "wb") as f:
            pickle.dump(unit, f, protocol=pickle.HIGHEST_PROTOCOL)
        with open(env_path, "wb") as f:
            pickle.dump(runtime_env, f, protocol=pickle.HIGHEST_PROTOCOL)

        proc = subprocess.Popen(
            [sys.executable, "-m", "artisan.orchestration.engine._curator_worker",
             str(unit_path), str(env_path), str(result_path)],
        )

        # Poll with cancellation support (matches existing 0.5s interval)
        while proc.poll() is None:
            if cancel_event is not None and cancel_event.is_set():
                proc.kill()
                proc.wait()
                raise RuntimeError("Curator interrupted by cancellation")
            time.sleep(0.5)

        # Detect OOM kill (SIGKILL = return code -9)
        if proc.returncode < 0:
            signal_num = -proc.returncode
            if signal_num == signal.SIGKILL:
                msg = _format_subprocess_kill_error(unit)
                raise _CuratorSubprocessKilled(msg)
            raise _CuratorSubprocessKilled(
                f"Curator subprocess killed by signal {signal_num}"
            )

        if proc.returncode != 0:
            raise RuntimeError(
                f"Curator subprocess exited with code {proc.returncode}"
            )

        with open(result_path, "rb") as f:
            result = pickle.load(f)

        if isinstance(result, WorkerError):
            raise RuntimeError(
                f"{result.error_type}: {result.message}\n{result.traceback}"
            )

        return result

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
```

#### New exception: `_CuratorSubprocessKilled`

Replace the `BrokenProcessPool` catch in `_execute_curator_step` with a
catch for `_CuratorSubprocessKilled`. Same handler logic, same diagnostic
output, just a different exception type.

```python
class _CuratorSubprocessKilled(Exception):
    """Raised when the curator subprocess is killed (typically OOM)."""
```

### What stays the same

- `_format_subprocess_kill_error` -- unchanged, `RUSAGE_CHILDREN` covers
  `subprocess.Popen` children
- `_execute_curator_step` -- same structure, catch `_CuratorSubprocessKilled`
  instead of `BrokenProcessPool`
- Cancellation -- same 0.5s polling loop with `cancel_event`
- All curator execution logic (`run_curator_flow`) -- untouched
- Creator path (`SIGINTSafeProcessPoolTaskRunner`) -- out of scope for this
  change (see Future Work)

### What changes

| Component | Before | After |
|---|---|---|
| Process creation | `ProcessPoolExecutor` + `multiprocessing.spawn` | `subprocess.Popen` |
| Child `__main__` | User's script (re-executed) | `artisan.orchestration.engine._curator_worker` |
| Argument passing | In-memory pickle via `pool.submit()` | Pickle files on disk |
| Result passing | In-memory via `future.result()` | Pickle file on disk |
| Exception propagation | Automatic (ProcessPoolExecutor) | `WorkerError` wrapper in result pickle |
| OOM detection | `BrokenProcessPool` | `returncode == -9` (SIGKILL) |

---

## Alternatives considered

### Neuter `__main__.__file__` before spawning

Temporarily set `sys.modules['__main__'].__file__ = None` before creating the
`ProcessPoolExecutor`, preventing CPython's spawn bootstrap from re-importing
the user's script.

**Rejected because:** Relies on a CPython implementation detail
(`multiprocessing/spawn.py:get_preparation_data` checking `__main__.__file__`).
Not a public API. Works today but creates undocumented coupling to CPython
internals. Appropriate as a hotfix but not as the long-term solution.

### `loky` process pool (joblib/scikit-learn)

`loky` was designed to fix the `__main__` re-import problem. Uses cloudpickle
and forkserver internally.

**Rejected because:** Adds a dependency for a single call site. Reusable worker
pools defeat the memory reclamation goal (curator memory stays in the
persistent worker). Would need `reuse=False` which negates loky's main benefit.

### In-process execution (no subprocess)

Call `run_curator_flow` directly in the orchestrator process.

**Rejected because:** Loses OOM containment. A curator that exhausts memory
kills the orchestrator. Also loses memory reclamation -- curator allocations
stay in the orchestrator's heap.

### `forkserver` start method

Use `multiprocessing.get_context("forkserver")` instead of `"spawn"`.

**Rejected because:** `_run_curator_in_subprocess` creates a fresh pool per
call, so the forkserver is started fresh each time, still triggering the
`__main__` re-import on each startup.

### In-process with `resource.setrlimit(RLIMIT_AS)`

Run the curator in-process but set a virtual memory limit so OOM becomes a
catchable `MemoryError`.

**Rejected because:** `RLIMIT_AS` limits virtual address space, not RSS.
Python, numpy, and mmap routinely allocate virtual memory far exceeding RSS.
False-positive `MemoryError` rate is too high for production use.

---

## Migration plan

### Phase 1: Hotfix (immediate)

Apply the `__main__.__file__ = None` workaround to `_run_curator_in_subprocess`
to unblock users now. This is a safe, minimal change while the proper fix is
implemented.

### Phase 2: Implement subprocess.Popen worker

1. Add `_curator_worker.py` module with `WorkerError` dataclass
2. Rewrite `_run_curator_in_subprocess` to use `subprocess.Popen`
3. Replace `BrokenProcessPool` catch with `_CuratorSubprocessKilled`
4. Add tests: normal execution, worker exception, OOM (SIGKILL), cancellation
5. Remove the Phase 1 hotfix

### Phase 3: Creator path (future work)

`SIGINTSafeProcessPoolTaskRunner` in `local.py` has the same root cause. The
creator path on `Backend.LOCAL` also spawns workers that re-import `__main__`.
This is masked because curators typically fail first, and because Prefect uses
cloudpickle for function dispatch (so the task itself works, but side effects
still run).

Fixing this is harder because it's inside Prefect's flow execution. Options:

- Apply the `__main__.__file__` neuter to `SIGINTSafeProcessPoolTaskRunner`
- Switch to `ThreadPoolTaskRunner` for local backends (loses OOM isolation)
- Replace `ProcessPoolTaskRunner` with a custom subprocess-based runner

This should be scoped as a separate piece of work.

---

## Testing

| Scenario | Test |
|---|---|
| Normal curator execution | Unit test: worker returns `StagingResult` |
| Curator raises exception | Unit test: worker returns `WorkerError`, parent re-raises |
| Curator OOM (SIGKILL) | Integration test: child calls `os.kill(os.getpid(), SIGKILL)`, parent detects `returncode == -9` and produces diagnostic |
| Cancellation | Unit test: set `cancel_event` during poll, verify child killed and `RuntimeError` raised |
| No `__main__` re-import | Integration test: script with `print("SHOULD NOT APPEAR")` at module level, verify it appears exactly once in stdout |
| Pickle round-trip | Unit test: `ExecutionUnit` and `RuntimeEnvironment` survive pickle serialization |
