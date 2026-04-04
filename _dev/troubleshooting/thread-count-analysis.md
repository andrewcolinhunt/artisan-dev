# Thread Count Analysis: 140 Threads Per Kernel

**Date:** 2026-04-03
**Trigger:** User reports ~140 threads per Jupyter kernel after running tutorials.

---

## Measured Thread Budget (Local Backend, Self-Hosted Prefect)

Ran `measure_threads.py` and `measure_threads_full.py` with 4 pipelines
(2 finalized, 2 leaked), 12 total step executions, local backend.

| Stage | Threads | What's there |
|---|---|---|
| Baseline (no imports) | 1 | MainThread |
| After all artisan imports | 1 | MainThread |
| After PipelineManager.create() | 1 | MainThread (executor thread is lazy) |
| After first pipeline.run() | 3 | + RunSyncEventLoopThread, pipeline-step_0 |
| After 3 steps + finalize | 2 | - pipeline-step_0 (executor shut down) |
| After 2 leaked pipelines (1 ran, 1 create-only) | 3 | + 1 pipeline-step_0 (from ran pipeline; create-only doesn't start the thread) |
| After 12s idle | 3 | No change (RunSyncEventLoopThread is permanent) |

**Artisan's total contribution: 1 thread per pipeline executor + 1 global
Prefect thread.** Leaked pipelines add 1 thread each (only if a step was
actually run — the ThreadPoolExecutor is lazy).

---

## What Did NOT Activate

These Prefect background services exist in the code but were **never started**
during our local-backend test:

| Service | Thread name | Why absent |
|---|---|---|
| GlobalEventLoopThread | `GlobalEventLoopThread` | No QueueService was started |
| APILogWorker | `APILogWorkerThread` | API log forwarding not triggered (self-hosted, no remote API logging) |
| EventsWorker | `EventsWorkerThread` | Events not emitted to worker (self-hosted server, no cloud) |
| Heartbeat thread | (unnamed) | `heartbeat_seconds` is None by default |
| AnyIO worker pool | `AnyIO worker thread` | No `anyio.to_thread.run_sync()` calls |

In our test, Prefect's sync-to-async bridge (`RunSyncEventLoopThread`) was
the **only** Prefect thread created. All flow execution goes through
`ProcessPoolExecutor` in subprocesses, not threads.

---

## The Gap: 3 Measured vs 140 Reported

Our measurement shows **3 threads** in steady state. The user reports
**~140**. The gap of ~137 threads is NOT explained by the design doc's
root causes (leaked executors, SettingsContext, unfinalised notebooks).

### Possible explanations to investigate

**Jupyter kernel baseline (~8 threads):**
IPykernel adds ~8 threads (ControlThread, ShellChannel, IOPub, Heartbeat,
2x fd-watcher, zmq GC). Not present in our standalone script. Accounts
for ~8 of the gap.

**Prefect Cloud vs self-hosted:**
If the user is connected to Prefect Cloud, all API services activate:
GlobalEventLoopThread, APILogWorkerThread, EventsWorkerThread, plus
anyio worker threads for each API call. Under load this could add 10-50+
threads depending on concurrency.

**SLURM backend:**
The reported notebooks include SLURM tutorials. The submitit task runner
may spawn monitoring threads, polling threads, or additional event loops.
We could not test this locally.

**Prefect server process confusion:**
The pstree shows `139*[{python}]` on one process. Need to verify this is
actually a kernel process and not the Prefect server process (which runs
uvicorn with many async workers).

**Server CPU count effect:**
Python's default `ThreadPoolExecutor` size is `min(32, os.cpu_count() + 4)`.
On a 128-core server, this caps at 32. If any library creates a default
executor, it could contribute up to 32 threads.

---

## Next Steps

To resolve the gap, we need the user to run this inside a Jupyter kernel
on their actual server:

```python
import threading

threads = threading.enumerate()
print(f"Thread count: {len(threads)}")
for t in sorted(threads, key=lambda t: t.name):
    print(f"  {'D' if t.daemon else ' '} {t.name}")
```

This will show exactly what the 140 threads are. Run it:
- In a fresh kernel (baseline)
- After running a full tutorial notebook
- After running one of the leaking tutorials without restart

The thread **names** will immediately reveal the source (Prefect services,
anyio pool, submitit, etc.).

---

## Impact on Design Doc

The design doc's Fix A (PipelineManager cleanup) is still correct and
worthwhile — leaked executors DO add threads. But each leak adds only
**1 thread**, not dozens. With 7 leaked pipelines across all tutorials,
Fix A + Fix C prevent at most ~7 threads.

The 140-thread report likely has a different primary cause that the design
doc doesn't address. The thread enumeration from the user's server will
tell us what it is.
