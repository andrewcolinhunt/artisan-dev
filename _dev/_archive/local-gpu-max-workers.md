# Design: Resource-Aware max_workers Default for Local Backend

**Date:** 2026-03-29
**Status:** Proposed
**Repo:** artisan-dev
**File:** `src/artisan/orchestration/backends/local.py`

---

## Problem

The local backend defaults to `max_workers=4`, regardless of whether the
operation requires a GPU. When GPU operations create multiple execution
units, the process pool dispatches them all simultaneously. On a single-GPU
machine this causes:

1. **GPU memory contention** — each process loads the full model into its own
   CUDA context. Two RFD3 inferences on one GPU will likely OOM.
2. **No CUDA_VISIBLE_DEVICES isolation** — both processes default to GPU 0.
   Even on a multi-GPU machine, there's no per-worker GPU assignment.
3. **Worse throughput** — GPU time-sharing via the CUDA scheduler is slower
   than sequential execution for memory-bound workloads like diffusion models.

The MASTER_PORT fix (separate design doc) prevents the immediate crash, but
even with unique ports, parallel GPU execution on the local backend is
counterproductive.

---

## Current Flow

```
LocalBackend.create_flow(resources, execution, step_number, job_name)
    max_workers = execution.max_workers or self._default_max_workers  # default 4
    return self._build_prefect_flow(
        SIGINTSafeProcessPoolTaskRunner(max_workers=max_workers)
    )
```

`create_flow()` receives both `ResourceConfig` and `ExecutionConfig` but
currently ignores `resources` entirely. The `resources.gpus` field already
declares how many GPUs the operation needs — it's the right signal.

---

## Proposed Fix

One conditional in `LocalBackend.create_flow()`:

```python
def create_flow(
    self,
    resources: ResourceConfig,
    execution: ExecutionConfig,
    step_number: int,
    job_name: str,
) -> Callable[[str, RuntimeEnvironment], list[dict]]:
    """Build a local ProcessPool flow.

    GPU operations default to sequential execution (max_workers=1) to
    avoid GPU memory contention and CUDA context conflicts. CPU
    operations use the configured pool size.
    """
    if execution.max_workers is not None:
        max_workers = execution.max_workers
    elif resources.gpus > 0:
        max_workers = 1
    else:
        max_workers = self._default_max_workers

    return self._build_prefect_flow(
        SIGINTSafeProcessPoolTaskRunner(max_workers=max_workers)
    )
```

### Resolution order

| `execution.max_workers` | `resources.gpus` | Result | Rationale |
|------------------------|-------------------|--------|-----------|
| `3` (explicit) | any | `3` | User override always wins |
| `None` | `1` | `1` | GPU step → sequential |
| `None` | `0` | `4` | CPU step → parallel (default) |

### Behavior by operation

| Operation | `resources.gpus` | Default workers | Effect |
|-----------|-----------------|-----------------|--------|
| RFD3Conditional | 1 | 1 | Sequential — no GPU contention |
| RFD3Unconditional | 1 | 1 | Sequential |
| AF3 | 1 | 1 | Sequential |
| AF3fromcif | 1 | 1 | Sequential |
| MPNN | 0 | 4 | Parallel — CPU partition |
| FusedMPNN | 0 | 4 | Parallel — CPU partition |
| All metrics | 0 | 4 | Parallel |
| All configs/curators | 0 | 4 | Parallel |

### User override

Pipeline scripts can always set `execution={"max_workers": N}` to override:

```python
pipeline.run(
    operation=RFD3Conditional,
    name="rfd3",
    inputs={...},
    execution={"artifacts_per_unit": 1, "max_workers": 2},  # force 2 workers
    backend=Backend.LOCAL,
)
```

This works because `step_executor.py` applies execution overrides via
`model_copy(update=execution)` before passing to the backend.

### SLURM backend

No change needed. SLURM handles GPU isolation natively via `--gres` and
`CUDA_VISIBLE_DEVICES`. The `SlurmBackend.create_flow()` uses SLURM job
arrays, not a local process pool.

---

## Tests

Unit tests for `LocalBackend.create_flow()`:
- `resources.gpus=1`, `execution.max_workers=None` → pool size 1
- `resources.gpus=0`, `execution.max_workers=None` → pool size 4 (default)
- `resources.gpus=1`, `execution.max_workers=3` → pool size 3 (explicit wins)
- `resources.gpus=0`, `execution.max_workers=2` → pool size 2 (explicit wins)

---

## Verification

1. Run artisan unit tests
2. Run phosphopeptide tutorial on local backend — confirm RFD3 seeds run
   sequentially (check timestamps in logs)
3. Run a CPU-only pipeline — confirm metrics steps still parallelize
