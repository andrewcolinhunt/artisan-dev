# Design: Auto-assign MASTER_PORT for GPU Container Workloads

**Date:** 2026-03-29
**Status:** Implemented
**Repo:** artisan-dev
**File:** `src/artisan/schemas/operation_config/environment_spec.py`

---

## Problem

When multiple GPU operations run concurrently on the same node, PyTorch
Lightning's DDP initialization calls `torch.distributed.init_process_group`,
which creates a `TCPStore` on the default `MASTER_PORT` (29500). The second
concurrent process fails with `EADDRINUSE`.

This affects any backend where GPU execution units share a node:
- **Local backend:** `ProcessPoolExecutor(max_workers=4)` dispatches units
  simultaneously. All workers share the host network. Collision is guaranteed.
- **SLURM:** Job array tasks that land on the same multi-GPU node collide.

**Confirmed failure:** RFD3Conditional in the phosphopeptide tutorial (2 seeds,
`artifacts_per_unit=1`, local backend).

---

## Prior Art: Shell Script Workaround

A previous SBATCH script used `$RANDOM + ss/netstat` to pick ports:

```bash
pick_free_port() {
  RANDOM=$(( (SLURM_JOB_ID + SLURM_ARRAY_TASK_ID * 97) % 32768 ))
  for _ in $(seq 1 25); do
    p=$(( 20000 + (RANDOM % 45001) ))
    if ! ss -ltn | grep -Eq ":${p}$"; then echo "$p"; return 0; fi
  done
  echo $(( 12000 + (SLURM_JOB_ID + SLURM_ARRAY_TASK_ID) % 20000 ))
}
```

| Aspect | Shell script | `socket.bind(("", 0))` |
|--------|-------------|------------------------|
| **Concurrent-call collision** | Possible: `$RANDOM` is 15-bit (32768 values) | Impossible: OS assigns ports atomically |
| **Deterministic fallback** | `(JOB_ID + TASK_ID) % 20000` can collide | Not needed: OS always returns a free port |
| **Dependencies** | `ss` or `netstat`, SLURM env vars | Standard library only |
| **Backend support** | SLURM only | Any (local, SLURM, Docker) |
| **Race window** | Between `ss` check and PyTorch bind | Between `socket.close()` and PyTorch bind |
| **Complexity** | ~25 lines bash | 3 lines Python |

Both approaches share a TOCTOU race after port release, but the window is
microseconds across ~16,000 ephemeral ports. This is the standard approach
used by PyTorch's own test infrastructure and pytest-xdist.

---

## Proposed Fix

### Where: `environment_spec.py` in artisan

Inject `MASTER_PORT` and `MASTER_ADDR` into `wrap_command()` for GPU container
specs. This is the right abstraction level because:

1. The environment spec already knows it's a GPU container (`gpu=True`)
2. `wrap_command()` runs once per `run_command()` invocation, inside the
   worker process, on the correct host
3. The `--env` flag on `apptainer exec` / `docker run` sets the variable
   inside the container where PyTorch reads it
4. Zero changes needed in any operation — every GPU container workload
   gets the fix automatically
5. Users can override by setting `MASTER_PORT` in the `env` dict

### What changes

Add a module-level helper:

```python
import socket

def _find_free_port() -> int:
    """Find a free ephemeral port by binding to port 0."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]
```

Inject in `ApptainerEnvironmentSpec.wrap_command()` (after the `self.env` loop,
before appending the image):

```python
    def wrap_command(self, cmd: list[str], cwd: Path | None = None) -> list[str]:
        parts = ["apptainer", "exec"]
        if self.gpu:
            parts.append("--nv")
        if cwd is not None:
            parts.extend(["--bind", f"{cwd.parent}:{cwd.parent}"])
        for host, container in self.binds:
            parts.extend(["--bind", f"{host}:{container}"])
        for k, v in self.env.items():
            parts.extend(["--env", f"{k}={v}"])
        if self.gpu and "MASTER_PORT" not in self.env:
            parts.extend(["--env", f"MASTER_PORT={_find_free_port()}"])
        if self.gpu and "MASTER_ADDR" not in self.env:
            parts.extend(["--env", "MASTER_ADDR=127.0.0.1"])
        parts.append(str(self.image))
        parts.extend(cmd)
        return parts
```

Same pattern in `DockerEnvironmentSpec.wrap_command()`.

### Why apply broadly to all GPU container specs?

No meaningful negative consequences:

- **Non-PyTorch GPU tools:** `MASTER_PORT` is just an env var nobody reads.
  Two extra `--env` flags is cosmetic noise, nothing more.
- **Multi-GPU DDP within one container:** Lightning spawns child processes
  inside the container. They all inherit the same `MASTER_PORT` from the
  container environment, so they agree. No problem.
- **Host-level `export MASTER_PORT=29500`:** Our `--env` flag overrides
  inherited env vars. This override is actually correct — a hardcoded port
  would cause the exact collision we're solving.
- **Multi-node DDP across execution units:** Not an artisan use case. Each
  execution unit is independent. Real multi-node training uses
  `torchrun`/`srun`.
- **Performance:** `socket.bind` is microseconds vs. seconds for container
  launch.

The `self.env` dict check provides an escape hatch: any operation that
explicitly sets `MASTER_PORT` or `MASTER_ADDR` in its environment spec
will not be overridden. Each variable is guarded independently so users
can set one without losing control of the other.

### Why not `prepare_env()`?

`prepare_env()` returns the env dict for the host-side subprocess (the
`apptainer` / `docker` binary itself). The `--env` flag in `wrap_command()`
explicitly sets variables inside the container, which is where PyTorch reads
them. For container specs, `wrap_command()` is the correct and sufficient
injection point.

### What this doesn't cover

`AF3fromcif` uses `Environments(active="local")` and internally spawns
apptainer via an external batch runner script. The local environment spec
has no `gpu` flag, so this fix doesn't apply. The batch runner manages its
own container invocation — it's outside artisan's control. This is an
architectural smell that should be addressed separately by refactoring
AF3fromcif to use apptainer directly.

---

## Affected Operations (automatic, no code changes needed)

| Operation | Environment | gpu | Fixed |
|-----------|------------|-----|-------|
| RFD3Conditional | Apptainer | True | Yes |
| RFD3Unconditional | Apptainer | True | Yes |
| AF3 | Apptainer | True | Yes |
| MPNN | Apptainer | True | Yes |
| FusedMPNN | Apptainer | True | Yes |
| AF3fromcif | Local | n/a | No (see above) |

---

## Tests

Unit tests for `_find_free_port()`:
- Returns a positive integer
- Consecutive calls return different ports

Unit tests for `ApptainerEnvironmentSpec.wrap_command()`:
- GPU spec without explicit MASTER_PORT: command includes `--env MASTER_PORT=<int>`
  and `--env MASTER_ADDR=127.0.0.1`
- GPU spec with explicit MASTER_PORT in `env`: uses the explicit value, no override
- Non-GPU spec: no MASTER_PORT injected

Same tests for `DockerEnvironmentSpec.wrap_command()`.

---

## Verification

1. Run artisan unit tests
2. Bump artisan version in pipelines, install
3. Re-run phosphopeptide tutorial with 2 RFD3 seeds on local backend —
   confirm no EADDRINUSE
