# Design: Sandbox `/tmp` Collisions on Shared SLURM Nodes

## Problem

When multiple SLURM array tasks land on the same compute node, concurrent
AF3 execution units share parent directories under `/tmp`. The first process
creates intermediate directories (e.g. `/tmp/1_af3/`); subsequent processes on
the same node fail with permission errors when trying to create their own
subdirectories inside these shared parents.

Observed at scale: 15 GPUs running AF3 concurrently for a single campaign.
Jobs co-located on the same node fail. Previously masked because debugging
used a scratch folder and smaller runs were unlikely to co-locate.

All failing tasks belong to the **same user** on the same node. This is not a
cross-user collision — it is same-user, multi-GPU, same-node.

**Failure log example:**
`logs/failures/step_1_af3/4d9003d32dfc3f9aea1e892d8008da20.log`

---

## Code Path

The directory hierarchy for a single execution unit (current):

```
working_root / {step}_{op} / {id[:2]} / {id[2:4]} / {id}
     │              │            │           │         │
   /tmp         1_af3          4d          90     4d9003d...
                  ↑              ↑           ↑
          guaranteed        probabilistic sharing
          collision:        (1/256, 1/65536)
          ALL units for
          this step
```

### Default working_root

`artisan/schemas/orchestration/pipeline_config.py`:

```python
working_root: Path = Field(
    default_factory=lambda: Path(tempfile.gettempdir()),  # → /tmp
    ...
)
```

`/tmp` is node-local on most HPC systems (local disk or tmpfs). This is
intentional — sandbox I/O should not flood the shared NFS filesystem.

### Shard path construction

`artisan/utils/path.py:shard_path()` builds:
`root / step_dir_name / id[:2] / id[2:4] / id`

The step directory (`1_af3`) is a guaranteed collision point — every
execution unit for the same pipeline step creates children under it. The two
shard levels add further shared parents.

### Sandbox creation

`artisan/execution/context/sandbox.py:create_sandbox()` currently both
computes the sandbox path (via `shard_path`) and creates the directory
structure:

```python
sandbox_path = shard_path(
    working_root, execution_run_id, step_number, operation_name=operation_name
)
sandbox_path.mkdir(parents=True, exist_ok=True)
```

No `mode=` parameter. Permissions follow the process umask (typically `0077`
on HPC → `drwx------`).

### Sandbox cleanup

`artisan/execution/executors/creator.py:run_creator_lifecycle()`:

```python
shutil.rmtree(sandbox_path, ignore_errors=True)  # removes leaf only
```

Parent directories (`/tmp/1_af3/`, `/tmp/1_af3/4d/`) are never cleaned up.
They persist across SLURM jobs indefinitely.

### Call chain

```
PipelineConfig.working_root
    → RuntimeEnvironment.working_root_path
        → run_creator_lifecycle() reads runtime_env.working_root_path
            → create_sandbox(working_root, execution_run_id, step_number, operation_name)
                → shard_path(working_root, ...) computes path
                → mkdir + return subdirs
```

The caller (`run_creator_lifecycle`) has access to both `working_root` and the
step/operation context. `create_sandbox` currently does two things: path
computation and directory creation.

---

## Root Cause

The leaf sandbox directory is unique per execution unit (32-char xxhash of
`spec_id:timestamp:worker_id`). The collision happens in the shared parents.

Same-user processes should not get permission errors on their own directories.
The likely cause is **Apptainer container UID mapping**.

AF3 runs inside Apptainer containers. Apptainer does not isolate `/tmp` by
default — the host `/tmp` is bind-mounted into the container. If the container
uses fakeroot or user namespace mapping, files created inside the container
appear on the host with a **different UID** (e.g. `root` or a mapped UID),
even though the SLURM job was submitted by the same user. The next SLURM task
on that node — same user, different container invocation — sees `/tmp/1_af3/`
owned by a UID it doesn't match, and fails with a permission error.

JAX/XLA (used by AF3) also creates compilation caches under `/tmp`, and the
`--nv` GPU passthrough flag may create additional setup files there.

### To confirm

Run on a compute node after a failure:

```bash
ls -lan /tmp/1_af3/
```

The `-n` flag shows numeric UIDs. If the owner UID doesn't match the
submitting user's UID, Apptainer UID mapping is confirmed.

### Collaborator workaround

A collaborator tested namespacing by username:

```python
def default_temp_working_root() -> Path:
    return Path(tempfile.gettempdir()) / getpass.getuser()

working_root = default_temp_working_root() if args.slurm else run_root / "working"
```

This appeared to fix the problem, but likely by accident — it introduced a
new path segment that had no stale parents from prior container runs. It does
not prevent the same UID-mismatch from accumulating on the new intermediate
directories over time.

---

## Proposed Fix

### Separate path computation from directory creation

`create_sandbox` currently does two things: compute the sandbox path (via
`shard_path`) and create the directory structure. These should be separated
because the right path layout depends on context that `create_sandbox` doesn't
have — whether the working root is a shared temp dir (needs flat paths) or a
user-specified NFS path (benefits from step dirs and sharding).

**Two layouts:**

| Context | Layout | Why |
|---------|--------|-----|
| Default `/tmp` (node-local) | `working_root / execution_run_id` | No shared intermediate parents to collide on. `/tmp` is `1777` so the only shared dir is safe. Bounded concurrency (tens of tasks) means no sharding needed. `preserve_working` loses step-based organization, but users who need to inspect sandboxes should use a custom NFS path, not `/tmp`. |
| User-specified NFS path | `shard_path(working_root, id, step, op)` | Step dirs provide legibility for `preserve_working` inspection. Sharding prevents directory listing bottlenecks at scale (large `preserve_working` runs). |

**Before (one path, computed inside `create_sandbox`):**
```
/tmp/1_af3/4d/90/4d9003d32dfc3f9aea1e892d8008da20/
     ^^^^^ ^^^^^ ← shared parents, never cleaned up
```

**After — temp default:**
```
/tmp/4d9003d32dfc3f9aea1e892d8008da20/
     ← no shared parents; only /tmp itself is shared
```

**After — user-specified NFS:**
```
/nfs/scratch/working/1_af3/4d/90/4d9003d32dfc3f9aea1e892d8008da20/
                     ^^^^^ ^^^^^ ← safe: user-owned filesystem, no UID mapping
```

### Design

Move path computation to the caller. `create_sandbox` takes a pre-computed
`sandbox_path` and only creates the directory structure:

```python
# artisan/execution/context/sandbox.py

def create_sandbox(
    sandbox_path: Path,
) -> tuple[Path, Path, Path, Path]:
    """Create sandbox directories for creator execution.

    Args:
        sandbox_path: Pre-computed path for this execution unit's sandbox.

    Returns:
        Tuple of (sandbox_path, preprocess_dir, execute_dir, postprocess_dir).
    """
    sandbox_path.mkdir(parents=True, exist_ok=True)

    preprocess_dir = sandbox_path / "preprocess"
    execute_dir = sandbox_path / "execute"
    postprocess_dir = sandbox_path / "postprocess"

    preprocess_dir.mkdir(exist_ok=True)
    execute_dir.mkdir(exist_ok=True)
    postprocess_dir.mkdir(exist_ok=True)

    return sandbox_path, preprocess_dir, execute_dir, postprocess_dir
```

The caller (`run_creator_lifecycle`) computes the path based on context:

```python
# artisan/execution/executors/creator.py (in run_creator_lifecycle)

working_root = runtime_env.working_root_path

if working_root == Path(tempfile.gettempdir()):
    sandbox_path = working_root / execution_run_id
else:
    sandbox_path = shard_path(
        working_root, execution_run_id, unit.step_number,
        operation_name=operation.name,
    )

sandbox_path, preprocess_dir, execute_dir, postprocess_dir = create_sandbox(
    sandbox_path
)
```

The `Path` equality check is a **conservative default-detection heuristic**.
`PipelineConfig.working_root` defaults to `Path(tempfile.gettempdir())`,
evaluated on the orchestrator. `_create_runtime_environment` passes that value
verbatim to `RuntimeEnvironment.working_root_path`. On the worker,
`tempfile.gettempdir()` is called again. On HPC systems where TMPDIR is not
set per-job, both sides resolve to `/tmp` and the comparison succeeds. If the
comparison fails for any reason (TMPDIR divergence between orchestrator and
worker, symlink resolution differences), the code falls back to the current
sharded behavior — no regression, just no improvement.

### Not proposed

| Alternative | Why not |
|-------------|---------|
| Namespace by username (`/tmp/{user}/...`) | Adds a new intermediate parent still subject to UID-mismatch from Apptainer. Flat path eliminates the problem structurally. |
| Always flat (both temp and NFS) | Loses step-dir legibility and sharding on NFS, which matters for large `preserve_working` runs. |
| Boolean flag on `create_sandbox` | Dead parameters (`step_number`, `operation_name`) when flat. Cleaner to separate path computation from directory creation entirely. |
| World-writable parents (`0o1777`) | Security risk on shared HPC nodes. |
| `tempfile.mkdtemp()` default | Random name breaks `preserve_working` debugging. |
| Rely on SLURM `$TMPDIR` | Not all HPC systems configure per-job TMPDIR. |
| `--contain` on apptainer | Fixes container UID mapping only; would need artisan-level config change. Worth investigating separately for JAX/XLA cache isolation. |
| Prune empty parents on cleanup | Unnecessary if there are no intermediate parents to prune (temp case), and undesirable for the NFS case where parents provide structure. |

---

## Changes

All changes in the `artisan` repo.

### `create_sandbox` — accept `sandbox_path` directly

`artisan/execution/context/sandbox.py`

Change signature from `(working_root, execution_run_id, step_number,
operation_name)` to `(sandbox_path)`. Remove `shard_path` import.

### `run_creator_lifecycle` — compute sandbox path

`artisan/execution/executors/creator.py`

Add path computation before calling `create_sandbox`: flat for default temp,
sharded for user-specified working root. Import `shard_path` here.

### Check other callers

`artisan/execution/executors/composite.py` also references
`working_root_path` — verify whether it calls `create_sandbox` and update
accordingly.

---

## Tests

- `test_create_sandbox_creates_subdirs` — verify `create_sandbox` creates
  preprocess/execute/postprocess under the given path.
- `test_sandbox_path_flat_for_temp` — verify the caller uses flat layout
  when `working_root` matches the system temp dir.
- `test_sandbox_path_sharded_for_custom_root` — verify the caller uses
  `shard_path` when `working_root` is a user-specified path.
- Update any tests that assert the old `create_sandbox` signature.

---

## What This Does NOT Cover

| Feature | Why deferred |
|---------|--------------|
| Apptainer /tmp isolation (`--contain`) | Separate concern; needs investigation of AF3/JAX cache behavior inside containers. Worth pursuing to prevent other `/tmp` surprises. |
| Stale `/tmp` cleanup from old sandbox layout | Old sharded parents from prior runs will remain until manually cleaned or node reboot. Harmless once no new code creates children under them. |
| Confirm `/tmp` is node-local on target cluster | Should be verified; if NFS-mounted, the default working root choice needs revisiting beyond this fix. |
