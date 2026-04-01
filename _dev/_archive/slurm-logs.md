# Design: Move SLURM logs into the runs directory

## Problem

SLURM submission files (sbatch scripts, stdout/stderr logs, pickled results)
currently land in `./slurm_logs/` relative to whatever directory the pipeline
script is launched from. This is a submitit default that doesn't follow the
artisan runs-directory convention, where all pipeline artifacts live under a
single `runs/<pipeline>/` tree.

**Consequences of the current behavior:**

- SLURM logs are disconnected from the pipeline they belong to. If you run
  multiple pipelines from the same directory, all their SLURM logs are
  interleaved in one flat `slurm_logs/` folder.
- There's no way to associate a SLURM job ID back to a specific pipeline run
  without reading the job's pickle files.
- Cleaning up a pipeline run requires remembering to also delete `./slurm_logs/`.
- The `runs/` directory is incomplete as a self-contained record of a run.

## Current behavior

### Where SLURM logs go today

`SlurmTaskRunner` (in `prefect-submitit`) defaults `log_folder="slurm_logs"`.
Since this is a relative path, submitit resolves it against the process CWD.

```
./slurm_logs/                          # relative to CWD of the orchestrator
  <job_id>/                            # one directory per SLURM job
    <job_id>_submission.sh             # the sbatch script
    <job_id>_submitted.pkl             # serialized callable (cloudpickle)
    <job_id>_<task_id>_log.out         # SLURM stdout
    <job_id>_<task_id>_log.err         # SLURM stderr
    <job_id>_<task_id>_result.pkl      # pickled return value
```

For local-mode execution, submitit uses the same folder but without the
`%j` job-id subdirectory.

### Where everything else goes (the artisan convention)

Artisan organizes pipeline output under three roots, typically placed inside a
`runs/<pipeline_name>/` directory:

```
runs/<pipeline_name>/
  delta/                               # permanent Delta Lake tables
    artifacts/
    provenance/
    orchestration/
  staging/                             # transient worker output (committed then cleaned)
    _dispatch/
    <step>_<op>/<shard>/<shard>/<run_id>/*.parquet
  working/                             # ephemeral execution sandboxes (cleaned after use)
    <step>_<op>/<shard>/<shard>/<run_id>/
      preprocess/
      execute/
      postprocess/
  logs/                                # created automatically (sibling of delta/)
    failures/
      step_<N>_<op>/
        <execution_run_id>.log
```

The `logs/` directory is created by artisan as a sibling of `delta_root`
(specifically `config.delta_root.parent / "logs" / "failures"`). This is the
only place that currently captures failure information within the runs tree.

## Proposed behavior

Route SLURM logs into the runs directory so that a single `runs/<pipeline>/`
tree is fully self-contained.

### Target layout

```
runs/<pipeline_name>/
  delta/
  staging/
  working/
  logs/
    failures/                          # existing failure logs (unchanged)
    slurm/                             # NEW: submitit job files
      <job_id>/
        <job_id>_submission.sh
        <job_id>_submitted.pkl
        <job_id>_<task_id>_log.out
        <job_id>_<task_id>_log.err
        <job_id>_<task_id>_result.pkl
```

### What needs to change

| Layer | Change |
|-------|--------|
| **`SlurmBackend.create_flow()`** (artisan) | Accept a `log_folder` path and pass it through to `SlurmTaskRunner`. |
| **`StepExecutor` / orchestration engine** (artisan) | Compute `log_folder = config.delta_root.parent / "logs" / "slurm"` and pass it to the backend's `create_flow()`. |
| **`SlurmTaskRunner`** (prefect-submitit) | No changes needed -- it already accepts `log_folder` as a constructor arg. |
| **`tutorial_setup()`** (artisan) | No changes needed -- `logs/` is already created as a sibling of `delta/`. |

### Key decisions

1. **`logs/slurm/` not `slurm_logs/`** -- keeps SLURM logs grouped with other
   log types (failure logs) under one `logs/` parent, consistent with the
   existing `logs/failures/` pattern.

2. **Derived from `delta_root`, not a new user-facing parameter** -- the path
   is computed as `delta_root.parent / "logs" / "slurm"`, matching how
   `failure_logs_root` is already derived. Users don't need to configure
   another path.

3. **Per-step subdirectories (optional, future)** -- the initial change puts
   all SLURM logs in a flat `logs/slurm/` directory. A future enhancement
   could subdivide by step (`logs/slurm/step_0_rfdiffusion/`) for easier
   debugging. The `%j` expansion already provides per-job isolation.

### Local-mode behavior

When `execution_mode=LOCAL`, submitit uses `LocalExecutor` which doesn't
expand `%j`. The `logs/slurm/` path still works; log files land directly in
that directory without a job-id subdirectory.

## Migration

- The `SlurmTaskRunner` default of `log_folder="slurm_logs"` should remain
  unchanged so that standalone (non-artisan) usage is unaffected.
- The change is entirely in how artisan's `SlurmBackend` calls the runner.
- Existing `./slurm_logs/` directories from prior runs are unaffected and can
  be deleted manually.
