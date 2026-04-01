# v0.1.2a3 — GPU defaults, output isolation, and HPC improvements

This release hardens Artisan for GPU and HPC workloads, adds output
isolation between step re-runs, and improves the developer experience
with cache bypass controls and better documentation.

## Highlights

- **GPU execution defaults** — GPU operations now default to sequential
  execution (max_workers=1) on the local backend, avoiding CUDA context
  conflicts. MASTER_PORT is auto-assigned per container invocation to
  prevent port collisions on shared nodes.

- **Step run isolation** — Each step execution attempt gets a unique
  `step_run_id` that scopes output queries during input resolution. Re-running
  a step no longer risks reading outputs from a prior failed or partial attempt.

- **skip_cache parameter** — `PipelineManager.create()`, `run()`, and
  `submit()` accept `skip_cache=True` to bypass all cache lookups. Useful for
  debugging, benchmarking, or when upstream data changed outside Artisan's
  tracking.

- **Prefect server discovery** — Automatic localhost fallback when the
  discovery file hostname is unreachable (common on HPC clusters), version
  compatibility checking between client and server, and diagnostic error
  messages with actionable remediation steps.

- **SLURM log routing** — Scheduler logs are now stored in
  `<pipeline_root>/logs/slurm/` instead of `~/.submitit/`, keeping them
  alongside pipeline data.

## Fixes

- **Subprocess re-import guard** — Fixed a bug where running Artisan from a
  script caused the script's top-level code to re-execute in each spawned
  worker process.

## Internal

- Separated sandbox path computation from directory creation for testability.
- Bumped `prefect-submitit` minimum version to `>= 0.1.5`.

## Docs

- New **Using Pixi** getting-started page covering environments, tasks,
  shells, and workspaces.
- Simplified installation page and orientation page.
- Cleaned up README — removed premature badges, converted hardcoded URLs
  to relative paths.
