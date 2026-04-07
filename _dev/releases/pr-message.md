# PR #22 — feat: merge stacked PRs #14–#20 into release/v0.1.2a4

## Summary

Merges the stacked PRs (#14–#20) that were reviewed and merged into the
`pr/01-subprocess-reimport-guard` branch but never landed on the release branch
during the v0.1.2a3 cut.

## Changes

- GPU execution defaults — sequential `max_workers=1` for GPU steps, automatic `MASTER_PORT` allocation (#14)
- SLURM log routing into the pipeline runs directory instead of `~/.submitit/` (#15)
- Sandbox path refactor — separated computation from directory creation (#16)
- Step run ID output isolation — unique subdirectory per step run (#17)
- `skip_cache` pipeline parameter for forced re-execution (#18)
- Prefect server discovery — version mismatch detection, stale process warnings, multi-source resolution (#19)
- Getting-started docs rewrite, README cleanup, new "Using Pixi" page (#20)

## Motivation

These PRs were merged into each other as stacked PRs but the stack was never
collapsed into the release branch. This brings `release/v0.1.2a4` up to date
with all reviewed work from the v0.1.2a3 development cycle.

## Testing

- [x] Existing tests pass (`pixi run -e dev test`)
- [x] New/updated tests cover changed behavior
- [x] Linting passes (`pixi run -e dev fmt && git diff --exit-code`)

## Checklist

- [x] I have read the [Contributing Guide](CONTRIBUTING.md)
- [x] All changes were previously reviewed in PRs #14–#20
- [x] Documentation is updated (if applicable)

---

# PR #23 — feat: SlurmIntraBackend, tutorial, and docs fixes

## Summary

Adds `SlurmIntraBackend` for zero-latency `srun` dispatch within existing SLURM
allocations, a tutorial and demo script, a docs fix, and the v0.1.2a4 changelog.

## Changes

- `SlurmIntraBackend` — new backend that dispatches via `srun` inside an `salloc` session, bypassing the scheduler queue
- SLURM intra-allocation tutorial (`10-slurm-intra-execution.ipynb`) and standalone demo script
- VS Code kernel slowness workaround restored to installation page
- v0.1.2a4 changelog entry

## Motivation

The SlurmIntraBackend enables a workflow where users allocate resources once
with `salloc` and dispatch pipeline steps instantly via `srun`, avoiding
repeated queue waits. This is the natural next step after the SLURM backend
for interactive HPC sessions.

## Testing

- [x] Existing tests pass (`pixi run -e dev test`)
- [x] New/updated tests cover changed behavior
- [x] Linting passes (`pixi run -e dev fmt && git diff --exit-code`)

## Checklist

- [x] I have read the [Contributing Guide](CONTRIBUTING.md)
- [x] Documentation is updated (if applicable)

> **Note:** Stacked on #22 — merge that first.
