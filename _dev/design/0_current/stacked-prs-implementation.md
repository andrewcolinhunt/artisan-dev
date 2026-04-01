# Implementation Plan: Stacked PRs for v0.1.2a3

## Context

The `feat/prefect-server-discovery` branch has 129 commits ahead of
`upstream/release/v0.1.2a3` (at `7d52da7`), containing 8 features/fixes
across 51 non-dev files (+1,727 / -179 lines). The design doc at
`_dev/design/0_current/stacked-prs-v0.1.2a3.md` decomposes these into 8
stacked PRs, ordered so changes to shared files layer cleanly.

The original commits are messy WIP with reverts and reformats. Each PR
branch is built from the **net diff** — one clean commit per branch, not
cherry-picked.

## Constraints

- Push only to `origin` — never push to `upstream`
- Never create PRs — user opens them manually on GitHub
- PR 1 targets `upstream/release/v0.1.2a3` as base
- PRs 2-8 each target the previous PR's branch
- Validate (`fmt` + relevant tests) on each branch before the next
- No dev-only files (`_dev/`, `CLAUDE.local.md`) on any PR branch

## Branch naming

```
pr/01-subprocess-reimport-guard
pr/02-gpu-execution-defaults
pr/03-slurm-log-routing
pr/04-sandbox-path-refactor
pr/05-step-run-id-isolation
pr/06-skip-cache
pr/07-prefect-server-discovery
pr/08-docs-getting-started
```

---

## Step 0: Save this plan

Before any git operations, save this implementation plan to
`_dev/design/0_current/stacked-prs-implementation.md` so it's tracked
alongside the design doc.

## Setup

```bash
git checkout feat/prefect-server-discovery
git status                    # verify clean
git tag pre-stacked-prs       # safety restore point
git fetch upstream
```

---

## Build strategy

For each PR branch:

1. Create branch from base (`upstream/release/v0.1.2a3` for PR 1, previous
   PR's branch for the rest)
2. **Non-shared files**: copy from HEAD via
   `git checkout feat/prefect-server-discovery -- <file>`
3. **Shared files**: either copy final HEAD version (if this is the LAST PR
   to touch the file) or construct an intermediate version (apply only this
   PR's changes)
4. Stage specific files, commit with PR title
5. Validate: run `fmt`, then run relevant tests

### Shared files: which PRs touch them

| File | Touched by | Intermediate on | Final (copy HEAD) on |
|------|-----------|----------------|---------------------|
| `src/artisan/orchestration/backends/local.py` | 1, 2, 3 | PR 1, PR 2 | **PR 3** |
| `src/artisan/orchestration/engine/step_executor.py` | 1, 3, 5, 6 | PR 1, PR 3, PR 5 | **PR 6** |
| `src/artisan/orchestration/pipeline_manager.py` | 5, 6 | PR 5 | **PR 6** |
| `src/artisan/execution/executors/creator.py` | 4, 5 | PR 4 | **PR 5** |
| `docs/myst.yml` | 6, 8 | PR 6 | **PR 8** |

### How to construct intermediate versions

Start from the file as it exists on the branch (either from
`upstream/release/v0.1.2a3` or from the previous PR's branch). Read the
HEAD version as reference. Use Edit to apply only the hunks for the
current PR. The hunk attributions are documented per-PR below.

---

## PR 1 — `fix: prevent subprocess re-import of user scripts`

**Base:** `upstream/release/v0.1.2a3`

### Non-shared files (copy from HEAD)

| File | Status |
|------|--------|
| `src/artisan/utils/spawn.py` | New |
| `tests/artisan/utils/test_spawn.py` | New |
| `tests/artisan/orchestration/backends/test_sigint_safe_runner.py` | New |

### Shared files — intermediate versions

**`src/artisan/orchestration/backends/local.py`** — apply to base version:

- Add import: `from artisan.utils.spawn import suppress_main_reimport`
- `SIGINTSafeProcessPoolTaskRunner`: extend docstring with spawn guard
  description
- `__enter__`: before `super().__enter__()`, create and enter
  `self._spawn_guard = suppress_main_reimport()`; after super, shut down
  original executor and replace with spawn-context pool
- Add `__exit__`: call `super().__exit__()` in try/finally that exits
  `self._spawn_guard`

Do NOT touch: `create_flow` body, `validate_operation`, `log_folder` param.

**`src/artisan/orchestration/engine/step_executor.py`** — apply to base version:

- Add import: `from artisan.utils.spawn import suppress_main_reimport`
- `_run_curator_in_subprocess`: wrap `ProcessPoolExecutor` in a compound
  `with` statement that also enters `suppress_main_reimport()`

Do NOT touch: any function signatures (`execute_step`,
`_execute_curator_step`, `_execute_creator_step`), `log_folder`,
`step_run_id`, `skip_cache`, `build_step_result`, `resolve_inputs`.

### Commit

```
fix: prevent subprocess re-import of user scripts

Add suppress_main_reimport context manager that sets __main__.__file__
to None during ProcessPoolExecutor lifetime, preventing CPython's spawn
bootstrap from re-importing the caller's script in worker processes.
```

### Validate

```bash
~/.pixi/bin/pixi run -e dev fmt
~/.pixi/bin/pixi run -e dev pytest tests/artisan/utils/test_spawn.py tests/artisan/orchestration/backends/test_sigint_safe_runner.py
```

---

## PR 2 — `feat: GPU execution defaults (sequential + MASTER_PORT)`

**Base:** `pr/01-subprocess-reimport-guard`

### Non-shared files (copy from HEAD)

| File | Status |
|------|--------|
| `src/artisan/schemas/operation_config/environment_spec.py` | Modified |
| `tests/artisan/orchestration/backends/test_local.py` | Modified |
| `tests/artisan/schemas/test_environment_spec.py` | Modified |

### Shared files — intermediate version

**`src/artisan/orchestration/backends/local.py`** — apply to PR 1 version
(already on branch):

- `create_flow`: replace `max_workers = execution.max_workers or self._default_max_workers`
  with three-way priority:
  1. Explicit `execution.max_workers` wins
  2. If `resources.gpus > 0`, default to 1
  3. Otherwise `self._default_max_workers`
- `validate_operation`: remove `r.gpus > 0` from warning condition, update
  warning message to remove `gpus={r.gpus!r}`

Do NOT touch: `create_flow` signature (no `log_folder` yet — that's PR 3).

### Commit

```
feat: GPU execution defaults (sequential + MASTER_PORT)

Default GPU operations to sequential execution (max_workers=1) on local
backend to avoid CUDA context conflicts. Auto-assign MASTER_PORT for GPU
container workloads to prevent port collisions.
```

### Validate

```bash
~/.pixi/bin/pixi run -e dev fmt
~/.pixi/bin/pixi run -e dev pytest tests/artisan/orchestration/backends/test_local.py tests/artisan/schemas/test_environment_spec.py
```

---

## PR 3 — `feat: route SLURM logs into pipeline runs directory`

**Base:** `pr/02-gpu-execution-defaults`

### Non-shared files (copy from HEAD)

| File | Status |
|------|--------|
| `src/artisan/orchestration/backends/base.py` | Modified |
| `src/artisan/orchestration/backends/slurm.py` | Modified |
| `tests/artisan/orchestration/backends/test_slurm.py` | Modified |

### Shared files

**`src/artisan/orchestration/backends/local.py`** — FINAL: copy from HEAD.
This is the last PR to touch this file.

**`src/artisan/orchestration/engine/step_executor.py`** — apply to PR 1
version (already on branch):

- In `_execute_creator_step`: add `log_folder=config.delta_root.parent / "logs" / "slurm"`
  to the `create_flow` call
- In `execute_composite_step`: same `log_folder` arg to `create_flow` call

Do NOT touch: function signatures, `step_run_id`, `skip_cache`,
`build_step_result`, `resolve_inputs`.

### Commit

```
feat: route SLURM logs into pipeline runs directory

Pass log_folder to BackendBase.create_flow so submitit stores scheduler
logs in <pipeline_root>/logs/slurm/ instead of ~/.submitit/.
```

### Validate

```bash
~/.pixi/bin/pixi run -e dev fmt
~/.pixi/bin/pixi run -e dev pytest tests/artisan/orchestration/backends/test_slurm.py
```

---

## PR 4 — `refactor: separate sandbox path computation from directory creation`

**Base:** `pr/03-slurm-log-routing`

### Non-shared files (copy from HEAD)

| File | Status |
|------|--------|
| `src/artisan/execution/context/sandbox.py` | Modified |
| `tests/artisan/execution/context/test_sandbox.py` | New |
| `tests/artisan/execution/test_executor_creator.py` | Modified |

### Shared files — intermediate version

**`src/artisan/execution/executors/creator.py`** — apply to base version:

- Add imports: `import tempfile`, `from pathlib import Path`,
  `from artisan.utils.path import shard_path`
- In `run_creator_lifecycle`: replace
  `create_sandbox(working_root, execution_run_id, step_number, operation_name=...)`
  with:
  - If `working_root == Path(tempfile.gettempdir())`: flat path
    `working_root / execution_run_id`
  - Else: `shard_path(working_root, execution_run_id)`
  - Then `create_sandbox(sandbox_path)` with single arg

Do NOT touch: `build_creator_execution_context` calls (no `step_run_id` yet).

### Commit

```
refactor: separate sandbox path computation from directory creation

Move path layout policy (flat for system temp, sharded for persistent
storage) from create_sandbox into the caller. create_sandbox now takes
a pre-computed sandbox_path and only creates the directory structure.
```

### Validate

```bash
~/.pixi/bin/pixi run -e dev fmt
~/.pixi/bin/pixi run -e dev pytest tests/artisan/execution/context/test_sandbox.py tests/artisan/execution/test_executor_creator.py
```

---

## PR 5 — `feat: step_run_id output isolation` (largest)

**Base:** `pr/04-sandbox-path-refactor`

### Non-shared files (copy from HEAD)

**Schemas:**
- `src/artisan/schemas/execution/execution_context.py`
- `src/artisan/schemas/execution/execution_record.py`
- `src/artisan/schemas/orchestration/step_result.py`
- `src/artisan/schemas/orchestration/step_state.py`

**Execution models/builders:**
- `src/artisan/execution/models/execution_unit.py`
- `src/artisan/execution/models/execution_composite.py`
- `src/artisan/execution/context/builder.py`

**Executors:**
- `src/artisan/execution/executors/curator.py`
- `src/artisan/execution/executors/composite.py`

**Staging:**
- `src/artisan/execution/staging/parquet_writer.py`
- `src/artisan/execution/staging/recorder.py`

**Orchestration:**
- `src/artisan/orchestration/engine/inputs.py`
- `src/artisan/orchestration/engine/step_tracker.py`

**Storage:**
- `src/artisan/storage/core/table_schemas.py`

**Tests (fixture updates — adding `step_run_id` column):**
- `tests/artisan/orchestration/test_inputs.py`
- `tests/artisan/orchestration/test_reference_resolver.py`
- `tests/artisan/storage/test_artifact_store.py`
- `tests/artisan/storage/test_cache_lookup.py`
- `tests/artisan/storage/test_commit.py`
- `tests/artisan/storage/test_table_schemas.py`
- `tests/artisan/visualization/graph/test_micro.py`
- `tests/artisan/visualization/graph/test_stepper.py`

**Cosmetic test changes:**
- `tests/artisan/execution/test_parquet_writer.py`
- `tests/integration/test_cancellation.py`
- `tests/integration/test_pipeline_api.py`

### Shared files

**`src/artisan/execution/executors/creator.py`** — FINAL: copy from HEAD.

**`src/artisan/orchestration/engine/step_executor.py`** — apply to PR 3
version (already on branch). This is the biggest shared-file edit. Changes:

- `build_step_result`: add `step_run_id: str | None = None` param, docstring
  entry, pass to `StepResultBuilder`
- `execute_step`: add `step_run_id: str | None = None` and
  `step_run_ids: dict[int, str] | None = None` params with docstrings,
  forward both to `_execute_curator_step` and `_execute_creator_step`
- `_execute_curator_step`: add same params, pass `step_run_ids` to
  `resolve_inputs`, pass `step_run_id` to `build_curator_execution_context`,
  error context builds, and final `build_step_result`
- `_execute_creator_step`: same pattern — params, `resolve_inputs`,
  unit builds, `build_step_result`
- `execute_composite_step`: add `step_run_ids`/`step_run_id` params,
  pass through to `resolve_inputs`, composite unit build, `build_step_result`

Do NOT touch: cache-checking logic, `skip_cache`.

**`src/artisan/orchestration/pipeline_manager.py`** — apply to base version:

- `__init__`: add `self._step_run_ids: dict[int, str] = {}`
- `resume()`: restore `step_run_id` from `step_state` into `_step_run_ids`
- `_try_cached_step`: record `cached.step_run_id` into `_step_run_ids`
- `_dispatch_step`: generate `step_run_id`, store in `_step_run_ids`,
  snapshot `upstream_step_run_ids = dict(self._step_run_ids)`, pass
  `step_run_id` and `step_run_ids` to `execute_step`, add `step_run_id`
  to `result.model_copy` update dict
- `_submit_composite_step`: generate `composite_step_run_id`, store,
  snapshot upstream IDs, pass through to `execute_composite_step` and
  `result.model_copy`

Do NOT touch: `create()`, `run()`, `submit()` signatures, cache guard
logic, `skip_cache`.

### Commit

```
feat: step_run_id output isolation

Thread step_run_id through the full execution stack from schemas to
storage. Output queries are scoped to the source step's step_run_id
during input resolution, preventing re-runs from reading outputs
produced by prior attempts.
```

### Validate

```bash
~/.pixi/bin/pixi run -e dev fmt
~/.pixi/bin/pixi run -e dev pytest tests/artisan/storage/ tests/artisan/orchestration/test_inputs.py tests/artisan/orchestration/test_reference_resolver.py tests/artisan/execution/
```

---

## PR 6 — `feat: skip_cache pipeline parameter`

**Base:** `pr/05-step-run-id-isolation`

### Non-shared files (copy from HEAD)

| File | Status |
|------|--------|
| `src/artisan/schemas/orchestration/pipeline_config.py` | Modified |
| `docs/tutorials/execution/09-skip-cache.ipynb` | New |

### Shared files

**`src/artisan/orchestration/engine/step_executor.py`** — FINAL: copy from HEAD.

**`src/artisan/orchestration/pipeline_manager.py`** — FINAL: copy from HEAD.

**`docs/myst.yml`** — intermediate: apply to base version. Add only the
skip-cache tutorial entry (`- file: tutorials/execution/09-skip-cache.ipynb`)
in the Execution tutorials section. Do NOT add the using-pixi entry (PR 8).

### Commit

```
feat: skip_cache pipeline parameter

Add skip_cache parameter to PipelineManager.create(), run(), and submit()
to bypass all cache lookups. Useful for debugging and benchmarking when
upstream data changed outside Artisan's tracking.
```

### Validate

```bash
~/.pixi/bin/pixi run -e dev fmt
~/.pixi/bin/pixi run -e dev pytest tests/artisan/orchestration/ tests/artisan/storage/
```

---

## PR 7 — `feat: Prefect server discovery improvements`

**Base:** `pr/06-skip-cache`

### Files (all non-shared, copy from HEAD)

| File | Status |
|------|--------|
| `src/artisan/orchestration/prefect_server.py` | Modified |
| `tests/artisan/orchestration/test_prefect_server.py` | Modified |
| `pyproject.toml` | Modified |
| `pixi.lock` | Modified |

### Commit

```
feat: Prefect server discovery improvements

Add localhost fallback when discovery file hostname is unreachable,
version compatibility checking between client and server, and rich
diagnostic error messages with actionable remediation steps.
```

### Validate

```bash
~/.pixi/bin/pixi run -e dev fmt
~/.pixi/bin/pixi run -e dev pytest tests/artisan/orchestration/test_prefect_server.py
```

---

## PR 8 — `docs: rewrite getting-started pages and README`

**Base:** `pr/07-prefect-server-discovery`

### Non-shared files (copy from HEAD)

| File | Status |
|------|--------|
| `README.md` | Modified |
| `docs/getting-started/installation.md` | Modified |
| `docs/getting-started/orientation.md` | Modified |
| `docs/getting-started/using-pixi.md` | New |

### Shared files

**`docs/myst.yml`** — FINAL: copy from HEAD.

### Commit

```
docs: rewrite getting-started pages and README

Simplify getting-started experience with new Using Pixi page, convert
hardcoded readthedocs URLs to relative paths, clean up orientation page,
and trim README to essentials.
```

### Validate

```bash
~/.pixi/bin/pixi run -e dev fmt
~/.pixi/bin/pixi run -e docs docs-build
```

---

## Post-build verification

### Diff equivalence check

The final branch must produce the same net diff as the source branch:

```bash
diff <(git diff upstream/release/v0.1.2a3..pr/08-docs-getting-started -- ':!_dev' ':!CLAUDE.local.md') \
     <(git diff upstream/release/v0.1.2a3..feat/prefect-server-discovery -- ':!_dev' ':!CLAUDE.local.md')
```

### Decontamination scan

Scan all PR branches for user-specific strings:

```bash
git diff --name-only upstream/release/v0.1.2a3..pr/08-docs-getting-started | \
  xargs grep -l 'ach94\|andrewhunt\|andrewcolinhunt' 2>/dev/null
```

Present findings to user. Do not auto-replace.

### Push

Ask user before pushing. Push only to `origin`:

```bash
git push origin pr/01-subprocess-reimport-guard pr/02-gpu-execution-defaults \
  pr/03-slurm-log-routing pr/04-sandbox-path-refactor \
  pr/05-step-run-id-isolation pr/06-skip-cache \
  pr/07-prefect-server-discovery pr/08-docs-getting-started
```

### PR creation reminders

Tell user to open PRs manually with these settings:

| PR | Head branch | Base |
|----|------------|------|
| 1 | `pr/01-subprocess-reimport-guard` | `upstream:release/v0.1.2a3` |
| 2 | `pr/02-gpu-execution-defaults` | `pr/01-subprocess-reimport-guard` |
| 3 | `pr/03-slurm-log-routing` | `pr/02-gpu-execution-defaults` |
| 4 | `pr/04-sandbox-path-refactor` | `pr/03-slurm-log-routing` |
| 5 | `pr/05-step-run-id-isolation` | `pr/04-sandbox-path-refactor` |
| 6 | `pr/06-skip-cache` | `pr/05-step-run-id-isolation` |
| 7 | `pr/07-prefect-server-discovery` | `pr/06-skip-cache` |
| 8 | `pr/08-docs-getting-started` | `pr/07-prefect-server-discovery` |

---

## Risk areas

- **Shared file intermediates** (8 intermediate file states to construct):
  highest risk. After each, diff against HEAD to confirm only later-PR
  hunks are missing.
- **Import ordering**: ruff enforces isort. Run `fmt` after every edit.
- **`pixi.lock`**: machine-generated, copy as-is from HEAD. Never edit manually.
- **Notebook JSON**: copy `09-skip-cache.ipynb` as-is. Do not reformat.
