# Stacked PRs: v0.1.2a3 → Next Release

Target base: `upstream/release/v0.1.2a3` (`7d52da7`)
Source: `feat/prefect-server-discovery` (`dbb21c4`)

Total commits ahead: 129 (most are dev-only `_dev/` changes, stripped before PR).

---

## Strategy

Eight stacked PRs, each targeting the previous PR's branch. Ordered so
changes to shared files (`step_executor.py`, `pipeline_manager.py`,
`local.py`, `creator.py`) flow cleanly without conflicts.

Since the original commits are messy WIP, each PR branch will be built from
the **net diff** against its base (not cherry-picked). The commit IDs below
are for traceability — they identify which original work contributes to each
PR.

---

## PR 1 — `fix: prevent subprocess re-import of user scripts`

**Why:** When Artisan runs from a script (`python my_pipeline.py`), CPython's
spawn-based multiprocessing re-imports `__main__` in each child, causing the
script's top-level code to re-execute. This guard sets `__main__.__file__` to
`None` during pool lifetime to prevent the re-import.

### Files

| File | Change |
|------|--------|
| `src/artisan/utils/spawn.py` | New — `suppress_main_reimport` context manager |
| `src/artisan/orchestration/backends/local.py` | `SIGINTSafeProcessPoolTaskRunner` enters/exits the guard |
| `src/artisan/orchestration/engine/step_executor.py` | `_run_curator_in_subprocess` wraps pool in the guard |
| `tests/artisan/utils/test_spawn.py` | New — 3 tests for the context manager |
| `tests/artisan/orchestration/backends/test_sigint_safe_runner.py` | 4 tests: enter/pool, neuter, subclass, create_flow |

### Source commits

```
dc04f99 fix: replace multiprocessing.spawn with subprocess.Popen for curator isolation
f43a00d Revert "fix: replace multiprocessing.spawn with subprocess.Popen for curator isolation"
33dbcaf feat: add suppress_main_reimport context manager
8ef3b00 fix: prevent subprocess re-import of user scripts in both code paths
ce69d94 test: add tests for suppress_main_reimport and task runner neuter
```

Note: `dc04f99` and `f43a00d` cancel out (revert). Net contribution is from
`33dbcaf`, `8ef3b00`, `ce69d94`.

---

## PR 2 — `feat: GPU execution defaults (sequential + MASTER_PORT)`

**Why:** GPU operations default to sequential execution on local backend to
avoid CUDA context conflicts and memory contention. MASTER_PORT is
auto-assigned per container invocation to prevent port collisions when
multiple GPU jobs share a node.

### Files

| File | Change |
|------|--------|
| `src/artisan/orchestration/backends/local.py` | `create_flow` max_workers priority (GPU→1), remove GPU warning from `validate_operation` |
| `src/artisan/schemas/operation_config/environment_spec.py` | `_find_free_port()`, MASTER_PORT/MASTER_ADDR injection in `_container_wrap` |
| `tests/artisan/orchestration/backends/test_local.py` | 5 new max_workers tests, GPU warning test updated |
| `tests/artisan/schemas/test_environment_spec.py` | New — port + injection tests |

### Source commits

```
2a99853 feat: default GPU operations to sequential execution on local backend
1c035a0 feat: auto-assign MASTER_PORT for GPU container workloads
```

---

## PR 3 — `feat: route SLURM logs into pipeline runs directory`

**Why:** Routes submitit log files to `<pipeline_root>/logs/slurm/` instead
of the default `~/.submitit/`, making them discoverable alongside pipeline
data.

### Files

| File | Change |
|------|--------|
| `src/artisan/orchestration/backends/base.py` | `create_flow` gains `log_folder` param |
| `src/artisan/orchestration/backends/slurm.py` | Passes `log_folder` to `SlurmTaskRunner` kwargs |
| `src/artisan/orchestration/backends/local.py` | Adds `log_folder` param (unused, interface compat) |
| `src/artisan/orchestration/engine/step_executor.py` | Computes and passes `log_folder` to `create_flow` |
| `tests/artisan/orchestration/backends/test_slurm.py` | Tests log_folder present/absent in kwargs |

### Source commits

```
8b86115 feat: route SLURM logs into runs directory under logs/slurm/
```

---

## PR 4 — `refactor: separate sandbox path computation from directory creation`

**Why:** `create_sandbox` previously computed the path AND created the
directory. Now the caller computes the path (flat layout for system temp,
sharded for persistent storage) and `create_sandbox` only creates it. Makes
the function independently testable and the layout policy explicit.

### Files

| File | Change |
|------|--------|
| `src/artisan/execution/context/sandbox.py` | Simplified to take `sandbox_path: Path` argument |
| `src/artisan/execution/executors/creator.py` | Path computation logic (flat vs sharded) before calling `create_sandbox` |
| `tests/artisan/execution/context/test_sandbox.py` | New — 4 tests for simplified `create_sandbox` |
| `tests/artisan/execution/test_executor_creator.py` | New `TestSandboxPathComputation` (2 tests) |

### Source commits

```
30c6285 feat: separate sandbox path computation from directory creation
```

---

## PR 5 — `feat: step_run_id output isolation`

**Why:** Each step execution attempt gets a unique `step_run_id` recorded in
the executions table. During input resolution, output queries are scoped to
the source step's `step_run_id`, preventing a re-run from reading outputs
produced by a prior run of the same step.

This is the largest PR — it threads `step_run_id` through the full vertical
stack from schemas to storage.

### Files

**Schemas:**

| File | Change |
|------|--------|
| `src/artisan/schemas/execution/execution_context.py` | New `step_run_id` field |
| `src/artisan/schemas/execution/execution_record.py` | New `step_run_id` field |
| `src/artisan/schemas/orchestration/step_result.py` | New `step_run_id` on `StepResult` and `StepResultBuilder` |
| `src/artisan/schemas/orchestration/step_state.py` | New `step_run_id` field, wired into `to_step_result()` |
| `src/artisan/schemas/orchestration/pipeline_config.py` | *(no change — touched by PR 6 only)* |

**Execution models and builders:**

| File | Change |
|------|--------|
| `src/artisan/execution/models/execution_unit.py` | New `step_run_id` field |
| `src/artisan/execution/models/execution_composite.py` | New `step_run_id` field |
| `src/artisan/execution/context/builder.py` | `step_run_id` param on all three builder functions |

**Executors:**

| File | Change |
|------|--------|
| `src/artisan/execution/executors/creator.py` | Passes `step_run_id` to builder |
| `src/artisan/execution/executors/curator.py` | Passes `step_run_id` through passthrough and main paths |
| `src/artisan/execution/executors/composite.py` | Passes `step_run_id` from transport to builder |

**Staging:**

| File | Change |
|------|--------|
| `src/artisan/execution/staging/parquet_writer.py` | `step_run_id` in `_stage_execution` and `_write_execution_record` |
| `src/artisan/execution/staging/recorder.py` | Passes `step_run_id` from execution context |

**Orchestration:**

| File | Change |
|------|--------|
| `src/artisan/orchestration/engine/step_executor.py` | `step_run_id`/`step_run_ids` params on `execute_step`, curator/creator/composite executors, `build_step_result` |
| `src/artisan/orchestration/engine/inputs.py` | `step_run_id` filter in `resolve_output_reference`, `step_run_ids` dict in `resolve_inputs` |
| `src/artisan/orchestration/engine/step_tracker.py` | Reads `step_run_id` from stored rows |
| `src/artisan/orchestration/pipeline_manager.py` | `_step_run_ids` dict, generation/recording in `_dispatch_step`/`_submit_composite_step`, restoration in `resume()` |

**Storage:**

| File | Change |
|------|--------|
| `src/artisan/storage/core/table_schemas.py` | New `step_run_id` column in `EXECUTIONS_SCHEMA` |

**Tests (fixture updates — adding `step_run_id` column):**

| File | Change |
|------|--------|
| `tests/artisan/orchestration/test_inputs.py` | Schema fixture |
| `tests/artisan/orchestration/test_reference_resolver.py` | Schema fixture |
| `tests/artisan/storage/test_artifact_store.py` | Schema fixture |
| `tests/artisan/storage/test_cache_lookup.py` | Schema fixture |
| `tests/artisan/storage/test_commit.py` | Schema fixture |
| `tests/artisan/storage/test_table_schemas.py` | Schema fixture + column count 15→16 |
| `tests/artisan/visualization/graph/test_micro.py` | Schema fixture |
| `tests/artisan/visualization/graph/test_stepper.py` | Schema fixture |

**Cosmetic test changes (absorbed here):**

| File | Change |
|------|--------|
| `tests/artisan/execution/test_parquet_writer.py` | Import reorder |
| `tests/integration/test_cancellation.py` | Line reformatting |
| `tests/integration/test_pipeline_api.py` | Remove unused import |

### Source commits

```
d35bcc4 feat: add step_run_id output isolation for scoped resolution
17a0e29 test: update execution fixtures with step_run_id column
f13a82a fix: pass step_run_id through curator executor and build_step_result
a1af21d fix: propagate step_run_id through passthrough curator and composite paths
52984a4 style: apply ruff formatting to test files
```

---

## PR 6 — `feat: skip_cache pipeline parameter`

**Why:** Allows users to bypass all cache lookups (step-level and
execution-level) for an entire pipeline or individual steps. Useful for
debugging, benchmarking, or when upstream data changed outside Artisan's
tracking.

### Files

| File | Change |
|------|--------|
| `src/artisan/schemas/orchestration/pipeline_config.py` | New `skip_cache` field |
| `src/artisan/orchestration/pipeline_manager.py` | `skip_cache` param on `create()`, `run()`, `submit()`, forwarded through dispatch |
| `src/artisan/orchestration/engine/step_executor.py` | `skip_cache` param on `execute_step`, curator/creator executors; cache checks gated |
| `docs/tutorials/execution/09-skip-cache.ipynb` | New tutorial notebook |
| `docs/myst.yml` | Register skip-cache tutorial in TOC |

### Source commits

```
9dbc6db feat: add skip_cache parameter to bypass step and execution caching
ceca1a0 docs: add skip_cache tutorial
```

---

## PR 7 — `feat: Prefect server discovery improvements`

**Why:** Make server discovery robust for HPC/cluster environments. Adds
localhost fallback when the discovery file points to a different node's
hostname, version compatibility checking between client and server, and rich
diagnostic error messages with actionable remediation steps.

### Files

| File | Change |
|------|--------|
| `src/artisan/orchestration/prefect_server.py` | `PrefectVersionMismatch`, `_try_localhost_fallback`, `_build_unreachable_message`, `_pid_alive`, `_read_discovery_file`, `_get_server_version`, `_versions_compatible`, `_check_version_compatibility`; `_validate_health` returns info; `discover_server` calls version check |
| `tests/artisan/orchestration/test_prefect_server.py` | ~305 new lines: `TestPidAlive`, `TestBuildUnreachableMessage`, `TestTryLocalhostFallback`, `TestGetServerVersion`, `TestVersionsCompatible`, `TestCheckVersionCompatibility`, extended `TestValidateHealth` |
| `pyproject.toml` | `prefect-submitit >= 0.1.5` (was `>= 0.1.4`) |
| `pixi.lock` | Lock file updated for dep bump |

### Source commits

```
7ca273f test: add Cloud URL tests and modernize settings assertions
4403815 style: apply ruff formatting
78e019d feat: add version check, localhost fallback, and diagnostics to server discovery
c3ec3ec chore: bump prefect-submitit minimum version to 0.1.5
4472995 chore: update pixi.lock
03dcf9c chore: update pixi.lock after v0.1.2a3 rebase
```

---

## PR 8 — `docs: rewrite getting-started pages and README`

**Why:** Simplify getting-started experience. Extract pixi usage into its own
page, convert hardcoded readthedocs URLs to relative paths, clean up
orientation page formatting, remove premature badges and Contributing section
from README.

### Net file changes (many intermediate commits created/deleted files that cancel out)

| File | Change |
|------|--------|
| `README.md` | Remove badges, convert readthedocs→relative links, remove Contributing section |
| `docs/getting-started/installation.md` | Replace inline pixi commands with link to using-pixi page |
| `docs/getting-started/orientation.md` | Table reformatting, de-italicize Pipelines section, trim code comments, fix deep-dive links |
| `docs/getting-started/using-pixi.md` | New — full pixi guide (environments, tasks, shells, workspaces) |
| `docs/myst.yml` | Register using-pixi in TOC |

### Source commits

These commits iteratively built, revised, and pruned the docs. Many created
files that were later deleted (e.g. `quick-start.md`,
`connect-to-prefect-cloud.md`, `using-claude-code.md`). Only the net diff
ships.

```
1a5225c docs: update installation.md, create using-claude-code.md, update README
74c7416 docs: create quick-start.md, simplify first-pipeline.md
ce15ee0 docs: update index, myst.yml, and orientation for new page order
94d55c6 docs: move Getting Started links up in README, add detail
f0fe4ad docs: add Connect to Prefect Cloud how-to guide
2417ea5 docs: add review TODOs and editorial fixes to getting-started and README
d29bdee docs: add review notes to Prefect Cloud how-to guide
ba1e2fd docs: add execution outputs to first-pipeline tutorial notebook
15bacad docs: remove quick-start.md and first-pipeline.md
618e156 docs: rewrite getting-started index links
7e33200 docs: revise installation.md — Pixi/Prefect sections, fix hierarchy, resolve TODOs
9c6c273 docs: trim README duplication and resolve TODOs
d9a9fac docs: fix Prefect Cloud guide commands and cross-references
89f41e1 docs: re-execute first-pipeline tutorial notebook
c7a3a3d docs: rewrite Prefect Cloud guide into comprehensive Prefect connection guide
f8010b0 docs: fix README quick-start wording and remove nonexistent finalize print
04ef52d docs: re-execute first-pipeline tutorial notebook
2dac9f4 docs: fix skill invocation prefix in docs
d8c7ab9 docs: rewrite Prefect guide to recommend self-hosted over Cloud
9193457 docs: fix skill prefix and update tagline in README
2e88de9 docs: reorder installation page — action first, explainers as dropdowns
3145eae docs: fix installation page consistency with updated Prefect guide
69167e4 docs: fix accuracy issues found in doc audit
7c46252 docs: simplify Prefect setup in first-pipeline tutorial
16adc0f docs: add Using Pixi page to Getting Started
ecf380d docs: register Using Pixi in TOC and cross-reference from installation
42b7f41 docs: move task reference below troubleshooting in Using Pixi
a95f0c4 docs: improve section headers and flow in Using Pixi
daf52ff docs: remove troubleshooting section from Using Pixi
23b2de8 docs: add local editable dependency section to Pixi guide
```

---

## Shared File Overlap

Files touched by multiple PRs, showing why the stacking order matters:

| File | PRs | Resolution |
|------|-----|------------|
| `step_executor.py` | 1, 3, 5, 6 | PR 1 touches `_run_curator_in_subprocess` only; PR 3 adds `log_folder` to `create_flow` calls; PR 5 adds `step_run_id` params; PR 6 adds `skip_cache` params |
| `pipeline_manager.py` | 5, 6 | PR 5 adds `_step_run_ids` dict and propagation; PR 6 adds `skip_cache` param and cache guards |
| `local.py` | 1, 2, 3 | PR 1 touches `SIGINTSafeProcessPoolTaskRunner`; PR 2 changes `create_flow` max_workers and `validate_operation`; PR 3 adds `log_folder` param to `create_flow` |
| `creator.py` | 4, 5 | PR 4 refactors sandbox path computation; PR 5 adds `step_run_id` to builder calls |
| `docs/myst.yml` | 6, 8 | PR 6 adds skip-cache tutorial entry; PR 8 adds using-pixi entry |

---

## Dev-Only Commits (excluded from all PRs)

68 commits that only touch `_dev/` or `CLAUDE.local.md`. These are stripped
during the `ach/dev` → `ach/dev-clean` process and are not listed
individually. They include design docs, analysis docs, todos, and archive
operations.
