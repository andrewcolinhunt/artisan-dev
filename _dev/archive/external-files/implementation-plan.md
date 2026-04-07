# Implementation Plan: External Files (Designs 1-3)

**Date:** 2026-04-04
**Status:** Approved

---

## Codebase Validation

All three designs validated against current code — no drift detected:

| Design | Key reference | Current state |
|---|---|---|
| 1 | `data.py:94` — `original_name` in filename | Confirmed |
| 1 | `metric.py:76-79` — conditional fallback | Confirmed |
| 1 | `execution_config.py:156` — `original_name or artifact_id` | Confirmed |
| 1 | `curator.py:150` — always calls `capture_lineage_metadata` | Confirmed (bug exists) |
| 1 | `creator.py:242` — checks `result.lineage` first | Confirmed |
| 2 | `pipeline_config.py` — no `files_root` field | Confirmed |
| 2 | `runtime_environment.py` — no `files_root_path` field | Confirmed |
| 2 | `artifact_store.py:29` — `__init__` takes only `base_path` | Confirmed |
| 2 | `step_executor.py:363` — `RuntimeEnvironment(...)` no `files_root` | Confirmed |
| 3 | `submit()` at line 1066 — no `post_step` param | Confirmed |
| 3 | `run()` at line 1005 — no `post_step` param | Confirmed |

---

## Open Questions — All Deferrable

| Design | Question | Assumption |
|---|---|---|
| 2 | File cleanup on re-run | No auto-delete; defer to `pipeline.cleanup_files()` utility |
| 2 | Path stability | Absolute paths work for target environment (fixed cluster) |
| 2 | Caching contract for external files | No cache validation of external file existence |
| 2 | Cross-filesystem atomicity | Standard assumptions; no special handling |
| 3 | Hidden step numbers UX | Both steps visible in results; user gets post_step number |
| 3 | Pipeline results display | Both steps visible as separate `StepResult` objects |
| 3 | Composites interaction | Deferred per design; composites use explicit steps in `compose()` |

---

## Risks and Considerations

- **Curator `_handle_artifact_result` (Design 1)** — the lineage bug fix changes control flow in a hot path. Must mirror the creator's pattern exactly: check `result.lineage` before calling `capture_lineage_metadata`.
- **`materialize_inputs` return type change (Design 1)** — changes a function signature used in `creator.py:184`. All callers must be updated. Search for all call sites beyond `creator.py`.
- **Frozen model validator (Design 2)** — `PipelineConfig` is `frozen=True`. The `@model_validator(mode="after")` needs `object.__setattr__` to set the default. This is the established Pydantic pattern for frozen model defaults.
- **`submit()` refactoring (Design 3)** — multiple early returns must be restructured. The most complex change. Must not alter existing early-exit semantics while adding the post_step pathway.
- **File overlap** — Designs 1 & 2 both touch `curator.py`; Designs 2 & 3 both touch `pipeline_manager.py`. Sequential branching handles this.

---

## Branch Chain

```
ach/dev
  └── feat/artifact-id-materialization  (Agent 1 — Design 1)
        └── feat/external-content-artifacts  (Agent 2 — Design 2)
              └── feat/post-step-sugar  (Agent 3 — Design 3)
```

---

## Agent 1: Artifact-ID Materialization

**Branch:** `feat/artifact-id-materialization` from `ach/dev`
**Design doc:** `_dev/design/0_current/external-files/1-artifact-id-materialization.md`

### Changes

- `src/artisan/schemas/artifact/data.py` — `_materialize_content()`: change filename from `self.original_name` to `self.artifact_id`. Remove the `original_name is None` guard (artifact_id is always set after finalization, but materialization happens on hydrated artifacts that were already finalized — verify this assumption).
- `src/artisan/schemas/artifact/metric.py` — `_materialize_content()`: simplify to always use `self.artifact_id`, removing the `if self.original_name` conditional.
- `src/artisan/schemas/artifact/execution_config.py` — `materialize_to()`: change `stem = self.original_name or self.artifact_id` to `stem = self.artifact_id`.
- `src/artisan/execution/inputs/materialization.py` — change return type to `tuple[dict[str, list[Artifact]], set[str]]`. Collect artifact_ids of materialized inputs into a set and return it as the second element.
- `src/artisan/execution/lineage/capture.py` — add optional `filesystem_match_map: dict[str, str] | None = None` parameter to `capture_lineage_metadata()`. When an output's stem is found in the match map, use the mapped input_id directly instead of stem matching. Fall back to existing stem matching when no match map entry exists.
- `src/artisan/execution/lineage/filesystem_match.py` — **new file**: `build_filesystem_match_map()` function. Matches output file stems to input artifact_ids by prefix.
- `src/artisan/execution/lineage/name_derivation.py` — **new file**: `derive_human_names()` function. Overwrites `original_name` on outputs with human-readable names derived from input names + operation suffix.
- `src/artisan/execution/executors/creator.py` — wire into `run_creator_lifecycle()`: (a) capture `materialized_artifact_ids` from `materialize_inputs()`, (b) build filesystem match map after `output_snapshot()`, (c) pass match map to `capture_lineage_metadata()`, (d) call `derive_human_names()` after lineage.
- `src/artisan/execution/executors/curator.py` — fix `_handle_artifact_result()`: check `result.lineage` before calling `capture_lineage_metadata()`, mirroring the creator pattern.

### Key Details

- `materialize_inputs` is also called indirectly from the composite path — but `composite.py` uses `run_creator_lifecycle()` which calls it. The return value change must be handled in `creator.py` where the function is called (line 184). Search for other call sites with `grep`.
- The `_materialize_content()` methods on DataArtifact and MetricArtifact are only called from `materialize_to()` on the base class. The base `materialize_to()` calls `_materialize_content(directory)` — follow this chain.
- For the curator lineage bug fix: the design shows the pattern. In `_handle_artifact_result`, add `if result.lineage is None:` before the existing `capture_lineage_metadata` call, and add an `else` branch that calls `validate_lineage_integrity` then uses `result.lineage` directly.

### Tests

- `tests/artisan/schemas/artifact/test_artifact_id_materialization.py` — DataArtifact, MetricArtifact, ExecutionConfigArtifact materialize with artifact_id filename; no collisions with duplicate original_name; extension preserved; FileRefArtifact excluded
- `tests/artisan/execution/test_filesystem_match.py` — prefix matching, no-match fallback, multiple inputs, edge cases (no suffix, extension-only)
- `tests/artisan/execution/test_name_derivation.py` — suffix extraction, human name derivation, unmatched outputs preserved, empty suffix case
- `tests/artisan/execution/test_creator_lifecycle.py` — end-to-end: materialization -> match map -> lineage -> name derivation -> correct original_name in staged artifacts
- `tests/artisan/execution/test_curator_explicit_lineage.py` — curator honors ArtifactResult.lineage, falls back to stem inference when None

### Verification

```bash
~/.pixi/bin/pixi run -e dev fmt
~/.pixi/bin/pixi run -e dev test
```

---

## Agent 2: External Content Artifacts

**Branch:** `feat/external-content-artifacts` from `feat/artifact-id-materialization`
**Design doc:** `_dev/design/0_current/external-files/2-external-content-artifacts.md`

### Changes

- `src/artisan/schemas/orchestration/pipeline_config.py` — add `files_root: Path | None = Field(default=None, ...)` with a `@model_validator(mode="after")` that defaults to `self.delta_root.parent / "files"`. Use `object.__setattr__` since the model is frozen.
- `src/artisan/schemas/execution/runtime_environment.py` — add `files_root_path: Path | None = Field(None, ...)` field. Frozen model, follows existing field pattern.
- `src/artisan/storage/core/artifact_store.py` — add optional `files_root: Path | None = None` parameter to `__init__()`. Store as `self.files_root`. Backward compatible — existing callers pass nothing.
- `src/artisan/orchestration/pipeline_manager.py` — (a) add `files_root` parameter to `create()`, pass it to `PipelineConfig`. (b) Thread `files_root` wherever `ArtifactStore` or `RuntimeEnvironment` is constructed.
- `src/artisan/orchestration/engine/step_executor.py` — in `_create_runtime_environment()`: pass `config.files_root` as `files_root_path`. In both `ArtifactStore(config.delta_root)` call sites: pass `files_root=config.files_root`.
- `src/artisan/execution/executors/curator.py` — `run_curator_flow()` line 284: pass `files_root` from `runtime_env.files_root_path` to `ArtifactStore()`.
- `src/artisan/execution/executors/composite.py` — line 62: pass `files_root` from `runtime_env.files_root_path` to `ArtifactStore()`.
- `src/artisan/execution/context/builder.py` — `_build_execution_context()`: pass `files_root` to `ArtifactStore()`. Requires adding `files_root` parameter to the builder functions.

### Key Details

- `PipelineConfig` uses `model_config = {"frozen": True}`. The model_validator pattern is: `object.__setattr__(self, "files_root", self.delta_root.parent / "files")`.
- `ArtifactStore.__init__` must remain backward compatible — `files_root` defaults to `None` so existing call sites (tests, interactive_filter, ingest_pipeline_step, docs) work unchanged.
- Search for ALL `ArtifactStore(` constructor calls in the codebase to ensure none are missed.
- `builder.py` has both `build_creator_execution_context` and `build_curator_execution_context` which share `_build_execution_context`. Thread `files_root` through the shared helper.

### Tests

- `tests/artisan/schemas/test_pipeline_config_files_root.py` — `files_root` default derivation from `delta_root`, explicit override, frozen model behavior
- `tests/artisan/storage/test_artifact_store.py` — add tests: ArtifactStore accepts and exposes `files_root`, `None` default
- `tests/artisan/orchestration/test_pipeline_manager.py` — `files_root` threaded through `create()` to ArtifactStore (may need to extend existing tests)
- `tests/artisan/schemas/test_runtime_environment.py` — `files_root_path` field present, default None, propagation

### Verification

```bash
~/.pixi/bin/pixi run -e dev fmt
~/.pixi/bin/pixi run -e dev test
```

---

## Agent 3: Post-Step Sugar

**Branch:** `feat/post-step-sugar` from `feat/external-content-artifacts`
**Design doc:** `_dev/design/0_current/external-files/3-post-step-sugar.md`

### Changes

- `src/artisan/orchestration/pipeline_manager.py` — add `post_step: type[OperationDefinition] | None = None` parameter to both `submit()` and `run()`. Restructure `submit()` so that:
  - Composite routing and early exit still return directly (post_step skipped)
  - Cache hit assigns `main_future` instead of returning
  - File path promotion failure returns directly (post_step skipped)
  - Dispatch assigns `main_future` instead of returning
  - After all paths converge: if `post_step is not None`, build `post_inputs` from `main_future.output(role)` for each role in `operation.outputs`, then recursively call `self.submit(post_step, inputs=post_inputs, ...)` and return its result
  - If `post_step is None`, return `main_future` as before

### Key Details

- The current `submit()` (lines 1066-1220) has these return paths:
  - Line 1117-1129: composite routing -> returns directly (post_step skipped, correct)
  - Line 1150-1151: early exit -> returns directly (post_step skipped, correct)
  - Line 1179-1180: cache hit -> currently returns directly, must change to assign `main_future`
  - Line 1197-1198: file path promotion failure -> returns directly (post_step skipped, correct)
  - Line 1204-1220: dispatch -> currently returns directly, must change to assign `main_future`
- `run()` simply calls `submit().result()`, so it just needs `post_step` in its signature and to forward it.
- Post_step input construction: `{role: main_future.output(role) for role in operation.outputs}`. Uses `operation.outputs` (the main step's declared output roles).
- Forward behavioral flags (`backend`, `compact`, `skip_cache`, `failure_policy`) but NOT overrides (`params`, `resources`, `execution`, `environment`, `tool`).
- Step naming: `f"{step_name}.post"` for the post_step's name.

### Tests

- `tests/artisan/orchestration/test_post_step.py`:
  - StepFuture returned points to post_step (correct step_number)
  - Two step numbers consumed
  - Downstream `OutputReference` resolution works through post_step
  - Caching: both steps cached, partial cache (main cached, post_step runs)
  - Role mismatch error (post_step inputs don't match main outputs)
  - `post_step=None` is a no-op (backward compatibility)
  - Post_step skipped when pipeline stopped/cancelled
  - Post_step skipped when file path promotion fails

### Verification

```bash
~/.pixi/bin/pixi run -e dev fmt
~/.pixi/bin/pixi run -e dev test
```
