# v0.1.2a5 — External content artifacts, DispatchHandle, and artifact-ID materialization

This release introduces external-content artifact types for files that live
outside Delta storage, replaces the internal dispatch lifecycle with a proper
handle abstraction, and reworks input materialization to use artifact IDs
instead of human-readable names — eliminating name collisions in multi-input
pipelines.

## Highlights

- **External-content artifacts** — Two new artifact types for content that
  lives outside Delta Lake storage. `LargeFileArtifact` stores a one-to-one
  reference to an external file (model weights, embeddings, HDF5 datasets).
  `AppendableArtifact` represents one record within a shared JSONL file,
  enabling many-records-per-file patterns with per-worker writes and
  post-step consolidation.

- **`files_root` threading** — New `files_root` parameter on
  `PipelineManager.create()` specifies where external-content artifacts
  store their data. Threads through `PipelineConfig`, `RuntimeEnvironment`,
  `ArtifactStore`, and all executor layers (step, curator, composite,
  builder) so operations receive a pre-configured output directory.

- **`post_step` parameter** — `submit()` and `run()` now accept a
  `post_step` argument for specifying a consolidation operation that runs
  after each step completes. Designed for aggregating per-worker outputs
  (e.g., concatenating per-worker JSONL files into a single combined file).

- **DispatchHandle** — New abstract base class replacing the internal
  `create_flow` mechanism. Provides a clean lifecycle for in-flight backend
  work: `dispatch()` → `is_done()` → `collect()`, with `cancel()` and a
  blocking `run()` template method. Copies `contextvars` into background
  threads so Prefect settings propagate correctly.

- **Artifact-ID materialization** — Input artifacts now materialize to
  `{artifact_id}{extension}` instead of `{original_name}{extension}`. This
  eliminates filename collisions when multiple inputs share the same human
  name. A filesystem match map links output files back to their source
  inputs via artifact-ID prefix matching, and human-readable names are
  restored automatically after lineage is established.

- **UnitResult dataclass** — Typed `UnitResult` replaces the informal
  `list[dict]` contract between dispatch, result aggregation, and backend
  log capture. Fields: `success`, `error`, `item_count`,
  `execution_run_ids`, `worker_log`.

## Fixes

- **Process/thread leak** — `PipelineManager` now cleans up
  `ThreadPoolExecutor` threads via `__del__`, context manager (`with
  PipelineManager.create(...) as pipeline:`), and `atexit` handler.
  `finalize()` is idempotent.
- **Prefect SettingsContext stacking** — `activate_server()` exits the
  previous context before entering a new one.
- **Prefect logging suppression** — Logging is suppressed before import
  triggers dict-config.
- **Missing `finalize()` calls** in 7 pipelines across 4 tutorial notebooks.
- **Curator lineage** — `_handle_artifact_result` now honors
  `ArtifactResult.lineage` instead of silently dropping it.

## Refactoring

- **RecordBundle renamed to Appendable** across the codebase.
- **Orchestration layer migrated from `dict` to `UnitResult`.**

## Docs

- New **external file storage tutorial** covering `LargeFileArtifact`,
  `files_root`, and the materialization workflow.
- New **post-step consolidation tutorial** covering `AppendableArtifact`,
  per-worker output patterns, and the `post_step` parameter.
- Updated cancellation docs for auto-scancel and `DispatchHandle`.
- Re-ran first-pipeline tutorial with clean Prefect logging output.
- Updated execution flow concepts page.

## Install

```bash
pip install dexterity-artisan==0.1.2a5
```

## Full Changelog

See [CHANGELOG.md](CHANGELOG.md) for the complete list of changes.
