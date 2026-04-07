# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.2a5] - 2026-04-06

### Added

- `LargeFileArtifact` — external-content artifact for large files (model
  weights, embeddings, HDF5) stored outside Delta Lake
- `AppendableArtifact` — external-content artifact representing one record
  within a shared JSONL file, supporting per-worker writes and consolidation
- `ConsolidateAppendables` curator operation for merging per-worker JSONL files
- `AppendableGenerator` and `LargeFileGenerator` example operations
- `files_root` parameter on `PipelineManager.create()` — threads through
  `PipelineConfig`, `RuntimeEnvironment`, `ArtifactStore`, and all executor
  layers for external-content artifact storage
- `files_dir` threaded to creator operations via `ExecuteInput`
- `post_step` parameter on `submit()` and `run()` for post-step consolidation
- `DispatchHandle` abstract base class — lifecycle handle for in-flight backend
  work with `dispatch()` / `is_done()` / `collect()` / `cancel()` semantics
- `UnitResult` dataclass — typed dispatch results replacing `list[dict]`
- Artifact-ID materialization — inputs materialize as `{artifact_id}{extension}`
  instead of `{original_name}{extension}`, eliminating name collisions
- Filesystem match map (`build_filesystem_match_map`) for linking output files
  back to source inputs via artifact-ID prefix matching
- Human-readable name derivation (`derive_human_names`) restores original names
  after lineage is established
- `num_files` parameter on `RecordBundleGenerator` for multi-file output
- External file storage tutorial (`11-external-file-storage`)
- Post-step consolidation tutorial (`12-post-step-consolidation`)

### Changed

- Orchestration layer migrated from `dict` to `UnitResult` throughout dispatch,
  result aggregation, and backend log capture
- Updated cancellation docs for auto-scancel and `DispatchHandle`
- Updated execution flow concepts page
- Re-ran first-pipeline tutorial with clean Prefect logging output

### Fixed

- Process/thread leak from unfinalized `PipelineManager` instances —
  `ThreadPoolExecutor` threads now cleaned up via `__del__`, context manager
  (`with PipelineManager.create(...) as pipeline:`), and `atexit` handler
- `finalize()` is now idempotent — safe to call multiple times, returns cached
  summary on subsequent calls
- `activate_server()` no longer stacks Prefect `SettingsContext` objects — exits
  the previous context before entering a new one
- Prefect logging suppressed before import triggers dict-config
- `_handle_artifact_result` now honors `ArtifactResult.lineage` instead of
  silently dropping it
- `contextvars` propagation to dispatch handle background threads
- Added missing `finalize()` calls to 7 pipelines across 4 tutorial notebooks
  (`02-resume-and-caching`, `04-error-visibility`, `07-slurm-execution`,
  `10-slurm-intra-execution`)

### Refactored

- Renamed `RecordBundle` to `Appendable` across the codebase

## [0.1.2a4] - 2026-04-03

### Added

- `SlurmIntraBackend` for zero-latency `srun` dispatch within an existing SLURM
  allocation (`salloc` session) — bypasses the scheduler queue entirely
- SLURM intra-allocation tutorial and demo script
- GPU execution defaults — sequential `max_workers=1` for GPU steps to avoid
  CUDA context conflicts, automatic `MASTER_PORT` allocation
- `skip_cache` pipeline parameter to force re-execution of all steps
- Prefect server discovery improvements — version mismatch detection, stale
  process warnings, multi-source resolution
- "Using Pixi" getting-started page covering environments, tasks, shells, and
  workspaces

### Changed

- Rewrote getting-started documentation pages and README with relative links
- SLURM logs now route into the pipeline runs directory instead of the working
  directory
- Step output isolation via `step_run_id` — each step run writes to a unique
  subdirectory, preventing collisions on re-runs

### Fixed

- Subprocess re-import guard — prevents user scripts from being re-executed
  when workers spawn child processes
- VS Code kernel slowness workaround restored to installation page

### Refactored

- Separated sandbox path computation from directory creation for testability

## [0.1.2a3] - 2026-04-01

### Fixed

- Release workflow now produces correct version — switched from hardcoded
  `version` in `pyproject.toml` to dynamic versioning via `hatch-vcs` (derives
  version from git tags at build time)
- Added `__version__` runtime export to `artisan` package

## [0.1.2a2] - 2026-03-17

### Added

- Prefect Cloud support — `discover_server()` now reads Prefect profiles as a
  fallback and skips health checks for Cloud URLs
- "Connect to Prefect" how-to guide covering self-hosted, Cloud, SLURM, and
  discovery priority
- "Using Claude Code" Getting Started page

### Changed

- Rewrote Getting Started documentation: installation (actions first, dropdowns
  for explainers), orientation (Diataxis table, expanded abstractions), and
  index descriptions
- Updated `activate_server()` to use Prefect v3 settings API (`model_copy`)
- Trimmed README — removed duplicated content, added Prefect server note after
  Quick Example
- Re-executed first-pipeline tutorial notebook with current outputs

### Fixed

- Skills directory path (`.claude-plugin/` → `skills/`)
- Removed fake `/plugin install` commands from Using Claude Code page
- Storage description ("JSON strings" → "JSON content serialized as bytes")
- Node.js listed as core dependency (now clarified as docs-only)
- Cross-reference anchors in tooling-decisions and comparison-to-alternatives

### Removed

- `first-pipeline.md` (replaced by the existing tutorial notebook)

## [0.1.2a1] - 2026-03-16

### Added

- `CompositeDefinition` base class for bundling operations into reusable units
- Collapsed and expanded composite execution modes
- Composite provenance tracking
- Pipeline cancellation via `SIGINT` / `Ctrl+C` with `StepTracker`
- `WaitOperation` example for testing cancellation
- `ProvenanceStore` for provenance queries
- `walk_forward_to_targets` traversal function
- Metric type preservation through tidy/wide DataFrame pipeline
- Claude Code skills: `/write-operation`, `/write-composite`, `/write-pipeline`,
  `/write-docs`
- Integration tests for composites, cross-pipeline, cache policies, error
  handling, filter, interactive filter, multi-input, step overrides, and
  topology gaps
- Community guidelines (CONTRIBUTING.md, CODE_OF_CONDUCT.md, SECURITY.md)
- Conda recipe (`recipe/meta.yaml`)
- Tutorials: run-vs-submit, resume-and-caching, batching, error visibility,
  storage-and-logging, step overrides, SLURM, provenance graphs, lineage
  tracing, timing analysis, composites

### Changed

- Rewrote `Filter` to use forward provenance walk for metric discovery
- Rewrote `InteractiveFilter` for parity with new Filter API
- Restructured tutorials into getting-started, pipeline-design, execution,
  analysis, and writing-operations sections
- Renamed package from `artisan` to `dexterity-artisan`

### Removed

- Chain executor and `ChainBuilder` (replaced by composites)

## [0.1.1] - 2026-03-05

### Added

- Initial open-source release of Artisan pipeline framework
- `PipelineManager` for orchestrating multi-step pipelines
- `OperationDefinition` base class for defining pipeline operations
- Built-in curator operations: `Filter`, `IngestData`, `IngestFiles`,
  `IngestPipelineStep`, `InteractiveFilter`, `Merge`
- Example operations: `DataGenerator`, `DataTransformer`, `MetricCalculator`
- Local and SLURM execution backends
- Delta Lake storage layer with content-addressed artifacts
- Provenance tracking with dual lineage (data + execution)
- Provenance graph visualization (macro and micro views)
- Pipeline timing analysis
- Caching and resume support
- Jupyter Book 2 documentation site

[Unreleased]: https://github.com/dexterity-systems/artisan/compare/v0.1.2a5...HEAD
[0.1.2a5]: https://github.com/dexterity-systems/artisan/compare/v0.1.2a4...v0.1.2a5
[0.1.2a4]: https://github.com/dexterity-systems/artisan/compare/v0.1.2a3...v0.1.2a4
[0.1.2a3]: https://github.com/dexterity-systems/artisan/compare/v0.1.2a2...v0.1.2a3
[0.1.2a2]: https://github.com/dexterity-systems/artisan/compare/v0.1.2a1...v0.1.2a2
[0.1.2a1]: https://github.com/dexterity-systems/artisan/compare/v0.1.1...v0.1.2a1
[0.1.1]: https://github.com/dexterity-systems/artisan/releases/tag/v0.1.1
