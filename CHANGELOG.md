# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Structured command evidence for local and endpoint executions, including actual
  wrapped argument lists, execution order, outcomes, redaction, and subprocess
  launch timing. Commands can be inspected without parsing tool output.
- Single-execution debug replay with fresh attempt identities, verbose output,
  preserved diagnostics, and explicit limits when reconstruction is unavailable.
- Per-step and composite `cache_policy` overrides with documented inheritance.
- Run-scoped through-N ingestion and an artifact-writing skill.
- MCP setup guidance and real stdio-client checks for development and installed
  release packages.
- Staging recovery before cache lookup, with independent `recover_staging` and
  `preserve_staging` controls. Finished executions from interrupted or cancelled
  steps can be reused without changing the source step's history.
- Explicit repair reports and recovery of interrupted logical commits and
  unplanned finished executions, with optional staging preservation.
- Exact execution-linked provider-log inspection through `inspect_worker_log()`.

### Changed

- Require Polars 1.30.0 or newer for execution-seal Parquet metadata.
- Ordinary reads use completion records for visibility and retain artifact checks
  on access. Historical effect verification is an explicit read-only store repair
  inspection; startup recovery validates only the work it must recover or clean.
- **Breaking: store format 5.** The release uses typed artifact identity,
  independent artifact locations, current-run cache-reuse relations,
  completion-gated logical commits, inventoried immutable execution seals,
  batched recovery commits, and canonical command/replay evidence.
  Existing stores must use a new Delta root; there is no migration or
  compatibility reader. Artifact identity is version 1 and cache identity is
  version 2.
- **Breaking: public imports and operation parameters.** Use the documented
  package facades; the root package no longer re-exports implementation types.
  Operations declare a nested `Params` model and its `params` field. Flat
  operation parameters and retired compatibility APIs have been removed.
- **Breaking: orchestration and runner contracts.** Native local execution
  replaces Prefect. SLURM runners come from `artisan-submitit`; pass a configured
  provider instance and use `artisan.orchestration.runner_api` for provider
  implementations. Routers settle one ordered `UnitResult` per submitted unit
  and attach worker logs there. Resuming an external-runner pipeline requires
  its configured provider instance.
- **Breaking: compute ownership.** The unused pipeline-wide
  `default_compute_provider` setting is removed. Operations declare compute
  configuration and steps/composites can override it explicitly.
- **Breaking: ingest selection.** `IngestPipelineStep` requires a source run and
  selects that run's accepted outputs. Through-N mode unions outputs through
  the inclusive boundary. Ingest reads the source again on every invocation;
  operations can declare when their inputs/configuration are insufficient for
  safe cache reuse.
- **Breaking: log layout.** Pipeline sessions and failure logs have sortable,
  unique paths. Readers use the same canonical layout as writers; old paths
  have no fallback.
- Mapping and typed configuration overrides share one validated recursive patch
  contract. Explicit defaults and null values retain their meaning, and hashing
  uses the same prepared configuration that execution consumes.
- Artifact identity separates types and semantic content from storage locations.
  Ordered input occurrences enter cache identities; loading external artifacts
  verifies their bytes before use.
- Step history uses explicit terminal states, guarded transitions, partial
  outcomes, and confirmed/rejected/indeterminate cancellation evidence. Cache
  hits have their own current-run attempt and preserve source execution links.
- Endpoint data policies enforce URI, credential, archive, and output-sink trust
  boundaries. Discovered Modal credentials are never forwarded to custom URLs.
- Worker identity is resolved once into an immutable worker runtime.
- Setup, contributor hooks, and notebook-kernel registration are explicit tasks.
- Authoring skills use noun-first names: `operation-write`, `composite-write`,
  `pipeline-write`, and `artifact-write`; old skill names have no aliases.
- Tutorial paths follow the numbered learning sequence and published examples
  use the supported API.

### Fixed

- Recovery batches finished executions from each source step through the ordinary
  commit writer. Completed work is skipped when no staging cleanup is needed;
  recovery no longer repeatedly verifies historical table effects.
- Cancellation retains uncommitted staging regardless of the preservation flag.
  Cleanup removes only verified committed files and retains changed or unlisted
  evidence. Provider-log delivery no longer mutates sealed worker results.
- Directed-ancestry lineage pairing no longer accepts sibling-only matches and
  fails clearly on ambiguous equal-distance candidates.
- Cancellation and provider/bootstrap failures settle affected units without
  discarding completed results or hiding execution failures.
- Pipeline logs remain isolated across managers and release their file handlers
  on finalization. Timing distinguishes subprocess creation from tool runtime.
- Streaming subprocesses close their output pipes on completion and interruption.
  Failure and cancellation diagnostics redact known credential values.
- CLI model results preserve JSON arrays and values instead of Python object
  representations.
- Run-scoped inspection, provenance, and MCP readers preserve cached membership
  and reject incomplete or inconsistent committed storage evidence.
- Identical artifacts produced or imported at later steps deduplicate while
  retaining their first committed origin.
- Loaded artifact payloads count as hydrated without optional origin metadata;
  decoded values cannot mutate finalized content under an existing identity.
- Nested composite waits include descendants and share one timeout across all
  owned steps. Filters preserve each criterion's selected metric source.
- Endpoint output-delivery errors distinguish unavailable dependencies and
  unsupported signing from transient transfer failures.
- External artifact I/O supports local and object-store backends consistently;
  staged artifact effects deduplicate by their natural identities.
- Optional dependencies load lazily, MCP surfaces are bounded, and packaged
  examples and documentation reflect the current release contracts.

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
