# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `compute_resources` kwarg on `submit_composite` / `run_composite` and
  on `ExpandedCompositeContext.run` / `_run_nested_composite`, plus
  `compute_provider`, `skip_cache`, `failure_policy`, and `compact`
  forwarding through the expanded composite path. Per-`ctx.run()`
  overrides for these kwargs now match the collapsed-mode surface.
- Typed-config acceptance on every override kwarg of `submit` / `run` /
  `submit_composite` / `run_composite`. Each kwarg accepts
  `dict | TypedModel | None` (plus `str` for the active-selector forms
  on `environment` and `compute_provider`); a single set of `_coerce_*`
  helpers normalizes typed models to dicts at the boundary, so the
  downstream pipeline consumes dicts unchanged.
- `_validate_compute_provider`, `_validate_compute_resources`, and
  `_reject_inactive_provider_config` validators on the public surface.
  Passing `environment={"docker": {...}}` (or a `compute_provider` dict)
  without the matching `active=` selector now raises `ValueError` at
  call time, closing a silent misconfiguration case.
- `tests/artisan/schemas/operation_config/test_compute_resources.py`
  (field validation, unknown-key rejection, round-trip) and end-to-end
  hash-stability fixtures that flow through `_merge_config_overrides`
  rather than calling the hashing primitive directly.

### Changed

- **BREAKING — Skill renames:** The artisan plugin's skill names are inverted
  to noun-first for tab-completion grouping. `write-operation` →
  `operation-write`, `write-composite` → `composite-write`, `write-pipeline`
  → `pipeline-write`. Any downstream invocations of `/write-operation`,
  `/write-composite`, `/write-pipeline` (or their `/artisan:` namespaced
  forms) must be updated to the new names. No aliases are kept.
- **Cache invalidation event:** `_merge_config_overrides` now emits
  `"compute_resources"` as a fourth payload key alongside `"environment"`,
  `"tool"`, and `"compute_provider"`. Two runs that differ only by
  `compute_resources` (e.g. A100 vs H100) previously cached to the
  same `step_spec_id` and silently reused each other's artifacts; they
  now hash distinctly. Any cached step dispatched with non-default
  `compute_resources` will miss after this change.
- `ComputeResources` schema now uses `extra="forbid"` so unknown keys
  raise `ValidationError`.
- Restructured tutorials with numeric prefixes (`01-` … `09-`) on
  every top-level category, split the 13-flat `execution/` dir into
  five peer categories (`03-caching/`, `04-batching/`,
  `05-errors-and-control/`, `06-storage/`, `07-compute-backends/`),
  and updated every inbound link in `docs/concepts/`,
  `docs/how-to-guides/`, `docs/getting-started/`,
  `docs/contributing/`, `docs/reference/`, peer notebooks, and demo
  scripts. Disk order now matches the teaching arc; the sidebar
  mirrors disk one-to-one (no synthetic groupings). Fixes seven
  occurrences of the broken
  `pipeline-design/06-composable-operations.ipynb` link (the file is
  `06-composites.ipynb`) and drops three references to the
  non-existent `01-run-vs-submit.ipynb`. Use `git log --follow` for
  history on moved files.
- Vocabulary sweep: completes the `backend` → `step runner` /
  `BackendBase` → `RunnerBase` rename across `docs/concepts/`,
  `docs/reference/`, `docs/contributing/`, `docs/how-to-guides/`, and
  the execution tutorial notebooks. The kwargs `resources` /
  `execution` / `compute` are renamed to `runner_resources` /
  `batch_strategy` / `compute_provider` in the corresponding doc
  examples (the source had already migrated). The `Compute` class is
  renamed to `ComputeProvider` in code samples. `pipeline.expand()`
  references in docs are replaced with
  `pipeline.submit_composite(..., expand=True)`.
- Renamed pytest marker `slow` to `integration` across `pyproject.toml`,
  pixi tasks, all 17 files in `tests/integration/`, and contributor docs.
  The new name describes the requirement (real infra: Delta Lake + Prefect
  harness) rather than a speed adjective. Select with `pytest -m integration`;
  deselect with `pytest -m 'not integration'`.
- Tightened pre-commit hook scope: per-hook excludes for blacken-docs (4
  files with intentional pseudo-code), check-yaml / prettier
  (`recipe/meta.yaml` Jinja template), name-tests-test
  (`tests/fixtures/csv.py` helper), and end-of-file-fixer /
  trailing-whitespace (Delta Lake fixture stores under
  `docs/tutorials/*/runs/`). Dropped Markdown from prettier's scope —
  contributors hand-format MyST; Python in Markdown fences stays
  covered by blacken-docs. Added codespell ignore for structural-biology
  token `SER`. Added `explicit_package_bases` / `namespace_packages` to
  mypy config so it can run past the duplicate-`conftest` issue. No
  behavior changes.
- Bumped pixi's `ruff` pin from `==0.6.2` to `==0.13.2` to match the
  pre-commit gate's `ruff-pre-commit` rev, then applied `ruff format`
  tree-wide (5 files touched). Previously `pixi run -e dev fmt` and
  the pre-commit gate could disagree on formatting; they now run the
  same ruff version.
- Resolved 532 `ruff check` violations: 184 via autofix (EM101/EM102
  message hoisting, SIM117 with-statement merges), the remainder via
  real fixes (undefined `OperationDefinition` type hint in local
  backend, `os.environ.get` default type, two `assert False` →
  `pytest.fail`, three loop-variable renames, en-dash → hyphen in
  one docstring) plus documented `[tool.ruff.lint.ignore]` and
  `[tool.ruff.lint.per-file-ignores]` entries covering G004,
  PLW0603, and the protocol-conformance `ARG` / pytest-style rules
  in tests.
- Mypy strict-mode now passes on `src/artisan/**`. Added
  `[[tool.mypy.overrides]]` relaxing `tests/**` (ignore_errors) and
  silencing third-party untyped imports (fsspec, graphviz,
  cloudpickle, prefect_submitit, ipywidgets, matplotlib, modal,
  IPython). Cleared ~290 real type errors in src via an 8-agent
  parallel dispatch plus coordinator residuals — primarily type
  annotations, narrowing casts, and specific-code `type: ignore`
  comments. No runtime behavior changes. `pixi run -e dev mypy
  src/artisan tests` and `pre-commit run mypy --all-files` now
  exit 0. Tests under `tests/**` remain parsed by mypy but
  errors are not reported — functional regression gate stays
  pytest; tightening tests back toward strict is deferred to a
  future phase.
- `pixi run -e dev setup` now auto-installs pre-commit hooks
  (reinstates the block removed in `c79990d` once the backlog was
  cleared). A new `pre-commit` CI job runs
  `pixi run -e dev pre-commit run --all-files` on every push/PR, so
  regressions on any hook (ruff, mypy, blacken-docs, codespell, ...)
  now block merge. Default-env `pixi run setup` still skips the hook
  install — pre-commit is a dev-feature dep.

### Removed

- Unused `fast` pytest marker (never applied to any test).

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
