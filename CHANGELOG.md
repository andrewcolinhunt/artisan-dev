# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/dexterity-systems/artisan/compare/v0.1.2a2...HEAD
[0.1.2a2]: https://github.com/dexterity-systems/artisan/compare/v0.1.2a1...v0.1.2a2
[0.1.2a1]: https://github.com/dexterity-systems/artisan/compare/v0.1.1...v0.1.2a1
[0.1.1]: https://github.com/dexterity-systems/artisan/releases/tag/v0.1.1
