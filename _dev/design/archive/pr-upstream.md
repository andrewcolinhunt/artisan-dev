# PR: Composites, pipeline cancellation, provenance refactor, and docs overhaul

> PR from `origin/main` → `upstream/main`

---

## Summary

This PR adds composite operations, pipeline cancellation, a forward-walk
Filter rewrite, an operation config redesign, provenance store extraction,
and a comprehensive documentation overhaul. It represents ~6 weeks of
development across 213 files (+26,600 / -10,400 lines), with 1,841 unit
tests passing and 40+ new integration tests.

---

## Features

### Composite operations

#### Summary

Composites allow users to package multi-operation workflows as reusable
building blocks with declared I/O contracts. Users subclass
`CompositeDefinition`, declare input/output specs with `InputRole`/`OutputRole`
enums, and implement a `compose(ctx)` method that wires internal operations
together.

#### Changes

- New `src/artisan/composites/` package with `CompositeDefinition` base class,
  `CompositeContext` (collapsed and expanded variants), and provenance handling
- `CollapsedCompositeContext`: all internal ops run eagerly in-process on a
  single worker with in-memory artifact passing
- `ExpandedCompositeContext`: each `ctx.run()` delegates to the parent
  pipeline's `submit()`, creating independent pipeline steps with per-operation
  resource/backend overrides
- Configurable intermediate provenance via `CompositeIntermediates` enum
  (DISCARD / PERSIST / EXPOSE)
- New schemas: `CompositeRef`, `CompositeStepHandle`, `ExpandedCompositeResult`
- New execution models: `ArtifactSource` (unified interface for Delta-backed
  and in-memory artifacts), `ExecutionComposite` (pickle-safe transport model)
- New `composite` executor in `src/artisan/execution/executors/composite.py`
- Pipeline manager updated to validate, dispatch, and track composite steps
- `compute_composite_spec_id()` for composite step cache keys

#### Motivation

Users were copy-pasting pipeline wiring logic when the same sequence of
operations appeared in multiple pipelines. Composites give this a first-class
abstraction with clean I/O contracts, provenance tracking, and support for
both local and distributed execution.

#### Testing

- `tests/artisan/composites/` — 43 unit tests covering context, definition,
  and provenance
- `tests/artisan/execution/models/test_execution_composite.py` — 6 tests
- `tests/artisan/schemas/composites/test_composite_ref.py` — 10 tests
- `tests/artisan/utils/test_composite_spec_id.py` — 7 tests
- `tests/integration/test_composite_collapsed.py` — 4 integration tests
- `tests/integration/test_composite_expanded.py` — 4 integration tests

---

### Pipeline cancellation

#### Summary

Users can now Ctrl-C a running pipeline for a clean shutdown. In-flight steps
complete, pending steps are recorded as cancelled, and the pipeline state
remains consistent for resume.

#### Changes

- `PipelineManager` gains `_cancel_event` (threading.Event), `cancel()` method,
  and SIGINT/SIGTERM signal handlers with escalation logic (first signal =
  graceful cancel, second signal = restore default handlers + force-kill)
- Cancel checks inserted at multiple points: before step submission, after
  waiting for dependencies, before dispatch, and before commit
- Cancelled steps recorded via `record_step_cancelled()` for resume awareness
- `step_executor.execute_step()` accepts a `cancel_event` parameter threaded
  through to creator and curator paths
- Curator execution uses subprocess polling loop with 0.5s timeout checks
  against the cancel event
- New `SIGINTSafeProcessPoolTaskRunner` in `LocalBackend` — workers ignore
  SIGINT so only the parent orchestrates cancellation
- New `StepTracker` class extracted to handle step-level state persistence

#### Motivation

Previously, Ctrl-C during a pipeline run would produce noisy tracebacks from
child processes and leave the pipeline in an inconsistent state, requiring
manual cleanup before resuming. Clean cancellation is essential for
interactive development and long-running SLURM pipelines.

#### Testing

- `tests/artisan/orchestration/test_pipeline_manager.py` — 12 new
  cancellation-specific tests (`test_cancel_sets_event_idempotent`,
  `test_submit_skips_steps_when_cancelled`,
  `test_finalize_returns_cleanly_after_cancellation`,
  `test_cancel_during_predecessor_wait`, etc.)
- `tests/artisan/orchestration/test_signal_handling.py` — 3 tests
- `tests/artisan/orchestration/backends/test_sigint_safe_runner.py` — 3 tests
- `tests/artisan/orchestration/engine/test_step_tracker.py` — 4 tests

---

### Filter rewrite (forward provenance walk)

#### Summary

Filter now uses a forward provenance walk for metric discovery instead of
scanning all metrics backward. This is more efficient, enables step-targeted
criteria for disambiguation, and supports chunked evaluation to prevent OOM
on large artifact sets.

#### Changes

- New `src/artisan/provenance/traversal.py` with `walk_forward()` and
  `walk_backward()` — DataFrame-based iterative BFS traversals through
  provenance edges
- Filter uses two-phase approach: Phase 1 discovers metrics via `walk_forward`
  from passthrough artifacts; Phase 2 does chunked hydration + evaluation with
  a streaming `_DiagnosticsAccumulator`
- New `step` / `step_number` fields on `Criterion` for step-targeted filtering
  when the same metric field name appears at multiple pipeline steps
- `_build_metric_namespace()` hydrates metric JSON, unnests structs
  recursively, and enriches with step info for collision detection
- `InteractiveFilter` rewritten to share code with Filter (same forward-walk
  metric discovery, same v4 diagnostics format) and gains `commit()` to write
  results as a proper pipeline step

#### Motivation

The backward-walk approach required scanning all metrics in the store and then
filtering by ancestry, which scaled poorly with large provenance graphs.
Forward walk starts from the passthrough artifacts and follows edges to metrics,
visiting only relevant nodes. Step-targeted criteria solve ambiguity when
multiple steps produce metrics with the same field name.

#### Testing

- `tests/artisan/operations/curator/test_filter.py` — rewritten (53 tests,
  consolidated from a larger, more repetitive suite)
- `tests/artisan/operations/curator/test_interactive_filter.py` — expanded to
  29 tests
- `tests/artisan/execution/inputs/test_provenance_walk.py` — updated
- `tests/integration/test_filter_advanced.py` — 4 integration tests
- `tests/integration/test_interactive_filter.py` — 4 integration tests

---

### Operation config redesign (CommandSpec → EnvironmentSpec + ToolSpec)

#### Summary

The monolithic `CommandSpec` hierarchy (which mixed "what tool" with "where to
run it") is replaced by two orthogonal concepts: `ToolSpec` (the
executable/script) and `EnvironmentSpec` (the execution environment), allowing
users to switch environments independently of the tool definition.

#### Changes

- New `ToolSpec` model: declares executable, optional interpreter, subcommand;
  has `parts()` for command building and `validate_tool()`
- New `EnvironmentSpec` hierarchy: `LocalEnvironmentSpec` (with optional venv),
  `DockerEnvironmentSpec`, `ApptainerEnvironmentSpec`, `PixiEnvironmentSpec` —
  each implements `wrap_command()`, `prepare_env()`, and
  `validate_environment()` via polymorphic dispatch
- New `Environments` model: holds all configured environments with an `active`
  selector and `current()` method
- `OperationDefinition` gains `tool` and `environments` fields
- `external_tools.py` rewritten around new hierarchy: `run_command()` takes an
  `EnvironmentSpec`, process management uses `process_group=0` for clean group
  kills
- `CommandSpec`, `ArgStyle`, and legacy external tool functions removed

#### Motivation

`CommandSpec` conflated the tool being run with the environment it runs in.
Users needed to create separate operation subclasses to run the same tool in
Docker vs. Apptainer vs. locally. The new design separates these concerns and
supports runtime environment switching.

#### Testing

- `tests/artisan/schemas/test_environment_spec.py` — 32 tests
- `tests/artisan/schemas/test_environments.py` — 9 tests
- `tests/artisan/schemas/test_tool_spec.py` — 11 tests
- `tests/artisan/schemas/test_command_spec.py` — removed (replaced by above)
- `tests/artisan/utils/test_external_tools.py` — rewritten

---

## Refactors

### Provenance store extraction

#### Summary

All provenance graph query methods extracted from `ArtifactStore` into a
dedicated `ProvenanceStore` class, accessible via `ArtifactStore.provenance`.

#### Changes

- New `src/artisan/storage/core/provenance_store.py` (613 lines) with backward
  /forward map loading, type/step maps, ancestor/descendant queries (both
  dict-based and DataFrame-based), step range queries, scoped edge loading,
  and transitive BFS walks
- `ArtifactStore` slimmed by ~400 lines; provenance methods become thin
  delegates to `self.provenance`
- `storage/provenance_utils.py` removed (absorbed into `ProvenanceStore`)

#### Motivation

`ArtifactStore` was becoming a god object. Provenance graph queries are a
distinct responsibility that Filter, InteractiveFilter, and the new traversal
module all depend on. A dedicated store makes these dependencies explicit.

#### Testing

- `tests/artisan/storage/test_provenance_store.py` — 12 new tests
- Existing `test_artifact_store.py` and `test_commit.py` updated

---

### Creator executor refactor

#### Summary

The monolithic `run_creator_flow()` is split into an inner lifecycle function
and an outer error-handling wrapper, enabling reuse by the composite executor.

#### Changes

- `run_creator_lifecycle()`: inner lifecycle (setup → preprocess → execute →
  postprocess → lineage) that raises on failure and returns a `LifecycleResult`
- `run_creator_flow()`: outer wrapper that catches exceptions, records
  success/failure to Delta, manages sandbox cleanup
- Accepts optional `sources: dict[str, ArtifactSource]` for composite
  executor in-memory artifact passing
- Helper exceptions `_PostprocessFailure` and `_ExecuteFailure` carry
  structured error context

#### Motivation

The composite executor needs to reuse the creator lifecycle without duplicating
the setup/preprocess/execute/postprocess/lineage sequence. Extracting the
lifecycle also improves testability.

#### Testing

- `tests/artisan/execution/test_executor_creator.py` — updated with new tests

---

### Lineage matching rewrite

#### Summary

`match_by_ancestry()` rewritten from dict-based BFS to DataFrame-based
approach using the new provenance traversal module.

#### Changes

- `_collect_ancestors()` helper uses iterative backward joins instead of
  per-candidate BFS
- `walk_provenance_to_targets` replaced by re-export of
  `provenance.traversal.walk_backward`
- `walk_forward_to_targets` re-exports `walk_forward`
- Input grouping updated to use DataFrame-based provenance edges

#### Motivation

DataFrame-based matching scales better for large provenance graphs and aligns
with the new traversal module.

#### Testing

- `tests/artisan/execution/test_lineage_matching.py` — rewritten
- `tests/artisan/execution/test_grouping.py` — updated

---

## Bug fixes

- **Chain staging path mismatch**: Fixed incorrect staging paths when
  operations were composed via the chain API, causing artifacts to be written
  to wrong locations
- **Graphviz plugin auto-registration**: Added `scripts/pixi/activate.sh`
  that runs `dot -c` on first activation to register Graphviz plugins,
  preventing "no usable render plugin" errors
- **Metric type preservation**: Fixed metric types being lost through
  tidy/wide DataFrame pipeline transformations; added `encode_metric_value()`
  and `is_scalar_metric()` helpers with `pivot_metrics_wide()` for
  type-preserving pivoting

---

## Infrastructure

- **Prefect server**: Rewritten to delegate to `prefect_submitit.server` for
  URL resolution and health checking; clearer error messages with remediation
  instructions
- **Prefect scripts removed**: `scripts/prefect/` deleted (3 shell scripts,
  ~800 lines) — replaced by `prefect-server` CLI from prefect-submitit
- **Pixi tasks**: Updated to use `prefect-server` CLI; added `build-dist`,
  `check-dist`, `upload-testpypi`, `upload-pypi` packaging tasks
- **Logging redesign**: Rich-based console handler with colorized output;
  `configure_logging()` gains `logs_root` for optional rotating file handler
- **Pipeline input validation**: New `_validate_required_inputs()` and
  `_validate_input_types()` catch missing/mismatched inputs at submit time
  rather than during execution
- **ResourceConfig**: Made portable with backend-agnostic field names
- **Wait operation**: New test/demo operation for cancellation and scheduling
  testing

---

## Documentation

### Full accuracy audit

Every existing documentation page (concepts, how-to guides, tutorials) was
audited against the codebase and updated. Pages with 100+ lines of changes:

- All 10 concept pages updated (architecture, design principles, execution
  flow, provenance, operations model, storage, error handling, artifacts)
- All 8 how-to guides updated (building pipelines, configuring execution,
  creating artifacts, exporting results, inspecting provenance, writing
  creators, writing curators)
- All 3 contributing pages updated (coding conventions, tooling decisions,
  writing docs)
- Getting-started pages updated (installation, first pipeline, orientation)
- Reference glossary expanded significantly (+376 lines)

### New documentation

- **Concept page**: composites and composition
- **How-to guide**: writing composite operations
- **Reference page**: CompositeDefinition API reference

### Tutorial restructuring

The tutorial tree was reorganized from 4 sections to 5 for clearer separation
of concerns:

| Old | New |
|-----|-----|
| getting-started/ (5 notebooks) | getting-started/ (2 notebooks, trimmed) |
| pipeline-patterns/ (6 notebooks) | pipeline-design/ (6 notebooks) |
| working-with-results/ (4 notebooks) | execution/ (8 notebooks) |
| execution-and-tuning/ (2 notebooks) | analysis/ (4 notebooks) |
| — | writing-operations/ (2 notebooks) |

New tutorials:
- Pipeline cancellation (`execution/08-pipeline-cancellation`)
- Storage and logging (`execution/05-storage-and-logging`)
- Composites (`pipeline-design/06-composites`)
- Lineage tracing (`analysis/02-lineage-tracing`)
- Writing a composite (`writing-operations/02-writing-a-composite`)
- Provenance graphs rewritten from scratch (`analysis/01-provenance-graphs`)

### Claude Code skills

New `skills/` directory with 4 AI-assisted scaffolding skills:
- `write-operation` — scaffold an OperationDefinition subclass
- `write-pipeline` — scaffold a pipeline script
- `write-composite` — scaffold a CompositeDefinition subclass
- `write-docs` — write or edit documentation pages

### README

Rewritten with sharper project description ("protocol not platform" framing),
updated quick example to use `pipeline.output()` / named-step API, and new
Claude Code integration section.

---

## Testing

- **1,841 unit tests** passing (53 deselected as `@slow`)
- **~358 new or changed test functions** across 31 new and 41 modified test
  files
- **40+ new integration tests** covering composites, cancellation, filter,
  cache policies, cross-pipeline sharing, error handling, step overrides,
  topology gaps, and multi-input patterns
- **New shared test fixtures** (`tests/fixtures/`) for CSV generation

- [x] Existing tests pass (`pixi run -e dev test`)
- [x] New/updated tests cover changed behavior
- [x] Linting passes (`pixi run -e dev fmt && git diff --exit-code`)

## Checklist

- [x] I have read the [Contributing Guide](../CONTRIBUTING.md)
- [x] Documentation is updated (if applicable)
