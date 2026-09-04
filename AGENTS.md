# AGENTS.md

Project conventions for contributors (human and AI).

---

## Environment

| Setting     | Value                       |
| ----------- | --------------------------- |
| Environment | Pixi (`~/.pixi/bin/pixi`)   |
| Formatting  | Ruff                        |
| Testing     | Pytest                      |
| Docs        | Jupyter Book 2 (MyST)       |

IMPORTANT: Always use the full pixi path (`~/.pixi/bin/pixi`) when running
commands. The short `pixi` form is for user-facing docs only.

### Pixi lock discipline

Use `--locked` for routine installs and tasks so Pixi stops instead of silently
rewriting `pixi.lock`. Run an unlocked Pixi command only when intentionally
changing dependencies, and commit `pyproject.toml` and `pixi.lock` together.

If `pixi.lock` becomes dirty unexpectedly, inspect the diff. When the only
changes are the editable project's Git-derived `version` and `sha256`, and no
dependency change was intended, restore `pixi.lock`. Do not discard it when
`pyproject.toml` or dependencies were intentionally changed.

---

## Commands

```bash
~/.pixi/bin/pixi install --locked
~/.pixi/bin/pixi run --locked -e dev test
~/.pixi/bin/pixi run --locked -e dev test-unit
~/.pixi/bin/pixi run --locked -e dev test-integration
~/.pixi/bin/pixi run --locked -e dev test-s3
~/.pixi/bin/pixi run --locked -e dev test-notebook
~/.pixi/bin/pixi run --locked -e dev test-notebook-modal
~/.pixi/bin/pixi run --locked -e dev test-seq
~/.pixi/bin/pixi run --locked -e dev fmt
~/.pixi/bin/pixi run --locked -e docs docs-build
~/.pixi/bin/pixi run --locked python script.py
```

---

## Code Style

- **DRY, YAGNI, KISS** — no premature abstractions
- **No backwards-compat shims** — when removing/renaming, delete completely
- **Fail fast** — validate inputs early, raise clear exceptions
- **Type hints** on all function signatures
- **Comments for "why"**, not "what"
- **Functions < 30 lines** ideally
- **Specific exceptions** — no bare `except:`
- **Google-style docstrings:**

```python
def example(param1: str, param2: int = 0) -> bool:
    """Short description.

    Args:
        param1: Description.
        param2: Description. Defaults to 0.

    Returns:
        Description.

    Raises:
        ValueError: When param1 is empty.
    """
```

---

## Testing

Tests mirror source structure: `tests/artisan/{module}/`

- Files: `test_<module>.py`
- Functions: `test_<function>_<scenario>`
- Cover: happy path, edge cases, error conditions
- Markers separate test type from resource needs:
  - `@pytest.mark.integration` — end-to-end pipeline tests (live in `tests/integration/`)
  - `@pytest.mark.s3` — needs MinIO/S3 (auto-applied to `s3_fs` users; `[local, s3]` backend params mark the s3 param explicitly)
- Integration and s3 tests run in parallel via pytest-xdist

---

## Git Conventions

### Branch Naming

```
feat/[name]      fix/[name]       refactor/[name]
docs/[name]      test/[name]      chore/[name]
```

### Commit Format

```
type: Brief description

```

Types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `perf`, `style`

### PR Validation Order

1. `~/.pixi/bin/pixi run --locked -e dev fmt`
2. `~/.pixi/bin/pixi run --locked -e dev test-unit`
3. `~/.pixi/bin/pixi run --locked -e dev test-integration`
4. `~/.pixi/bin/pixi run --locked -e dev test-s3`
5. `~/.pixi/bin/pixi run --locked -e dev test-notebook`
6. `~/.pixi/bin/pixi run --locked -e docs docs-build`

---

## Architecture

```
src/artisan/                # Framework (domain-agnostic)
├── composites/             # Composite operations (reusable op compositions)
│   └── base/               # CompositeDefinition base class, context, runtime results
├── execution/              # Worker execution
│   ├── compute/            # Compute-provider routing (local passthrough, tool-endpoint invoke)
│   ├── context/            # Execution context builder + sandbox setup
│   ├── executors/          # Creator/curator phase executors
│   ├── inputs/             # Input grouping, instantiation, lineage matching, materialization
│   ├── lineage/            # Lineage capture, enrichment, name derivation, validation
│   ├── models/             # Execution unit + artifact source models
│   ├── recording/          # Execution-record recorder + parquet writer
│   ├── tool_endpoint/      # Operation-as-tool endpoint (client, server, deploy, docker)
│   ├── transport/          # Transport log constants
│   ├── exceptions.py       # Execution-phase exceptions
│   └── utils.py            # Execution helpers (run-id generation, artifact finalization)
├── operations/             # Base class + framework ops
│   ├── base/               # OperationDefinition base class
│   ├── curator/            # Curator ops (Filter, Merge, Ingest*, DeclareLineage, InteractiveFilter)
│   └── examples/           # Example/demo operations (DataGenerator, DataTransformer, MetricCalculator)
├── orchestration/          # Pipeline engine, dispatch, step execution
│   ├── engine/             # Batching, dispatch, step executor/tracker, lifecycle router
│   ├── runners/            # Native local runner and public provider contract
│   ├── pipeline_manager.py # PipelineManager orchestration entry point
│   ├── runner_api.py       # Stable API for external runner providers
│   ├── run_history.py      # Run-history aggregation reader
│   └── step_future.py      # Step future handle
├── provenance/             # Domain-agnostic provenance traversal (Polars BFS)
├── registry/               # Operation discovery and registration
├── schemas/                # All data models
│   ├── artifact/           # Artifact base, registry, types, metric, file_ref, data, execution_config
│   ├── composites/         # CompositeRef dataclass (runtime results in composites/base/results.py)
│   ├── execution/          # Execution context, record, results, runtime environment, storage config
│   ├── operation_config/   # Command/resource/environment configuration schemas
│   ├── orchestration/      # Pipeline/step config + result schemas
│   ├── provenance/         # Provenance edge + lineage-mapping schemas
│   ├── specs/              # Input/output spec schemas
│   └── enums.py            # Framework enums (CachePolicy, FailurePolicy, GroupByStrategy, TablePath)
├── storage/                # Artifact storage and persistence
│   ├── cache/              # Cache lookup
│   ├── core/               # Artifact store, provenance store, table schemas
│   └── io/                 # Commit, staging, staging verification
├── utils/                  # Hashing, paths, filenames, external tools, dotenv, logging
├── visualization/          # Provenance graphs and analytics
│   ├── graph/              # Micro/macro provenance rendering (Graphviz) + interactive stepper
│   ├── inspect.py          # Pipeline/step/metric/data/failure inspection readers
│   └── timing.py           # Timing analytics
├── cli.py                  # CLI entry point (execute_as_tool op runner, schema export)
└── errors.py               # ArtisanError base + domain exception hierarchy
```

---

## Documentation

```
docs/
├── getting-started/             # Installation, first pipeline, core concepts
├── tutorials/                   # Interactive notebooks (Diataxis)
│   ├── 01-getting-started/      # First pipeline, exploring results
│   ├── 02-pipeline-design/      # Sources, branching, filtering, multi-input, diamonds, composites
│   ├── 03-caching/              # Resume, cache lookup, force re-run
│   ├── 04-batching/             # Two-level batching, per-artifact dispatch
│   ├── 05-errors-and-control/   # Step overrides, error visibility, pipeline cancellation
│   ├── 06-storage/              # Layout, logging, external files
│   ├── 07-compute-backends/     # Compute routing, external runners, Modal
│   ├── 08-analysis/             # Provenance graphs, interactive filter, timing
│   └── 09-writing-operations/   # Writing operations and composites
├── concepts/                    # Architecture, design principles, provenance, execution flow
├── how-to-guides/               # Writing operations, configuring execution, provenance
├── reference/                   # Glossary, comparison to alternatives
└── contributing/                # Writing docs, coding conventions, tooling decisions
```

---

## Pre-PR Checklist

- [ ] Code: no debug prints, no commented-out code
- [ ] Tests pass (`~/.pixi/bin/pixi run --locked -e dev test`), new code has tests
- [ ] Formatted and linted (`~/.pixi/bin/pixi run --locked -e dev fmt`)
- [ ] Docs build (`~/.pixi/bin/pixi run --locked -e docs docs-build`)
- [ ] Commits are atomic with proper messages
- [ ] Self-reviewed all changes

---

## Personal Overrides

Personal agent settings are tool-specific and must remain gitignored. Claude
Code users may use `CLAUDE.local.md`, typically symlinked into the personal
notes repo under `_dev/`. See the project README for details.
