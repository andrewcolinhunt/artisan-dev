# CLAUDE.md

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

---

## Commands

```bash
pixi install                              # Install dependencies
pixi run -e dev test                      # Run all tests (unit seq + integration parallel)
pixi run -e dev test-unit                 # Run only unit tests
pixi run -e dev test-integration          # Run only integration tests (parallel)
pixi run -e dev test-seq                  # All tests sequentially (for debugging)
pixi run -e dev fmt                       # Format and lint
pixi run -e docs docs-build              # Build docs
pixi run python script.py                # Run scripts
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
- `@pytest.mark.slow` for integration tests
- Integration tests in `tests/integration/` run in parallel via pytest-xdist

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

Co-Authored-By: Claude <noreply@anthropic.com>
```

Types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `perf`, `style`

### PR Validation Order

1. `pixi run -e dev fmt`
2. `pixi run -e dev test-unit`
3. `pixi run -e dev test-integration`
4. `pixi run -e docs docs-build`

---

## Architecture

```
src/artisan/               # Framework (domain-agnostic)
├── execution/             # Worker execution (context, executors, inputs, lineage, models, staging)
├── operations/            # Base class + framework ops
│   ├── base/              # OperationDefinition base class
│   ├── curator/           # Curator ops (Filter, Merge, IngestFiles, IngestPipelineStep, IngestData)
│   └── examples/          # Example operations (DataGenerator, DataTransformer, MetricCalculator)
├── orchestration/         # Pipeline engine, dispatch, step execution
│   └── engine/            # Batching, dispatch, step executor
├── schemas/               # All data models
│   ├── artifact/          # Artifact base, registry, types, metric, file_ref, data, execution_config
│   ├── execution/         # Execution context, record, curator result
│   ├── operation_config/  # Command/resource configuration schemas
│   ├── orchestration/     # Pipeline/step result schemas
│   ├── provenance/        # Provenance edge schemas
│   └── specs/             # Input/output spec schemas
├── storage/               # Artifact storage and persistence
│   ├── cache/             # Cache lookup
│   ├── core/              # Artifact store + table schemas
│   └── io/                # Commit, staging, staging verification
├── utils/                 # Hashing, paths, filenames, external tools, logging
└── visualization/         # Provenance graphs and analytics
    ├── graph/             # Micro/macro provenance graph rendering (Graphviz)
    └── timing.py
```

---

## Documentation

```
docs/
├── getting-started/         # Installation, first pipeline, core concepts
├── tutorials/               # Interactive notebooks (Diataxis)
│   ├── getting-started/     # First pipeline, exploring results, run vs submit
│   ├── pipeline-patterns/   # Sources, branching, filtering, multi-input, advanced
│   ├── execution-and-tuning/ # Batching, step overrides
│   └── working-with-results/ # Provenance graphs, resume, filtering, timing
├── concepts/                # Architecture, design principles, provenance, execution flow
├── how-to-guides/           # Writing operations, configuring execution, provenance
├── reference/               # Glossary, comparison to alternatives
└── contributing/            # Writing docs, coding conventions, tooling decisions
```

---

## Pre-PR Checklist

- [ ] Code: no debug prints, no commented-out code
- [ ] Tests pass (`pixi run -e dev test`), new code has tests
- [ ] Formatted and linted (`pixi run -e dev fmt`)
- [ ] Docs build (`pixi run -e docs docs-build`)
- [ ] Commits are atomic with proper messages
- [ ] Self-reviewed all changes

---

## Personal Overrides

`CLAUDE.local.md` contains personal workflow overrides (base branch, push
behavior, repo layout).
