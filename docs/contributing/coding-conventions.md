# Coding Conventions

Standards and patterns for contributing code to the Artisan framework.

---

## Directory Naming

- All directories use **lowercase snake_case**
- Directory names are **nouns describing a responsibility**, not verbs:

```
execution/context/    # not "building_context"
execution/lineage/    # not "track_lineage"
storage/cache/        # not "caching"
```

## File Naming

- All files are `snake_case.py`
- **One primary class per file**, file named after the class:

```
storage/core/artifact_store.py  → ArtifactStore
schemas/specs/input_spec.py     → InputSpec
operations/curator/filter.py    → Filter
```

### Tightly coupled classes share a file

When classes form a logical unit (variants, a type hierarchy, or a union),
they go in one file:

```
operations/tool_a/tool_a.py
  → ToolA (Params nested inside the class)

schemas/operation_config/command_spec.py
  → CommandSpec, ApptainerCommandSpec, DockerCommandSpec, LocalCommandSpec

schemas/execution/curator_result.py
  → ArtifactResult, PassthroughResult, CuratorResult
```

The rule: **if the classes only make sense together, keep them together.**

### Reserved file names

| Name | Purpose | Example |
|------|---------|---------|
| `base.py` | Base class for the package | `schemas/artifact/base.py` → `Artifact` |
| `utils.py` | Helper functions for the package | `operations/tool_a/utils.py` |
| `shared.py` | Code shared between variant operations | `operations/tool_c/shared.py` |
| `config.py` | Config generation operation | `operations/tool_b/config.py` → `ToolBConfig` |
| `constants.py` | Package-level constants | `orchestration/engine/constants.py` |
| `exceptions.py` | Package-level exception classes | `execution/exceptions.py` |
| `enums.py` | Enum definitions (one file for all enums) | `schemas/enums.py` |
| `common.py` | Shared types within a sub-package | `schemas/artifact/common.py` |

## Flat vs. Nested Packages

This is the most important structural decision.

**Use flat** when:
- Files are **independent peers** — each stands alone, none depend on siblings
- Files share a package only because they share a **category** (all metrics, all
  curator ops)
- The package has **~6 or fewer files**

```
operations/metrics/
├── __init__.py
├── component_interface.py
├── sample_interface.py
└── summary_metrics.py
```

**Use nested** when:
- Files need to be **grouped by sub-responsibility** (lineage has builder +
  capture + enrich + validation)
- A group of 2+ files forms a **cohesive unit** with internal dependencies
- The flat alternative would put **10+ files** in one directory

```
execution/
├── __init__.py
├── exceptions.py        ← package-level, used across sub-packages
├── utils.py             ← package-level, used across sub-packages
├── context/
├── executors/
├── inputs/
├── lineage/
├── models/
└── staging/
```

**The heuristic:** If you can describe the package as "a collection of
independent X", keep it flat. If you need sub-groups to explain what's inside,
nest it.

### Package-level files in nested packages

Nested packages can have files at the root level for cross-cutting concerns
(`exceptions.py`, `utils.py`). These are for things that don't belong to any
one sub-package but are used across several.

## The `__init__.py` Contract

Every package's `__init__.py` must contain three things:

1. **Docstring** — what this package provides
2. **Re-exports** — import public symbols from internal modules
3. **`__all__`** — explicit list of public symbols

```python
"""Curator operations for pipeline execution.

This package contains curator operations that support pipeline construction:
    - Filter: Conditional artifact routing with structured criteria
    - Merge: Union multiple datastreams into a single stream
    - IngestFiles: Abstract base for file ingestion operations
    - IngestPipelineStep: Import artifacts from another pipeline's Delta Lake store
"""
from __future__ import annotations

from artisan.operations.curator.filter import Filter
from artisan.operations.curator.ingest_files import IngestFiles
from artisan.operations.curator.ingest_pipeline_step import IngestPipelineStep
from artisan.operations.curator.merge import Merge

__all__ = ["Filter", "IngestFiles", "IngestPipelineStep", "Merge"]
```

### Re-export depth

| Package level | Re-exports | Example |
|---------------|------------|---------|
| Leaf | Own public classes | `operations/tool_a/` re-exports `ToolA` |
| Mid-level hub | Everything from all children | `schemas/` re-exports all artifacts, specs, configs |
| Root (`artisan/`) | Top-level public API | Users import from sub-packages or root |

The mid-level hub pattern means users can write
`from artisan.schemas import DataArtifact` instead of reaching into
`schemas.artifact.data`. When adding a new type, you must add re-exports
to both the leaf `__init__.py` and the hub `__init__.py`.

## Domain Operation Package Structure

Each external tool gets its own package under `operations/`. The internal
structure scales with complexity:

| Scenario | Structure |
|----------|-----------|
| 1 operation, no heavy utils | Main file only (operation + params in one file) |
| 1 operation, heavy utils (100+ lines) | Main file + `utils.py` |
| 2+ operations sharing logic | One file per operation + `shared.py` + optional `util.py` |
| Operation + config generator | Separate files: `my_op.py` + `config.py` |

Naming pattern for variants:
- `tool_c.py` — the simple/base variant uses the tool name
- `conditional.py`, `unconditional.py` — named by what makes them different
- `conditional_config.py` — config generators get `_config` suffix

See [Writing Creator Operations](../how-to-guides/writing-creator-operations.md#real-world-package-examples)
for concrete examples (ToolA, ToolC, ToolB).

## Schema Sub-packages

Schemas are grouped by **what domain they describe**, not by how they are
consumed:

| Sub-package | Question it answers | Content |
|-------------|-------------------|---------|
| `artifact/` | "What is the data?" | Artifact type definitions |
| `execution/` | "How did it run?" | Runtime state, records, results |
| `operation_config/` | "How should it run?" | Command specs, resource specs |
| `orchestration/` | "What's the plan?" | Pipeline config, step results |
| `provenance/` | "Where did it come from?" | Lineage mappings, edges |
| `specs/` | "What's the contract?" | Input/output specs, phase models |

Inside each sub-package, files are flat — one model per file. Schema files are
small (typically 20–170 lines).

## Execution Sub-packages

The `execution/` package is split into sub-packages by responsibility. Each
sub-package owns a distinct phase of the worker-side execution flow:

| Sub-package | Responsibility | Key modules |
|-------------|---------------|-------------|
| `executors/` | Orchestrate creator and curator flows end-to-end | `creator.py`, `curator.py` |
| `context/` | Build execution context, run identity, sandbox environment | `builder.py`, `sandbox.py` |
| `inputs/` | Instantiate and materialize artifacts for execution | `instantiation.py`, `materialization.py` |
| `lineage/` | Capture lineage, build provenance edges, validate completeness | `builder.py`, `capture.py`, `enrich.py`, `validation.py` |
| `staging/` | Stage artifacts and metadata, record outcomes | `parquet_writer.py`, `recorder.py` |

## Dependency Direction

A file's location in the package tree determines what it may import.
Dependencies flow strictly downward:

| Package | Can import from |
|---------|----------------|
| `utils/` | stdlib, third-party only |
| `schemas/` | `utils/`, other schemas |
| `operations/` | `schemas/`, `utils/` |
| `execution/` | `operations/`, `schemas/`, `utils/`, `storage/` |
| `storage/` | `schemas/`, `utils/` |
| `orchestration/` | All lower layers |

See [Architecture Overview](../concepts/architecture-overview.md#dependency-direction)
for the full diagram.

### Import boundaries

Import from a package's re-exports (`__init__.py`), not from internal modules.
Each package's `__all__` defines its public API — if a symbol isn't re-exported,
treat it as internal.

```python
# Good — import from the package boundary
from artisan.schemas import DataArtifact
from artisan.operations.curator import Filter
from artisan.execution.executors import run_creator_flow

# Bad — reaching past the boundary into internal modules
from artisan.schemas.artifact.data import DataArtifact
from artisan.operations.curator.filter import Filter
from artisan.execution.lineage.capture import _build_edges
```

For nested packages, import from the sub-package that owns the responsibility
rather than from deeper internal paths. Within a package, sibling imports are
fine.

## Where Does New Code Go?

| I'm adding... | Put it in | Structure |
|----------------|-----------|-----------|
| A new ML tool operation | `operations/<tool_name>/` | Package with `__init__.py`, main file, optional `utils.py` |
| A new metric computation | `operations/metrics/<name>.py` | Single file, flat alongside siblings |
| A new curator operation | `operations/curator/<name>.py` | Single file, flat alongside siblings |
| A new artifact type | `schemas/artifact/<name>.py` | Single file + re-exports in `artifact/__init__.py` and `schemas/__init__.py` |
| A new enum value | `schemas/enums.py` | Add to existing enum class |
| A new enum type | `schemas/enums.py` | New class in same file |
| A new execution concern | `execution/<existing_subpackage>/` | File in the sub-package that owns the responsibility |
| A shared utility function | `utils/<topic>.py` | New file or add to existing file by topic, add re-export |
| A helper used by one operation only | `operations/<tool>/utils.py` | Inside the operation's own package |

## Naming

- **Classes**: `PascalCase` — `ArtifactStore`, `ExecutionUnit`, `DataTransformer`
- **Functions/methods**: `snake_case` — `compute_artifact_id`, `run_external_command`
- **Constants**: `UPPER_SNAKE_CASE` — `DEFAULT_BATCH_SIZE`
- **Test functions**: `test_<function>_<scenario>` — `test_filter_empty_input`

## Type Hints

Type hints are required on all function signatures:

```python
def compute_artifact_id(content: bytes, artifact_type: str) -> str:
    ...
```

## Operations

- Operations declare `inputs` and `outputs` as `ClassVar` with explicit
  `InputSpec` / `OutputSpec`
- Parameters are validated via Pydantic fields with type annotations
- The `name` class variable must match the class name in snake_case
- Creator operations implement the three-phase lifecycle:
  `preprocess` → `execute` → `postprocess`
- Curator operations implement `execute_curator`

## Error Handling

General rules:

- **Fail fast** — validate inputs early, raise clear exceptions
- Use **specific exception types** — no bare `except:`
- Include context in error messages (artifact IDs, step numbers, operation names)

For the design rationale behind these patterns, see
[Error Handling Concepts](../concepts/error-handling.md).

### The worker catch pattern

Every worker entry point (`run_creator_flow`, `run_curator_flow`) follows
the same structure: minimal setup outside the try/except (just enough to create
the `execution_context`), then everything else inside it:

```python
def run_creator_flow(unit, runtime_env, worker_id=0) -> StagingResult:
    execution_context = build_execution_context(...)

    try:
        # Everything that can fail:
        # - input instantiation and materialization
        # - operation execution (preprocess -> execute -> postprocess)
        # - output validation and lineage
        # - staging results to parquet
        ...
        return record_execution_success(...)

    except Exception as exc:
        logger.exception("Creator flow failed: %s", execution_context.execution_run_id)
        return record_execution_failure(
            execution_context=execution_context,
            hydrated_inputs=inputs,
            error=f"{type(exc).__name__}: {exc}",
        )
    finally:
        # Cleanup (sandbox deletion, etc.)
        ...
```

If even the setup fails, the dispatch layer (Layer 2) catches it.

### The dispatch catch pattern

The dispatch layer wraps each worker call so the task never fails:

```python
@task
def execute_unit_task(unit, runtime_env, worker_id=None):
    try:
        result = run_creator_flow(unit, runtime_env, worker_id=worker_id)
        return {"success": result.success, "error": result.error, ...}
    except Exception as exc:
        # Layer 1 leaked -- catch it here
        logger.exception("execute_unit_task failed: %s", unit.execution_spec_id)
        return {"success": False, "error": f"{type(exc).__name__}: {exc}", ...}
```

### The futures collection pattern

When collecting results from parallel workers, never let one failure discard
the others:

```python
# Bad: one failure loses everything
return [f.result() for f in futures]

# Good: each future handled independently
results = []
for f in futures:
    try:
        results.append(f.result())
    except Exception as exc:
        logger.exception("Future failed")
        results.append({"success": False, "error": f"{type(exc).__name__}: {exc}", ...})
return results
```

### The double-fault pattern

When the failure-recording mechanism itself fails, preserve the original
error:

```python
def record_execution_failure(error, ...) -> StagingResult:
    try:
        # Normal path: write failure to parquet
        _stage_execution(success=False, error=error, ...)
        return StagingResult(success=False, error=error, ...)
    except Exception as staging_exc:
        # Staging broke -- return a structured result with the original error
        logger.exception("Failed to stage failure record (original: %s)", error)
        return StagingResult(
            success=False,
            error=f"[staging failed: {staging_exc}] Original: {error}",
            staging_path=None,
            ...
        )
```

The priority order: (1) return a structured result, (2) preserve the original
error message, (3) persist to disk. If (3) fails, (1) and (2) still succeed.

### The batch continuation pattern

In batched execution (SLURM `units_per_worker > 1`), one item failure must
not kill the batch:

```python
results = []
for i, item in enumerate(batch_items):
    try:
        result = execute(item)
        results.append(result)
    except Exception as e:
        logger.error("Item %s/%s failed: %s", i + 1, len(batch_items), e)
        results.append({"success": False, "error": f"{type(e).__name__}: {e}", ...})
# All items processed, all results collected
return results
```

### Guidance for operation authors

Operation authors (people writing `OperationDefinition` subclasses) do not
need to implement any of the patterns above. The framework handles all error
conversion and recording. Operations should:

- **Raise exceptions normally** when something goes wrong. The framework
  catches them.
- **Return `ArtifactResult(success=False, error="...")`** for expected,
  recoverable failures (e.g., "no valid output produced"). This avoids the
  overhead of exception handling for anticipated outcomes.
- **Not catch-and-swallow exceptions.** If an operation catches an exception,
  it should either handle it meaningfully or re-raise. Silently ignoring
  errors defeats the recording system.

## Code Style

- **Functions < 30 lines** ideally
- **Comments explain "why"**, not "what"
- **DRY, YAGNI, KISS** — no premature abstractions
- **No backwards-compat shims** — when removing or renaming, delete completely
- Formatting enforced by **Ruff** (`pixi run -e dev fmt`)

## Docstrings (Google Style)

```python
def get_artifact(
    self,
    artifact_id: str,
    artifact_type: str,
) -> Artifact:
    """Retrieve a single artifact by ID and type.

    Args:
        artifact_id: Content-addressed artifact identifier.
        artifact_type: Expected type ("data", "metric", etc.).

    Returns:
        The hydrated artifact with content loaded.

    Raises:
        KeyError: If the artifact does not exist.
        TypeError: If the artifact type does not match.
    """
```

## Testing

- Cover: happy path, edge cases, error conditions
- Use `@pytest.mark.slow` for integration tests
- Integration tests live in `tests/integration/` and run in parallel
- Test functions: `test_<function>_<scenario>`
- Tests mirror source structure: `tests/artisan/{module}/test_<file>.py`

## Imports

- Standard library first, then third-party, then local — enforced by Ruff
- Prefer explicit imports over `from module import *`
- Use relative imports within a package only when necessary

---

## Cross-References

- [Architecture Overview](../concepts/architecture-overview.md) — Package map and
  dependency direction
- [Writing Creator Operations](../how-to-guides/writing-creator-operations.md) —
  Operation development guide
