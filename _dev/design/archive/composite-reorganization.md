# Design: Composite Package Reorganization

**Date:** 2026-03-11  **Status:** Draft  **Author:** Claude + ach94

---

## Summary

Reorganize `src/artisan/composites/` from a flat, self-contained feature
module into a concern-separated structure that follows codebase conventions.
Move executors to `execution/executors/`, transport models to
`execution/models/`, and data models to `schemas/composites/`. Introduce a
`base/` subpackage inside `composites/` mirroring `operations/base/` for core
framework logic, leaving room for concrete composite implementations in
sibling subpackages.

---

## Problem

The composites package is organized as a feature module (everything about
composites in one place), but the codebase is organized by concern (schemas
here, executors there, models over there). This creates several
inconsistencies:

**Executor placement.** Creator and curator executors live in
`execution/executors/{creator,curator}.py`. The composite executor lives in
`composites/executor.py`. The dispatch router imports from two different
locations to get all three executors. Someone looking for "where do executors
live?" wouldn't find it.

**Transport model placement.** `ExecutionUnit` (the operation transport model)
lives in `execution/models/execution_unit.py`. Its composite counterpart
`ExecutionComposite` lives in `composites/execution_composite.py`. Same
structural role, different homes.

**Data models mixed with execution logic.** The codebase separates schemas
(`schemas/`) from execution logic (`execution/`). Composites bundles both:
`CompositeRef`, `CompositeStepHandle`, `ExpandedCompositeResult` are pure data
models sitting alongside `executor.py` and `composite_context.py` (heavy
execution logic). No other package does this.

**No room for concrete composites.** `operations/` has `base/` for the
framework and `curator/`, `examples/` for implementations. Composites has
everything flat — no separation between framework and user-facing code.

---

## Current Structure

```
src/artisan/composites/
    __init__.py
    composite_definition.py     # Base class + registry
    composite_context.py        # ABC + Collapsed/Expanded impls
    composite_ref.py            # CompositeRef, CompositeStepHandle, ExpandedCompositeResult
    execution_composite.py      # Transport model + CompositeIntermediates enum
    executor.py                 # run_composite() worker entry point
    provenance.py               # Ancestor map, shortcut edges, artifact/edge collection
```

---

## Target Structure

```
src/artisan/composites/
    __init__.py                 # Re-exports public API (no breaking changes)
    base/
        __init__.py
        composite_definition.py # CompositeDefinition base class + registry
        composite_context.py    # CompositeContext ABC + Collapsed/Expanded impls
        provenance.py           # Ancestor map, shortcut edges, artifact/edge collection

src/artisan/execution/
    executors/
        composite.py            # run_composite() — was composites/executor.py
    models/
        execution_composite.py  # ExecutionComposite + CompositeIntermediates — was composites/execution_composite.py

src/artisan/schemas/
    composites/
        __init__.py
        composite_ref.py        # CompositeRef, CompositeStepHandle, ExpandedCompositeResult
```

---

## Design Decisions

### `composites/base/` mirrors `operations/base/`

The `operations/` package has `base/` for `OperationDefinition` and related
framework code, with `curator/` and `examples/` as sibling subpackages for
concrete implementations. Composites follows the same pattern:

- `base/` holds `CompositeDefinition`, `CompositeContext`, and provenance logic
- Future composite implementations (analogous to `operations/examples/`) go in
  sibling subpackages

### Executor moves to `execution/executors/composite.py`

All executors in one place: `creator.py`, `curator.py`, `composite.py`. The
dispatch router imports from a single package. The file is renamed from
`executor.py` to `composite.py` for consistency with the existing naming
convention (file named after what it executes).

### Transport model moves to `execution/models/execution_composite.py`

`ExecutionComposite` is the composite equivalent of `ExecutionUnit`. Both are
pickle-serializable transport models sent to workers via Prefect dispatch.
They belong together in `execution/models/`.

`CompositeIntermediates` moves with the transport model. It's a
composite-specific enum used primarily by the transport model and executor.
Keeping it co-located avoids a stray enum in `schemas/enums.py` that only
one feature uses.

### Data models move to `schemas/composites/`

`CompositeRef`, `CompositeStepHandle`, and `ExpandedCompositeResult` are pure
data models (frozen dataclasses, no execution logic beyond validation). They
follow the same pattern as `schemas/artifact/`, `schemas/orchestration/`, etc.

### Public API preserved via `composites/__init__.py`

The top-level `from artisan.composites import CompositeDefinition` continues
to work. The `__init__.py` re-exports from the new locations. This means:

- Docs and user-facing code (`from artisan.composites import ...`) need no
  changes
- Only internal imports using full module paths need updating

### Provenance stays in `composites/base/`

`provenance.py` contains composite-specific provenance logic (ancestor maps,
shortcut edges, intermediates-based collection). It has no natural home in
`schemas/` (it's logic, not data) or `execution/` (it's composite-specific,
not general execution). Keeping it in `composites/base/` with the context
that uses it is the right call.

---

## Import Changes

### Internal cross-references (within moved files)

Files in `composites/base/` update their intra-package imports:

```python
# composite_context.py
from artisan.composites.composite_ref import ...
# becomes
from artisan.schemas.composites.composite_ref import ...

from artisan.composites.composite_definition import ...
# becomes
from artisan.composites.base.composite_definition import ...

from artisan.composites.execution_composite import ...
# becomes
from artisan.execution.models.execution_composite import ...
```

`executor.py` (now `execution/executors/composite.py`) updates:

```python
from artisan.composites.composite_context import CollapsedCompositeContext
# becomes
from artisan.composites.base.composite_context import CollapsedCompositeContext

from artisan.composites.execution_composite import ExecutionComposite
# becomes
from artisan.execution.models.execution_composite import ExecutionComposite

from artisan.composites.provenance import ...
# becomes
from artisan.composites.base.provenance import ...
```

### Framework consumers

`orchestration/pipeline_manager.py`, `orchestration/engine/dispatch.py`,
`orchestration/engine/step_executor.py` — update their deferred imports to
the new module paths.

### `composites/__init__.py` re-exports

```python
from artisan.composites.base.composite_context import (
    CollapsedCompositeContext,
    CompositeContext,
    ExpandedCompositeContext,
)
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.execution.models.execution_composite import (
    CompositeIntermediates,
    ExecutionComposite,
)
from artisan.schemas.composites.composite_ref import (
    CompositeRef,
    CompositeStepHandle,
    ExpandedCompositeResult,
)
```

### `execution/executors/__init__.py` updated

Adds `run_composite` to the public surface (or leaves the import deferred
in dispatch — matching the existing lazy-import pattern in `dispatch.py`).

### `execution/models/__init__.py` updated

Adds `ExecutionComposite` and `CompositeIntermediates`.

---

## Test Reorganization

Tests mirror source structure per project convention.

| Current | New | Reason |
|---|---|---|
| `tests/artisan/composites/test_composite_definition.py` | `tests/artisan/composites/test_composite_definition.py` | Source stayed in `composites/base/` |
| `tests/artisan/composites/test_composite_context.py` | `tests/artisan/composites/test_composite_context.py` | Source stayed in `composites/base/` |
| `tests/artisan/composites/test_composite_provenance.py` | `tests/artisan/composites/test_composite_provenance.py` | Source stayed in `composites/base/` |
| `tests/artisan/composites/test_composite_ref.py` | `tests/artisan/schemas/composites/test_composite_ref.py` | Source moved to `schemas/composites/` |
| `tests/artisan/composites/test_execution_composite.py` | `tests/artisan/execution/models/test_execution_composite.py` | Source moved to `execution/models/` |

Integration tests (`tests/integration/test_composite_*.py`,
`tests/integration/test_topology_gaps.py`) currently use internal module paths
(e.g., `from artisan.composites.composite_context import ...`). These imports
should be switched to the public API (`from artisan.composites import ...`) as
part of this work.

---

## Docs and Skills

Docs and the Claude skill use `from artisan.composites import ...` (the public
API) for executable imports — those need no changes.

However, `docs/reference/composite-definition.md` references internal module
paths as fully-qualified class locations (e.g.,
`` `artisan.composites.composite_definition.CompositeDefinition` ``,
`` `artisan.composites.composite_ref.CompositeStepHandle` ``). These should be
updated to reflect the new paths.

The design doc at `_dev/design/0_current/composites.md` should update its
"New Files" section to reflect the new layout.

---

## Migration Checklist

- [x] Create `composites/base/` with `__init__.py`
- [x] Move `composite_definition.py`, `composite_context.py`, `provenance.py`
      to `composites/base/`
- [x] Move `executor.py` to `execution/executors/composite.py`
- [x] Move `execution_composite.py` to `execution/models/execution_composite.py`
- [x] Create `schemas/composites/` with `__init__.py`
- [x] Move `composite_ref.py` to `schemas/composites/composite_ref.py`
- [x] Update `composites/__init__.py` to re-export from new locations
- [x] Update `execution/executors/__init__.py`
- [x] Update `execution/models/__init__.py`
- [x] Update all internal imports in moved files
- [x] Update imports in `pipeline_manager.py`, `dispatch.py`, `step_executor.py`
- [x] Update imports in unit tests
- [x] Move `test_composite_ref.py` to `tests/artisan/schemas/composites/`
- [x] Move `test_execution_composite.py` to `tests/artisan/execution/models/`
- [x] Update imports in moved test files
- [x] Switch integration test imports to public API (`from artisan.composites import ...`)
- [x] Update fully-qualified paths in `docs/reference/composite-definition.md`
- [x] Delete old files (verify `git mv` handled this)
- [x] Run `fmt`, `test-unit`, `test-integration`
- [x] Update composites design doc "New Files" section
