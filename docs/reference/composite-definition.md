# CompositeDefinition Reference

API reference for the composite system. For conceptual background, see
[Composites and Composition](../concepts/composites-and-composition.md).
For a step-by-step guide, see
[Writing Composite Operations](../how-to-guides/writing-composite-operations.md).

**Source:** `src/artisan/composites/base/composite_definition.py`

---

## CompositeDefinition

`artisan.composites.base.composite_definition.CompositeDefinition`

Base class for composite operations. Subclasses declare inputs, outputs,
and a `compose()` method that wires internal operations together.

### Class variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `name` | `str` | `""` | Composite name. Empty means abstract (skips validation and registration) |
| `description` | `str` | `""` | Human-readable description |
| `inputs` | `dict[str, InputSpec]` | `{}` | Declared input roles |
| `outputs` | `dict[str, OutputSpec]` | `{}` | Declared output roles |

### Instance fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `resources` | `ResourceConfig` | `ResourceConfig()` | Worker resource allocation (collapsed mode) |
| `execution` | `ExecutionConfig` | `ExecutionConfig()` | Batching and scheduling config (collapsed mode) |

### Inner classes

| Class | Base | Purpose |
|-------|------|---------|
| `InputRole` | `StrEnum` | Enum whose values match `inputs` dict keys. Required when `inputs` is non-empty |
| `OutputRole` | `StrEnum` | Enum whose values match `outputs` dict keys. Required when `outputs` is non-empty |
| `Params` | `BaseModel` | Optional Pydantic model for composite-level parameters |

### Methods

#### `compose(ctx: CompositeContext) -> None`

Wire internal operations together. Override this in every concrete
subclass.

**Args:**
- `ctx` — `CompositeContext` providing `input()`, `run()`, and `output()`

**Raises:** `NotImplementedError` if not overridden.

#### `get(name: str) -> type[CompositeDefinition]` *(classmethod)*

Look up a registered composite by name.

**Raises:** `KeyError` if the name is not registered.

#### `get_all() -> dict[str, type[CompositeDefinition]]` *(classmethod)*

Return a copy of the composite registry.

### Subclass validation

When a concrete subclass (non-empty `name`) is defined, the framework
validates at class definition time:

- `compose()` must be overridden
- `OutputRole` enum values must match `outputs` keys
- `InputRole` enum values must match `inputs` keys (when inputs exist)

Violations raise `TypeError` at import time.

### Skeleton

```python
from __future__ import annotations

from enum import StrEnum
from typing import ClassVar

from pydantic import BaseModel, Field

from artisan.composites import CompositeDefinition, CompositeContext
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class MyComposite(CompositeDefinition):
    name = "my_composite"
    description = "Short description."

    class InputRole(StrEnum):
        DATA = "data"

    class OutputRole(StrEnum):
        RESULT = "result"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATA: InputSpec(artifact_type="data", required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.RESULT: OutputSpec(artifact_type="metric"),
    }

    class Params(BaseModel):
        threshold: float = Field(default=0.5, ge=0.0)

    params: Params = Params()

    def compose(self, ctx: CompositeContext) -> None:
        step_a = ctx.run(OpA, inputs={"data": ctx.input("data")})
        step_b = ctx.run(OpB, inputs={"data": step_a.output("result")},
                         params={"threshold": self.params.threshold})
        ctx.output("result", step_b.output("result"))
```

---

## CompositeContext

`artisan.composites.base.composite_context.CompositeContext`

Abstract base class for composite execution contexts. Two concrete
implementations exist: `CollapsedCompositeContext` (single-worker,
in-memory) and `ExpandedCompositeContext` (separate pipeline steps).

### Methods

#### `input(role: str) -> CompositeRef`

Reference a declared input of this composite.

**Args:**
- `role` — input role name (must match a key in `inputs`)

**Returns:** `CompositeRef` backed by the resolved input source.

**Raises:** `ValueError` if role is not a declared input.

#### `run(operation, *, inputs=None, params=None, resources=None, execution=None, backend=None, environment=None, tool=None) -> CompositeStepHandle`

Execute an operation or nested composite.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `operation` | `type` | — | `OperationDefinition` or `CompositeDefinition` subclass |
| `inputs` | `dict[str, Any] \| None` | `None` | Input wiring as `{role: CompositeRef}` |
| `params` | `dict[str, Any] \| None` | `None` | Parameter overrides |
| `resources` | `dict[str, Any] \| None` | `None` | Resource overrides (expanded mode only) |
| `execution` | `dict[str, Any] \| None` | `None` | Execution overrides (expanded mode only) |
| `backend` | `str \| BackendBase \| None` | `None` | Backend override (expanded mode only) |
| `environment` | `str \| dict[str, Any] \| None` | `None` | Environment override |
| `tool` | `dict[str, Any] \| None` | `None` | Tool overrides |

**Returns:** `CompositeStepHandle` wrapping the operation's results.

In collapsed mode, `resources`, `execution`, and `backend` are ignored
(logged as debug). In expanded mode, they are forwarded to the parent
pipeline's `submit()`.

#### `output(role: str, ref: CompositeRef) -> None`

Map an internal result to a declared output of this composite.

**Args:**
- `role` — composite output role name (must match a key in `outputs`)
- `ref` — `CompositeRef` from an internal `ctx.run().output()`

**Raises:** `ValueError` if role is not a declared output.

---

## CompositeStepHandle

`artisan.schemas.composites.composite_ref.CompositeStepHandle`

Handle returned by `ctx.run()`. Wraps either in-memory artifacts
(collapsed mode) or a `StepFuture` (expanded mode).

### Methods

#### `output(role: str) -> CompositeRef`

Reference an output role of this internal operation.

**Args:**
- `role` — output role name of the operation that was run

**Returns:** `CompositeRef` for wiring to downstream `ctx.run()` calls
or to `ctx.output()`.

**Raises:**
- `ValueError` if role is not a valid output of the operation.
- `KeyError` if role has no artifacts in collapsed mode.

---

## CompositeRef

`artisan.schemas.composites.composite_ref.CompositeRef`

Frozen dataclass. A lightweight reference used as input wiring between
internal operations. Either `source` or `output_reference` is set,
never both.

| Field | Type | Description |
|-------|------|-------------|
| `source` | `ArtifactSource \| None` | In-memory artifact source (collapsed mode) |
| `output_reference` | `OutputReference \| None` | Lazy pipeline reference (expanded mode) |
| `role` | `str` | Output role name this ref points to |

---

## CompositeIntermediates

`artisan.execution.models.execution_composite.CompositeIntermediates`

`StrEnum` controlling intermediate artifact handling in collapsed mode.

| Value | Behavior |
|-------|----------|
| `DISCARD` | Default. Intermediates discarded after composite completes |
| `PERSIST` | Intermediates committed to Delta Lake with `step_boundary=False` |
| `EXPOSE` | Intermediates committed to Delta Lake with `step_boundary=True` |

Pass as `intermediates=` to `pipeline.run()`:

```python
pipeline.run(
    operation=MyComposite,
    inputs={"data": output("gen", "datasets")},
    intermediates="persist",
)
```

---

## ExpandedCompositeResult

`artisan.schemas.composites.composite_ref.ExpandedCompositeResult`

Returned by `pipeline.expand()`. Maps composite outputs to internal
pipeline steps. Duck-types with `StepResult` and `StepFuture`.

### Methods

#### `output(role: str) -> OutputReference`

Get the `OutputReference` for a composite output role.

**Args:**
- `role` — composite output role name

**Returns:** `OutputReference` pointing at the internal step that
produces it.

**Raises:** `ValueError` if role is not a declared output.

---

## See also

- [Composites and Composition](../concepts/composites-and-composition.md) — conceptual overview
- [Writing Composite Operations](../how-to-guides/writing-composite-operations.md) — step-by-step guide
- [Composable Operations Tutorial](../tutorials/pipeline-design/06-composable-operations.ipynb) — interactive examples
- [Glossary](glossary.md) — key terms
