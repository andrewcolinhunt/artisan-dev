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
        step_b = ctx.run(
            OpB,
            inputs={"data": step_a.output("result")},
            params={"threshold": self.params.threshold},
        )
        ctx.output("result", step_b.output("result"))
```

---

## Pipeline methods

`artisan.orchestration.pipeline_manager.PipelineManager`

Composites run through `submit_composite` (non-blocking) and
`run_composite` (blocking). Each internal `ctx.run()` becomes a real
pipeline step. Composite-level override kwargs act as defaults for every
child step; a per-op `ctx.run()` value wins for the knob it sets.

#### `submit_composite(...) -> CompositeResult`

```python
def submit_composite(
    composite: type[CompositeDefinition],
    *,
    inputs: dict[str, OutputReference | list[str]] | None = None,
    params: dict[str, Any] | None = None,
    name: str | None = None,
    step_runner: str | RunnerBase | None = None,
    runner_resources: dict[str, Any] | RunnerResources | None = None,
    batch_strategy: dict[str, Any] | BatchStrategy | None = None,
    environment: str | dict[str, Any] | Environments | None = None,
    tool: dict[str, Any] | ToolSpec | None = None,
    compute_provider: str | dict[str, Any] | ComputeProvider | None = None,
    compute_resources: dict[str, Any] | ComputeResources | None = None,
    failure_policy: FailurePolicy | None = None,
    compact: bool = True,
    skip_cache: bool = False,
) -> CompositeResult
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `composite` | `type[CompositeDefinition]` | — | Composite class to run |
| `inputs` | `dict[str, OutputReference \| list[str]] \| None` | `None` | Input wiring by role |
| `params` | `dict[str, Any] \| None` | `None` | Composite parameter overrides |
| `name` | `str \| None` | `None` | Child-step name prefix. Defaults to `composite.name` |
| `step_runner` | `str \| RunnerBase \| None` | `None` | Default step runner for child steps |
| `runner_resources` | `dict[str, Any] \| RunnerResources \| None` | `None` | Default runner resources for child steps |
| `batch_strategy` | `dict[str, Any] \| BatchStrategy \| None` | `None` | Default batching/scheduling for child steps |
| `environment` | `str \| dict[str, Any] \| Environments \| None` | `None` | Default environment for child steps |
| `tool` | `dict[str, Any] \| ToolSpec \| None` | `None` | Default tool config for child steps |
| `compute_provider` | `str \| dict[str, Any] \| ComputeProvider \| None` | `None` | Default compute provider for child steps |
| `compute_resources` | `dict[str, Any] \| ComputeResources \| None` | `None` | Default compute resources for child steps |
| `failure_policy` | `FailurePolicy \| None` | `None` | Default failure policy for child steps |
| `compact` | `bool` | `True` | Default Delta Lake compaction for child steps |
| `skip_cache` | `bool` | `False` | Default cache-bypass for child steps |

**Returns:** `CompositeResult` (non-blocking); `.output(role)` wires
downstream steps, `.wait()` blocks on the children.

**Raises:** `TypeError` if `composite` is not a `CompositeDefinition`
subclass, or if a `CompositeDefinition` is passed to `run`/`submit`.

#### `run_composite(...) -> CompositeResult`

Same signature as `submit_composite`. Blocks until every child step
completes (via `CompositeResult.wait()`), then returns the resolved
`CompositeResult`.

---

## CompositeContext

`artisan.composites.base.composite_context.CompositeContext`

Build-time context passed to `CompositeDefinition.compose`. A single
concrete class; each `run()` submits a real pipeline step.

### Methods

#### `input(role: str) -> CompositeRef`

Reference a declared input of this composite.

**Args:**
- `role` — input role name (must match a key in `inputs`)

**Returns:** `CompositeRef` backed by the resolved input source.

**Raises:** `ValueError` if role is not a declared input.

#### `run(operation, inputs=None, params=None, ...) -> CompositeStepHandle`

Submit an operation or nested composite as a pipeline step.

```python
def run(
    operation: type,
    inputs: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    runner_resources: dict[str, Any] | None = None,
    batch_strategy: dict[str, Any] | None = None,
    step_runner: str | RunnerBase | None = None,
    environment: str | dict[str, Any] | None = None,
    tool: dict[str, Any] | None = None,
    compute_resources: dict[str, Any] | ComputeResources | None = None,
    compute_provider: str | dict[str, Any] | ComputeProvider | None = None,
    skip_cache: bool | None = None,
    failure_policy: FailurePolicy | None = None,
    compact: bool | None = None,
) -> CompositeStepHandle
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `operation` | `type` | — | `OperationDefinition` or `CompositeDefinition` subclass |
| `inputs` | `dict[str, Any] \| None` | `None` | Input wiring as `{role: CompositeRef}` |
| `params` | `dict[str, Any] \| None` | `None` | Parameter overrides |
| `runner_resources` | `dict[str, Any] \| None` | `None` | Runner resource overrides for this step |
| `batch_strategy` | `dict[str, Any] \| None` | `None` | Batching/scheduling overrides for this step |
| `step_runner` | `str \| RunnerBase \| None` | `None` | Step-runner override for this step |
| `environment` | `str \| dict[str, Any] \| None` | `None` | Environment override for this step |
| `tool` | `dict[str, Any] \| None` | `None` | Tool overrides for this step |
| `compute_resources` | `dict \| ComputeResources \| None` | `None` | Compute-resource overrides for this step |
| `compute_provider` | `str \| dict \| ComputeProvider \| None` | `None` | Compute-provider override for this step |
| `skip_cache` | `bool \| None` | `None` | Cache-bypass override for this step |
| `failure_policy` | `FailurePolicy \| None` | `None` | Failure-policy override for this step |
| `compact` | `bool \| None` | `None` | Delta Lake compaction override for this step |

Any override left at its default (`None`) falls back to the
composite-level value passed to `submit_composite`/`run_composite`.

**Returns:** `CompositeStepHandle` wrapping the step's `StepFuture`.

#### `output(role: str, ref: CompositeRef) -> None`

Map an internal result to a declared output of this composite.

**Args:**
- `role` — composite output role name (must match a key in `outputs`)
- `ref` — `CompositeRef` from an internal `ctx.run().output()`

**Raises:** `ValueError` if role is not a declared output.

---

## CompositeStepHandle

`artisan.composites.base.results.CompositeStepHandle`

Handle returned by `ctx.run()`. Wraps the child step's `StepFuture`.

### Methods

#### `output(role: str) -> CompositeRef`

Reference an output role of this internal operation.

**Args:**
- `role` — output role name of the operation that was run

**Returns:** `CompositeRef` for wiring to downstream `ctx.run()` calls
or to `ctx.output()`.

**Raises:** `ValueError` if role is not a valid output of the operation.

---

## CompositeRef

`artisan.schemas.composites.composite_ref.CompositeRef`

Frozen dataclass. A lightweight reference used as input wiring between
internal operations.

| Field | Type | Description |
|-------|------|-------------|
| `source` | `ArtifactSource \| None` | Currently unused (always `None`); retained for the frozen dataclass shape |
| `output_reference` | `OutputReference \| None` | Pipeline reference to the producing step's output |
| `role` | `str` | Output role name this ref points to |

---

## CompositeResult

`artisan.composites.base.results.CompositeResult`

Returned by `submit_composite`/`run_composite`. Maps composite outputs to
their producing pipeline steps. Duck-types with `StepResult` and
`StepFuture` for `.output(role) -> OutputReference`.

### Methods

#### `output(role: str) -> OutputReference`

Get the `OutputReference` for a composite output role.

**Args:**
- `role` — composite output role name

**Returns:** `OutputReference` pointing at the internal step that
produces it.

**Raises:** `ValueError` if role is not a declared output.

#### `wait(*, timeout: float | None = None) -> CompositeResult`

Block until every child step completes.

**Args:**
- `timeout` — optional total deadline in seconds. `None` waits
  indefinitely.

**Returns:** Self, with all child steps resolved.

**Raises:** `TimeoutError` if the timeout expires before all children
resolve.

---

## See also

- [Composites and Composition](../concepts/composites-and-composition.md) — conceptual overview
- [Writing Composite Operations](../how-to-guides/writing-composite-operations.md) — step-by-step guide
- [Composable Operations Tutorial](../tutorials/02-pipeline-design/07-composites.ipynb) — interactive examples
- [Glossary](glossary.md) — key terms
