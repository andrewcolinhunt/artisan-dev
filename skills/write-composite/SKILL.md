---
name: write-composite
description: Write or scaffold an Artisan composite operation. Use this skill when the user asks to create a composite, write a composite operation, scaffold a composite, compose operations, or any request involving CompositeDefinition subclasses. Trigger on phrases like "write a composite", "create a composite operation", "scaffold a composite", "compose operations", or any request to combine multiple operations into a reusable unit.
argument-hint: "[CompositeClassName or description of what the composite should do]"
---

# Write an Artisan Composite

Write or scaffold a composite operation for `$ARGUMENTS`.

Before writing, read the example composites in integration tests
(`tests/integration/test_composite_collapsed.py`,
`tests/integration/test_composite_expanded.py`) and the base class at
`src/artisan/composites/base/composite_definition.py` to match established patterns.

---

## When to Use a Composite

- **Reusable multi-operation sequence** — a fixed chain of operations that
  appears in multiple pipelines
- **Tightly coupled ops** — operations that logically belong together and should
  run as a single pipeline step (collapsed) or be expandable for debugging
- **Dual-mode execution** — same definition works with `pipeline.run()`
  (collapsed, single step) and `pipeline.expand()` (expanded, one step per
  internal op)

If you only need a single operation, use `write-operation` instead.

---

## Composite Template

Follow this structure. Use the `# ---------- Section ----------` comment style.
Declare sections in this order: Metadata, Inputs, Outputs, Parameters, Compose.

```python
"""One-line module docstring describing what this composite does."""

from __future__ import annotations

from enum import StrEnum
from typing import ClassVar

from artisan.composites import CompositeContext, CompositeDefinition
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

# Import operations used in compose()
from artisan.operations.examples.data_transformer import DataTransformer
from artisan.operations.examples.metric_calculator import MetricCalculator


class TransformAndScore(CompositeDefinition):
    """Transform data then compute metrics.

    Runs DataTransformer followed by MetricCalculator, exposing only the
    final metrics as output.
    """

    # ---------- Metadata ----------
    name = "transform_and_score"
    description = "Transform data then compute metrics"

    # ---------- Inputs ----------
    class InputRole(StrEnum):
        DATA = "data"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATA: InputSpec(
            artifact_type="data",
            required=True,
            description="Input dataset to transform and score",
        ),
    }

    # ---------- Outputs ----------
    class OutputRole(StrEnum):
        METRICS = "metrics"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.METRICS: OutputSpec(
            artifact_type="metric",
            description="Computed metrics from the transformed data",
        ),
    }

    # ---------- Compose ----------
    def compose(self, ctx: CompositeContext) -> None:
        """Define the internal operation graph."""
        transformed = ctx.run(
            DataTransformer,
            inputs={"dataset": ctx.input("data")},
            params={"scale_factor": 2.0},
        )
        metrics = ctx.run(
            MetricCalculator,
            inputs={"dataset": transformed.output("dataset")},
        )
        ctx.output("metrics", metrics.output("metrics"))
```

---

## compose() Method

The `compose()` method defines the internal operation graph using three
primitives on the `CompositeContext`:

| Method | Purpose |
|---|---|
| `ctx.input(role)` | Get a `CompositeRef` for an external input by role name |
| `ctx.run(operation, inputs=..., params=..., ...)` | Run an internal operation, returns `CompositeStepHandle` |
| `ctx.output(role, ref)` | Map an internal output to an external output role |

`ctx.run()` returns a `CompositeStepHandle` whose `.output(role)` returns a
`CompositeRef` for wiring to downstream internal operations or to `ctx.output()`.

---

## Per-Operation Overrides in compose()

`ctx.run()` accepts optional overrides that are forwarded to the operation:

```python
ctx.run(
    DataTransformer,
    inputs={"dataset": ctx.input("data")},
    params={"scale_factor": 2.0},
    resources={"cpus": 4, "memory_gb": 16},
    execution={"artifacts_per_unit": 10},
    backend="slurm",
    environment="my_container",
    tool={"executable": "/path/to/tool"},
)
```

In collapsed mode, `resources`/`backend`/`environment`/`tool` are ignored (the
composite runs as a single unit). In expanded mode, they are forwarded to each
pipeline step.

---

## Nesting Composites

Composites can contain other composites via `ctx.run()`:

```python
class OuterComposite(CompositeDefinition):
    name = "outer"
    description = "Runs an inner composite then scores"

    class OutputRole(StrEnum):
        METRICS = "metrics"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "metrics": OutputSpec(artifact_type="metric"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        inner = ctx.run(InnerComposite)
        metrics = ctx.run(MetricCalculator,
            inputs={"dataset": inner.output("dataset")})
        ctx.output("metrics", metrics.output("metrics"))
```

---

## Intermediates Modes

Controls what happens to artifacts from internal operations (collapsed mode only):

| Mode | Behavior |
|---|---|
| `"discard"` (default) | Only final outputs are committed; intermediates are discarded |
| `"persist"` | Intermediates committed to Delta but not visible as step boundaries |
| `"expose"` | Intermediates committed to Delta with full step boundaries |

Set via `pipeline.run(MyComposite, intermediates="persist", ...)`. Not
applicable to `pipeline.expand()` where all steps are naturally visible.

---

## Differences from OperationDefinition

Composites are **not** operations. Key differences:

| Feature | OperationDefinition | CompositeDefinition |
|---|---|---|
| Core method | `execute()` or `execute_curator()` | `compose()` |
| Lifecycle hooks | `preprocess()`, `postprocess()` | None |
| `tool` / `environments` | Supported | Not supported (set per-op in compose) |
| `infer_lineage_from` on outputs | Required for creators | Not supported |
| `runtime_defined_inputs` | Supported | Not supported |
| `group_by` | Supported | Not supported |
| Registry | `OperationDefinition._registry` | `CompositeDefinition._registry` |

---

## Validation Rules

The framework validates at class definition time (`__pydantic_init_subclass__`):

- `compose()` must be overridden (raises `TypeError` otherwise)
- `OutputRole(StrEnum)` values must match `outputs` keys exactly
- `InputRole(StrEnum)` values must match `inputs` keys exactly (if inputs
  are defined)
- Registered automatically in `CompositeDefinition._registry` by `name`

---

## Running Composites

Two execution modes — see `write-pipeline` for full API details:

```python
# Collapsed: single pipeline step, in-memory passing
pipeline.run(TransformAndScore, name="ts",
    inputs={"data": output("generate", "datasets")},
    intermediates="discard")

# Expanded: each internal op becomes its own pipeline step
expanded = pipeline.expand(TransformAndScore, name="ts",
    inputs={"data": output("generate", "datasets")})
pipeline.run(NextOp, inputs={"data": expanded.output("metrics")})
```

---

## Testing Patterns

### Integration test (collapsed)

```python
def test_composite_collapsed(tmp_path):
    pipeline = PipelineManager.create(
        name="test", delta_root=tmp_path / "delta", staging_root=tmp_path / "staging",
    )
    output = pipeline.output

    pipeline.run(DataGenerator, name="gen", params={"count": 2, "seed": 42})
    pipeline.run(TransformAndScore, name="ts",
        inputs={"data": output("gen", "datasets")})

    summary = pipeline.finalize()
    assert summary.steps_completed == 2
```

### Integration test (expanded)

```python
def test_composite_expanded(tmp_path):
    pipeline = PipelineManager.create(
        name="test", delta_root=tmp_path / "delta", staging_root=tmp_path / "staging",
    )
    output = pipeline.output

    pipeline.run(DataGenerator, name="gen", params={"count": 2, "seed": 42})
    expanded = pipeline.expand(TransformAndScore, name="ts",
        inputs={"data": output("gen", "datasets")})

    summary = pipeline.finalize()
    # Expanded creates one step per internal operation
    assert summary.steps_completed >= 3
```

---

## Style Rules

Follow these conventions (mirrors write-operation):

- **Module docstring**: One line, describes what the composite does
- **Class docstring**: Summary line + optional extended description
- **Section comments**: Use `# ---------- Section ----------` with exactly 10
  dashes on each side
- **Section order**: Metadata, Inputs, Outputs, Parameters, Compose
- **compose() docstring**: One-line imperative summary
- **name value**: `snake_case` matching the class name's snake_case form
- **Imports**: Group stdlib, then artisan composites, then artisan operations.
  Use `from __future__ import annotations`
- **No bare constants**: Put configurable values in a `Params` class or as
  `params` in `ctx.run()` calls
