---
name: composite-write
description: Write or scaffold an Artisan composite operation. Use this skill when the user asks to create a composite, write a composite operation, scaffold a composite, compose operations, or any request involving CompositeDefinition subclasses. Trigger on phrases like "write a composite", "create a composite operation", "scaffold a composite", "compose operations", or any request to combine multiple operations into a reusable unit.
---

# Write an Artisan Composite

Write or scaffold the composite operation described in the user's request. If
the invoking client supplies explicit skill arguments, treat them as the
request details.

Before writing, read the example composites in the integration test
(`tests/integration/test_composite.py`) and the base class at
`src/artisan/composites/base/composite_definition.py` to match established patterns.

---

## When to Use a Composite

- **Reusable multi-operation sequence** — a fixed chain of operations that
  appears in multiple pipelines
- **Tightly coupled ops** — operations that logically belong together and are
  named as one reusable unit
- **Named grouping over real steps** — running a composite expands each
  internal `ctx.run()` into its own pipeline step with independent caching,
  batching, dispatch, and provenance

If you only need a single operation, use `operation-write` instead.

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

`ctx.run()` accepts optional execution overrides applied to that step:

```python
from artisan.orchestration import Runner

ctx.run(
    DataTransformer,
    inputs={"dataset": ctx.input("data")},
    params={"scale_factor": 2.0},
    runner_resources={"cpus": 4, "memory_gb": 16},
    batch_strategy={"artifacts_per_unit": 10},
    step_runner=Runner.LOCAL,
    environment="my_container",
    tool={"executable": "/path/to/tool"},
)
```

Composite-level overrides passed to `run_composite`/`submit_composite` are the
defaults for every child step; a value set on a `ctx.run()` call wins for that
step and that knob.

Core recognizes only the built-in `"local"` name. Pass an initialized runner
from an optional provider at the pipeline boundary when creator children need
external dispatch:

```python
from artisan_submitit import SlurmRunner

pipeline.run_composite(
    TransformAndScore,
    inputs={"data": output("generate", "datasets")},
    step_runner=SlurmRunner(slurm_partition="gpu"),
)
```

That default applies to creator children. Curator children always run in an
isolated local subprocess; do not put provider instances on curator
`ctx.run()` calls.

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
        metrics = ctx.run(MetricCalculator, inputs={"dataset": inner.output("dataset")})
        ctx.output("metrics", metrics.output("metrics"))
```

---

## Differences from OperationDefinition

Composites are **not** operations. Key differences:

| Feature | OperationDefinition | CompositeDefinition |
|---|---|---|
| Core method | `execute_function()` / `execute_command()` or `execute_curator()` | `compose()` |
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

Composites run via `run_composite` (blocking) or `submit_composite`
(non-blocking) — see `pipeline-write` for full API details:

```python
# Blocking: expands into real pipeline steps, one per internal ctx.run()
pipeline.run_composite(
    TransformAndScore,
    name="ts",
    inputs={"data": output("generate", "datasets")},
)

# Non-blocking: returns a CompositeResult for downstream wiring
result = pipeline.submit_composite(
    TransformAndScore, name="ts", inputs={"data": output("generate", "datasets")}
)
pipeline.run(NextOp, inputs={"data": result.output("metrics")})
```

---

## Testing Patterns

### Integration test

```python
def test_composite(tmp_path):
    pipeline = PipelineManager.create(
        name="test",
        delta_root=tmp_path / "delta",
        staging_root=tmp_path / "staging",
    )
    output = pipeline.output

    pipeline.run(DataGenerator, name="gen", params={"count": 2, "seed": 42})
    pipeline.run_composite(
        TransformAndScore, name="ts", inputs={"data": output("gen", "datasets")}
    )

    summary = pipeline.finalize()
    # The composite expands into one step per internal operation
    assert summary.steps_completed >= 3
```

---

## Style Rules

Follow these conventions (mirrors operation-write):

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
