# Write Composite Operations

How to group tightly coupled operations into a reusable composite with
declared inputs, outputs, and internal wiring.

**Prerequisites:** [Operations Model](../concepts/operations-model.md),
[Writing Creator Operations](writing-creator-operations.md)

**Key types:** `CompositeDefinition`, `CompositeContext`,
`CompositeStepHandle`, `CompositeRef`

---

## Minimal working example

A composite that transforms data and computes quality metrics:

```python
from __future__ import annotations

from enum import StrEnum
from typing import ClassVar

from artisan.composites import CompositeDefinition, CompositeContext
from artisan.operations.examples import DataTransformer, MetricCalculator
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class TransformAndScore(CompositeDefinition):
    """Transform data then compute quality metrics."""

    name = "transform_and_score"

    class InputRole(StrEnum):
        DATASET = "dataset"

    class OutputRole(StrEnum):
        METRICS = "metrics"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATASET: InputSpec(artifact_type="data", required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.METRICS: OutputSpec(artifact_type="metric"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        transformed = ctx.run(
            DataTransformer,
            inputs={"dataset": ctx.input("dataset")},
            params={"scale_factor": 2.0, "variants": 1, "seed": 100},
        )
        scored = ctx.run(
            MetricCalculator,
            inputs={"dataset": transformed.output("dataset")},
        )
        ctx.output("metrics", scored.output("metrics"))
```

---

## Use the composite in a pipeline

### Collapsed (single step)

```python
from artisan.orchestration import PipelineManager
from artisan.operations.examples import DataGenerator

pipeline = PipelineManager.create(
    name="example", delta_root="runs/delta", staging_root="runs/staging",
)
output = pipeline.output

pipeline.run(operation=DataGenerator, name="generate", params={"count": 5})
pipeline.run(
    operation=TransformAndScore,
    inputs={"dataset": output("generate", "datasets")},
)
result = pipeline.finalize()
```

The composite runs as a single pipeline step. Internal artifacts pass
in-memory between operations.

### Expanded (separate steps)

```python
pipeline.run(operation=DataGenerator, name="generate", params={"count": 5})
expanded = pipeline.expand(
    TransformAndScore,
    inputs={"dataset": output("generate", "datasets")},
)
result = pipeline.finalize()
```

Each internal `ctx.run()` becomes its own pipeline step with independent
caching, batching, and worker dispatch.

---

## Define metadata and role enums

Every composite needs a `name`. Composites with inputs define
`InputRole(StrEnum)` whose values match the `inputs` dict keys.
Composites with outputs define `OutputRole(StrEnum)` whose values match
the `outputs` dict keys. The framework validates this match at class
definition time.

```python
class MyComposite(CompositeDefinition):
    name = "my_composite"
    description = "Short human-readable summary"

    class InputRole(StrEnum):
        DATA = "data"

    class OutputRole(StrEnum):
        RESULT = "result"
```

Composites without inputs (generative composites) omit `InputRole`.

---

## Declare inputs and outputs

### Inputs

Each entry maps a role name to an `InputSpec`:

```python
inputs: ClassVar[dict[str, InputSpec]] = {
    InputRole.DATA: InputSpec(artifact_type="data", required=True),
}
```

### Outputs

Each entry maps a role name to an `OutputSpec`. Unlike creator
operations, composites do not set `infer_lineage_from` — lineage is
handled by the internal operations:

```python
outputs: ClassVar[dict[str, OutputSpec]] = {
    OutputRole.RESULT: OutputSpec(artifact_type="metric"),
}
```

---

## Implement `compose()`

`compose()` receives a `CompositeContext` and wires internal operations
using three methods:

```python
def compose(self, ctx: CompositeContext) -> None:
    # 1. Reference declared inputs
    data_ref = ctx.input("data")

    # 2. Run internal operations, wiring outputs to inputs
    step_a = ctx.run(OpA, inputs={"data": data_ref})
    step_b = ctx.run(OpB, inputs={"data": step_a.output("result")})

    # 3. Map internal results to declared outputs
    ctx.output("result", step_b.output("result"))
```

`ctx.input()` returns a `CompositeRef`. `ctx.run()` returns a
`CompositeStepHandle` whose `.output()` method produces another
`CompositeRef`. `ctx.output()` maps an internal ref to a declared
composite output.

---

## Add parameters

Group composite-level configuration into a nested `Params` model:

```python
from pydantic import BaseModel, Field

class TransformAndScore(CompositeDefinition):
    # ... name, roles, inputs, outputs ...

    class Params(BaseModel):
        scale_factor: float = Field(default=2.0, ge=0.0)

    params: Params = Params()

    def compose(self, ctx: CompositeContext) -> None:
        transformed = ctx.run(
            DataTransformer,
            inputs={"dataset": ctx.input("dataset")},
            params={"scale_factor": self.params.scale_factor},
        )
        # ...
```

Override at the pipeline level:

```python
pipeline.run(
    operation=TransformAndScore,
    inputs={"dataset": output("gen", "datasets")},
    params={"scale_factor": 3.0},
)
```

---

## Control intermediate artifacts

In collapsed mode, pass `intermediates=` to `pipeline.run()`:

```python
# Default: discard intermediates
pipeline.run(operation=TransformAndScore, inputs={"dataset": output("gen", "datasets")})

# Persist for debugging
pipeline.run(
    operation=TransformAndScore,
    inputs={"dataset": output("gen", "datasets")},
    intermediates="persist",
)
```

| Mode | Intermediates in Delta Lake | Use when |
|------|---------------------------|----------|
| `"discard"` (default) | No | Production: minimize storage |
| `"persist"` | Yes (internal provenance edges) | Debugging: inspect intermediate results |
| `"expose"` | Yes (step-boundary edges) | Downstream steps need intermediate outputs |

In expanded mode, intermediates are always full pipeline steps.

---

## Common patterns

### Multi-input composite

A composite that accepts multiple input roles:

```python
class AlignAndScore(CompositeDefinition):
    name = "align_and_score"

    class InputRole(StrEnum):
        DATA = "data"
        REFERENCE = "reference"

    class OutputRole(StrEnum):
        METRICS = "metrics"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATA: InputSpec(artifact_type="data", required=True),
        InputRole.REFERENCE: InputSpec(artifact_type="data", required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.METRICS: OutputSpec(artifact_type="metric"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        aligned = ctx.run(
            Aligner,
            inputs={"data": ctx.input("data"), "reference": ctx.input("reference")},
        )
        scored = ctx.run(
            MetricCalculator,
            inputs={"dataset": aligned.output("aligned")},
        )
        ctx.output("metrics", scored.output("metrics"))
```

### Generate-then-process

A composite with no inputs that generates and processes data:

```python
class GenerateAndAnalyze(CompositeDefinition):
    name = "generate_and_analyze"

    class OutputRole(StrEnum):
        METRICS = "metrics"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.METRICS: OutputSpec(artifact_type="metric"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        generated = ctx.run(DataGenerator, params={"count": 10})
        scored = ctx.run(
            MetricCalculator,
            inputs={"dataset": generated.output("datasets")},
        )
        ctx.output("metrics", scored.output("metrics"))
```

### Nesting composites

A composite can contain other composites:

```python
class FullPipeline(CompositeDefinition):
    name = "full_pipeline"

    class OutputRole(StrEnum):
        METRICS = "metrics"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.METRICS: OutputSpec(artifact_type="metric"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        generated = ctx.run(DataGenerator, params={"count": 5})
        scored = ctx.run(
            TransformAndScore,  # nested composite
            inputs={"dataset": generated.output("datasets")},
        )
        ctx.output("metrics", scored.output("metrics"))
```

### Curator inside a composite

Composites can run curator operations. In collapsed mode, pending
artifacts are pre-committed to Delta Lake before the curator executes:

```python
def compose(self, ctx: CompositeContext) -> None:
    generated = ctx.run(DataGenerator, params={"count": 10})
    filtered = ctx.run(
        Filter,
        inputs={"passthrough": generated.output("datasets")},
        params={"criteria": [{"metric": "score", "operator": "gt", "value": 0.5}]},
    )
    ctx.output("filtered", filtered.output("passthrough"))
```

---

## Common pitfalls

| Problem | Cause | Fix |
|---------|-------|-----|
| `TypeError: must implement compose()` | Missing `compose()` override | Implement `compose(self, ctx)` |
| `TypeError: must define OutputRole` | Missing `OutputRole(StrEnum)` inner class | Add enum with values matching `outputs` keys |
| `TypeError: must define InputRole` | Missing `InputRole(StrEnum)` inner class | Add enum with values matching `inputs` keys |
| `ValueError: Unknown input role` | Typo in `ctx.input("role")` | Check `InputRole` enum values |
| `ValueError: Unknown output role` | Typo in `ctx.output("role", ref)` | Check `OutputRole` enum values |
| `TypeError: Expected CompositeRef` | Passed raw value instead of `ctx.input()` or `handle.output()` result | Use `CompositeRef` objects from the context API |
| Resources ignored in collapsed mode | Per-operation `resources`/`backend` not supported in collapsed mode | Use expanded mode for per-operation resource control |

---

## Verify

Test your composite end-to-end in a minimal pipeline:

```python
from artisan.orchestration import PipelineManager
from artisan.operations.examples import DataGenerator

pipeline = PipelineManager.create(
    name="test", delta_root="test/delta", staging_root="test/staging",
)
output = pipeline.output
pipeline.run(operation=DataGenerator, name="generate", params={"count": 3})

# Collapsed
step = pipeline.run(
    operation=TransformAndScore,
    inputs={"dataset": output("generate", "datasets")},
)
assert step.success
assert step.succeeded_count > 0
pipeline.finalize()

# Expanded (in a separate pipeline)
pipeline2 = PipelineManager.create(
    name="test_expanded", delta_root="test2/delta", staging_root="test2/staging",
)
output2 = pipeline2.output
pipeline2.run(operation=DataGenerator, name="generate", params={"count": 3})
expanded = pipeline2.expand(
    TransformAndScore,
    inputs={"dataset": output2("generate", "datasets")},
)
result = pipeline2.finalize()
```

---

## Cross-references

- [Composites and Composition](../concepts/composites-and-composition.md) — why
  composites exist and how they work
- [CompositeDefinition Reference](../reference/composite-definition.md) — API
  signatures and field tables
- [Composable Operations Tutorial](../tutorials/pipeline-design/06-composable-operations.ipynb) —
  interactive examples
- [Writing Creator Operations](writing-creator-operations.md) — the operations
  that composites compose
- [Building a Pipeline](building-a-pipeline.md) — using composites in pipelines
