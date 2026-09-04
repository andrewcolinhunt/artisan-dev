# Write Composite Operations

How to group tightly coupled operations into a reusable composite with
declared inputs, outputs, and internal wiring.

**Prerequisites:** [Operations Model](../concepts/operations-model.md),
[Writing Creator Operations](writing-creator-operations.md)

**Key types:** `CompositeDefinition`, `CompositeContext`,
`CompositeStepHandle`, `CompositeRef`, `CompositeResult`

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

## Run the composite in a pipeline

Run a composite with `pipeline.run_composite()`. Each internal
`ctx.run()` becomes its own pipeline step with independent caching,
batching, and worker dispatch. Step names are prefixed with the
composite name.

```python
from artisan.orchestration import PipelineManager
from artisan.operations.examples import DataGenerator

pipeline = PipelineManager.create(
    name="example",
    delta_root="runs/delta",
    staging_root="runs/staging",
)
output = pipeline.output

pipeline.run(operation=DataGenerator, name="generate", params={"count": 5})
pipeline.run_composite(
    TransformAndScore,
    inputs={"dataset": output("generate", "datasets")},
)
result = pipeline.finalize()
```

`run_composite` blocks until every child step completes. Use
`submit_composite` for the non-blocking form; it returns a
`CompositeResult` whose `.output(role)` wires downstream steps and whose
`.wait()` blocks on the children.

```python
scored = pipeline.submit_composite(
    TransformAndScore,
    inputs={"dataset": output("generate", "datasets")},
)
pipeline.run(
    operation=MetricCalculator,
    name="rescore",
    inputs={"dataset": scored.output("metrics")},
)
```

Composites run only through `run_composite`/`submit_composite`. Passing a
`CompositeDefinition` to `pipeline.run()` raises `TypeError`.

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
pipeline.run_composite(
    TransformAndScore,
    inputs={"dataset": output("gen", "datasets")},
    params={"scale_factor": 3.0},
)
```

`params` configure the composite itself and are consumed inside
`compose()`. They are not forwarded to child steps.

---

## Forward execution overrides

`run_composite`/`submit_composite` accept the same execution overrides an
ordinary step takes: `step_runner`, `runner_resources`, `batch_strategy`,
`environment`, `tool`, `compute_provider`, `compute_resources`,
`failure_policy`, `compact`, and `skip_cache`. Each becomes the
**default for every child step**. A value set explicitly on a `ctx.run()`
call wins for that step and that knob; anything the child leaves unset
falls back to the composite-level default.

```python
# batch_strategy here is the default for every child step
pipeline.run_composite(
    TransformAndScore,
    inputs={"dataset": output("gen", "datasets")},
    batch_strategy={"artifacts_per_unit": 4},
)
```

```python
# A child step that sets the same knob wins for that step only
def compose(self, ctx: CompositeContext) -> None:
    transformed = ctx.run(
        DataTransformer,
        inputs={"dataset": ctx.input("dataset")},
        batch_strategy={"artifacts_per_unit": 1},  # overrides the default here
    )
    scored = ctx.run(  # inherits the composite-level default
        MetricCalculator,
        inputs={"dataset": transformed.output("dataset")},
    )
    ctx.output("metrics", scored.output("metrics"))
```

To co-locate a composite's steps in one compute allocation, install the
optional `artisan-submitit` package and pass
`run_composite(..., step_runner=SlurmIntraRunner())`. It dispatches every child
step via `srun` inside the current allocation. See
[Configure Execution](configuring-execution.md).

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

A composite can contain other composites. The inner composite's internal
operations expand into their own steps, with dot-separated names:

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

Composites can run curator operations. A curator inside `compose()` runs
as a real pipeline step, so its upstream artifacts are already committed
and its provenance edges are recorded by the ordinary step path:

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
| `TypeError: Invalid input type for role ...` | Passed a raw value instead of a `ctx.input()` or `handle.output()` result | Wire steps with the `CompositeRef` objects the context API returns |
| `TypeError` from `pipeline.run()` | Passed a composite to `run`/`submit` | Use `run_composite`/`submit_composite` for composites |
| A `ctx.run()` ignores a composite-level override | The child set the same knob explicitly | Per-op values win per knob; remove the child's value to inherit the default |

---

## Verify

Test your composite end-to-end in a minimal pipeline:

```python
from artisan.orchestration import PipelineManager
from artisan.operations.examples import DataGenerator

pipeline = PipelineManager.create(
    name="test",
    delta_root="test/delta",
    staging_root="test/staging",
)
output = pipeline.output
pipeline.run(operation=DataGenerator, name="generate", params={"count": 3})

result = pipeline.run_composite(
    TransformAndScore,
    inputs={"dataset": output("generate", "datasets")},
)
# result exposes .output(role) for the composite's declared outputs
assert result.output("metrics") is not None
pipeline.finalize()
```

---

## Cross-references

- [Composites and Composition](../concepts/composites-and-composition.md) — why
  composites exist and how they execute
- [CompositeDefinition Reference](../reference/composite-definition.md) — API
  signatures and field tables
- [Composable Operations Tutorial](../tutorials/02-pipeline-design/07-composites.ipynb) —
  interactive examples
- [Writing Creator Operations](writing-creator-operations.md) — the operations
  that composites compose
- [Building a Pipeline](building-a-pipeline.md) — using composites in pipelines
