# Write Composite Operations

A composite connects creators and selects the outputs to keep. Submit it through
`pipeline.run()` or `pipeline.submit()`, just like an operation. Each execution
unit runs its children in one worker and persists one boundary result.

## Minimal working example

Add this definition to an installed application package, for example in
`example_ops/compositions.py`. It transforms a dataset and returns both the
transformed data and its metrics. Pipeline workers need installed, importable
definitions; a class defined only in a script or notebook is not sufficient.

```python
from enum import StrEnum
from typing import ClassVar

from artisan.composites import CompositeContext, CompositeDefinition
from artisan.operations.examples import DataTransformer, MetricCalculator
from artisan.schemas import InputSpec, OutputSpec


class TransformAndScore(CompositeDefinition):
    name = "transform_and_score"

    class InputRole(StrEnum):
        DATASET = "dataset"

    class OutputRole(StrEnum):
        DATASET = "dataset"
        METRICS = "metrics"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATASET: InputSpec(artifact_type="csv"),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.DATASET: OutputSpec(artifact_type="csv"),
        OutputRole.METRICS: OutputSpec(artifact_type="metric"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        transformed = ctx.call(
            DataTransformer,
            name="transform",
            inputs={"dataset": ctx.input("dataset")},
            params={"scale_factor": 2.0, "variants": 1, "seed": 100},
        )
        scored = ctx.call(
            MetricCalculator,
            name="score",
            inputs={"dataset": transformed.output("dataset")},
        )
        ctx.output("dataset", transformed.output("dataset"))
        ctx.output("metrics", scored.output("metrics"))
```

`ctx.call` declares wiring. It does not execute anything during `compose`.
`ctx.output` selects the public outputs. The child operations declare ordinary
lineage, so the composite does not repeat their lineage mappings.

## Run the composite in a pipeline

After installing that package in your worker environment, run this separate
script with a fresh output directory:

```python
from pathlib import Path
from tempfile import TemporaryDirectory

from example_ops.compositions import TransformAndScore

from artisan.operations.examples import DataGenerator
from artisan.orchestration import PipelineManager, StepStatus
from artisan.visualization import inspect_step

with TemporaryDirectory() as directory:
    root = Path(directory)
    with PipelineManager.create(
        name="composition_example",
        delta_root=str(root / "delta"),
        staging_root=str(root / "staging"),
    ) as pipeline:
        generated = pipeline.run(DataGenerator, params={"count": 2, "seed": 42})
        scored = pipeline.run(
            TransformAndScore,
            inputs={"dataset": generated.output("datasets")},
        )
        assert scored.status is StepStatus.SUCCEEDED, scored.error
        outputs = inspect_step(
            str(root / "delta"),
            scored.step_number,
            pipeline_run_id=pipeline.config.pipeline_run_id,
        )
        assert outputs.height == 4  # Two datasets and their two metrics.
        assert len(pipeline) == 2  # Generate, then the whole composite.
```

The temporary directory is deleted when the example ends. Use a persistent path
for results you want to keep. For asynchronous submission, use
`pipeline.submit(TransformAndScore, ...)`; its `StepFuture.output(role)` wires a
dependent step and `.result()` returns the completed step result.

## Choose what persists

This example exports both the transformed dataset and its metric, preserving
their direct relationship. To keep only metrics, remove the dataset output role,
its `OutputSpec`, and its `ctx.output` call. Intermediate datasets then disappear
unless a retained artifact type explicitly requires one as a semantic reference.

For a fixed sequence of refinement iterations, use a parameter-dependent Python
loop to declare repeated calls, then export only the final result. An operation
can produce a metric containing a whole history; it remains an ordinary metric.
Its lineage should name the final structure if that is the relationship the
operation intends to retain. Discarded structures do not acquire stored rows
merely because they contributed to that metric's values.

`ctx.output(role, [first, second])` concatenates compatible output streams in
order. Repeating a reference preserves repeated occurrences. Every declared
output role must be bound once. An optional output can be explicitly empty.

## Add parameters

Use a nested Pydantic `Params` model and a `params` field, as with an operation.
The composite validates parameters before building its plan. Forward values to
children explicitly through `ctx.call(..., params=...)`.

Loops and conditionals may depend on those parameters. They cannot depend on
future artifact values: `CompositeRef` describes wiring and contains no result
data. Do not read stores, dispatch workers, or install software inside `compose`.

## Forward execution overrides

Choose placement, resources, batching, caching, and failure policy on the outer
`pipeline.run` or `submit` call. For example, continuing with an existing
`pipeline`, `generated` result, and configured `runner`:

```python
scored = pipeline.run(
    TransformAndScore,
    inputs={"dataset": generated.output("datasets")},
    step_runner=runner,
    batch_strategy={"artifacts_per_unit": 2, "units_per_worker": 2},
)
```

These settings govern the submitted boundary. `ctx.call` accepts scientific
wiring and parameters; it has no child runner, persistence, cache, or retry
settings. Separate pipeline submissions when an intermediate needs an independent
checkpoint. Tool environments remain declarations on the child operations;
different command environments do not require different workers.

(author-composite-nesting)=
## Nesting composites

Pass another `CompositeDefinition` to `ctx.call` to expand it inside the same
boundary. A nested composite does not create a separate persisted step or cache.
Store-bound curators such as `Filter` and `Merge` stay top-level pipeline steps;
they cannot query uncommitted children inside a scientific composite.

See [Composites and Composition](../concepts/composites-and-composition.md) for
the boundaries and [Prepare Execute Environments](execute-environments.md) for
worker and tool software.
