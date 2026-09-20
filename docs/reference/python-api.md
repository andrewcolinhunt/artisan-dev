# Python API Lookup

Use the public packages below to find an interface. Inspect its signature and
docstring in your installed version for exact arguments, defaults, return values,
and exceptions. The guides explain how to use those interfaces together.

## Find an interface

| Task | Public package | Start with |
| --- | --- | --- |
| Run a pipeline or inspect its run history | `artisan.orchestration` | `PipelineManager`, `PipelineConfig`, `StepResult`, `list_runs` |
| Define an operation | `artisan.operations.base` | `OperationDefinition`, `PerArtifact` |
| Configure operations and define artifacts | `artisan.schemas` | `BatchStrategy`, `InputSpec`, `Artifact`, `ArtifactTypeDef` |
| Reuse built-in operations | `artisan.operations.curator`, `artisan.operations.examples` | `Filter`, `Merge`, `DataGenerator` |
| Compose operations | `artisan.composites` | `CompositeDefinition`, `CompositeContext`, `CompositeResult` |
| Read stored artifacts | `artisan.storage` | `ArtifactStore` |
| Inspect results, failures, and timing | `artisan.visualization` | `inspect_pipeline`, `inspect_data`, `inspect_failures`, `PipelineTimings` |
| Traverse provenance | `artisan.provenance` | `provenance_edges`, `walk_forward`, `walk_backward` |
| Implement a runner provider | `artisan.orchestration.runner_api` | `RunnerBase`, `LifecycleRouter`, `UnitResult` |

For structured error definitions, see `artisan.errors`: `ArtisanError`,
`ArtisanErrorEnvelope`, and `ErrorCode`. See
[error handling](../concepts/error-handling.md#structured-failure-identity) for
how these relate to failed steps and execution records.

## Inspect the installed version

An editor's hover information and “go to definition” show the same source and
docstrings you can inspect from Python:

```python
import inspect

from artisan.orchestration import PipelineManager

print(inspect.signature(PipelineManager.run))
help(PipelineManager.run)
print(inspect.getsourcefile(PipelineManager))
```

For a Pydantic configuration model, its generated JSON schema lists fields,
defaults, and constraints. It also includes descriptions declared on fields:

```python
from pprint import pprint

from artisan.schemas import BatchStrategy

pprint(BatchStrategy.model_json_schema())
```

From a terminal, Python's documentation viewer is available in the project
environment:

```bash
pixi run --locked python -m pydoc artisan.orchestration.PipelineManager.run
```

Use `pixi run --locked artisan --help` to find CLI commands, then append
`--help` to a command for its options.

## Find a working example

- [Build a pipeline](../how-to-guides/building-a-pipeline.md).
- [Configure execution](../how-to-guides/configuring-execution.md).
- [Write an operation](../how-to-guides/writing-creator-operations.md).
- [Write a composite](../how-to-guides/writing-composite-operations.md).
- [Inspect a particular run](../how-to-guides/inspecting-provenance.md#select-the-run-you-want-to-inspect).
- [Implement a runner provider](../contributing/coding-conventions.md#external-step-runner-providers).
