# Write Creator Operations

How to build operations that run computation and produce artifacts using the
three-phase lifecycle.

**Prerequisites:** [Operations Model](../concepts/operations-model.md),
[Orientation](../getting-started/orientation.md)

**Key types:** `OperationDefinition`, `PreprocessInput`, `ExecuteInput`,
`PostprocessInput`, `ArtifactResult`

---

## Minimal working examples

### Generative (no inputs)

The simplest creator produces artifacts from nothing:

```python
from __future__ import annotations
from enum import StrEnum
from typing import ClassVar

from artisan.operations.base import OperationDefinition
from artisan.schemas import ArtifactResult, OutputSpec
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.specs.input_models import ExecuteInput, PostprocessInput


class HelloGenerator(OperationDefinition):
    name = "hello_generator"

    class OutputRole(StrEnum):
        DATASETS = "datasets"

    inputs: ClassVar[dict] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.DATASETS: OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": []},
        ),
    }

    def execute(self, inputs: ExecuteInput) -> None:
        (inputs.execute_dir / "hello.csv").write_text("id,value\n1,42\n")

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts = [
            DataArtifact.draft(
                content=f.read_bytes(),
                original_name=f.name,
                step_number=inputs.step_number,
            )
            for f in inputs.file_outputs
            if f.suffix == ".csv"
        ]
        return ArtifactResult(success=True, artifacts={"datasets": drafts})
```

No `InputRole`, no `preprocess`. Generative outputs use
`infer_lineage_from={"inputs": []}` to declare they have no parents.

### With inputs

A creator that consumes artifacts adds an `InputRole`, `inputs` spec, and
`preprocess`:

```python
from __future__ import annotations
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base import OperationDefinition
from artisan.schemas import ArtifactResult, InputSpec, OutputSpec
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)


class ScaleData(OperationDefinition):
    name = "scale_data"

    class InputRole(StrEnum):
        DATASET = "dataset"

    class OutputRole(StrEnum):
        DATASET = "dataset"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATASET: InputSpec(artifact_type="data", required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.DATASET: OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": ["dataset"]},
        ),
    }

    class Params(BaseModel):
        factor: float = Field(default=2.0, ge=0.0)

    params: Params = Params()

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: ExecuteInput) -> None:
        for path_str in inputs.inputs["dataset"]:
            path = Path(path_str)
            lines = path.read_text().splitlines()
            header, rows = lines[0], lines[1:]
            scaled = []
            for row in rows:
                parts = row.split(",")
                parts[1] = str(float(parts[1]) * self.params.factor)
                scaled.append(",".join(parts))
            out = inputs.execute_dir / path.name
            out.write_text(header + "\n" + "\n".join(scaled) + "\n")

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts = [
            DataArtifact.draft(
                content=f.read_bytes(),
                original_name=f.name,
                step_number=inputs.step_number,
            )
            for f in inputs.file_outputs
            if f.suffix == ".csv"
        ]
        return ArtifactResult(success=True, artifacts={"dataset": drafts})
```

---

## How data flows through the three phases

For how data flows between the three phases, see
[Operations Model](../concepts/operations-model.md#the-creator-lifecycle).
The summary: `preprocess` adapts inputs (receives `PreprocessInput`, returns a
plain dict), `execute` runs computation (receives `ExecuteInput`, writes files
to `execute_dir`), `postprocess` constructs artifacts from results (receives
`PostprocessInput`, returns `ArtifactResult`).

The framework passes each phase's output to the next. You never call one
phase from another.

---

## Step 1: Define metadata and role enums

Every operation needs a `name`. Operations with inputs define
`InputRole(StrEnum)` whose values match the `inputs` dict keys. Operations
with outputs define `OutputRole(StrEnum)` whose values match the `outputs`
dict keys. The framework validates this match at class definition time.

```python
class MyOp(OperationDefinition):
    name = "my_op"

    class InputRole(StrEnum):
        DATA = "data"

    class OutputRole(StrEnum):
        PROCESSED = "processed"
        SCORES = "scores"
```

Generative operations (no inputs) omit `InputRole`.

---

## Step 2: Declare inputs and outputs

### Inputs

Each entry maps a role name to an `InputSpec`:

```python
inputs: ClassVar[dict[str, InputSpec]] = {
    InputRole.DATA: InputSpec(artifact_type="data", required=True),
}
```

Key `InputSpec` fields:

| Field | Default | Effect |
|-------|---------|--------|
| `artifact_type` | `"any"` | Type constraint on accepted artifacts |
| `required` | `True` | Pipeline fails if this input is missing |
| `materialize` | `True` | Write artifact to disk (file path in preprocess) vs. pass content in memory |
| `hydrate` | `True` | Load full content vs. ID-only (for passthrough-style ops) |
| `with_associated` | `()` | Auto-resolve related artifacts via provenance (e.g., annotations) |

Set `materialize=False` for inputs you'll process in Python without needing a
file on disk (metrics, configs).

### Outputs

Each entry maps a role name to an `OutputSpec`. Every creator output must set
`infer_lineage_from`:

```python
outputs: ClassVar[dict[str, OutputSpec]] = {
    OutputRole.PROCESSED: OutputSpec(
        artifact_type="data",
        infer_lineage_from={"inputs": ["data"]},
    ),
    OutputRole.SCORES: OutputSpec(
        artifact_type="metric",
        infer_lineage_from={"outputs": ["processed"]},
    ),
}
```

### Lineage patterns

| Pattern | Syntax | Use when |
|---------|--------|----------|
| Derived from input(s) | `{"inputs": ["role_name"]}` | Output transforms a named input |
| Derived from output | `{"outputs": ["role_name"]}` | Output derives from another output of the same operation |
| Generative | `{"inputs": []}` | Output has no parents |

`None` is only valid for curator operations. `{}` (empty dict) always raises
`ValidationError`.

---

## Step 3: Add parameters

Group algorithm-specific configuration into a nested `Params` model. Use
Pydantic `Field` for defaults and validation:

```python
class Params(BaseModel):
    scale_factor: float = Field(default=1.5, ge=0.0)
    seed: int | None = Field(default=None)

params: Params = Params()
```

Access in lifecycle methods via `self.params`:

```python
def execute(self, inputs: ExecuteInput) -> Any:
    value = some_value * self.params.scale_factor
    ...
```

Override at the pipeline step level:

```python
pipeline.run(operation=MyOp, inputs=..., params={"scale_factor": 2.0})
```

Parameters are optional. If your operation has no configurable behavior, skip
this step (see `MetricCalculator` in `artisan.operations.examples`).

---

## Step 4: Implement preprocess

**Required** for operations with inputs. The framework raises `TypeError` at
class definition time if missing. Generative operations skip this (the
default returns `{}`).

`preprocess` receives `PreprocessInput` containing the materialized artifacts
and returns a plain dict. The most common pattern extracts file paths:

```python
def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
    return {
        role: [a.materialized_path for a in artifacts]
        for role, artifacts in inputs.input_artifacts.items()
    }
```

`inputs.input_artifacts` is a `dict[str, list[Artifact]]` keyed by role name.
Each artifact's `materialized_path` points to the file the framework wrote to
the sandbox. Inputs are always lists, even when `artifacts_per_unit=1`.

For non-materialized inputs (`materialize=False` in the `InputSpec`), access
content directly:

```python
def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
    configs = inputs.input_artifacts["config"]
    return {"config": configs[0].content}
```

---

## Step 5: Implement execute

**Required** for all creator operations. Receives `ExecuteInput` with the dict
from preprocess and a working directory.

Write output files to `inputs.execute_dir`. Access parameters via
`self.params`. The return value is passed to postprocess as
`inputs.memory_outputs` — return computed data when your outputs are in-memory
rather than file-based.

```python
def execute(self, inputs: ExecuteInput) -> Any:
    output_dir = inputs.execute_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    for path_str in inputs.inputs["dataset"]:
        data = Path(path_str).read_text()
        transformed = do_something(data)
        (output_dir / Path(path_str).name).write_text(transformed)

    return None  # or return computed data for memory_outputs
```

`ExecuteInput` is frozen — you cannot modify its fields.

---

## Step 6: Implement postprocess

**Optional.** The default returns `ArtifactResult(success=True)` with no
artifacts. Override when your operation produces output artifacts.

`postprocess` receives `PostprocessInput` with two sources of data:
- `inputs.file_outputs` — all files found in `execute_dir` after execute ran
- `inputs.memory_outputs` — whatever `execute` returned

Build draft artifacts and return them keyed by output role:

```python
def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
    drafts = [
        DataArtifact.draft(
            content=f.read_bytes(),
            original_name=f.name,
            step_number=inputs.step_number,
        )
        for f in inputs.file_outputs
        if f.suffix == ".csv"
    ]
    return ArtifactResult(success=True, artifacts={"dataset": drafts})
```

`original_name` matters: the lineage matching algorithm uses it to pair
output artifacts with their parent inputs. Use the input filename as the
stem when there's a 1:1 relationship.

---

## Common patterns

### Metric outputs (in-memory)

When `execute` computes values rather than writing files, return them and
construct artifacts from `memory_outputs` in postprocess:

```python
def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
    return {"accuracy": 0.95, "f1": 0.87}

def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
    metric = MetricArtifact.draft(
        content=inputs.memory_outputs,
        original_name=f"metrics_{inputs.step_number}",
        step_number=inputs.step_number,
    )
    return ArtifactResult(success=True, artifacts={"metrics": [metric]})
```

### External tool operations

Set `tool` to a `ToolSpec` declaring the binary or script to invoke, and
configure the execution environment with `environments`:

```python
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.operation_config.environment_spec import (
    ApptainerEnvironmentSpec,
    LocalEnvironmentSpec,
)
from artisan.schemas.operation_config.environments import Environments

class MyToolOp(OperationDefinition):
    name = "my_tool"

    tool: ToolSpec = ToolSpec(
        executable="/tools/run.sh",
        interpreter="bash",
    )
    environments: Environments = Environments(
        local=LocalEnvironmentSpec(),
        apptainer=ApptainerEnvironmentSpec(
            image=Path("/tools/my_tool.sif"),
        ),
    )
    ...
```

`ToolSpec` declares the binary (`executable`) and optional `interpreter` or
`subcommand`. The environment spec wraps the command for container execution.
In `execute`, use `self.tool.parts()` to build the command prefix and
`self.environments.current().wrap_command(...)` to apply the environment.

### Multi-input operations

When an operation consumes multiple input roles, set `group_by` to control
how artifacts are paired across roles, and use `inputs.grouped()` in
preprocess:

```python
from artisan.schemas.enums import GroupByStrategy

class AlignOp(OperationDefinition):
    name = "align"
    group_by: ClassVar[GroupByStrategy | None] = GroupByStrategy.LINEAGE
    ...

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            "pairs": [
                {
                    "data": g["data"].materialized_path,
                    "reference": g["reference"].materialized_path,
                }
                for g in inputs.grouped()
            ]
        }
```

| Strategy | Behavior | Use when |
|----------|----------|----------|
| `LINEAGE` | Pairs artifacts sharing provenance ancestry | Inputs from different steps that process the same original |
| `ZIP` | Pairs by position (index-aligned) | Inputs in a known, consistent order |
| `CROSS_PRODUCT` | Every combination across roles | Every input combined with every other |

### Resources and execution config

Set defaults on the class. Override per-step at the pipeline level:

```python
class HeavyOp(OperationDefinition):
    name = "heavy_op"
    resources: ResourceConfig = ResourceConfig(
        cpus=4, memory_gb=32, gpus=1,
        extra={"partition": "gpu"},
    )
    execution: ExecutionConfig = ExecutionConfig(
        artifacts_per_unit=5, estimated_seconds=3600.0,
    )
    ...
```

See [Configuring Execution](configuring-execution.md) for the full set of
resource and batching options.

---

## Common pitfalls

| Problem | Cause | Fix |
|---------|-------|-----|
| `TypeError: must define OutputRole` | Missing `OutputRole(StrEnum)` inner class | Add enum with values matching `outputs` keys |
| `TypeError: must define InputRole` | Missing `InputRole(StrEnum)` inner class | Add enum with values matching `inputs` keys |
| `TypeError: must implement preprocess()` | Creator with non-empty `inputs` but no preprocess | Override `preprocess()` |
| `TypeError: must set infer_lineage_from` | Creator output with `infer_lineage_from=None` | Set to `{"inputs": [...]}` or `{"inputs": []}` |
| `ValidationError` on `OutputSpec` | Used `{}` for lineage | Use `{"inputs": []}` for generative outputs |
| Empty artifacts after postprocess | Wrong file extension filter or missing files | Check `file_outputs` contents in the execute directory |
| Wrong lineage connections | `original_name` doesn't match input filenames | Use input filename as the stem for 1:1 transforms |

---

## Verify

Test your operation outside a pipeline by constructing inputs directly:

```python
from pathlib import Path
from tempfile import TemporaryDirectory

from artisan.schemas.specs.input_models import ExecuteInput, PostprocessInput

op = ScaleData(params={"factor": 3.0})

with TemporaryDirectory() as tmp:
    execute_dir = Path(tmp) / "execute"
    execute_dir.mkdir()

    # Write a test input file
    test_csv = execute_dir / "test.csv"
    test_csv.write_text("id,value\n1,10\n2,20\n")

    # Run execute
    execute_input = ExecuteInput(
        execute_dir=execute_dir,
        inputs={"dataset": [str(test_csv)]},
    )
    result = op.execute(execute_input)

    # Run postprocess
    post_input = PostprocessInput(
        step_number=0,
        postprocess_dir=Path(tmp) / "post",
        file_outputs=list(execute_dir.iterdir()),
        memory_outputs=result,
    )
    artifact_result = op.postprocess(post_input)

    assert artifact_result.success
    assert len(artifact_result.artifacts["dataset"]) > 0
```

For a full integration test, run in a pipeline with `Backend.LOCAL`:

```python
from artisan.orchestration import PipelineManager

pipeline = PipelineManager.create(
    name="test", delta_root="test/delta", staging_root="test/staging",
)
output = pipeline.output
pipeline.run(operation=DataGenerator, name="source", params={"count": 3})
step = pipeline.run(operation=ScaleData, inputs={"dataset": output("source", "datasets")})
assert step.success
assert step.succeeded_count > 0
```

---

## Cross-references

- [Operations Model](../concepts/operations-model.md) — why the three-phase
  lifecycle exists
- [Configuring Execution](configuring-execution.md) — resources, batching,
  backends
- [Writing Curator Operations](writing-curator-operations.md) — filter, merge,
  ingest operations
- [Build a Pipeline](building-a-pipeline.md) — wiring operations into pipelines
