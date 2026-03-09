---
name: write-operation
description: Write, scaffold, or review an Artisan pipeline operation. Use this skill when the user asks to create a new operation, write a creator or curator, scaffold an operation class, or review an existing operation for correctness. Trigger on phrases like "write an operation", "create a creator", "new curator", "scaffold operation", or any request involving OperationDefinition subclasses.
argument-hint: "[OperationClassName or description of what the operation should do]"
---

# Write an Artisan Operation

Write or scaffold an operation for `$ARGUMENTS`.

Before writing, read at least one example operation from
`src/artisan/operations/examples/` to match the established style. Also read the
base class at `src/artisan/operations/base/operation_definition.py` if you need
to confirm API details.

---

## Decision: Creator or Curator?

Pick one. The framework detects the type by which method you override.

| Type | Override | Use when | Runs on |
|---|---|---|---|
| **Creator** | `execute()` | Heavy computation, file I/O, external tools | Workers (ThreadPool / SLURM) |
| **Curator** | `execute_curator()` | Lightweight metadata: filter, merge, ingest, route | Local process |

---

## Creator Operation Template

Follow this structure exactly. Use the `# ---------- Section ----------` comment
style. Declare sections in this order: Metadata, Inputs, Outputs, Parameters,
Resources, Execution, Lifecycle.

```python
"""One-line module docstring describing what this operation does."""

from __future__ import annotations

from enum import StrEnum, auto
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class MyOperation(OperationDefinition):
    """One-line summary of what the operation does.

    Extended description if needed. Explain the algorithm or approach.
    """

    # ---------- Metadata ----------
    name = "my_operation"
    description = "One-line summary matching the docstring"

    # ---------- Inputs ----------
    class InputRole(StrEnum):
        DATASET = "dataset"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATASET: InputSpec(
            artifact_type="data",
            required=True,
            description="Input CSV dataset",
        ),
    }

    # ---------- Outputs ----------
    class OutputRole(StrEnum):
        DATASET = "dataset"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.DATASET: OutputSpec(
            artifact_type="data",
            description="Transformed dataset",
            infer_lineage_from={"inputs": ["dataset"]},
        ),
    }

    # ---------- Parameters ----------
    class Params(BaseModel):
        """Algorithm parameters for MyOperation."""

        threshold: float = Field(
            default=0.5,
            ge=0.0,
            le=1.0,
            description="Filtering threshold",
        )

    params: Params = Params()

    # ---------- Resources ----------
    resources: ResourceConfig = ResourceConfig(time_limit="00:30:00")

    # ---------- Execution ----------
    execution: ExecutionConfig = ExecutionConfig(job_name="my_operation")

    # ---------- Lifecycle ----------
    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """Extract materialized paths from input artifacts."""
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: ExecuteInput) -> Any:
        """Core computation. Read from inputs, write to execute_dir."""
        ...

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Build draft artifacts from execution outputs."""
        drafts = []
        for file_path in inputs.file_outputs:
            if file_path.suffix == ".csv":
                drafts.append(
                    DataArtifact.draft(
                        content=file_path.read_bytes(),
                        original_name=file_path.name,
                        step_number=inputs.step_number,
                    )
                )

        return ArtifactResult(
            success=True,
            artifacts={"dataset": drafts},
        )
```

---

## Curator Operation Template

```python
"""One-line module docstring."""

from __future__ import annotations

from enum import StrEnum, auto
from typing import TYPE_CHECKING, ClassVar

import polars as pl

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.curator_result import PassthroughResult
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

if TYPE_CHECKING:
    from artisan.storage.core.artifact_store import ArtifactStore


class MyCurator(OperationDefinition):
    """One-line summary."""

    # ---------- Metadata ----------
    name = "my_curator"
    description = "One-line summary"

    # ---------- Inputs ----------
    class InputRole(StrEnum):
        stream = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.stream: InputSpec(
            artifact_type=ArtifactTypes.ANY,
            required=True,
            description="Artifacts to process",
        ),
    }

    # ---------- Outputs ----------
    class OutputRole(StrEnum):
        stream = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.stream: OutputSpec(
            artifact_type=ArtifactTypes.ANY,
            description="Processed artifacts",
        ),
    }

    # ---------- Lifecycle ----------
    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> PassthroughResult:
        """Route artifact IDs without creating new artifacts."""
        ids = inputs["stream"]["artifact_id"].to_list()
        return PassthroughResult(
            success=True,
            passthrough={"stream": ids},
        )
```

---

## Lineage Patterns

Every creator output **must** set `infer_lineage_from`. Curator outputs may use `None`.

| Pattern | Value | When to use |
|---|---|---|
| Derived from input | `{"inputs": ["role_name"]}` | Output traces back to a named input role |
| Generative | `{"inputs": []}` | No parent artifacts (data generation) |
| Output-to-output | `{"outputs": ["other_role"]}` | Co-produced artifact (e.g. metrics alongside data) |
| Curator passthrough | `None` | Curator routing existing artifacts |

**Stem-matching rule:** The framework infers 1:1 lineage by matching the
`original_name` stem of output drafts against input artifact names. For
transforms, preserve the input filename stem in the output name.

---

## InputSpec Fields

| Field | Type | Default | Effect |
|---|---|---|---|
| `artifact_type` | `str` | `"any"` | Type constraint on accepted artifacts |
| `required` | `bool` | `True` | Pipeline fails if input is missing |
| `materialize` | `bool` | `True` | `True`: write to disk (file path). `False`: in-memory access only |
| `hydrate` | `bool` | `True` | `True`: load content. `False`: ID-only mode |
| `with_associated` | `tuple[str, ...]` | `()` | Auto-resolve related artifacts via provenance |

## OutputSpec Fields

| Field | Type | Effect |
|---|---|---|
| `artifact_type` | `str` | Type of artifact produced |
| `description` | `str` | Human-readable description |
| `required` | `bool` | Whether the output must be non-empty |
| `infer_lineage_from` | `dict \| None` | Lineage declaration (see patterns above) |

---

## Variant: Generative Creator (No Inputs)

- Omit `InputRole`
- Set `inputs: ClassVar[dict] = {}`
- Set `infer_lineage_from={"inputs": []}` on all outputs
- Implement only `execute()` and `postprocess()` (no `preprocess()`)

See `src/artisan/operations/examples/data_generator.py`.

## Variant: Multi-input with group_by

- Define multiple roles in `InputRole` and `inputs`
- Set `group_by: ClassVar[GroupByStrategy | None] = GroupByStrategy.LINEAGE`
  (or `ZIP` or `CROSS_PRODUCT`)
- In `preprocess`, iterate paired groups via `inputs.grouped()`

```python
def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
    prepared = []
    for group in inputs.grouped():
        prepared.append({
            "dataset": str(group["dataset"].materialized_path),
            "config": str(group["config"].materialized_path),
        })
    return {"items": prepared}
```

See `src/artisan/operations/examples/data_transformer_script.py`.

## Variant: External Tool via ToolSpec

- Set `tool: ToolSpec = ToolSpec(executable=SCRIPT_PATH, interpreter="python")`
- Configure `environments: Environments = Environments(local=..., docker=...)`
- In `execute`, call `run_command(env, [*self.tool.parts(), *args])`
- Import `from artisan.utils.external_tools import format_args, run_command`

See `src/artisan/operations/examples/data_transformer_script.py`.

## Variant: Config Artifacts with $artifact References

- Produce `ExecutionConfigArtifact` drafts with `{"$artifact": artifact_id}`
  placeholders in the content dict
- The framework resolves `$artifact` references to materialized paths at
  execution time
- Set `materialize=False` on the input to access artifact IDs without writing
  files to disk

See `src/artisan/operations/examples/data_transformer_config.py`.

## Variant: Output-to-output Lineage

- Use `infer_lineage_from={"outputs": ["other_role"]}` when one output derives
  from another co-produced output (e.g. metrics computed alongside data)
- The primary output uses `{"inputs": []}` or `{"inputs": ["role"]}`
- The derived output points to the primary via `{"outputs": ["primary_role"]}`

See `src/artisan/operations/examples/data_generator_with_metrics.py`.

---

## Validation Rules

The framework validates at class definition time (import). These cause
`TypeError` immediately:

- Must override either `execute()` or `execute_curator()` (not neither, not both)
- Creator outputs must set `infer_lineage_from` (cannot be `None`)
- Creator operations with inputs must implement `preprocess()`
- Must define `OutputRole(StrEnum)` with values matching `outputs` keys exactly
- Must define `InputRole(StrEnum)` with values matching `inputs` keys exactly
  (unless `inputs` is empty or `runtime_defined_inputs=True`)

---

## Artifact Draft Methods

Use the appropriate `draft()` class method in `postprocess`:

```python
# File-based data (CSV, binary, etc.)
DataArtifact.draft(content=bytes, original_name=str, step_number=int)

# Key-value metrics (JSON-serializable dict)
MetricArtifact.draft(content=dict, original_name=str, step_number=int)

# Execution configs with $artifact references
ExecutionConfigArtifact.draft(content=dict, original_name=str, step_number=int)
```

The `original_name` is critical: the framework uses filename stems for lineage
matching. For 1:1 transforms, preserve the input's filename stem.

---

## Testing Patterns

### Unit test a creator

```python
def test_my_operation(tmp_path):
    op = MyOperation(params=MyOperation.Params(threshold=0.8))

    # Prepare input files
    input_csv = tmp_path / "input.csv"
    input_csv.write_text("id,value\n1,0.9\n2,0.3\n")

    execute_dir = tmp_path / "execute"
    execute_input = ExecuteInput(
        execute_dir=execute_dir,
        inputs={"dataset": [str(input_csv)]},
    )
    memory_outputs = op.execute(execute_input)

    post_input = PostprocessInput(
        step_number=0,
        postprocess_dir=tmp_path / "post",
        file_outputs=list(execute_dir.iterdir()),
        memory_outputs=memory_outputs,
    )
    result = op.postprocess(post_input)
    assert result.success
    assert "dataset" in result.artifacts
```

### Unit test a curator

```python
def test_my_curator():
    op = MyCurator()
    result = op.execute_curator(
        inputs={"stream": pl.DataFrame({"artifact_id": ["abc123", "def456"]})},
        step_number=1,
        artifact_store=Mock(),
    )
    assert result.success
    assert len(result.passthrough["stream"]) == 2
```

---

## Style Rules

Follow these conventions from the existing examples:

- **Module docstring**: One line, describes what the operation does
- **Class docstring**: Summary line + optional extended description. Do not
  manually write Input/Output Roles sections (auto-generated by the framework)
- **Section comments**: Use `# ---------- Section ----------` with exactly 10
  dashes on each side
- **Section order**: Metadata, Inputs, Outputs, Parameters, Resources, Execution,
  Lifecycle (omit sections that use defaults)
- **Lifecycle docstrings**: One-line imperative summary (e.g. "Extract
  materialized paths from input artifacts.")
- **name value**: `snake_case` matching the class name's snake_case form
- **Imports**: Group stdlib, then pydantic, then artisan. Use
  `from __future__ import annotations`
- **Params class**: Nest inside the operation class. Use `Field()` with
  `default`, constraints (`ge`, `le`), and `description` for every parameter
- **No bare constants**: Put algorithm-specific values in `Params`, not as
  module-level constants
- **execute() is a black box**: It reads files and writes files. No framework
  imports, no Artifact objects, no ArtifactStore access
- **preprocess() bridges in**: Converts Artifact objects to plain paths/dicts
- **postprocess() bridges out**: Converts files/memory_outputs to draft Artifacts
- **Return metadata**: Include operation name and key params in `ArtifactResult.metadata`
